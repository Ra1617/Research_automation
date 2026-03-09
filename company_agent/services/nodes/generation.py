"""Provider generation nodes for parallel LLM inference.

Each provider (Gemini, Groq, OpenRouter) runs in parallel with its own generation node.
Nodes handle:
- Lazy-loaded LLM chains (built on first invocation)
- Attempt tracking (hard limit at max_validation_attempts)
- Retry feedback and target field resolution
- Error handling and fallback logic
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict

from langchain_core.runnables import RunnableLambda
from services.state import GraphState


def create_generation_node(
    provider_name: str,
    build_chain_fn: Callable[[], Any],  # Returns LLM chain
    validator,  # MetadataRuleEngine instance
    max_attempts: int,
) -> RunnableLambda:
    """Factory function to create a generation node for a provider.
    
    Args:
        provider_name: 'gemini', 'groq', or 'openrouter'
        build_chain_fn: Function that returns built LLM chain on call
        validator: MetadataRuleEngine instance
        max_attempts: Hard limit for generation attempts
        
    Returns:
        RunnableLambda for parallel execution in StateGraph
    """
    
    def invoke_generation(state: GraphState) -> Dict[str, Any]:
        """Execute generation for this provider.
        
        Flow:
        1. Check if provider already has error → skip
        2. Retrieve prior record (if retry) or create new
        3. Increment attempt counter (with hard limit)
        4. Prepare target fields and retry feedback
        5. Invoke LLM chain
        6. Parse and validate JSON response
        7. Merge with prior valid fields
        8. Return updated provider record
        """
        company_name = state["company_name"]
        existing_record = state.get("records", {}).get(provider_name, {})
        
        # Extract attempt counters from prior state
        previous_generation_attempts = int(existing_record.get("_generation_attempts", 0))
        previous_validation_attempts = int(existing_record.get("_validation", {}).get("attempts", 0))
        attempts_used = max(previous_generation_attempts, previous_validation_attempts) + 1
        
        # Resolve target fields (retry passes subset, initial passes all)
        target_fields = existing_record.get("_target_fields", validator.field_names)
        if isinstance(target_fields, str):
            target_fields = [target_fields]
        
        # Retry feedback for LLM guidance
        retry_feedback = existing_record.get(
            "_retry_feedback",
            "Initial request: generate complete output for all required parameters.",
        )
        
        # ===== HARD SAFEGUARD =====
        # Never exceed max_attempts - return error marker
        if attempts_used > max_attempts:
            print(f"[{provider_name}] HARD STOP: Attempt {attempts_used} exceeds max {max_attempts}")
            return {
                "records": {
                    provider_name: {
                        "_error": f"Max generation attempts ({max_attempts}) exceeded",
                        "_model_used": str(existing_record.get("_model_used", "")),
                        "_validation": {
                            "attempts": attempts_used,
                            "required_parameters": validator.total_fields,
                            "passed_parameters": 0,
                            "failed_parameters": validator.total_fields,
                        },
                        "_retry_pending": False,
                    }
                }
            }
        
        print(
            f"[{provider_name}] Generation attempt {attempts_used}/{max_attempts}. "
            f"Target fields: {len(target_fields)}/{validator.total_fields}"
        )
        
        # Build and invoke chain
        try:
            chain = build_chain_fn()  # Lazy chain construction
        except Exception as e:
            print(f"[{provider_name}] Chain build error: {e}")
            return {
                "records": {
                    provider_name: {
                        "_error": f"Provider chain initialization failed: {str(e)[:100]}",
                        "_validation": {
                            "attempts": attempts_used,
                            "required_parameters": validator.total_fields,
                            "passed_parameters": 0,
                            "failed_parameters": validator.total_fields,
                        },
                        "_retry_pending": False,
                    }
                }
            }
        
        input_data = {
            "company_name": company_name,
            "target_parameters": ", ".join(target_fields),
            "retry_feedback": retry_feedback,
        }
        
        try:
            result = chain.invoke(input_data)
        except Exception as e:
            print(f"[{provider_name}] LLM invocation error: {e}")
            return {
                "records": {
                    provider_name: {
                        "_error": f"LLM call failed: {str(e)[:100]}",
                        "_model_used": str(existing_record.get("_model_used", "")),
                        "_validation": {
                            "attempts": attempts_used,
                            "required_parameters": validator.total_fields,
                            "passed_parameters": 0,
                            "failed_parameters": validator.total_fields,
                        },
                        "_retry_pending": False,
                    }
                }
            }
        
        # Validate response type
        if not isinstance(result, dict):
            print(f"[{provider_name}] Unexpected response type: {type(result)}")
            return {
                "records": {
                    provider_name: {
                        "_error": "Provider returned non-JSON/dict response",
                        "_validation": {
                            "attempts": attempts_used,
                            "required_parameters": validator.total_fields,
                            "passed_parameters": 0,
                            "failed_parameters": validator.total_fields,
                        },
                        "_retry_pending": False,
                    }
                }
            }
        
        # Check for provider-side error (e.g., rate limit, invalid API key)
        if result.get("_error"):
            print(f"[{provider_name}] Provider error: {result['_error']}")
            return {
                "records": {
                    provider_name: {
                        "_error": str(result.get("_error")),
                        "_model_used": str(result.get("_model_used", "")),
                        "_validation": {
                            "attempts": attempts_used,
                            "required_parameters": validator.total_fields,
                            "passed_parameters": 0,
                            "failed_parameters": validator.total_fields,
                        },
                        "_retry_pending": False,
                    }
                }
            }
        
        # Prepare valid record for next node (preserve prior valid fields + new)
        base_record = validator.merge_valid_fields({}, existing_record)
        merged_record = validator.merge_valid_fields(base_record, result)
        
        # Assemble provider record with metadata
        provider_record = dict(merged_record)
        provider_record["_model_used"] = str(result.get("_model_used", ""))
        provider_record["_generation_attempts"] = attempts_used
        provider_record["_retry_pending"] = False
        
        print(f"[{provider_name}] Generation succeeded (attempt {attempts_used}/{max_attempts})")
        return {"records": {provider_name: provider_record}}
    
    return RunnableLambda(invoke_generation)
