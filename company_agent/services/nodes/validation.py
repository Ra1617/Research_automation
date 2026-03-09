"""Validation node for field-by-field checks against metadata rules.

The validation node:
1. Validates all 163 fields per provider record
2. Tracks passed/failed counts
3. Sets _retry_pending flag if eligible for retry
4. Builds retry feedback from failed fields
5. Enforces hard attempt limits
"""

from __future__ import annotations

from typing import Any, Dict

from langchain_core.runnables import RunnableLambda
from services.state import GraphState


def create_validation_node(
    validator,  # MetadataRuleEngine instance
    max_attempts: int,
) -> RunnableLambda:
    """Factory function to create a validation node.
    
    Args:
        validator: MetadataRuleEngine instance with field definitions
        max_attempts: Hard limit for validation attempts
        
    Returns:
        RunnableLambda to validate all provider records in parallel
    """
    
    def invoke_validation_for_provider(provider_name: str, existing_record: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a single provider's record."""
        
        # Skip validation if provider had an error during generation
        if existing_record.get("_error"):
            existing_record["_retry_pending"] = False
            print(f"[{provider_name}] Validation skipped (provider error exists)")
            return existing_record
        
        # Merge valid fields for validation
        merged_record = validator.merge_valid_fields({}, existing_record)
        model_used = str(existing_record.get("_model_used", ""))
        attempts_used = int(existing_record.get("_generation_attempts", 1))
        
        # Validate all fields
        report = validator.validate_record(merged_record)
        final_report = report
        
        # Build provider record with validation metadata
        provider_record = dict(merged_record)
        provider_record["_model_used"] = model_used
        provider_record["_generation_attempts"] = attempts_used
        provider_record["_validation"] = {
            "attempts": attempts_used,
            "required_parameters": validator.total_fields,
            "passed_parameters": final_report.passed_count,
            "failed_parameters": final_report.failed_count,
        }
        provider_record["_failed_parameters"] = final_report.failed_fields
        
        print(
            f"[{provider_name}] Validation attempt {attempts_used}: "
            f"{final_report.passed_count}/{validator.total_fields} params passed "
            f"(failed: {final_report.failed_count})"
        )
        
        # ===== RETRY DECISION =====
        # Retry eligible: has failed fields AND below attempt limit
        should_retry = final_report.failed_count > 0 and attempts_used < max_attempts
        provider_record["_retry_pending"] = should_retry
        
        if should_retry:
            # Prepare targeted retry
            provider_record["_target_fields"] = final_report.failed_fields
            provider_record["_retry_feedback"] = validator.build_retry_feedback(final_report)
            print(
                f"[{provider_name}] Retry queued for {final_report.failed_count} failed fields "
                f"(attempt {attempts_used + 1}/{max_attempts} next)"
            )
        else:
            provider_record["_retry_pending"] = False
            if final_report.failed_count > 0 and attempts_used >= max_attempts:
                provider_record["_error"] = (
                    f"Max attempts ({max_attempts}) reached with {final_report.failed_count} "
                    f"failed parameters remaining"
                )
                print(f"[{provider_name}] Max attempts reached with failures - marking error")
        
        return provider_record
    
    def invoke_validation(state: GraphState) -> Dict[str, Any]:
        """Validate all providers in parallel."""
        records = state.get("records", {})
        validated_records: Dict[str, Dict[str, Any]] = {}
        
        for provider_name, existing_record in records.items():
            validated_records[provider_name] = invoke_validation_for_provider(provider_name, existing_record)
        
        return {"records": validated_records}
    
    return RunnableLambda(invoke_validation)
