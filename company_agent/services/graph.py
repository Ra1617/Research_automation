"""LangGraph workflow definition for the Company Intelligence Aggregator.

This module defines the multi-provider orchestration pipeline:

    Entry
      ↓
    Generation (parallel: Gemini, Groq, OpenRouter)
      ↓
    Validation (checks all 163 fields per provider)
      ↓
    Router (conditional: retry if failed fields and attempts < max)
      ↓
    Consolidation (score and select best values across providers)
      ↓
    Excel Export
      ↓
    End

The graph is lazy-loaded (built only when invoked) for faster imports and
LangGraph Studio compatibility.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, START, StateGraph

from services.state import GraphState, InputState, OutputState
from services.validation_engine import MetadataRuleEngine
from services.nodes import (
    create_generation_node,
    create_validation_node,
    create_consolidation_node,
    should_retry,
)
import config


def parse_json(text: str) -> Dict[str, Any]:
    """Extract and parse JSON from LLM output."""
    import re
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{[\s\S]*\}", text)
        if not match:
            return {}
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return {}


def normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Pass through parsed JSON, preserving structure."""
    return record if record else {}


def build_prompt(system_text: str, user_text: str, max_attempts: int):
    """Build ChatPromptTemplate with system, user, and retry prompts."""
    retry_user_text = (
        f"{user_text}\n\n"
        "Target parameters for this pass:\n"
        "{target_parameters}\n\n"
        "Validation feedback from previous pass (if any):\n"
        "{retry_feedback}\n\n"
        "Rules:\n"
        "- Return ONLY a valid JSON object.\n"
        "- Use exact parameter names as provided.\n"
        "- Include only requested target parameters for retry passes.\n"
        "- No markdown fences and no explanatory text."
    )
    return ChatPromptTemplate.from_messages(
        [
            ("system", system_text),
            ("user", retry_user_text),
        ]
    )


def build_graph() -> Any:
    """Build the complete StateGraph with all nodes and edges.
    
    Called lazily by get_graph() to enable faster imports and
    LangGraph Studio discovery.
    """
    # Enable LangSmith observability
    # Build LLM provider configs
    project_root = Path(__file__).resolve().parents[1]
    metadata_path = project_root / "meta_data_complete.json"
    schema_path = project_root / "schema_validation.json"
    
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
    
    # Initialize validator and configure max attempts
    validator = MetadataRuleEngine.from_metadata_file(metadata_path)
    max_attempts = max(1, int(os.getenv("VALIDATION_MAX_ATTEMPTS", "5")))
    provider_configs = {}
    startup_errors = {}
    
    if config.GROQ_API_KEY:
        provider_configs["groq"] = {
            "type": "groq",
            "api_key": config.GROQ_API_KEY,
            "model": config.GROQ_MODEL,
        }
    else:
        startup_errors["groq"] = "Missing GROQ_API_KEY"
    
    if config.CODESTRAL_API_KEY:
        provider_configs["codestral"] = {
            "type": "codestral",
            "api_key": config.CODESTRAL_API_KEY,
            "model": config.CODESTRAL_MODEL,
        }
    else:
        startup_errors["codestral"] = "Missing CODESTRAL_API_KEY"
    
    if config.MISTRAL_API_KEY:
        provider_configs["mistral"] = {
            "type": "mistral",
            "api_key": config.MISTRAL_API_KEY,
            "model": config.MISTRAL_MODEL,
        }
    else:
        startup_errors["mistral"] = "Missing MISTRAL_API_KEY"
    
    # Simple prompt loading directly
    prompts_dir = project_root / "prompts"
    if prompts_dir.exists():
        system_text_template = (prompts_dir / "system.txt").read_text("utf-8")
        user_text = (prompts_dir / "user.txt").read_text("utf-8")
    else:
        raise FileNotFoundError(f"Prompts directory not found at {prompts_dir}")
    
    # Inject actual field names from the validator into the system prompt
    field_list_str = ", ".join(f'"{f}"' for f in validator.field_names)
    system_text = system_text_template.replace("{field_list}", field_list_str)
    
    prompt = build_prompt(system_text, user_text, max_attempts)
    parser = StrOutputParser()
    
    # Create StateGraph
    workflow = StateGraph(
        state_schema=GraphState,
        input_schema=InputState,
        output_schema=OutputState,
    )
    
    # Entry node: initialize state
    def entry_node(input_state: InputState | Dict[str, Any]) -> GraphState:
        """Initialize graph state from input."""
        if isinstance(input_state, dict):
            company_name = input_state.get("company_name", "")
        else:
            company_name = input_state.company_name
        
        return {
            "company_name": company_name,
            "records": {},
            "result": {},
        }
    
    workflow.add_node("entry", entry_node)
    workflow.add_edge(START, "entry")
    
    # Build and add provider nodes
    from models.groq import build_groq_chain
    from models.codestral import build_codestral_chain
    from models.mistral import build_mistral_chain
    
    # Helper to avoid closure-in-loop bug (binds cfg at call time)
    def _make_chain_factory(cfg, builder_fn):
        def chain_fn():
            return builder_fn(
                api_key=cfg["api_key"],
                model=cfg["model"],
                make_chain_fn=lambda llm: prompt | llm | parser | RunnableLambda(parse_json) | RunnableLambda(normalize_record),
            )
        return chain_fn
    
    builder_map = {
        "groq": build_groq_chain,
        "codestral": build_codestral_chain,
        "mistral": build_mistral_chain,
    }
    
    for provider_name, provider_config in provider_configs.items():
        builder_fn = builder_map.get(provider_config["type"])
        if not builder_fn:
            continue
        make_chain_fn = _make_chain_factory(provider_config, builder_fn)
        
        # Add generation and validation nodes
        gen_node_name = f"{provider_name}_generate"
        val_node_name = f"{provider_name}_validate"
        
        workflow.add_node(
            gen_node_name,
            create_generation_node(provider_name, make_chain_fn, validator, max_attempts)
        )
        workflow.add_node(
            val_node_name,
            create_validation_node(validator, max_attempts)
        )
        
        # Wire entry → generation → validation
        workflow.add_edge("entry", gen_node_name)
        workflow.add_edge(gen_node_name, val_node_name)
        
        # Conditional: validation → retry or consolidate
        workflow.add_conditional_edges(
            val_node_name,
            should_retry,
            {
                "retry_generation": gen_node_name,  # Retry routes back to generation
                "consolidate": "consolidate",
            },
        )
    
    # Consolidation node
    consolidation_node = create_consolidation_node(
        expected_fields=validator.field_names,
        schema_path=schema_path,
        metadata_path=metadata_path,
    )
    workflow.add_node("consolidate", consolidation_node)
    
    # Excel export node removed - handled by main.py
    workflow.add_edge("consolidate", END)
    
    return workflow.compile()


# Lazy-loaded cache for graph
_graph_cache: Any = None


def get_graph() -> Any:
    """Get or build the graph (lazy-loaded for faster imports).
    
    Returns:
        Compiled StateGraph ready for invocation
    """
    global _graph_cache
    if _graph_cache is None:
        _graph_cache = build_graph()
    return _graph_cache


# Lazy-loaded graph instance for LangSmith Studio discovery
graph = get_graph()

