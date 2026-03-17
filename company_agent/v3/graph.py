"""
v3 LangGraph Pipeline
======================
StateGraph orchestrating the 9-step company research pipeline.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List

from dotenv import load_dotenv
from langsmith import traceable
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from v3.db.repository import insert_consolidated_output, insert_raw_model_output
from v3.state import GraphState
from v3.models import InputModel
from v3.validators import (
    validate_llm_output,
    validate_final_output,
    run_pytest_assertions,
)
from v3.agents import (
    worker_groq,
    worker_mistral,
    worker_nvidia,
    run_consolidation,
)

load_dotenv(override=True)

logger = logging.getLogger(__name__)

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# ─────────────────────────────────────────────────────────────────────────────
# LANGSMITH CONFIG
# ─────────────────────────────────────────────────────────────────────────────

_lc_key = os.getenv("LANGCHAIN_API_KEY", "")

if _lc_key and "your_" not in _lc_key.lower():
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ["LANGCHAIN_PROJECT"] = "company-agent-v3"
    print("[LangSmith] Tracing enabled -> project: company-agent-v3")
else:
    print("[LangSmith] No valid LANGCHAIN_API_KEY - tracing disabled")


# ═════════════════════════════════════════════════════════════════════════════
# NODE FUNCTIONS
# ═════════════════════════════════════════════════════════════════════════════

@traceable(name="validate_input", run_type="chain")
def validate_input_node(state: GraphState) -> Dict[str, Any]:

    raw_input = state.get("input", {})

    try:
        model = InputModel.model_validate(raw_input)

        logger.info("[validate_input] company_name=%s", model.company_name)

        return {
            "input": model.model_dump(),
            "llm_outputs": [],
            "validated_outputs": [],
            "consolidated_output": {},
            "consolidation_model": "",
            "retry_count": state.get("retry_count", 0),
            "errors": state.get("errors", []),
            "partial_result": False,
        }

    except Exception as exc:
        raise ValueError(f"Input validation failed: {exc}") from exc


def dispatch_to_workers(state: GraphState) -> List[Send]:

    logger.info("[dispatch] Sending to 3 LLM workers in parallel")

    worker_state = {"input": state["input"]}

    return [
        Send("worker_groq", worker_state),
        Send("worker_mistral", worker_state),
        Send("worker_nvidia", worker_state),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# WORKERS
# ─────────────────────────────────────────────────────────────────────────────

@traceable(name="groq_worker", run_type="tool")
def worker_groq_node(state: Dict[str, Any]) -> Dict[str, Any]:
    return worker_groq(state)


@traceable(name="mistral_worker", run_type="tool")
def worker_mistral_node(state: Dict[str, Any]) -> Dict[str, Any]:
    return worker_mistral(state)


@traceable(name="nvidia_worker", run_type="tool")
def worker_nvidia_node(state: Dict[str, Any]) -> Dict[str, Any]:
    return worker_nvidia(state)


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATE LLM OUTPUTS
# ─────────────────────────────────────────────────────────────────────────────

@traceable(name="validate_llm_outputs", run_type="chain")
def validate_llm_outputs_node(state: GraphState) -> Dict[str, Any]:

    llm_outputs: List[Dict[str, Any]] = state.get("llm_outputs", [])
    company_name = state["input"]["company_name"]

    validated: List[Dict[str, Any]] = []

    new_errors: List[str] = list(state.get("errors", []))

    for item in llm_outputs:

        provider = item.get("provider", "unknown")
        raw_to_store = item.get("raw_original") or item.get("raw") or {}

        if item.get("error"):
            raw_to_store = {"error": item["error"], "raw": raw_to_store}

        try:
            insert_raw_model_output(
                company_name=company_name,
                model_name=provider,
                agent_stage="agent_1_raw",
                raw_json=raw_to_store,
            )
        except Exception as exc:
            logger.warning("[persist_raw] Failed for %s: %s", provider, exc)

        if item.get("error"):
            validated.append(
                {"provider": provider, "raw": {}, "error": item["error"]}
            )
            new_errors.append(item["error"])
            continue

        validated_dict, err = validate_llm_output(item.get("raw", {}), provider)

        if err:
            new_errors.append(err)
            validated.append(
                {"provider": provider, "raw": item.get("raw", {}), "error": err}
            )
        else:
            validated.append(
                {"provider": provider, "raw": validated_dict, "error": None}
            )

    valid_count = sum(1 for v in validated if v.get("error") is None)

    logger.info("[validate_llm_outputs] %d/%d outputs valid", valid_count, len(validated))

    return {"validated_outputs": validated, "errors": new_errors}


# ─────────────────────────────────────────────────────────────────────────────
# CONSOLIDATION
# ─────────────────────────────────────────────────────────────────────────────

@traceable(name="consolidate", run_type="chain")
def consolidate_node(state: GraphState) -> Dict[str, Any]:

    company_name = state["input"]["company_name"]

    validated_outputs = state.get("validated_outputs", [])

    logger.info("[consolidate] Merging outputs from %d providers", len(validated_outputs))

    merged, consolidation_model = run_consolidation(validated_outputs, company_name)

    return {
        "consolidated_output": merged,
        "consolidation_model": consolidation_model,
    }


# ─────────────────────────────────────────────────────────────────────────────
# FINAL VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

@traceable(name="validate_final", run_type="chain")
def validate_final_node(state: GraphState) -> Dict[str, Any]:

    consolidated = state.get("consolidated_output", {})

    validated_dict, err = validate_final_output(consolidated)

    new_errors = list(state.get("errors", []))

    if err:
        logger.warning("[validate_final] %s", err)
        new_errors.append(err)
        return {"errors": new_errors}

    return {"consolidated_output": validated_dict, "errors": new_errors}


# ─────────────────────────────────────────────────────────────────────────────
# ASSERTIONS
# ─────────────────────────────────────────────────────────────────────────────

@traceable(name="run_assertions", run_type="chain")
def run_assertions_node(state: GraphState) -> Dict[str, Any]:

    consolidated = state.get("consolidated_output", {})

    passed, failures = run_pytest_assertions(consolidated)

    new_errors = list(state.get("errors", []))

    if not passed:
        logger.warning("[run_assertions] %d failures: %s", len(failures), failures)
        new_errors.extend(failures)
    else:
        try:
            insert_consolidated_output(
                company_name=state["input"]["company_name"],
                consolidation_model=state.get("consolidation_model", "unknown"),
                agent_stage="agent_2_consolidated",
                consolidated_json=consolidated,
            )
        except Exception as exc:
            logger.warning("[persist_consolidated] Failed: %s", exc)

    return {"errors": new_errors}


# ═════════════════════════════════════════════════════════════════════════════
# RETRY LOGIC
# ═════════════════════════════════════════════════════════════════════════════

def retry_or_end(state: GraphState) -> str:

    errors = state.get("errors", [])

    assertion_failures = [e for e in errors if not e.startswith("[")]

    if not assertion_failures:
        logger.info("[retry_or_end] All assertions passed -> END")
        return "end"

    retry_count = state.get("retry_count", 0)

    if retry_count < MAX_RETRIES:

        backoff = 2 ** retry_count

        logger.info(
            "[retry_or_end] Retry %d/%d (backoff %ds)",
            retry_count + 1,
            MAX_RETRIES,
            backoff,
        )

        time.sleep(backoff)

        return "retry"

    logger.warning("[retry_or_end] Max retries hit -> partial result")

    return "end_partial"


@traceable(name="prepare_retry", run_type="chain")
def prepare_retry_node(state: GraphState) -> Dict[str, Any]:

    return {
        "retry_count": state.get("retry_count", 0) + 1,
        "llm_outputs": [],
        "validated_outputs": [],
        "consolidated_output": {},
        "consolidation_model": "",
        "errors": [],
    }


@traceable(name="mark_partial", run_type="chain")
def mark_partial_node(state: GraphState) -> Dict[str, Any]:

    return {"partial_result": True}


# ═════════════════════════════════════════════════════════════════════════════
# GRAPH BUILDER
# ═════════════════════════════════════════════════════════════════════════════

def build_graph():

    builder = StateGraph(GraphState)

    builder.add_node("validate_input", validate_input_node)

    builder.add_node("worker_groq", worker_groq_node)
    builder.add_node("worker_mistral", worker_mistral_node)
    builder.add_node("worker_nvidia", worker_nvidia_node)

    builder.add_node("validate_llm_outputs", validate_llm_outputs_node)

    builder.add_node("consolidate", consolidate_node)

    builder.add_node("validate_final", validate_final_node)

    builder.add_node("run_assertions", run_assertions_node)

    builder.add_node("prepare_retry", prepare_retry_node)

    builder.add_node("mark_partial", mark_partial_node)

    builder.add_edge(START, "validate_input")

    builder.add_conditional_edges(
        "validate_input",
        dispatch_to_workers,
        ["worker_groq", "worker_mistral", "worker_nvidia"],
    )

    builder.add_edge("worker_groq", "validate_llm_outputs")
    builder.add_edge("worker_mistral", "validate_llm_outputs")
    builder.add_edge("worker_nvidia", "validate_llm_outputs")

    builder.add_edge("validate_llm_outputs", "consolidate")

    builder.add_edge("consolidate", "validate_final")

    builder.add_edge("validate_final", "run_assertions")

    builder.add_conditional_edges(
        "run_assertions",
        retry_or_end,
        {
            "end": END,
            "retry": "prepare_retry",
            "end_partial": "mark_partial",
        },
    )

    builder.add_edge("mark_partial", END)

    builder.add_conditional_edges(
        "prepare_retry",
        dispatch_to_workers,
        ["worker_groq", "worker_mistral", "worker_nvidia"],
    )

    return builder.compile()


_graph_cache = None


def get_graph():

    global _graph_cache

    if _graph_cache is None:
        _graph_cache = build_graph()

    return _graph_cache


graph = get_graph()
