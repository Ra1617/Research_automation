"""
v3 State Schema
===============
TypedDict state flowing through the LangGraph v3 pipeline.

State fields:
  input              — validated InputModel dict
  llm_outputs        — list of 3 raw LLM response dicts (one per provider)
  validated_outputs  — list of Pydantic-validated LLMOutputModel dicts
  consolidated_output — final merged FinalOutputModel dict
  retry_count        — number of retries attempted (max 3)
  errors             — list of error strings from any stage
  partial_result     — True if max retries hit, returning best-effort data
"""

from __future__ import annotations

from typing import Annotated, Any, Dict, List, Optional, TypedDict


def _append_list(left: List[Any], right: List[Any]) -> List[Any]:
    """Append fan-out results, but allow an explicit empty list to reset state."""
    if right == []:
        return []
    return left + right


class GraphState(TypedDict, total=False):
    """Full graph state for the v3 pipeline."""

    # ── Input ──────────────────────────────────────────────────────────────
    input: Dict[str, Any]                    # validated InputModel dict

    # ── Agent 1 outputs ────────────────────────────────────────────────────
    llm_outputs: Annotated[List[Dict[str, Any]], _append_list]
    # Each item: {"provider": str, "raw": dict, "error": Optional[str]}

    # ── Pydantic-validated per-LLM outputs ─────────────────────────────────
    validated_outputs: List[Dict[str, Any]]  # list of LLMOutputModel dicts

    # ── Agent 2 output ─────────────────────────────────────────────────────
    consolidated_output: Dict[str, Any]      # FinalOutputModel dict
    consolidation_model: str                 # consolidation strategy used

    # ── Retry / error tracking ─────────────────────────────────────────────
    retry_count: int                         # incremented on assertion failure
    errors: List[str]                        # all error messages collected
    partial_result: bool                     # True when max retries exhausted
