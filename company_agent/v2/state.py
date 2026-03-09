"""
GraphState — v2
================
Each cluster gets its own output slot in state.
Retry counters per cluster.
"""
from typing import TypedDict, Dict, Any, List


class GraphState(TypedDict, total=False):

    # ── Input ──────────────────────────────────────────────────────────────
    company_name: str

    # ── Per-cluster research results (list of field dicts) ─────────────────
    cluster_company_basics:    List[Dict[str, Any]]
    cluster_digital_leadership: List[Dict[str, Any]]
    cluster_financials_growth: List[Dict[str, Any]]
    cluster_innovation_market: List[Dict[str, Any]]
    cluster_workplace:         List[Dict[str, Any]]
    cluster_career_values:     List[Dict[str, Any]]

    # ── Retry counters per cluster ─────────────────────────────────────────
    retry_company_basics:     int
    retry_digital_leadership: int
    retry_financials_growth:  int
    retry_innovation_market:  int
    retry_workplace:          int
    retry_career_values:      int

    # ── Consolidation retry ────────────────────────────────────────────────
    retry_consolidation: int

    # ── Pipeline outputs ───────────────────────────────────────────────────
    golden_record:  List[Dict[str, Any]]   # Final 163 rows
