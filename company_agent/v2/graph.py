"""
LangGraph Pipeline — v2
========================

Architecture:
  Each of the 6 schema clusters runs as an independent sequential node.
  After all clusters complete, consolidation merges them into ~163 golden rows.
  Retry logic is handled via conditional edges per cluster.

Flow:
  company_basics → [retry?] →
  digital_leadership → [retry?] →
  financials_growth → [retry?] →
  innovation_market → [retry?] →
  workplace → [retry?] →
  career_values → [retry?] →
  consolidate → [retry?] → save → END

Why sequential instead of parallel?
  Free-tier API rate limits. Sequential with small delays is more
  reliable than parallel which triggers 429 errors on free accounts.
  Switch add_edge to Send() API for true parallelism on paid tiers.

Token budget per call:
  ~600-900 input tokens (cluster prompt) + ~500-1500 output tokens
  vs old approach: ~4000 input + 6000 output = frequent truncation
"""

import os
from dotenv import load_dotenv

load_dotenv(override=True)

# ── Enable LangSmith tracing ────────────────────────────────────────────────
langsmith_key = os.getenv("LANGCHAIN_API_KEY", "")
_placeholder_markers = ("your_", "placeholder", "xxx", "changeme")
_is_placeholder = any(m in langsmith_key.lower() for m in _placeholder_markers) or not langsmith_key

if not _is_placeholder:
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ["LANGCHAIN_API_KEY"] = langsmith_key
    os.environ.setdefault("LANGCHAIN_PROJECT", "company-agent-v2")
    print("[LangSmith] Tracing enabled → project: company-agent-v2")
else:
    # Must fully unset the key — langsmith library auto-traces if key is present
    os.environ["LANGCHAIN_TRACING_V2"] = "false"
    os.environ.pop("LANGCHAIN_API_KEY", None)
    os.environ.pop("LANGCHAIN_ENDPOINT", None)
    print("[LangSmith] No valid LANGCHAIN_API_KEY — tracing disabled")

from langgraph.graph import StateGraph, END

from v2.state import GraphState
from v2.schema_clusters import get_all_cluster_names
from v2.agents.cluster_node import (
    make_cluster_node,
    make_retry_checker,
    make_retry_incrementer,
)
from v2.agents.consolidation import (
    run_consolidation,
    should_retry_consolidation,
    increment_consolidation_retry,
)
from v2.agents.save import save_output


def build_graph():
    """Build and compile the v2 LangGraph pipeline."""

    builder = StateGraph(GraphState)
    CLUSTERS = get_all_cluster_names()

    # ── Register all cluster nodes + their retry nodes ───────────────────
    for cluster in CLUSTERS:
        builder.add_node(cluster, make_cluster_node(cluster))
        builder.add_node(f"retry_inc_{cluster}", make_retry_incrementer(cluster))

    # ── Consolidation + save ──────────────────────────────────────────────
    builder.add_node("consolidate", run_consolidation)
    builder.add_node("retry_inc_consolidate", increment_consolidation_retry)
    builder.add_node("save", save_output)

    # ── Entry point ───────────────────────────────────────────────────────
    builder.set_entry_point(CLUSTERS[0])

    # ── Cluster chain with conditional retry edges ────────────────────────
    for i, cluster in enumerate(CLUSTERS):
        next_node = CLUSTERS[i + 1] if i + 1 < len(CLUSTERS) else "consolidate"

        # After cluster runs → check if retry needed
        builder.add_conditional_edges(
            cluster,
            make_retry_checker(cluster),
            {
                f"retry_{cluster}": f"retry_inc_{cluster}",  # fail → bump counter
                "pass": next_node,                              # pass → next cluster
            }
        )
        # Retry increment → loop back to same cluster
        builder.add_edge(f"retry_inc_{cluster}", cluster)

    # ── Consolidation conditional retry ──────────────────────────────────
    builder.add_conditional_edges(
        "consolidate",
        should_retry_consolidation,
        {
            "retry_consolidation": "retry_inc_consolidate",
            "save": "save",
        }
    )
    builder.add_edge("retry_inc_consolidate", "consolidate")

    # ── Terminal ──────────────────────────────────────────────────────────
    builder.add_edge("save", END)

    return builder.compile()


# Build and expose the graph instance
graph = build_graph()
