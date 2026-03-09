"""
Schema Clusters — v2
=====================
Distributes all 163 parameters from parameters.json into 7 non-overlapping
clusters. Each cluster is a manageable chunk (~20-35 fields) that fits
comfortably within a single LLM call's token budget.

Cluster assignments are based on the reference project's category grouping
from core/models.py, mapped to the actual field keys in parameters.json.
"""

import json
from pathlib import Path
from typing import Dict, List, Tuple, Any


# ── Cluster definitions: name → list of parameter IDs ────────────────────────
# These are contiguous, non-overlapping ranges covering all 163 IDs exactly once.
CLUSTER_ID_RANGES = {
    "company_basics":       list(range(1, 32)),       # IDs 1–31  (31 fields)
    "digital_leadership":   list(range(32, 60)),       # IDs 32–59 (28 fields)
    "financials_growth":    list(range(60, 81)),       # IDs 60–80 (21 fields)
    "innovation_market":    list(range(81, 111)),       # IDs 81–110 (30 fields)
    "workplace":            list(range(111, 133)),      # IDs 111–132 (22 fields)
    "career_values":        list(range(133, 164)),      # IDs 133–163 (31 fields)
}

# Ordered cluster names (pipeline execution order)
CLUSTER_ORDER = [
    "company_basics",
    "digital_leadership",
    "financials_growth",
    "innovation_market",
    "workplace",
    "career_values",
]


def _load_parameters() -> List[Dict[str, Any]]:
    """Load parameters.json from the schema directory."""
    schema_path = Path(__file__).resolve().parents[1] / "schema" / "parameters.json"
    if not schema_path.exists():
        raise FileNotFoundError(f"parameters.json not found at {schema_path}")
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _build_clusters() -> Dict[str, Dict[str, Any]]:
    """Build cluster definitions from parameters.json."""
    all_params = _load_parameters()
    params_by_id = {p["id"]: p for p in all_params}

    clusters = {}
    for cluster_name in CLUSTER_ORDER:
        ids = CLUSTER_ID_RANGES[cluster_name]
        fields = []
        for id_ in ids:
            if id_ in params_by_id:
                p = params_by_id[id_]
                fields.append((p["id"], p["category"], p["key"], p.get("description", "")))
        clusters[cluster_name] = {"fields": fields}

    return clusters


# Module-level cache
_CLUSTERS_CACHE = None


def get_clusters() -> Dict[str, Dict[str, Any]]:
    """Get all cluster definitions (cached)."""
    global _CLUSTERS_CACHE
    if _CLUSTERS_CACHE is None:
        _CLUSTERS_CACHE = _build_clusters()
    return _CLUSTERS_CACHE


def get_all_cluster_names() -> List[str]:
    """Return ordered list of cluster names."""
    return list(CLUSTER_ORDER)


def build_schema_block(cluster_name: str) -> str:
    """
    Build a compact schema text block for a cluster prompt.

    Format per field:
        ID <id> | <category> | <key> | <description>
    """
    clusters = get_clusters()
    if cluster_name not in clusters:
        raise ValueError(f"Unknown cluster: {cluster_name}")

    lines = []
    for id_, category, key, description in clusters[cluster_name]["fields"]:
        lines.append(f"ID {id_} | {category} | {key} | {description}")

    return "\n".join(lines)


def get_cluster_field_count(cluster_name: str) -> int:
    """Return the number of fields in a cluster."""
    clusters = get_clusters()
    return len(clusters[cluster_name]["fields"])


def get_total_field_count() -> int:
    """Return total number of fields across all clusters."""
    clusters = get_clusters()
    return sum(len(c["fields"]) for c in clusters.values())


def get_cluster_field_keys(cluster_name: str) -> List[str]:
    """Return the field keys (parameter names) for a cluster."""
    clusters = get_clusters()
    return [f[2] for f in clusters[cluster_name]["fields"]]
