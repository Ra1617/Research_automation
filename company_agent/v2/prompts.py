"""
Prompts — v2
=============
Compact, token-efficient prompts for:
1. Cluster research — one prompt per cluster (~20-35 fields)
2. Consolidation — picks best row per field ID from candidates
"""

import json
from v2.schema_clusters import get_clusters, build_schema_block


# ── Shared instruction block (used by all cluster prompts) ───────────────────
_INSTRUCTIONS = """RULES:
- Return ONLY a valid JSON array. No markdown, no explanation, no code fences.
- Each object: {{"ID": int, "Category": str, "Parameter": str, "Research Output / Data": str, "Source": str}}
- "Source" should indicate which model/source provided the data (e.g., "Groq/Llama-3.3").
- Never leave "Research Output / Data" blank. Use "Not Found" only if truly unavailable.
- For text fields with multiple values, separate with semicolons e.g. "Val1; Val2; Val3"
- All values must be strings. No null values.
- Financial data must include currency and timeframe (e.g., "$1.2B revenue FY2023").
- Inferred values must include "(estimated)".
- Output exactly {count} objects for the IDs listed."""


def build_cluster_prompt(company_name: str, cluster_name: str) -> str:
    """
    Build a compact cluster research prompt.
    ~600-900 tokens input vs ~3000+ for full schema.
    """
    clusters = get_clusters()
    cluster = clusters[cluster_name]
    count = len(cluster["fields"])
    schema = build_schema_block(cluster_name)
    ids = [str(f[0]) for f in cluster["fields"]]
    id_range = f"{ids[0]}–{ids[-1]}"

    instructions = _INSTRUCTIONS.replace("{count}", str(count))

    return f"""You are a corporate research analyst with access to public data.
Research "{company_name}" and return data for parameter IDs {id_range} only.

{instructions}

SCHEMA (ID | Category | Parameter | Description):
{schema}

Return the JSON array now."""


def build_consolidation_prompt(candidates: list) -> str:
    """
    Consolidation prompt — receives candidate rows per ID from multiple clusters.
    Picks the best one per ID based on completeness and quality.
    Token-efficient: only sends the candidates, not the full schema.
    """
    unique_ids = set(r.get("ID") for r in candidates if r.get("ID") is not None)
    count = len(unique_ids)

    return f"""You are a data quality engine. Select the single best row per ID from the candidates below.

RULES:
- Return ONLY a valid JSON array. No markdown, no explanation, no code fences.
- Output exactly {count} objects — one per unique ID.
- For each ID pick the row with the most complete, specific "Research Output / Data".
- Discard rows where data is "Not Found", "N/A", "Unknown", or blank — unless all candidates are like that.
- Never invent or combine data. Pick one existing row as-is.
- Keep the "Source" field from the row you selected.
- Sort output by ID ascending.

CANDIDATES:
{json.dumps(candidates, indent=2)}

Return the consolidated JSON array now."""
