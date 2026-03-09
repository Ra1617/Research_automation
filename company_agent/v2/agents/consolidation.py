"""
Consolidation Node — v2
========================
Merges per-cluster research results into a single golden record.

If multiple clusters produced overlapping IDs (unlikely in v2 since
clusters are non-overlapping), the consolidation LLM picks the best row.
Otherwise, it simply collects and validates all rows.
"""

import json
import time
from typing import Dict, Any

from v2.llms import get_consolidation_llm
from v2.prompts import build_consolidation_prompt
from v2.schema_clusters import get_all_cluster_names


MAX_RETRIES = 3
MIN_ROWS = 50


def run_consolidation(state: Dict[str, Any]) -> Dict[str, Any]:
    """Collect all cluster outputs and consolidate into golden record."""
    retries = state.get("retry_consolidation", 0)

    # Gather all cluster outputs
    all_rows = []
    for cluster_name in get_all_cluster_names():
        cluster_rows = state.get(f"cluster_{cluster_name}", [])
        all_rows.extend(cluster_rows)

    print(f"\n[Consolidation] Attempt {retries + 1}/{MAX_RETRIES}")
    print(f"  Total rows from all clusters: {len(all_rows)}")

    if not all_rows:
        print("[Consolidation] ❌ No data from any cluster.")
        return {"golden_record": [], "retry_consolidation": retries}

    # Check for duplicate IDs (shouldn't happen with non-overlapping clusters)
    from collections import Counter
    id_counts = Counter(row.get("ID") for row in all_rows if isinstance(row, dict))
    duplicated_ids = {id_: count for id_, count in id_counts.items() if count > 1}

    if not duplicated_ids:
        # No overlapping IDs — just sort and use directly
        print(f"[Consolidation] ✅ No duplicate IDs — using cluster outputs directly")
        all_rows.sort(key=lambda x: int(x.get("ID", 0)) if isinstance(x, dict) else 0)
        return {"golden_record": all_rows, "retry_consolidation": retries}

    # Has duplicates — use LLM to pick best rows
    print(f"[Consolidation] Found {len(duplicated_ids)} duplicate IDs — invoking LLM")

    if retries > 0:
        wait = retries * 5
        print(f"[Consolidation] 🔄 Retry {retries}/{MAX_RETRIES} — waiting {wait}s...")
        time.sleep(wait)

    try:
        llm = get_consolidation_llm()
    except RuntimeError as e:
        print(f"[Consolidation] ❌ {e}")
        # Fallback: pick first row per ID
        return _fallback_consolidation(all_rows, retries)

    # Process in chunks to stay within token limits
    golden = []
    non_duplicated = [r for r in all_rows if isinstance(r, dict) and id_counts.get(r.get("ID"), 0) == 1]
    duplicated_rows = [r for r in all_rows if isinstance(r, dict) and id_counts.get(r.get("ID"), 0) > 1]

    # Add all non-duplicated rows directly
    golden.extend(non_duplicated)

    if duplicated_rows:
        try:
            prompt = build_consolidation_prompt(duplicated_rows)
            response = llm.invoke(prompt)
            content = response.content if hasattr(response, 'content') else str(response)

            # Parse JSON
            content = content.strip()
            if content.startswith("```"):
                parts = content.split("```")
                content = parts[1] if len(parts) > 1 else content
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()

            consolidated = json.loads(content)
            golden.extend(consolidated)
            print(f"[Consolidation] LLM resolved {len(consolidated)} duplicate IDs")
        except Exception as e:
            print(f"[Consolidation] LLM consolidation failed: {e} — using fallback")
            golden.extend(_pick_first_per_id(duplicated_rows))

    golden.sort(key=lambda x: int(x.get("ID", 0)) if isinstance(x, dict) else 0)
    print(f"[Consolidation] Result: {len(golden)} golden records")

    return {"golden_record": golden, "retry_consolidation": retries}


def _pick_first_per_id(rows: list) -> list:
    """Fallback: pick the first row per ID."""
    seen = set()
    result = []
    for row in rows:
        if isinstance(row, dict):
            id_ = row.get("ID")
            if id_ not in seen:
                seen.add(id_)
                result.append(row)
    return result


def _fallback_consolidation(all_rows: list, retries: int) -> Dict[str, Any]:
    """Fallback when no LLM is available."""
    golden = _pick_first_per_id(all_rows)
    golden.sort(key=lambda x: int(x.get("ID", 0)) if isinstance(x, dict) else 0)
    return {"golden_record": golden, "retry_consolidation": retries}


def should_retry_consolidation(state: Dict[str, Any]) -> str:
    """Conditional edge — retry consolidation if output is too small."""
    rows = len(state.get("golden_record", []))
    retries = state.get("retry_consolidation", 0)

    if rows < MIN_ROWS and retries < MAX_RETRIES:
        print(f"[Router] Consolidation: {rows} rows (need {MIN_ROWS}) — retry {retries + 1}/{MAX_RETRIES}")
        return "retry_consolidation"

    if rows < MIN_ROWS:
        print(f"[Router] Consolidation: {rows} rows — max retries hit, saving anyway")
    else:
        print(f"[Router] Consolidation: {rows} rows — pass ✅")

    return "save"


def increment_consolidation_retry(state: Dict[str, Any]) -> Dict[str, Any]:
    """Bump consolidation retry counter."""
    new_count = state.get("retry_consolidation", 0) + 1
    print(f"[Retry] consolidation → attempt {new_count + 1}")
    return {"retry_consolidation": new_count}
