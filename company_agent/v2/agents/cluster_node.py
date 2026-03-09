"""
Cluster Research Node — v2
============================
Factory functions that create LangGraph node functions for each schema cluster.

Each cluster node:
1. Reads company_name from state
2. Picks a random available LLM
3. Builds a cluster-specific prompt
4. Invokes the LLM and parses JSON response
5. Writes results to the cluster's state slot

Retry logic:
- make_retry_checker: conditional edge that checks row quality
- make_retry_incrementer: bumps retry counter per cluster
"""

import json
import time
from typing import Dict, Any, Callable

from v2.llms import get_random_llm
from v2.prompts import build_cluster_prompt
from v2.schema_clusters import get_cluster_field_count


MAX_RETRIES = 3
MIN_RATIO = 0.5  # Retry if we get less than 50% of expected fields


def _parse_json_response(content: str) -> list:
    """Parse JSON array from LLM response, handling markdown fences."""
    content = content.strip()

    # Remove markdown code fences if present
    if content.startswith("```"):
        parts = content.split("```")
        content = parts[1] if len(parts) > 1 else content
        if content.startswith("json"):
            content = content[4:]
        content = content.strip()

    # Try direct parse
    try:
        result = json.loads(content)
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            return [result]
        return []
    except json.JSONDecodeError:
        # Try to find JSON array in the response
        import re
        match = re.search(r'\[[\s\S]*\]', content)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        return []


def make_cluster_node(cluster_name: str) -> Callable:
    """Create a research node function for a specific cluster.

    Returns a function compatible with LangGraph's add_node().
    """

    def cluster_node(state: Dict[str, Any]) -> Dict[str, Any]:
        company_name = state.get("company_name", "Unknown")
        retries = state.get(f"retry_{cluster_name}", 0)
        expected = get_cluster_field_count(cluster_name)

        print(f"\n[{cluster_name}] Research attempt {retries + 1}/{MAX_RETRIES}")
        print(f"  Company: {company_name} | Expected fields: {expected}")

        # Small delay between retries to avoid rate limits
        if retries > 0:
            wait = retries * 3
            print(f"  🔄 Retry {retries}/{MAX_RETRIES} — waiting {wait}s...")
            time.sleep(wait)

        # Pick a random LLM
        llm, provider_name = get_random_llm()
        if llm is None:
            print(f"  ❌ No LLM available!")
            return {f"cluster_{cluster_name}": []}

        print(f"  Using: {provider_name}")

        # Build prompt and invoke
        prompt = build_cluster_prompt(company_name, cluster_name)

        try:
            response = llm.invoke(prompt)
            content = response.content if hasattr(response, 'content') else str(response)
            rows = _parse_json_response(content)

            # Tag each row with source
            for row in rows:
                if isinstance(row, dict):
                    row["Source"] = row.get("Source", provider_name)

            print(f"  ✅ Got {len(rows)} rows from {provider_name}")

        except Exception as e:
            print(f"  ❌ LLM call failed: {e}")
            rows = []

        return {f"cluster_{cluster_name}": rows}

    return cluster_node


def make_retry_checker(cluster_name: str) -> Callable:
    """Create a conditional edge function that decides retry vs pass.

    Returns:
        - f"retry_{cluster_name}" if row count is too low and retries remain
        - "pass" to move to the next cluster
    """

    def check_retry(state: Dict[str, Any]) -> str:
        rows = state.get(f"cluster_{cluster_name}", [])
        retries = state.get(f"retry_{cluster_name}", 0)
        expected = get_cluster_field_count(cluster_name)
        min_rows = max(1, int(expected * MIN_RATIO))

        if len(rows) < min_rows and retries < MAX_RETRIES:
            print(f"[Router] {cluster_name}: {len(rows)}/{expected} rows — retry {retries + 1}/{MAX_RETRIES}")
            return f"retry_{cluster_name}"

        if len(rows) < min_rows:
            print(f"[Router] {cluster_name}: {len(rows)}/{expected} rows — max retries hit, moving on")
        else:
            print(f"[Router] {cluster_name}: {len(rows)}/{expected} rows — pass ✅")

        return "pass"

    return check_retry


def make_retry_incrementer(cluster_name: str) -> Callable:
    """Create a node that bumps the retry counter for a cluster."""

    def increment_retry(state: Dict[str, Any]) -> Dict[str, Any]:
        new_count = state.get(f"retry_{cluster_name}", 0) + 1
        print(f"[Retry] {cluster_name} → attempt {new_count + 1}")
        return {f"retry_{cluster_name}": new_count}

    return increment_retry
