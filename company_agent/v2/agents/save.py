"""
Save Node — v2
================
Saves the golden record to output/ as a timestamped JSON file.
Runs as the final node in the graph.
"""

import json
import os
from datetime import datetime
from typing import Dict, Any


def save_output(state: Dict[str, Any]) -> Dict[str, Any]:
    """Save golden record to output/ directory."""
    golden_record = state.get("golden_record", [])
    company_name = state.get("company_name", "unknown")

    if not golden_record:
        print("[Save] ⚠️  No golden record found — skipping save.")
        return {}

    # Resolve output/ relative to the project root (company_agent/)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(project_root, "output")
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = company_name.lower().replace(" ", "_").replace("/", "-")
    filename = os.path.join(output_dir, f"{safe_name}_golden_record_{timestamp}.json")

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(golden_record, f, indent=2, ensure_ascii=False)

    print(f"\n[Save] ✅ Golden record saved → {filename}")
    print(f"[Save] 📊 Total rows: {len(golden_record)}")

    return {}
