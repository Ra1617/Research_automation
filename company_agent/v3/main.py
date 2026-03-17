"""
v3 CLI Runner
=============
Provides a simple command‑line interface around the compiled LangGraph v3
pipeline.  The FastAPI server has been removed; call this script directly.

Usage:
    python -m v3.main --company "OpenAI"
    python -m v3.main           # prompts for name

Output is printed as JSON containing:
  status, company_name, result, retry_count, errors, partial_result
"""
from __future__ import annotations

import os
from langsmith import traceable

# Enable LangSmith tracing
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = "company-agent-v3"

# Your existing imports below

import logging
import os
import sys
import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv
from langsmith import traceable

load_dotenv(override=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ── Import graph (lazy-loaded — no LLM init until first call) ───
from v3.graph import get_graph  # noqa: E402


OUTPUT_DIR = Path(__file__).resolve().parent / "outpul"


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_-]+", "_", value.strip())
    return slug.strip("_") or "company"


def save_output_json(response: Dict[str, Any], company_name: str, output_dir: Path | None = None) -> Path:
    target_dir = output_dir or OUTPUT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{_safe_slug(company_name)}_{ts}.json"
    destination = target_dir / filename
    destination.write_text(json.dumps(response, indent=2), encoding="utf-8")
    return destination


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════





# ── Command-line interface ───────────────────────────────────────────────────
@traceable(name="run_company")
def run_company(company_name: str) -> Dict[str, Any]:
    """Execute the v3 pipeline synchronously and return a response dict."""
    logger.info("Pipeline run for company_name=%s", company_name)
    graph = get_graph()
    initial_state = {
        "input": {"company_name": company_name},
        "llm_outputs": [],
        "validated_outputs": [],
        "consolidated_output": {},
        "consolidation_model": "",
        "retry_count": 0,
        "errors": [],
        "partial_result": False,
    }
    final_state = graph.invoke(initial_state, config={"recursion_limit": 100})
    consolidated = final_state.get("consolidated_output", {})
    partial = final_state.get("partial_result", False)
    return {
        "status": "partial" if partial else "success",
        "company_name": company_name,
        "result": consolidated,
        "retry_count": final_state.get("retry_count", 0),
        "errors": final_state.get("errors", []),
        "partial_result": partial,
    }


def main():
    parser = argparse.ArgumentParser(description="Run v3 company intelligence pipeline")
    parser.add_argument("--company", type=str, help="Company name to research")
    args = parser.parse_args()

    company = args.company or input("Enter company name: ").strip()
    if not company:
        print("Error: company name is required.")
        sys.exit(1)

    resp = run_company(company)
    saved_file = save_output_json(resp, company)
    print(json.dumps(resp, indent=2))
    print(f"Saved JSON output to: {saved_file}")


if __name__ == "__main__":
    main()
