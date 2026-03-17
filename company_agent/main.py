"""
Company Research Agent
======================
Queries 3 LLMs (Gemini, Groq, OpenRouter) for company parameters,
consolidates via consensus, and writes to Excel with confidence scoring.

Usage:
    python main.py
    python main.py --company "Blinkit"
    python main.py --company "Zepto" --output "output/results.xlsx"
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import logging

from dotenv import load_dotenv

from schema.schema_validator import load_params, validate_result
from services.graph import get_graph
from services.audit_logger import save_audit_log
from utils.excel_writer import write_to_excel
from config import OUTPUT_FILE

load_dotenv()

# -----------------------------------------------------------------------------
# LangSmith tracing
# -----------------------------------------------------------------------------

LC_API_KEY = os.getenv("LANGCHAIN_API_KEY")

if LC_API_KEY and "your_" not in LC_API_KEY.lower():
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ["LANGCHAIN_PROJECT"] = "company-agent-v3"
    print("[LangSmith] Tracing enabled")
else:
    print("[LangSmith] Tracing disabled (no API key)")

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

logger = logging.getLogger("main")


# -----------------------------------------------------------------------------
# UI helpers
# -----------------------------------------------------------------------------

def print_banner():
    print("=" * 60)
    print("     COMPANY RESEARCH AGENT — 3-LLM CONSENSUS PIPELINE")
    print("     Models: Gemini | Groq | OpenRouter")
    print("=" * 60)


def print_summary(company_name: str, result: dict, validation: dict):

    total = validation["total_params"]

    high = sum(1 for v in result.values() if v.get("score", 0) >= 80)
    low = sum(1 for v in result.values() if 0 < v.get("score", 0) < 80)
    none = sum(1 for v in result.values() if v.get("score", 0) == 0)

    print("\n" + "=" * 60)
    print(f" RESULTS FOR: {company_name.upper()}")
    print("=" * 60)

    print(f"  Total parameters : {total}")
    print(f"  ✅ High confidence: {high} ({high/total*100:.1f}%)")
    print(f"  ⚠️  Low confidence : {low} ({low/total*100:.1f}%)")
    print(f"  ❌ No data found  : {none} ({none/total*100:.1f}%)")

    if validation["errors"]:
        print(f"\n  VALIDATION ERRORS ({len(validation['errors'])}):")

        for e in validation["errors"][:5]:
            print(f"    • {e}")

        if len(validation["errors"]) > 5:
            print(
                f"    ... and {len(validation['errors'])-5} more (see audit log)"
            )

    if validation["warnings"]:

        print(f"\n  WARNINGS ({len(validation['warnings'])}):")

        for w in validation["warnings"][:3]:
            print(f"    • {w}")

    low_required = [
        k for k, v in result.items()
        if 0 < v.get("score", 0) < 80
    ]

    if low_required:

        print(f"\n  Low-confidence fields ({len(low_required)}):")

        for k in low_required[:10]:
            print(f"    • {k}: {result[k]['value']}")

        if len(low_required) > 10:
            print(f"    ... and {len(low_required)-10} more")

    print("=" * 60)


# -----------------------------------------------------------------------------
# Main pipeline
# -----------------------------------------------------------------------------

def main():

    parser = argparse.ArgumentParser(description="Company Research Agent")

    parser.add_argument(
        "--company",
        type=str,
        help="Company name to research",
    )

    parser.add_argument(
        "--output",
        type=str,
        default=OUTPUT_FILE,
        help="Output Excel path",
    )

    args = parser.parse_args()

    print_banner()

    # -------------------------------------------------------------------------
    # Get company name
    # -------------------------------------------------------------------------

    company_name = args.company

    if not company_name:
        company_name = input("\n Enter company name: ").strip()

    if not company_name:
        print("Error: Company name is required.")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # Load parameters
    # -------------------------------------------------------------------------

    all_params = load_params("schema/parameters.json")

    print(f"\n Loaded {len(all_params)} parameters from schema")
    print(f" Output file: {args.output}\n")

    # -------------------------------------------------------------------------
    # Run LangGraph pipeline
    # -------------------------------------------------------------------------

    print(f"\n Starting LangGraph pipeline for {company_name}...\n")

    graph = get_graph()

    state = graph.invoke(
        {
            "input": {"company_name": company_name},
            "retry_count": 0,
            "errors": [],
        }
    )

    # -------------------------------------------------------------------------
    # Extract results safely
    # -------------------------------------------------------------------------

    result_data = state.get("result", {})

    result = result_data.get("selected_by_field", {})

    raw_outputs = result_data.get("records", {})

    # -------------------------------------------------------------------------
    # Validate output
    # -------------------------------------------------------------------------

    validation = validate_result(result, all_params)

    # -------------------------------------------------------------------------
    # Print summary
    # -------------------------------------------------------------------------

    print_summary(company_name, result, validation)

    # -------------------------------------------------------------------------
    # Save Excel
    # -------------------------------------------------------------------------

    write_to_excel(
        company_name,
        result,
        all_params,
        args.output,
    )

    # -------------------------------------------------------------------------
    # Save audit log
    # -------------------------------------------------------------------------

    save_audit_log(
        company_name,
        result,
        raw_outputs,
    )

    print(f"\n✅ ALL DONE! Excel saved to: {args.output}\n")


# -----------------------------------------------------------------------------

if __name__ == "__main__":
    main()