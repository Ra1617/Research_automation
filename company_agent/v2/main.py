"""
Main entry point — v2 Cluster Pipeline
========================================
Usage:
    python -m v2.main
    python -m v2.main --company "Dell Technologies"
    python -m v2.main --company "Google" --output "output/google.json"
"""

import argparse
import json
import sys
import os

# Ensure parent directory is on path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)


def main():
    parser = argparse.ArgumentParser(description="Company Intelligence Pipeline v2")
    parser.add_argument("--company", type=str, help="Company name to research")
    args = parser.parse_args()

    # Get company name
    company_name = args.company
    if not company_name:
        company_name = input("\n Enter company name: ").strip()
    if not company_name:
        print("Error: Company name is required.")
        sys.exit(1)

    # Import here to ensure dotenv is loaded first
    from v2.graph import graph
    from v2.schema_clusters import get_all_cluster_names, get_total_field_count

    clusters = get_all_cluster_names()
    total_fields = get_total_field_count()

    print(f"\n{'='*60}")
    print(f"  Company Intelligence Pipeline v2")
    print(f"  Target : {company_name}")
    print(f"  Clusters: {len(clusters)} | Fields: {total_fields}")
    print(f"  Models: NVIDIA (Llama 4) | Groq | Mistral")
    print(f"{'='*60}\n")

    # Run the pipeline
    result = graph.invoke({"company_name": company_name})

    golden = result.get("golden_record", [])
    print(f"\n{'='*60}")
    print(f"  ✅ Complete — {len(golden)} golden records")
    print(f"{'='*60}")

    if golden:
        print("\nPreview (first 3):")
        print(json.dumps(golden[:3], indent=2))

    return result


if __name__ == "__main__":
    main()
