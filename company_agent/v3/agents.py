"""
v3 Agents
=========
This module provides the callable nodes used by the v3 state graph.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional
from collections import Counter
from enum import Enum

from dotenv import load_dotenv
from langsmith import traceable

from v3.chains import (
    groq_chain,
    mistral_chain,
    nvidia_chain,
    hf_consolidation_chain,
    run_groq_chain,
    run_mistral_chain,
    run_nvidia_chain,
    run_hf_consolidation,
)

load_dotenv(override=True)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# Helper
# ═══════════════════════════════════════════════════════════════════════════════

def _clean_enum(value: Any) -> Any:
    """Convert Enum values to raw value strings."""
    if isinstance(value, Enum):
        return value.value
    return value


def _normalize_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _clean_enum(v) for k, v in d.items()}


# ═══════════════════════════════════════════════════════════════════════════════
# AGENT 1 — WORKER NODES
# ═══════════════════════════════════════════════════════════════════════════════

@traceable(name="groq_worker", run_type="tool")
def worker_groq(state: Dict[str, Any]) -> Dict[str, Any]:
    """Groq worker node."""
    company_name = state["input"]["company_name"]
    logger.info("[groq] Researching: %s", company_name)

    try:
        raw = run_groq_chain({"company_name": company_name})
        raw_original = dict(raw) if isinstance(raw, dict) else {}

        if not raw:
            raise ValueError("Empty response from Groq")

        raw = _normalize_dict(raw)

        logger.info("[groq] Got %d fields", len(raw))

        return {
            "llm_outputs": [{
                "provider": "groq",
                "raw": raw,
                "raw_original": raw_original,
                "error": None,
            }]
        }

    except Exception as exc:
        logger.error("[groq] Failed: %s", exc)
        return {
            "llm_outputs": [{
                "provider": "groq",
                "raw": {},
                "raw_original": {},
                "error": str(exc),
            }]
        }


@traceable(name="mistral_worker", run_type="tool")
def worker_mistral(state: Dict[str, Any]) -> Dict[str, Any]:
    """Mistral worker node."""
    company_name = state["input"]["company_name"]
    logger.info("[mistral] Researching: %s", company_name)

    try:
        raw = run_mistral_chain({"company_name": company_name})
        raw_original = dict(raw) if isinstance(raw, dict) else {}

        if not raw:
            raise ValueError("Empty response from Mistral")

        raw = _normalize_dict(raw)

        logger.info("[mistral] Got %d fields", len(raw))

        return {
            "llm_outputs": [{
                "provider": "mistral",
                "raw": raw,
                "raw_original": raw_original,
                "error": None,
            }]
        }

    except Exception as exc:
        logger.error("[mistral] Failed: %s", exc)
        return {
            "llm_outputs": [{
                "provider": "mistral",
                "raw": {},
                "raw_original": {},
                "error": str(exc),
            }]
        }


@traceable(name="nvidia_worker", run_type="tool")
def worker_nvidia(state: Dict[str, Any]) -> Dict[str, Any]:
    """NVIDIA worker node."""
    company_name = state["input"]["company_name"]
    logger.info("[nvidia] Researching: %s", company_name)

    try:
        raw = run_nvidia_chain({"company_name": company_name})
        raw_original = dict(raw) if isinstance(raw, dict) else {}

        if not raw:
            raise ValueError("Empty response from NVIDIA")

        raw = _normalize_dict(raw)

        logger.info("[nvidia] Got %d fields", len(raw))

        return {
            "llm_outputs": [{
                "provider": "nvidia",
                "raw": raw,
                "raw_original": raw_original,
                "error": None,
            }]
        }

    except Exception as exc:
        logger.error("[nvidia] Failed: %s", exc)
        return {
            "llm_outputs": [{
                "provider": "nvidia",
                "raw": {},
                "raw_original": {},
                "error": str(exc),
            }]
        }


# ═══════════════════════════════════════════════════════════════════════════════
# CONSOLIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def _hf_consolidate(outputs: List[Dict[str, Any]], company_name: str) -> Optional[Dict[str, Any]]:

    if run_hf_consolidation is None:
        logger.warning("[hf] No HF consolidation helper configured")
        return None

    while len(outputs) < 3:
        outputs.append({"provider": "unknown", "raw": {}, "error": "missing"})

    variables = {
        "p1": outputs[0]["provider"],
        "out1": json.dumps(outputs[0]["raw"], indent=2),
        "p2": outputs[1]["provider"],
        "out2": json.dumps(outputs[1]["raw"], indent=2),
        "p3": outputs[2]["provider"],
        "out3": json.dumps(outputs[2]["raw"], indent=2),
    }

    try:
        result = run_hf_consolidation(variables)

        if result:
            logger.info("[hf] Consolidation succeeded — %d fields", len(result))
            return result

        logger.warning("[hf] Empty output")
        return None

    except Exception as exc:
        logger.warning("[hf] Failed: %s", exc)
        return None


def _rule_based_consolidate(outputs: List[Dict[str, Any]]) -> Dict[str, Any]:

    ENUM_FIELDS = {
        "employee_size", "hiring_velocity", "employee_turnover",
        "profitability_status", "company_maturity",
        "sales_motion", "ai_ml_adoption_level",
    }

    NUMERIC_FIELDS = {
        "incorporation_year", "annual_revenue", "valuation",
        "total_capital_raised", "yoy_growth_rate",
        "glassdoor_rating", "brand_sentiment_score", "tam", "sam",
    }

    LIST_FIELDS = {
        "operating_countries", "focus_sectors", "key_competitors",
        "tech_stack", "competitive_advantages", "key_investors",
    }

    merged: Dict[str, Any] = {}

    all_keys = {
        "name", "website_url", "headquarters_address", "incorporation_year",
        "employee_size", "hiring_velocity", "employee_turnover",
        "operating_countries", "focus_sectors", "key_competitors",
        "tech_stack", "ceo_name", "linkedin_url", "annual_revenue",
        "valuation", "total_capital_raised", "yoy_growth_rate",
        "profitability_status", "glassdoor_rating", "brand_sentiment_score",
        "tam", "sam", "sales_motion", "ai_ml_adoption_level",
        "company_maturity", "vision_statement", "mission_statement",
        "competitive_advantages", "key_investors", "remote_policy_details",
    }

    for key in all_keys:

        values = [o["raw"].get(key) for o in outputs if o.get("error") is None]
        non_null = [v for v in values if v is not None]

        if not non_null:
            merged[key] = None

        elif key in ENUM_FIELDS:
            counts = Counter(str(v) for v in non_null)
            merged[key] = counts.most_common(1)[0][0]

        elif key in NUMERIC_FIELDS:
            try:
                nums = [float(v) for v in non_null]
                merged[key] = round(sum(nums) / len(nums), 2)
            except Exception:
                merged[key] = non_null[0]

        elif key in LIST_FIELDS:

            combined = []
            for v in non_null:
                if isinstance(v, list):
                    combined.extend(v)

            seen = set()

            merged[key] = [
                x for x in combined
                if not (x in seen or seen.add(x))
            ]

        else:
            merged[key] = non_null[0]

    return merged


# ═══════════════════════════════════════════════════════════════════════════════
# AGENT 2 ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

@traceable(name="consolidation_agent", run_type="chain")
def run_consolidation(validated_outputs: List[Dict[str, Any]], company_name: str) -> tuple[Dict[str, Any], str]:

    good_outputs = [o for o in validated_outputs if o.get("error") is None]

    sources = [o["provider"] for o in good_outputs]

    if not good_outputs:
        logger.error("No valid outputs")

        return {
            "name": company_name,
            "sources": [],
            "confidence": 0.0,
            "error": "All workers failed",
        }, "none"

    consolidation_model = "rule_based"
    merged = _hf_consolidate(good_outputs, company_name)

    if merged:
        consolidation_model = "huggingface"
    else:
        logger.info("[consolidation] Using rule merge")
        merged = _rule_based_consolidate(good_outputs)

    total_fields = 30
    filled = sum(1 for v in merged.values() if v is not None)

    confidence = round(filled / total_fields, 3)

    merged["sources"] = sources
    merged["confidence"] = confidence

    if not merged.get("name"):
        merged["name"] = company_name

    return merged, consolidation_model
