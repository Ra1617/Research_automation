"""
v3 Validators
=============
Two validation layers:

  Layer 1 — validate_llm_output(data, provider)
    Parses a raw LLM dict through LLMOutputModel (Pydantic).
    Returns (model_dict, error_str | None).

  Layer 2 — run_pytest_assertions(output_dict)
    Programmatic business-rule checks on the consolidated output.
    Returns (passed: bool, failed: list[str]).
    On failure → graph increments retry_count and loops back.

Test cases (28 total):
  Group 1 — Required fields    (1)
  Group 2 — Year / Date        (1)
  Group 3 — URL fields         (2)
  Group 4 — Numeric ranges     (8)
  Group 5 — Enum fields        (7)
  Group 6 — List fields        (6)
  Group 7 — Cross-field        (3)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from pydantic import ValidationError

from v3.models import (
    LLMOutputModel,
    FinalOutputModel,
    EmployeeSizeEnum,
    HiringVelocityEnum,
    EmployeeTurnoverEnum,
    ProfitabilityEnum,
    CompanyMaturityEnum,
    SalesMotionEnum,
    AIMLAdoptionEnum,
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 1 — PYDANTIC VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def validate_llm_output(
    data: Dict[str, Any],
    provider: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Validate a raw LLM response dict through LLMOutputModel.

    Args:
        data:     Raw dict from LLM (may have wrong types / missing fields)
        provider: Provider name tag ("groq" | "mistral" | "nvidia")

    Returns:
        (validated_dict, None)  on success
        (None, error_string)    on ValidationError
    """
    try:
        # Inject provider so the model can tag itself
        data_with_meta = {**data, "provider": provider}
        model = LLMOutputModel.model_validate(data_with_meta)
        return model.model_dump(mode="json"), None
    except ValidationError as exc:
        errors = "; ".join(
            f"{'.'.join(str(l) for l in e['loc'])}: {e['msg']}"
            for e in exc.errors()
        )
        error_str = f"[{provider}] Pydantic validation failed: {errors}"
        logger.warning(error_str)
        return None, error_str
    except Exception as exc:
        error_str = f"[{provider}] Unexpected error during validation: {exc}"
        logger.error(error_str)
        return None, error_str


def validate_final_output(
    data: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Validate consolidated output through FinalOutputModel.

    Returns:
        (validated_dict, None)  on success
        (None, error_string)    on failure
    """
    try:
        model = FinalOutputModel.model_validate(data)
        return model.model_dump(mode="json"), None
    except ValidationError as exc:
        errors = "; ".join(
            f"{'.'.join(str(l) for l in e['loc'])}: {e['msg']}"
            for e in exc.errors()
        )
        return None, f"Final output validation failed: {errors}"
    except Exception as exc:
        return None, f"Final output unexpected error: {exc}"


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 2 — PYTEST-STYLE ASSERTIONS
# ═══════════════════════════════════════════════════════════════════════════════

# ── Assertion helpers ─────────────────────────────────────────────────────────

def _check_required(failures: List[str], value: Any, field: str) -> bool:
    """Hard check — fails if value is None/empty."""
    if value is None or (isinstance(value, str) and not value.strip()):
        failures.append(f"REQUIRED '{field}' is missing or empty")
        return False
    return True


def _check_range(
    failures: List[str], value: Any, field: str, lo: float, hi: float
) -> None:
    """Skip if None, fail if out of range."""
    if value is None:
        return
    try:
        v = float(value)
        if not (lo <= v <= hi):
            failures.append(
                f"'{field}' must be {lo}–{hi}, got {v}"
            )
    except (TypeError, ValueError):
        failures.append(f"'{field}' is not numeric, got {value!r}")


def _check_positive(failures: List[str], value: Any, field: str) -> None:
    if value is None:
        return
    try:
        if float(value) < 0:
            failures.append(f"'{field}' must be >= 0, got {value}")
    except (TypeError, ValueError):
        failures.append(f"'{field}' is not numeric, got {value!r}")


def _check_url(failures: List[str], value: Any, field: str) -> None:
    import re
    if value is None:
        return
    if not re.match(r"^https?://", str(value), re.IGNORECASE):
        failures.append(f"'{field}' must start with http(s)://, got {value!r}")


def _check_enum(failures: List[str], value: Any, field: str, allowed: set) -> None:
    if value is None:
        return
    if value not in allowed:
        failures.append(f"'{field}' must be one of {sorted(allowed)}, got {value!r}")


def _check_list_not_empty(failures: List[str], value: Any, field: str) -> None:
    if value is None:
        return
    if not isinstance(value, list) or len(value) == 0:
        failures.append(f"'{field}' must be a non-empty list when present, got {value!r}")


# ── Main assertion runner ─────────────────────────────────────────────────────

def run_pytest_assertions(output: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Run all 28 business-rule assertions on the consolidated output dict.

    Returns:
        (True, [])          all checks passed
        (False, [reasons])  one or more checks failed
    """
    failures: List[str] = []

    # ── GROUP 1: Required fields ───────────────────────────────────────────
    _check_required(failures, output.get("name"), "name")

    # ── GROUP 2: Year ──────────────────────────────────────────────────────
    _check_range(failures, output.get("incorporation_year"), "incorporation_year", 1800, 2025)

    # ── GROUP 3: URL fields ────────────────────────────────────────────────
    _check_url(failures, output.get("website_url"), "website_url")
    _check_url(failures, output.get("linkedin_url"), "linkedin_url")

    # ── GROUP 4: Numeric ranges ────────────────────────────────────────────
    _check_range(failures, output.get("glassdoor_rating"), "glassdoor_rating", 0, 5)
    _check_range(failures, output.get("brand_sentiment_score"), "brand_sentiment_score", -1.0, 1.0)
    _check_positive(failures, output.get("annual_revenue"), "annual_revenue")
    _check_positive(failures, output.get("valuation"), "valuation")
    _check_positive(failures, output.get("total_capital_raised"), "total_capital_raised")
    _check_range(failures, output.get("yoy_growth_rate"), "yoy_growth_rate", -100, 1000)
    _check_positive(failures, output.get("tam"), "tam")
    _check_positive(failures, output.get("sam"), "sam")

    # ── GROUP 5: Enum fields ───────────────────────────────────────────────
    _check_enum(failures, output.get("employee_size"), "employee_size",
                {e.value for e in EmployeeSizeEnum})
    _check_enum(failures, output.get("hiring_velocity"), "hiring_velocity",
                {e.value for e in HiringVelocityEnum})
    _check_enum(failures, output.get("employee_turnover"), "employee_turnover",
                {e.value for e in EmployeeTurnoverEnum})
    _check_enum(failures, output.get("profitability_status"), "profitability_status",
                {e.value for e in ProfitabilityEnum})
    _check_enum(failures, output.get("company_maturity"), "company_maturity",
                {e.value for e in CompanyMaturityEnum})
    _check_enum(failures, output.get("sales_motion"), "sales_motion",
                {e.value for e in SalesMotionEnum})
    _check_enum(failures, output.get("ai_ml_adoption_level"), "ai_ml_adoption_level",
                {e.value for e in AIMLAdoptionEnum})

    # ── GROUP 6: List fields ───────────────────────────────────────────────
    _check_list_not_empty(failures, output.get("operating_countries"), "operating_countries")
    _check_list_not_empty(failures, output.get("focus_sectors"), "focus_sectors")
    _check_list_not_empty(failures, output.get("key_competitors"), "key_competitors")
    _check_list_not_empty(failures, output.get("tech_stack"), "tech_stack")
    _check_list_not_empty(failures, output.get("competitive_advantages"), "competitive_advantages")
    _check_list_not_empty(failures, output.get("key_investors"), "key_investors")

    # ── GROUP 7: Cross-field consistency ──────────────────────────────────
    tam = output.get("tam")
    sam = output.get("sam")
    if tam is not None and sam is not None:
        try:
            if float(tam) < float(sam):
                failures.append(f"TAM ({tam}) must be >= SAM ({sam})")
        except (TypeError, ValueError):
            pass

    sources = output.get("sources", [])
    if not sources:
        failures.append("'sources' must have at least 1 contributing provider")

    confidence = output.get("confidence", 0)
    try:
        if not (0.0 <= float(confidence) <= 1.0):
            failures.append(f"'confidence' must be 0–1, got {confidence}")
    except (TypeError, ValueError):
        failures.append(f"'confidence' is not numeric, got {confidence!r}")

    passed = len(failures) == 0
    if not passed:
        logger.warning(
            "Pytest assertions failed (%d): %s", len(failures), failures
        )
    return passed, failures
