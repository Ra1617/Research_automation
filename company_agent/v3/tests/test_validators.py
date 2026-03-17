"""
v3 Unit Tests
=============
Tests for all validator functions — zero LLM calls, zero network I/O.
Covers:
  - validate_llm_output() — Pydantic Gate 2
  - validate_final_output() — Pydantic Gate 3
  - run_pytest_assertions() — 28 business-rule checks

Run with:
  cd company_agent
  python -m pytest v3/tests/test_validators.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pytest
from v3.validators import validate_llm_output, validate_final_output, run_pytest_assertions


# ── Sample data ───────────────────────────────────────────────────────────────

VALID_LLM_OUTPUT = {
    "name": "OpenAI",
    "provider": "groq",
    "website_url": "https://openai.com",
    "headquarters_address": "San Francisco, CA",
    "incorporation_year": 2015,
    "employee_size": "1001-5000",
    "hiring_velocity": "high",
    "employee_turnover": "low",
    "operating_countries": ["USA", "UK", "Japan"],
    "focus_sectors": ["AI", "Research", "SaaS"],
    "key_competitors": ["Google DeepMind", "Anthropic", "Meta AI"],
    "tech_stack": ["Python", "Kubernetes", "CUDA"],
    "ceo_name": "Sam Altman",
    "linkedin_url": "https://linkedin.com/company/openai",
    "annual_revenue": 2000000000.0,
    "valuation": 90000000000.0,
    "total_capital_raised": 11000000000.0,
    "yoy_growth_rate": 300.0,
    "profitability_status": "loss_making",
    "glassdoor_rating": 4.2,
    "brand_sentiment_score": 0.6,
    "tam": 1000000000000.0,
    "sam": 100000000000.0,
    "sales_motion": "product_led",
    "ai_ml_adoption_level": "cutting_edge",
    "company_maturity": "growth",
    "vision_statement": "Ensure AGI benefits all of humanity",
    "mission_statement": "Build safe and beneficial general-purpose AI",
    "competitive_advantages": ["Research depth", "GPT brand", "API ecosystem"],
    "key_investors": ["Microsoft", "Khosla Ventures", "Reid Hoffman"],
    "remote_policy_details": "Hybrid with flexible remote options",
}

VALID_FINAL_OUTPUT = {
    **VALID_LLM_OUTPUT,
    "sources": ["groq", "mistral", "nvidia"],
    "confidence": 0.93,
}
# Remove 'provider' from final (it doesn't have that field)
VALID_FINAL_OUTPUT.pop("provider", None)


# ═══════════════════════════════════════════════════════════════════════════════
# TEST: validate_llm_output — PYDANTIC GATE 2
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidateLLMOutput:

    def test_valid_full_output(self):
        result, err = validate_llm_output(VALID_LLM_OUTPUT.copy(), "groq")
        assert err is None
        assert result is not None
        assert result["name"] == "OpenAI"
        assert result["provider"] == "groq"

    def test_missing_required_name(self):
        data = {k: v for k, v in VALID_LLM_OUTPUT.items() if k != "name"}
        result, err = validate_llm_output(data, "groq")
        assert err is not None
        assert "name" in err.lower()
        assert result is None

    def test_invalid_glassdoor_rating_over_5(self):
        data = {**VALID_LLM_OUTPUT, "glassdoor_rating": 9.9}
        result, err = validate_llm_output(data, "groq")
        assert err is not None
        assert result is None

    def test_invalid_employee_size_enum(self):
        data = {**VALID_LLM_OUTPUT, "employee_size": "thousands"}
        result, err = validate_llm_output(data, "mistral")
        assert err is not None
        assert result is None

    def test_url_auto_fix_adds_https(self):
        data = {**VALID_LLM_OUTPUT, "website_url": "openai.com"}
        result, err = validate_llm_output(data, "groq")
        assert err is None
        assert result["website_url"].startswith("https://")

    def test_null_optional_fields_pass(self):
        minimal = {"name": "TestCo", "provider": "groq"}
        result, err = validate_llm_output(minimal, "groq")
        assert err is None
        assert result["name"] == "TestCo"

    def test_negative_annual_revenue_fails(self):
        data = {**VALID_LLM_OUTPUT, "annual_revenue": -100.0}
        result, err = validate_llm_output(data, "groq")
        assert err is not None

    def test_invalid_brand_sentiment_out_of_range(self):
        data = {**VALID_LLM_OUTPUT, "brand_sentiment_score": 2.0}
        result, err = validate_llm_output(data, "nvidia")
        assert err is not None

    def test_provider_tag_injected(self):
        result, err = validate_llm_output(VALID_LLM_OUTPUT.copy(), "nvidia")
        assert err is None
        assert result["provider"] == "nvidia"

    def test_tam_sam_auto_swap(self):
        """If TAM < SAM is provided, validator should auto-swap them."""
        data = {**VALID_LLM_OUTPUT, "tam": 1e9, "sam": 5e9}  # inverted
        result, err = validate_llm_output(data, "groq")
        assert err is None
        assert result["tam"] >= result["sam"]


# ═══════════════════════════════════════════════════════════════════════════════
# TEST: validate_final_output — PYDANTIC GATE 3
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidateFinalOutput:

    def test_valid_final_output(self):
        result, err = validate_final_output(VALID_FINAL_OUTPUT.copy())
        assert err is None
        assert result is not None
        assert 0.0 <= result["confidence"] <= 1.0

    def test_missing_name_fails(self):
        data = {k: v for k, v in VALID_FINAL_OUTPUT.items() if k != "name"}
        result, err = validate_final_output(data)
        assert err is not None

    def test_confidence_out_of_range(self):
        data = {**VALID_FINAL_OUTPUT, "confidence": 1.5}
        result, err = validate_final_output(data)
        assert err is not None


# ═══════════════════════════════════════════════════════════════════════════════
# TEST: run_pytest_assertions — GATE 4 (28 checks)
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunPytestAssertions:

    def test_all_pass_on_valid_output(self):
        passed, failures = run_pytest_assertions(VALID_FINAL_OUTPUT)
        assert passed, f"Expected all to pass. Failures: {failures}"
        assert failures == []

    def test_missing_name_fails(self):
        data = {**VALID_FINAL_OUTPUT, "name": ""}
        passed, failures = run_pytest_assertions(data)
        assert not passed
        assert any("name" in f.lower() for f in failures)

    def test_glassdoor_rating_over_5_fails(self):
        data = {**VALID_FINAL_OUTPUT, "glassdoor_rating": 6.0}
        passed, failures = run_pytest_assertions(data)
        assert not passed
        assert any("glassdoor_rating" in f for f in failures)

    def test_invalid_employee_size_fails(self):
        data = {**VALID_FINAL_OUTPUT, "employee_size": "lots"}
        passed, failures = run_pytest_assertions(data)
        assert not passed

    def test_tam_less_than_sam_fails(self):
        data = {**VALID_FINAL_OUTPUT, "tam": 1e6, "sam": 1e9}
        passed, failures = run_pytest_assertions(data)
        assert not passed
        assert any("TAM" in f for f in failures)

    def test_brand_sentiment_out_of_range_fails(self):
        data = {**VALID_FINAL_OUTPUT, "brand_sentiment_score": -2.0}
        passed, failures = run_pytest_assertions(data)
        assert not passed

    def test_negative_revenue_fails(self):
        data = {**VALID_FINAL_OUTPUT, "annual_revenue": -500}
        passed, failures = run_pytest_assertions(data)
        assert not passed

    def test_empty_sources_fails(self):
        data = {**VALID_FINAL_OUTPUT, "sources": []}
        passed, failures = run_pytest_assertions(data)
        assert not passed
        assert any("sources" in f for f in failures)

    def test_confidence_over_1_fails(self):
        data = {**VALID_FINAL_OUTPUT, "confidence": 1.5}
        passed, failures = run_pytest_assertions(data)
        assert not passed

    def test_null_optional_fields_skip_not_fail(self):
        """Optional fields being null should NOT cause assertion failures."""
        data = {
            "name": "MinimalCo",
            "sources": ["groq"],
            "confidence": 0.1,
            # All other fields null
        }
        passed, failures = run_pytest_assertions(data)
        # Only 'name' check is hard; others should be skipped
        assert passed, f"Unexpected failures: {failures}"

    def test_invalid_incorporation_year_fails(self):
        data = {**VALID_FINAL_OUTPUT, "incorporation_year": 1700}
        passed, failures = run_pytest_assertions(data)
        assert not passed

    def test_invalid_url_fails(self):
        data = {**VALID_FINAL_OUTPUT, "website_url": "not-a-url"}
        passed, failures = run_pytest_assertions(data)
        assert not passed

    def test_hiring_velocity_invalid_enum_fails(self):
        data = {**VALID_FINAL_OUTPUT, "hiring_velocity": "rocket"}
        passed, failures = run_pytest_assertions(data)
        assert not passed

    def test_empty_list_field_fails(self):
        data = {**VALID_FINAL_OUTPUT, "key_competitors": []}
        passed, failures = run_pytest_assertions(data)
        assert not passed
