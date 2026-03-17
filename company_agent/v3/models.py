"""
v3 Pydantic Models
==================
Three validation gates:
  1. InputModel       — FastAPI request body
  2. LLMOutputModel   — per-LLM output (30 fields, strict validators)
  3. FinalOutputModel — consolidated output (30 fields + meta)

Enumerations and validators are adapted from Agent_kooshi's CompanySchema.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# ═══════════════════════════════════════════════════════════════════════════════
# ENUMERATIONS
# ═══════════════════════════════════════════════════════════════════════════════

class EmployeeSizeEnum(str, Enum):
    micro      = "1-10"
    small      = "11-50"
    small_med  = "51-200"
    medium     = "201-500"
    med_large  = "501-1000"
    large      = "1001-5000"
    very_large = "5001-10000"
    enterprise = "10000+"


class HiringVelocityEnum(str, Enum):
    low       = "low"
    medium    = "medium"
    high      = "high"
    very_high = "very_high"


class EmployeeTurnoverEnum(str, Enum):
    low      = "low"
    moderate = "moderate"
    high     = "high"
    critical = "critical"


class ProfitabilityEnum(str, Enum):
    profitable  = "profitable"
    break_even  = "break_even"
    pre_revenue = "pre_revenue"
    loss_making = "loss_making"


class CompanyMaturityEnum(str, Enum):
    idea       = "idea"
    startup    = "startup"
    early      = "early_stage"
    growth     = "growth"
    mature     = "mature"
    enterprise = "enterprise"


class SalesMotionEnum(str, Enum):
    inbound     = "inbound"
    outbound    = "outbound"
    product_led = "product_led"
    channel     = "channel"
    hybrid      = "hybrid"


class AIMLAdoptionEnum(str, Enum):
    none         = "none"
    basic        = "basic"
    intermediate = "intermediate"
    advanced     = "advanced"
    cutting_edge = "cutting_edge"


# ═══════════════════════════════════════════════════════════════════════════════
# GATE 1 — INPUT MODEL
# ═══════════════════════════════════════════════════════════════════════════════

class InputModel(BaseModel):
    """Request body validated by FastAPI before the graph starts."""

    company_name: str = Field(
        ...,
        min_length=1,
        description="Name of the company to research",
        examples=["OpenAI", "Zepto", "Blinkit"],
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {"company_name": "OpenAI"}
        }
    )


# ═══════════════════════════════════════════════════════════════════════════════
# GATE 2 — PER-LLM OUTPUT MODEL  (30 fields)
# ═══════════════════════════════════════════════════════════════════════════════

class LLMOutputModel(BaseModel):
    """
    Validated output from a single LLM worker.
    Only `name` is strictly required; all other fields are optional.
    """

    model_config = ConfigDict(populate_by_name=True)

    # ── Meta ──────────────────────────────────────────────────────────────
    provider: str = Field(..., description="LLM provider name: groq | mistral | nvidia")
    error: Optional[str] = Field(None, description="Error message if LLM call failed")

    # ── 1. Identity ───────────────────────────────────────────────────────
    name: str = Field(..., min_length=1, description="Official company name")
    website_url: Optional[str] = None
    headquarters_address: Optional[str] = None
    incorporation_year: Optional[int] = Field(None, ge=1800, le=2025)

    # ── 2. Workforce ──────────────────────────────────────────────────────
    employee_size: Optional[EmployeeSizeEnum] = None
    hiring_velocity: Optional[HiringVelocityEnum] = None
    employee_turnover: Optional[EmployeeTurnoverEnum] = None

    # ── 3. Presence ───────────────────────────────────────────────────────
    operating_countries: Optional[List[str]] = None
    focus_sectors: Optional[List[str]] = None
    key_competitors: Optional[List[str]] = None
    tech_stack: Optional[List[str]] = None

    # ── 4. Leadership ─────────────────────────────────────────────────────
    ceo_name: Optional[str] = None
    linkedin_url: Optional[str] = None

    # ── 5. Financials ─────────────────────────────────────────────────────
    annual_revenue: Optional[float] = Field(None, ge=0)
    valuation: Optional[float] = Field(None, ge=0)
    total_capital_raised: Optional[float] = Field(None, ge=0)
    yoy_growth_rate: Optional[float] = Field(None, ge=-100)
    profitability_status: Optional[ProfitabilityEnum] = None

    # ── 6. Ratings & Scores ───────────────────────────────────────────────
    glassdoor_rating: Optional[float] = Field(None, ge=0, le=5)
    brand_sentiment_score: Optional[float] = Field(None, ge=-1.0, le=1.0)

    # ── 7. Market Sizing ──────────────────────────────────────────────────
    tam: Optional[float] = Field(None, ge=0, description="Total Addressable Market USD")
    sam: Optional[float] = Field(None, ge=0, description="Serviceable Addressable Market USD")

    # ── 8. GTM & Strategy ─────────────────────────────────────────────────
    sales_motion: Optional[SalesMotionEnum] = None
    ai_ml_adoption_level: Optional[AIMLAdoptionEnum] = None
    company_maturity: Optional[CompanyMaturityEnum] = None

    # ── 9. Culture & Vision ───────────────────────────────────────────────
    vision_statement: Optional[str] = None
    mission_statement: Optional[str] = None
    competitive_advantages: Optional[List[str]] = None
    key_investors: Optional[List[str]] = None
    remote_policy_details: Optional[str] = None

    # ── Validators ────────────────────────────────────────────────────────

    @field_validator("website_url", "linkedin_url", mode="before")
    @classmethod
    def validate_url(cls, v: Any) -> Optional[str]:
        if v is None:
            return v
        s = str(v).strip()
        if s and not re.match(r"^https?://", s, re.IGNORECASE):
            s = "https://" + s
        return s

    @model_validator(mode="after")
    def validate_tam_sam(self) -> "LLMOutputModel":
        if self.tam and self.sam and self.tam < self.sam:
            # Auto-correct: swap if inverted
            self.tam, self.sam = self.sam, self.tam
        return self


# ═══════════════════════════════════════════════════════════════════════════════
# GATE 3 — FINAL OUTPUT MODEL
# ═══════════════════════════════════════════════════════════════════════════════

class FinalOutputModel(BaseModel):
    """
    Consolidated output from Agent 2.
    Same 30 fields as LLMOutputModel plus pipeline metadata.
    """

    model_config = ConfigDict(populate_by_name=True)

    # ── Pipeline meta ─────────────────────────────────────────────────────
    sources: List[str] = Field(
        default_factory=list,
        description="Providers that contributed to this output"
    )
    confidence: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Fraction of fields filled (0–1)"
    )

    # ── 30 company fields (same as LLMOutputModel) ────────────────────────
    name: str = Field(..., min_length=1)
    website_url: Optional[str] = None
    headquarters_address: Optional[str] = None
    incorporation_year: Optional[int] = Field(None, ge=1800, le=2025)
    employee_size: Optional[EmployeeSizeEnum] = None
    hiring_velocity: Optional[HiringVelocityEnum] = None
    employee_turnover: Optional[EmployeeTurnoverEnum] = None
    operating_countries: Optional[List[str]] = None
    focus_sectors: Optional[List[str]] = None
    key_competitors: Optional[List[str]] = None
    tech_stack: Optional[List[str]] = None
    ceo_name: Optional[str] = None
    linkedin_url: Optional[str] = None
    annual_revenue: Optional[float] = Field(None, ge=0)
    valuation: Optional[float] = Field(None, ge=0)
    total_capital_raised: Optional[float] = Field(None, ge=0)
    yoy_growth_rate: Optional[float] = Field(None, ge=-100)
    profitability_status: Optional[ProfitabilityEnum] = None
    glassdoor_rating: Optional[float] = Field(None, ge=0, le=5)
    brand_sentiment_score: Optional[float] = Field(None, ge=-1.0, le=1.0)
    tam: Optional[float] = Field(None, ge=0)
    sam: Optional[float] = Field(None, ge=0)
    sales_motion: Optional[SalesMotionEnum] = None
    ai_ml_adoption_level: Optional[AIMLAdoptionEnum] = None
    company_maturity: Optional[CompanyMaturityEnum] = None
    vision_statement: Optional[str] = None
    mission_statement: Optional[str] = None
    competitive_advantages: Optional[List[str]] = None
    key_investors: Optional[List[str]] = None
    remote_policy_details: Optional[str] = None

    @field_validator("website_url", "linkedin_url", mode="before")
    @classmethod
    def validate_url(cls, v: Any) -> Optional[str]:
        if v is None:
            return v
        s = str(v).strip()
        if s and not re.match(r"^https?://", s, re.IGNORECASE):
            s = "https://" + s
        return s

    @model_validator(mode="after")
    def validate_tam_sam(self) -> "FinalOutputModel":
        if self.tam and self.sam and self.tam < self.sam:
            self.tam, self.sam = self.sam, self.tam
        return self
