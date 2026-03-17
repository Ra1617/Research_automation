"""
v3 LangChain artifacts
======================

This module centralises all of the LangChain prompt templates and chain
construction logic so that the pipeline (v3.agents) can operate on
ready-to-use "Runnable" objects. By separating the prompts and chains from
the business logic we make it easier to hook the pipeline into LangSmith
UI, LangGraph tracing, or swap out models later.

The three worker chains (`groq_chain`, `mistral_chain`, `nvidia_chain`) each
implement the same research prompt; they simply differ in the LLM class and
API key.  `hf_consolidation_chain` is an optional chain that calls
HuggingFace's Llama-3.2-3B for merging.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv(override=True)
logger = logging.getLogger(__name__)

# ── Config pulled from environment (same as in v3.agents) ───────────────────
GROQ_API_KEY    = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL      = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_MODEL   = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
NVIDIA_API_KEY  = os.getenv("NVIDIA_API_KEY", "")
NVIDIA_MODEL    = os.getenv("NVIDIA_MODEL", "meta/llama-4-maverick-17b-128e-instruct")
HF_TOKEN        = os.getenv("HF_TOKEN", "")
HF_MODEL_ID     = os.getenv("HF_MODEL_ID", "meta-llama/Llama-3.2-3B-Instruct")
NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"

# ── LangChain imports (optional in tests if chain is None) ────────────────
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableLambda

# the specific client wrappers
from langchain_groq import ChatGroq
from langchain_mistralai import ChatMistralAI
from langchain_openai import ChatOpenAI


# ── Helpers ───────────────────────────────────────────────────────────────

def _parse_json(text: str) -> Dict[str, Any]:
    """Extract JSON object from LLM output, stripping markdown fences."""
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
    return {}


# ── Prompt templates ──────────────────────────────────────────────────────

RESEARCH_PROMPT = ChatPromptTemplate.from_messages([
    ("system",
     "You are a company research specialist. Extract factual data about companies. "
     "Return ONLY a valid JSON object with no markdown fences, no extra text. "
     "Use null for unknown values."),
    ("user",
     "Research the company: {company_name}\n\n"
     "Return a JSON object with EXACTLY these 30 keys:\n"
     "name, website_url, headquarters_address, incorporation_year, "
     "employee_size, hiring_velocity, employee_turnover, operating_countries, "
     "focus_sectors, key_competitors, tech_stack, ceo_name, linkedin_url, "
     "annual_revenue, valuation, total_capital_raised, yoy_growth_rate, "
     "profitability_status, glassdoor_rating, brand_sentiment_score, "
     "tam, sam, sales_motion, ai_ml_adoption_level, company_maturity, "
     "vision_statement, mission_statement, competitive_advantages, "
     "key_investors, remote_policy_details\n\n"
     "Enum constraints:\n"
     "- employee_size: '1-10','11-50','51-200','201-500','501-1000','1001-5000','5001-10000','10000+'\n"
     "- hiring_velocity: 'low','medium','high','very_high'\n"
     "- employee_turnover: 'low','moderate','high','critical'\n"
     "- profitability_status: 'profitable','break_even','pre_revenue','loss_making'\n"
     "- company_maturity: 'idea','startup','early_stage','growth','mature','enterprise'\n"
     "- sales_motion: 'inbound','outbound','product_led','channel','hybrid'\n"
     "- ai_ml_adoption_level: 'none','basic','intermediate','advanced','cutting_edge'\n"
     "- glassdoor_rating: float 0-5, brand_sentiment_score: float -1 to 1\n"
     "- TAM/SAM: numbers in USD, operating_countries/focus_sectors/key_competitors/"
     "tech_stack/competitive_advantages/key_investors: arrays of strings"
    ),
])

CONSOLIDATION_PROMPT = ChatPromptTemplate.from_messages([
    ("system", "You are a data consolidation expert. Return only valid JSON."),
    ("user",
     "You have 3 research outputs about the same company from different LLMs.\n"
     "Merge them into ONE best consolidated JSON with EXACTLY the same 30 keys.\n"
     "Pick the most accurate/complete value for each field.\n"
     "For list fields, merge and deduplicate. For numeric fields, prefer the most specific non-null value.\n"
     "Return ONLY valid JSON. No markdown, no explanation.\n\n"
     "Output 1 ({p1}):\n{out1}\n\n"
     "Output 2 ({p2}):\n{out2}\n\n"
     "Output 3 ({p3}):\n{out3}\n\n"
     "Required keys: name, website_url, headquarters_address, incorporation_year,\n"
     "employee_size, hiring_velocity, employee_turnover, operating_countries,\n"
     "focus_sectors, key_competitors, tech_stack, ceo_name, linkedin_url,\n"
     "annual_revenue, valuation, total_capital_raised, yoy_growth_rate,\n"
     "profitability_status, glassdoor_rating, brand_sentiment_score,\n"
     "tam, sam, sales_motion, ai_ml_adoption_level, company_maturity,\n"
     "vision_statement, mission_statement, competitive_advantages,\n"
     "key_investors, remote_policy_details"
    ),
])


def _build_research_chain(llm) -> Any:
    """Return a Runnable that executes RESEARCH_PROMPT through the given LLM."""
    return RESEARCH_PROMPT | llm | StrOutputParser() | RunnableLambda(_parse_json)


# ── Worker chains ──────────────────────────────────────────────────────────

groq_chain    = _build_research_chain(ChatGroq(api_key=GROQ_API_KEY, model=GROQ_MODEL, temperature=0.1))
mistral_chain = _build_research_chain(ChatMistralAI(api_key=MISTRAL_API_KEY, model=MISTRAL_MODEL, temperature=0.1))
nvidia_chain   = _build_research_chain(
    ChatOpenAI(api_key=NVIDIA_API_KEY, base_url=NVIDIA_BASE_URL, model=NVIDIA_MODEL, temperature=0.1)
)

# ── Traceable run helpers for LangSmith UI ─────────────────────────────────
from langsmith import traceable

@traceable(name="run_groq_chain")
def run_groq_chain(input_vars: Dict[str, Any]) -> Dict[str, Any]:
    return groq_chain.invoke(input_vars)

@traceable(name="run_mistral_chain")
def run_mistral_chain(input_vars: Dict[str, Any]) -> Dict[str, Any]:
    return mistral_chain.invoke(input_vars)

@traceable(name="run_nvidia_chain")
def run_nvidia_chain(input_vars: Dict[str, Any]) -> Dict[str, Any]:
    return nvidia_chain.invoke(input_vars)



# ── Consolidation chain (optional – returns None if HF_TOKEN missing) ──────

def _build_hf_consolidation_chain() -> Optional[Any]:
    if not HF_TOKEN:
        return None
    hf_llm = ChatOpenAI(api_key=HF_TOKEN, model=HF_MODEL_ID, temperature=0.1)
    return CONSOLIDATION_PROMPT | hf_llm | StrOutputParser() | RunnableLambda(_parse_json)

hf_consolidation_chain = _build_hf_consolidation_chain()

@traceable(name="run_hf_consolidation")
def run_hf_consolidation(input_vars: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if hf_consolidation_chain is None:
        return None
    return hf_consolidation_chain.invoke(input_vars)

