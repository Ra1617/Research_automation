"""
FastAPI wrapper for the v3 company research pipeline.
"""

from __future__ import annotations

from typing import Any, Dict, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from v3.db.connection import init_db
from v3.main import run_company


class ResearchRequest(BaseModel):
    company_name: str = Field(..., min_length=1, description="Company name to research")


class ResearchResponse(BaseModel):
    status: Literal["success", "partial"]
    company_name: str
    result: Dict[str, Any]
    retry_count: int
    errors: list[str]
    partial_result: bool


app = FastAPI(
    title="Company Agent v3 API",
    version="1.0.0",
    description="HTTP wrapper around the LangGraph-based company research pipeline.",
)


@app.on_event("startup")
def startup_event() -> None:
    init_db()


def _run_research(company_name: str) -> ResearchResponse:
    company_name = company_name.strip()
    if not company_name:
        raise HTTPException(status_code=400, detail="company_name is required")

    try:
        response = run_company(company_name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return ResearchResponse.model_validate(response)


@app.get("/")
def root() -> Dict[str, str]:
    return {
        "message": "Company Agent v3 API is running",
        "docs": "/docs",
        "openapi": "/openapi.json",
        "health": "/health",
        "research": "/research",
    }


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/research", response_model=ResearchResponse)
def research_company(payload: ResearchRequest) -> ResearchResponse:
    return _run_research(payload.company_name)


@app.get("/research/{company_name}", response_model=ResearchResponse)
def research_company_by_path(company_name: str) -> ResearchResponse:
    return _run_research(company_name)
