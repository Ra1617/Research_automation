"""Shared state schema for the Company Intelligence Aggregator graph.

This module defines all TypedDict state schemas used across the multi-provider
LLM orchestration pipeline. The state flows through:

1. Generation (parallel across 3 providers: Gemini, Groq, OpenRouter)
2. Validation (field-by-field checks against metadata rules)
3. Conditional Retry (if validation fails and attempts < max)
4. Consolidation (score and select best values per field)

State is persisted at graph level for LangGraph memory support (thread_id).
"""

from __future__ import annotations

from typing import Annotated, Any, Dict, List, TypedDict

from pydantic import BaseModel, ConfigDict, Field


def merge_records(
    left: Dict[str, Dict[str, Any]],
    right: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Merge two record dictionaries for Annotated reducer."""
    merged = dict(left)
    merged.update(right)
    return merged


class GraphState(TypedDict):
    """Full graph state persisted across all nodes and retries.
    
    Attributes:
        company_name: The target company being researched
        records: Dict keyed by provider name (gemini, groq, openrouter)
                 Values are provider-specific extracted records with metadata
        result: Final consolidated output ready for user consumption
    """
    company_name: str
    records: Annotated[Dict[str, Dict[str, Any]], merge_records]
    result: Dict[str, Any]


class InputState(BaseModel):
    """Input schema for the graph (shown in LangSmith Studio and API)."""
    company_name: str = Field(
        ..., 
        description="Name of the company to research",
        title="Company Name",
        examples=["OpenAI", "Google", "Microsoft"]
    )
    
    model_config = ConfigDict(
        json_schema_extra={
            "type": "object",
            "properties": {
                "company_name": {
                    "type": "string",
                    "description": "Name of the company to research",
                    "title": "Company Name"
                }
            },
            "required": ["company_name"]
        }
    )


class OutputState(TypedDict):
    """Output schema returned to the user."""
    records: Dict[str, Dict[str, Any]]
    result: Dict[str, Any]
