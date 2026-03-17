"""Repository helpers for inserting pipeline outputs."""

from __future__ import annotations

from typing import Any, Dict

from v3.db.connection import get_session_factory, init_db
from v3.db.models import ConsolidatedOutput, RawModelOutput


def insert_raw_model_output(
    company_name: str,
    model_name: str,
    agent_stage: str,
    raw_json: Dict[str, Any],
) -> bool:
    if not init_db():
        return False

    session_factory = get_session_factory()
    if session_factory is None:
        return False

    with session_factory() as session:
        session.add(
            RawModelOutput(
                company_name=company_name,
                model_name=model_name,
                agent_stage=agent_stage,
                raw_json=raw_json,
            )
        )
        session.commit()

    return True


def insert_consolidated_output(
    company_name: str,
    consolidation_model: str,
    agent_stage: str,
    consolidated_json: Dict[str, Any],
) -> bool:
    if not init_db():
        return False

    session_factory = get_session_factory()
    if session_factory is None:
        return False

    with session_factory() as session:
        session.add(
            ConsolidatedOutput(
                company_name=company_name,
                consolidation_model=consolidation_model,
                agent_stage=agent_stage,
                consolidated_json=consolidated_json,
            )
        )
        session.commit()

    return True
