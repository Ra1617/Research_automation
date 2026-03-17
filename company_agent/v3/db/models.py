"""ORM models for pipeline persistence."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RawModelOutput(Base):
    __tablename__ = "raw_model_outputs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_name: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    model_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    agent_stage: Mapped[str] = mapped_column(String(50), nullable=False)
    raw_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class ConsolidatedOutput(Base):
    __tablename__ = "consolidated_outputs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_name: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    consolidation_model: Mapped[str] = mapped_column(String(100), nullable=False)
    agent_stage: Mapped[str] = mapped_column(String(50), nullable=False)
    consolidated_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
