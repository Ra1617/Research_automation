"""SQLAlchemy connection helpers for PostgreSQL/Supabase."""

from __future__ import annotations

import logging
import os
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

_engine: Optional[Engine] = None
_session_factory: Optional[sessionmaker] = None
_initialized = False
_missing_url_warned = False


def get_database_url() -> str:
    return os.getenv("DATABASE_URL", "").strip()


def get_engine() -> Optional[Engine]:
    global _engine

    database_url = get_database_url()
    if not database_url:
        return None

    if _engine is None:
        _engine = create_engine(
            database_url,
            future=True,
            pool_pre_ping=True,
        )

    return _engine


def get_session_factory() -> Optional[sessionmaker]:
    global _session_factory

    engine = get_engine()
    if engine is None:
        return None

    if _session_factory is None:
        _session_factory = sessionmaker(
            bind=engine,
            autoflush=False,
            autocommit=False,
            future=True,
        )

    return _session_factory


def init_db() -> bool:
    """Create tables if DATABASE_URL is configured."""
    global _initialized, _missing_url_warned

    if _initialized:
        return True

    engine = get_engine()
    if engine is None:
        if not _missing_url_warned:
            logger.warning("DATABASE_URL is not configured; persistence is disabled")
            _missing_url_warned = True
        return False

    from v3.db.models import Base

    Base.metadata.create_all(bind=engine)
    _initialized = True
    return True
