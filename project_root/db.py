"""Database initialization and session management."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator, Iterable

from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from project_root.config import Config
from project_root.models import Base, GlobalState, SourceChannel


def create_db_engine() -> Engine:
    """Create a SQLite engine."""
    return create_engine(
        "sqlite:///./tg_post_service.db",
        connect_args={"check_same_thread": False},
        future=True,
    )


ENGINE = create_db_engine()
SessionLocal = sessionmaker(bind=ENGINE, expire_on_commit=False, class_=Session)


def init_db(config: Config) -> None:
    """Create tables and seed initial data based on configuration."""
    Base.metadata.create_all(bind=ENGINE)
    _ensure_global_state_schema()
    with SessionLocal() as session:
        _ensure_source_channels(session, config.SOURCE_CHANNELS)
        _ensure_global_state(session)
        session.commit()


def _ensure_source_channels(session: Session, channels: Iterable[str]) -> None:
    for identifier in channels:
        exists = session.execute(
            select(SourceChannel).where(SourceChannel.channel_identifier == identifier)
        ).scalar_one_or_none()
        if exists is None:
            session.add(SourceChannel(channel_identifier=identifier))


def _ensure_global_state(session: Session) -> None:
    state = session.get(GlobalState, 1)
    if state is None:
        session.add(GlobalState(id=1, current_channel_index=0, post_counter=0))


def _ensure_global_state_schema() -> None:
    with ENGINE.connect() as connection:
        result = connection.execute(text("PRAGMA table_info(global_state)"))
        columns = {row[1] for row in result.fetchall()}
        if "post_counter" not in columns:
            connection.execute(
                text("ALTER TABLE global_state ADD COLUMN post_counter INTEGER DEFAULT 0")
            )
            connection.commit()


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
