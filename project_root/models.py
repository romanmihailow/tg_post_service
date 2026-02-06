"""SQLAlchemy ORM models."""

from __future__ import annotations

from typing import Optional

from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for ORM models."""


class SourceChannel(Base):
    """Represents a source Telegram channel and its state."""

    __tablename__ = "source_channels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_identifier: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    last_message_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)


class GlobalState(Base):
    """Stores global state like the current round-robin index."""

    __tablename__ = "global_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    current_channel_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    post_counter: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
