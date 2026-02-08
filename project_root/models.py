"""SQLAlchemy ORM models."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


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


class Pipeline(Base):
    """Represents a posting pipeline configuration."""

    __tablename__ = "pipelines"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    account_name: Mapped[str] = mapped_column(
        String, nullable=False, default="default"
    )
    is_enabled: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    destination_channel: Mapped[str] = mapped_column(String, nullable=False)
    posting_mode: Mapped[str] = mapped_column(String, nullable=False)
    pipeline_type: Mapped[str] = mapped_column(String, nullable=False, default="STANDARD")
    interval_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    blackbox_every_n: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    sources: Mapped[list["PipelineSource"]] = relationship(
        "PipelineSource", back_populates="pipeline", cascade="all, delete-orphan"
    )
    state: Mapped[Optional["PipelineState"]] = relationship(
        "PipelineState", back_populates="pipeline", uselist=False, cascade="all, delete-orphan"
    )
    discussion_settings: Mapped[Optional["DiscussionSettings"]] = relationship(
        "DiscussionSettings",
        back_populates="pipeline",
        uselist=False,
        cascade="all, delete-orphan",
    )


class PipelineSource(Base):
    """Links a pipeline to its source channels."""

    __tablename__ = "pipeline_sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_id: Mapped[int] = mapped_column(ForeignKey("pipelines.id"), nullable=False)
    source_channel: Mapped[str] = mapped_column(String, nullable=False)
    last_message_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    pipeline: Mapped["Pipeline"] = relationship("Pipeline", back_populates="sources")


class PipelineState(Base):
    """Stores per-pipeline runtime state."""

    __tablename__ = "pipeline_state"

    pipeline_id: Mapped[int] = mapped_column(
        ForeignKey("pipelines.id"), primary_key=True
    )
    current_source_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_posts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    pipeline: Mapped["Pipeline"] = relationship("Pipeline", back_populates="state")


class PostHistory(Base):
    """Stores recent original texts for deduplication."""

    __tablename__ = "post_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_id: Mapped[int] = mapped_column(ForeignKey("pipelines.id"), nullable=False)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    pipeline: Mapped["Pipeline"] = relationship("Pipeline")


class BotInvite(Base):
    """Stores admin-created invite tokens for onboarding."""

    __tablename__ = "bot_invites"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    token: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    created_by: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    used_by: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    used_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class BotInviteCode(Base):
    """Stores one-time codes for invite confirmation."""

    __tablename__ = "bot_invite_codes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    token: Mapped[str] = mapped_column(String, nullable=False, index=True)
    code: Mapped[str] = mapped_column(String, nullable=False)
    created_for: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    used_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class DiscussionSettings(Base):
    """Configuration for discussion pipeline."""

    __tablename__ = "discussion_settings"

    pipeline_id: Mapped[int] = mapped_column(
        ForeignKey("pipelines.id"), primary_key=True
    )
    target_chat: Mapped[str] = mapped_column(String, nullable=False)
    source_pipeline_name: Mapped[str] = mapped_column(String, nullable=False)
    k_min: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    k_max: Mapped[int] = mapped_column(Integer, nullable=False, default=8)
    reply_to_reply_probability: Mapped[int] = mapped_column(
        Integer, nullable=False, default=15
    )
    activity_windows_weekdays_json: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )
    activity_windows_weekends_json: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )
    activity_timezone: Mapped[str] = mapped_column(
        String, nullable=False, default="Europe/Moscow"
    )
    min_interval_minutes: Mapped[int] = mapped_column(
        Integer, nullable=False, default=90
    )
    max_interval_minutes: Mapped[int] = mapped_column(
        Integer, nullable=False, default=180
    )
    inactivity_pause_minutes: Mapped[int] = mapped_column(
        Integer, nullable=False, default=60
    )
    max_auto_replies_per_chat_per_day: Mapped[int] = mapped_column(
        Integer, nullable=False, default=30
    )
    user_reply_max_age_minutes: Mapped[int] = mapped_column(
        Integer, nullable=False, default=30
    )

    pipeline: Mapped["Pipeline"] = relationship("Pipeline", back_populates="discussion_settings")


class DiscussionState(Base):
    """Runtime state for discussion pipeline."""

    __tablename__ = "discussion_state"

    pipeline_id: Mapped[int] = mapped_column(
        ForeignKey("pipelines.id"), primary_key=True
    )
    question_message_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    question_created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    replies_planned: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    replies_sent: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_bot_reply_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_reply_parent_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_bot_reply_message_id: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )
    next_due_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class DiscussionReply(Base):
    """Scheduled replies for a discussion."""

    __tablename__ = "discussion_replies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_id: Mapped[int] = mapped_column(ForeignKey("pipelines.id"), nullable=False)
    kind: Mapped[str] = mapped_column(String, nullable=False, default="discussion")
    chat_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    account_name: Mapped[str] = mapped_column(String, nullable=False)
    reply_text: Mapped[str] = mapped_column(Text, nullable=False)
    send_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="pending")
    reply_to_message_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    source_message_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, nullable=True
    )
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    cancelled_reason: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class ChatState(Base):
    """Tracks reading state for discussion chats."""

    __tablename__ = "chat_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_id: Mapped[int] = mapped_column(ForeignKey("pipelines.id"), nullable=False)
    chat_id: Mapped[str] = mapped_column(String, nullable=False)
    last_seen_message_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    last_human_message_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    replies_today: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    replies_today_date: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    next_scan_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class DiscussionBotWeight(Base):
    """Weights and limits for discussion bots."""

    __tablename__ = "discussion_bot_weights"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_id: Mapped[int] = mapped_column(ForeignKey("pipelines.id"), nullable=False)
    account_name: Mapped[str] = mapped_column(String, nullable=False)
    weight: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    daily_limit: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    cooldown_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    used_today: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    used_today_date: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class UserbotPersona(Base):
    """Presentation-only persona for userbot text generation."""

    __tablename__ = "userbot_persona"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    persona_tone: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    persona_verbosity: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    persona_style_hint: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    persona_topics: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    persona_offtopic_tolerance: Mapped[int] = mapped_column(
        Integer, nullable=False, default=50
    )
    persona_topic_priority: Mapped[int] = mapped_column(
        Integer, nullable=False, default=50
    )
