"""Configuration loading for the service."""

from __future__ import annotations

import json
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """Application configuration loaded from environment variables and .env."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    TELEGRAM_API_ID: int
    TELEGRAM_API_HASH: str
    TELEGRAM_SESSION_NAME: str = Field(default="tg_post_service")
    DEST_CHANNEL: str
    SOURCE_CHANNELS: List[str]
    OPENAI_API_KEY: str
    POSTING_MODE: str = Field(default="TEXT")
    POSTING_INTERVAL_SECONDS: int = Field(default=300)
    MIN_TEXT_LENGTH: int = Field(default=100)
    MAX_POSTS_PER_RUN: int = Field(default=1)
    TELEGRAM_REQUEST_DELAY_SECONDS: float = Field(default=1.0)
    TELEGRAM_HISTORY_LIMIT: int = Field(default=30)
    FLOOD_WAIT_ANTIBLOCK: bool = Field(default=True)
    FLOOD_WAIT_MAX_SECONDS: int = Field(default=300)
    BLACKBOX_EVERY_N_POSTS: int = Field(default=0)

    @field_validator("SOURCE_CHANNELS", mode="before")
    @classmethod
    def parse_source_channels(cls, value: object) -> List[str]:
        """Parse source channels from JSON array or comma-separated string."""
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return []
            if raw.startswith("["):
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise ValueError("SOURCE_CHANNELS contains invalid JSON") from exc
                if not isinstance(parsed, list):
                    raise ValueError("SOURCE_CHANNELS JSON must be a list")
                return [str(item).strip() for item in parsed if str(item).strip()]
            return [item.strip() for item in raw.split(",") if item.strip()]
        raise ValueError("SOURCE_CHANNELS must be a list or string")

    @field_validator("POSTING_MODE", mode="before")
    @classmethod
    def normalize_posting_mode(cls, value: object) -> str:
        """Normalize posting mode to an allowed value."""
        if not isinstance(value, str):
            raise ValueError("POSTING_MODE must be a string")
        normalized = value.strip().upper()
        if normalized not in {"TEXT", "TEXT_IMAGE"}:
            raise ValueError("POSTING_MODE must be TEXT or TEXT_IMAGE")
        return normalized
