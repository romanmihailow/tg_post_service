"""Configuration loading for the service."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


def resolve_session_path(session: str) -> str:
    """Путь к сессии: если без пути (только имя) — подставляем sessions/ для монтирования в контейнере."""
    if "/" in session or os.sep in session:
        return session
    return f"sessions/{session}"

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _parse_profile_level(value: object) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        numeric = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Profile level must be an integer") from exc
    if numeric < 1 or numeric > 5:
        raise ValueError("Profile level must be between 1 and 5")
    return numeric


def _parse_activity_percent(value: object) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        numeric = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Activity percent must be an integer") from exc
    if numeric < 0 or numeric > 100:
        raise ValueError("Activity percent must be between 0 and 100")
    return numeric


class TelegramCredentials(BaseModel):
    api_id: int
    api_hash: str
    session: str


class OpenAIAccountConfig(BaseModel):
    api_key: Optional[str] = Field(default=None)
    system_prompt_path: Optional[str] = Field(default=None)
    text_model: Optional[str] = Field(default=None)
    vision_model: Optional[str] = Field(default=None)
    image_model: Optional[str] = Field(default=None)
    text_input_price_per_1m: Optional[float] = Field(default=None)
    text_output_price_per_1m: Optional[float] = Field(default=None)
    image_price_1024_usd: Optional[float] = Field(default=None)


class BehaviorProfileConfig(BaseModel):
    simple_profile_level: Optional[int] = Field(default=None)
    group_tempo_level: Optional[int] = Field(default=None)
    group_load_level: Optional[int] = Field(default=None)
    group_safety_level: Optional[int] = Field(default=None)
    group_content_level: Optional[int] = Field(default=None)

    _validate_simple = field_validator("simple_profile_level", mode="before")(
        _parse_profile_level
    )
    _validate_tempo = field_validator("group_tempo_level", mode="before")(
        _parse_profile_level
    )
    _validate_load = field_validator("group_load_level", mode="before")(
        _parse_profile_level
    )
    _validate_safety = field_validator("group_safety_level", mode="before")(
        _parse_profile_level
    )
    _validate_content = field_validator("group_content_level", mode="before")(
        _parse_profile_level
    )


class TelegramAccountConfig(BaseModel):
    name: str
    reader: TelegramCredentials
    writer: Optional[TelegramCredentials] = Field(default=None)
    openai: Optional[OpenAIAccountConfig] = Field(default=None)
    behavior: Optional[BehaviorProfileConfig] = Field(default=None)
    discussion_activity_percent: Optional[int] = Field(default=None)
    user_reply_activity_percent: Optional[int] = Field(default=None)

    _validate_discussion_activity = field_validator(
        "discussion_activity_percent", mode="before"
    )(_parse_activity_percent)
    _validate_user_reply_activity = field_validator(
        "user_reply_activity_percent", mode="before"
    )(_parse_activity_percent)


@dataclass
class BehaviorSettings:
    TELEGRAM_REQUEST_DELAY_SECONDS: float
    RANDOM_JITTER_SECONDS: float
    TELEGRAM_HISTORY_LIMIT: int
    MAX_POSTS_PER_RUN: int
    FLOOD_WAIT_ANTIBLOCK: bool
    FLOOD_WAIT_MAX_SECONDS: int
    SOURCE_SELECTION_MODE: str
    SKIP_POST_PROBABILITY: float


@dataclass
class OpenAISettings:
    api_key: str
    system_prompt_path: str
    text_model: str
    vision_model: str
    image_model: str
    text_input_price_per_1m: float
    text_output_price_per_1m: float
    image_price_1024_usd: float


class PipelineConfig(BaseModel):
    """Configuration for a single posting pipeline."""

    name: str
    account: str = "default"
    destination: str
    sources: List[str]
    mode: str = "TEXT"
    pipeline_type: str = "STANDARD"
    interval_seconds: int = 300
    blackbox_every_n: int = 0
    discussion_target_chat: Optional[str] = Field(default=None)
    discussion_source_pipeline: Optional[str] = Field(default=None)
    discussion_k_min: int = 15
    discussion_k_max: int = 20
    discussion_reply_to_reply_probability: int = 15
    discussion_activity_windows_weekdays_json: Optional[str] = Field(default=None)
    discussion_activity_windows_weekends_json: Optional[str] = Field(default=None)
    discussion_activity_timezone: str = "Asia/Yekaterinburg"  # Ufa, matches user screenshots
    discussion_min_interval_minutes: int = 90
    discussion_max_interval_minutes: int = 180
    discussion_inactivity_pause_minutes: int = 60
    discussion_max_auto_replies_per_chat_per_day: int = 30
    discussion_user_reply_max_age_minutes: int = 30

    @field_validator("mode", mode="before")
    @classmethod
    def normalize_mode(cls, value: object) -> str:
        if not isinstance(value, str):
            raise ValueError("Pipeline mode must be a string")
        normalized = value.strip().upper()
        if normalized not in {"TEXT", "TEXT_IMAGE", "TEXT_MEDIA", "PLAGIAT"}:
            raise ValueError(
                "Pipeline mode must be TEXT, TEXT_IMAGE, TEXT_MEDIA, or PLAGIAT"
            )
        return normalized

    @field_validator("sources", mode="before")
    @classmethod
    def normalize_sources(cls, value: object) -> List[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        raise ValueError("Pipeline sources must be a list")

    @field_validator("account", mode="before")
    @classmethod
    def normalize_account(cls, value: object) -> str:
        if value is None:
            return "default"
        if not isinstance(value, str):
            raise ValueError("Pipeline account must be a string")
        normalized = value.strip()
        if not normalized:
            return "default"
        return normalized

    @field_validator("pipeline_type", mode="before")
    @classmethod
    def normalize_pipeline_type(cls, value: object) -> str:
        if value is None:
            return "STANDARD"
        if not isinstance(value, str):
            raise ValueError("Pipeline type must be a string")
        normalized = value.strip().upper()
        if normalized not in {"STANDARD", "DISCUSSION"}:
            raise ValueError("Pipeline type must be STANDARD or DISCUSSION")
        return normalized

    @model_validator(mode="after")
    def validate_discussion_settings(self) -> "PipelineConfig":
        if self.pipeline_type != "DISCUSSION":
            return self
        if not self.discussion_target_chat:
            self.discussion_target_chat = self.destination
        if not self.discussion_source_pipeline:
            self.discussion_source_pipeline = self.name
        if self.discussion_k_min <= 0 or self.discussion_k_max <= 0:
            raise ValueError("Discussion k_min/k_max must be > 0")
        if self.discussion_k_min > self.discussion_k_max:
            raise ValueError("Discussion k_min must be <= k_max")
        if not (0 <= self.discussion_reply_to_reply_probability <= 100):
            raise ValueError("Discussion reply-to-reply probability must be 0..100")
        if self.discussion_min_interval_minutes <= 0:
            raise ValueError("Discussion min_interval_minutes must be > 0")
        if self.discussion_max_interval_minutes <= 0:
            raise ValueError("Discussion max_interval_minutes must be > 0")
        if self.discussion_min_interval_minutes > self.discussion_max_interval_minutes:
            raise ValueError("Discussion min_interval_minutes must be <= max_interval_minutes")
        if self.discussion_inactivity_pause_minutes < 0:
            raise ValueError("Discussion inactivity_pause_minutes must be >= 0")
        if self.discussion_max_auto_replies_per_chat_per_day < 0:
            raise ValueError(
                "Discussion max_auto_replies_per_chat_per_day must be >= 0"
            )
        if self.discussion_user_reply_max_age_minutes <= 0:
            raise ValueError("Discussion user_reply_max_age_minutes must be > 0")
        return self


class Config(BaseSettings):
    """Application configuration loaded from environment variables and .env."""

    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parents[1] / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",  # IMAGE_REPO и др. переменные только для docker compose
    )

    TELEGRAM_ACCOUNTS_JSON: Optional[str] = Field(default=None)
    TELEGRAM_READER_API_ID: Optional[int] = Field(default=None)
    TELEGRAM_READER_API_HASH: Optional[str] = Field(default=None)
    TELEGRAM_READER_SESSION_NAME: Optional[str] = Field(default=None)
    TELEGRAM_WRITER_API_ID: Optional[int] = Field(default=None)
    TELEGRAM_WRITER_API_HASH: Optional[str] = Field(default=None)
    TELEGRAM_WRITER_SESSION_NAME: Optional[str] = Field(default=None)
    PIPELINES_JSON: Optional[str] = Field(default=None)
    # Override discussion post window for all DISCUSSION pipelines (used by init_db). If set, applied on every startup.
    DISCUSSION_K_MIN: Optional[int] = Field(default=None)
    DISCUSSION_K_MAX: Optional[int] = Field(default=None)
    DEST_CHANNEL: Optional[str] = Field(default=None)
    SOURCE_CHANNELS: List[str] = Field(default_factory=list)
    OPENAI_API_KEY: Optional[str] = Field(default=None)
    TG_BOT_TOKEN: Optional[str] = Field(default=None)
    TG_BOT_ADMINS_JSON: Optional[str] = Field(default=None)
    OPENAI_SYSTEM_PROMPT_PATH: str = Field(default="openai_system_prompt.txt")
    # Optional: path to system prompt for Pipeline 2 (live chat replies). If set and file exists, used instead of OPENAI_SYSTEM_PROMPT_PATH for generate_user_reply.
    OPENAI_SYSTEM_PROMPT_CHAT_PATH: Optional[str] = Field(default=None)
    OPENAI_TEXT_MODEL: str = Field(default="gpt-4.1-mini")
    OPENAI_VISION_MODEL: str = Field(default="gpt-4.1-mini")
    OPENAI_IMAGE_MODEL: str = Field(default="gpt-image-1")
    OPENAI_TEXT_INPUT_PRICE_PER_1M: float = Field(default=0.15)
    OPENAI_TEXT_OUTPUT_PRICE_PER_1M: float = Field(default=0.60)
    OPENAI_IMAGE_PRICE_1024_USD: float = Field(default=0.042)
    POSTING_MODE: Optional[str] = Field(default="TEXT")
    POSTING_INTERVAL_SECONDS: int = Field(default=300)
    MIN_TEXT_LENGTH: int = Field(default=100)
    MAX_POSTS_PER_RUN: int = Field(default=1)
    TELEGRAM_REQUEST_DELAY_SECONDS: float = Field(default=1.0)
    RANDOM_JITTER_SECONDS: float = Field(default=0.0)
    TELEGRAM_HISTORY_LIMIT: int = Field(default=30)
    FLOOD_WAIT_ANTIBLOCK: bool = Field(default=True)
    FLOOD_WAIT_MAX_SECONDS: int = Field(default=300)
    BLACKBOX_EVERY_N_POSTS: int = Field(default=0)
    BLACKBOX_WORD_RATIO: float = Field(default=0.10)
    BLACKBOX_MIN_WORD_LEN: int = Field(default=6)
    BLACKBOX_DISTORT_MIN: int = Field(default=2)
    BLACKBOX_DISTORT_MAX: int = Field(default=4)
    DEDUP_ENABLED: bool = Field(default=True)
    DEDUP_WINDOW_SIZE: int = Field(default=30)
    DEDUP_BM25_THRESHOLD: float = Field(
        default=8.5,
        description="BM25 similarity threshold: posts with score >= threshold are filtered as duplicates. "
        "7.0 was too strict for sports/news (shared vocabulary). 8.5 filters only near-duplicates.",
    )

    # Pipeline 1 Discussion anti-repeat (fingerprint ring buffer)
    DISCUSSION_FINGERPRINT_RING_SIZE: int = Field(default=10)
    AD_FILTER_ENABLED: bool = Field(default=False)
    AD_FILTER_THRESHOLD: int = Field(default=3)
    AD_FILTER_KEYWORDS: Optional[str] = Field(default=None)
    SERVICE_SLEEP_MIN_SECONDS: float = Field(default=30.0)
    SERVICE_SLEEP_MAX_SECONDS: float = Field(default=90.0)
    SOURCE_SELECTION_MODE: str = Field(default="ROUND_ROBIN")
    SKIP_POST_PROBABILITY: float = Field(default=0.0)
    SIMPLE_PROFILE_LEVEL: int = Field(default=5)
    GROUP_TEMPO_LEVEL: Optional[int] = Field(default=None)
    GROUP_LOAD_LEVEL: Optional[int] = Field(default=None)
    GROUP_SAFETY_LEVEL: Optional[int] = Field(default=None)
    GROUP_CONTENT_LEVEL: Optional[int] = Field(default=None)

    # Pipeline 1 reactions (channel news posts)
    REACTIONS_ENABLED: bool = Field(default=False)
    REACTION_PROBABILITY: float = Field(default=0.35)
    REACTION_DAILY_LIMIT_PER_BOT: int = Field(default=10)
    REACTION_COOLDOWN_MINUTES: int = Field(default=30)
    REACTION_EMOJIS: str = Field(default='["👍","🔥","🤔"]')
    REACTION_MAX_REACTIONS_PER_POST_PER_DAY: int = Field(default=1)
    REACTION_USE_ALLOWED_FROM_TELEGRAM: bool = Field(default=True)
    REACTION_ALLOWED_SAMPLE_LIMIT: int = Field(default=80)
    REACTION_MIN_BOTS_PER_POST: int = Field(default=1)

    # Pipeline 2 chat reactions (on user messages we reply to)
    CHAT_REACTIONS_ENABLED: bool = Field(default=False)
    CHAT_REACTION_PROBABILITY: float = Field(default=0.15)
    CHAT_REACTION_DAILY_LIMIT_PER_BOT: int = Field(default=20)
    CHAT_REACTION_COOLDOWN_MINUTES: int = Field(default=10)
    CHAT_REACTION_EMOJIS: str = Field(default='["👍","🤔","😂","🔥"]')
    CHAT_REACTION_ON_USER_MESSAGE: bool = Field(default=True)
    CHAT_REACTION_ON_BOT_MESSAGE: bool = Field(default=False)
    # Pipeline 2: model-driven reaction (OpenAI returns reply_text + reaction_emoji in one call)
    CHAT_REACTIONS_MODEL_DRIVEN: bool = Field(default=False)
    CHAT_REACTIONS_MODEL_NULL_RATE: float = Field(default=0.65)  # target share of null (no reaction)

    # Admin reaction on channel post when Pipeline 1 publishes question to chat
    ADMIN_REACTIONS_ENABLED: bool = Field(default=False)
    ADMIN_REACTION_ACCOUNT_NAME: Optional[str] = Field(default=None)
    ADMIN_REACTION_EMOJI: str = Field(default="👀")
    ADMIN_REACTION_FALLBACK_EMOJI: str = Field(default="👍")
    ADMIN_REACTION_SKIP_IF_UNAVAILABLE: bool = Field(default=False)

    @field_validator("REACTION_PROBABILITY", mode="before")
    @classmethod
    def validate_reaction_probability(cls, value: object) -> float:
        if value is None or value == "":
            return 0.35
        try:
            numeric = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("REACTION_PROBABILITY must be a number") from exc
        if numeric < 0 or numeric > 1.0:
            raise ValueError("REACTION_PROBABILITY must be between 0 and 1")
        return numeric

    @field_validator("REACTION_EMOJIS", mode="before")
    @classmethod
    def parse_reaction_emojis(cls, value: object) -> str:
        if value is None or value == "":
            return '["👍","🔥","🤔"]'
        return value

    def reaction_emojis_list(self) -> List[str]:
        """Parse REACTION_EMOJIS JSON to list. Fallback to default if invalid."""
        raw = (self.REACTION_EMOJIS or "").strip()
        if not raw:
            return ["👍", "🔥", "🤔"]
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(e).strip() for e in parsed if str(e).strip()]
        except json.JSONDecodeError:
            pass
        return ["👍", "🔥", "🤔"]

    def chat_reaction_emojis_list(self) -> List[str]:
        """Parse CHAT_REACTION_EMOJIS JSON to list. Fallback if invalid."""
        raw = (getattr(self, "CHAT_REACTION_EMOJIS", None) or "").strip()
        if not raw:
            return ["👍", "🤔", "😂", "🔥"]
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(e).strip() for e in parsed if str(e).strip()]
        except json.JSONDecodeError:
            pass
        return ["👍", "🤔", "😂", "🔥"]

    @property
    def pipelines(self) -> List[PipelineConfig]:
        """Return configured pipelines from PIPELINES_JSON or fallback settings."""
        raw = (self.PIPELINES_JSON or "").strip()
        if raw:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError("PIPELINES_JSON contains invalid JSON") from exc
            if isinstance(data, dict):
                data = [data]
            if not isinstance(data, list):
                raise ValueError("PIPELINES_JSON must be a list of pipeline objects")
            result = [PipelineConfig(**item) for item in data]
            # Env override: DISCUSSION_K_MIN/K_MAX apply to all DISCUSSION pipelines on init (CI/CD-friendly).
            if self.DISCUSSION_K_MIN is not None or self.DISCUSSION_K_MAX is not None:
                for i, p in enumerate(result):
                    if p.pipeline_type == "DISCUSSION":
                        kw = {}
                        if self.DISCUSSION_K_MIN is not None:
                            kw["discussion_k_min"] = self.DISCUSSION_K_MIN
                        if self.DISCUSSION_K_MAX is not None:
                            kw["discussion_k_max"] = self.DISCUSSION_K_MAX
                        if kw:
                            result[i] = p.model_copy(update=kw)
            return result
        if self.DEST_CHANNEL and self.SOURCE_CHANNELS:
            return [
                PipelineConfig(
                    name="default",
                    account="default",
                    destination=self.DEST_CHANNEL,
                    sources=self.SOURCE_CHANNELS,
                    mode=self.POSTING_MODE or "TEXT",
                    interval_seconds=self.POSTING_INTERVAL_SECONDS,
                    blackbox_every_n=self.BLACKBOX_EVERY_N_POSTS,
                )
            ]
        return []

    @property
    def bot_admin_ids(self) -> List[int]:
        return list(self.bot_admins().keys())

    def admin_role(self, user_id: int | None) -> str | None:
        if user_id is None:
            return None
        info = self.bot_admins().get(user_id)
        if not info:
            return None
        return str(info.get("role", "owner"))

    def admin_accounts(self, user_id: int | None) -> list[str]:
        if user_id is None:
            return []
        info = self.bot_admins().get(user_id)
        if not info:
            return []
        accounts = info.get("accounts", ["*"])
        if isinstance(accounts, list):
            return [str(item) for item in accounts]
        return [str(accounts)]

    def bot_admins(self) -> dict[int, dict[str, object]]:
        raw = (self.TG_BOT_ADMINS_JSON or "").strip()
        if raw.startswith("'") and raw.endswith("'"):
            raw = raw[1:-1].strip()
        if raw.startswith('"') and raw.endswith('"'):
            raw = raw[1:-1].strip()
        if not raw:
            return {}
        if raw.startswith("["):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError("TG_BOT_ADMINS_JSON contains invalid JSON") from exc
            if not isinstance(parsed, list):
                raise ValueError("TG_BOT_ADMINS_JSON must be a list")
            admins: dict[int, dict[str, object]] = {}
            for item in parsed:
                if isinstance(item, dict):
                    admin_id = int(item.get("id"))
                    role = str(item.get("role", "owner")).lower()
                    accounts = item.get("accounts", ["*"])
                    admins[admin_id] = {"role": role, "accounts": accounts}
                else:
                    admins[int(item)] = {"role": "owner", "accounts": ["*"]}
            return admins
        return {
            int(item.strip()): {"role": "owner", "accounts": ["*"]}
            for item in raw.split(",")
            if item.strip()
        }

    @field_validator("SOURCE_CHANNELS", mode="before")
    @classmethod
    def parse_source_channels(cls, value: object) -> List[str]:
        """Parse source channels from JSON array or comma-separated string."""
        if value is None:
            return []
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
        if value is None:
            return "TEXT"
        if not isinstance(value, str):
            raise ValueError("POSTING_MODE must be a string")
        normalized = value.strip().upper()
        if normalized not in {"TEXT", "TEXT_IMAGE", "TEXT_MEDIA", "PLAGIAT"}:
            raise ValueError(
                "POSTING_MODE must be TEXT, TEXT_IMAGE, TEXT_MEDIA, or PLAGIAT"
            )
        return normalized

    @field_validator("SOURCE_SELECTION_MODE", mode="before")
    @classmethod
    def normalize_source_selection_mode(cls, value: object) -> str:
        if value is None:
            return "ROUND_ROBIN"
        if not isinstance(value, str):
            raise ValueError("SOURCE_SELECTION_MODE must be a string")
        normalized = value.strip().upper()
        if normalized not in {"ROUND_ROBIN", "RANDOM"}:
            raise ValueError("SOURCE_SELECTION_MODE must be ROUND_ROBIN or RANDOM")
        return normalized

    @field_validator("SKIP_POST_PROBABILITY", mode="before")
    @classmethod
    def validate_skip_post_probability(cls, value: object) -> float:
        if value is None or value == "":
            return 0.0
        try:
            numeric = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("SKIP_POST_PROBABILITY must be a number") from exc
        if numeric < 0 or numeric > 0.5:
            raise ValueError("SKIP_POST_PROBABILITY must be between 0 and 0.5")
        return numeric

    @field_validator("RANDOM_JITTER_SECONDS", mode="before")
    @classmethod
    def validate_random_jitter_seconds(cls, value: object) -> float:
        if value is None or value == "":
            return 0.0
        try:
            numeric = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("RANDOM_JITTER_SECONDS must be a number") from exc
        if numeric < 0:
            raise ValueError("RANDOM_JITTER_SECONDS must be >= 0")
        return numeric

    @field_validator(
        "TELEGRAM_WRITER_API_ID",
        "TELEGRAM_WRITER_API_HASH",
        "TELEGRAM_WRITER_SESSION_NAME",
        mode="before",
    )
    @classmethod
    def normalize_optional_writer_fields(cls, value: object) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator(
        "SIMPLE_PROFILE_LEVEL",
        "GROUP_TEMPO_LEVEL",
        "GROUP_LOAD_LEVEL",
        "GROUP_SAFETY_LEVEL",
        "GROUP_CONTENT_LEVEL",
        mode="before",
    )
    @classmethod
    def validate_profile_level(cls, value: object) -> Optional[int]:
        return _parse_profile_level(value)

    @model_validator(mode="after")
    def validate_account_credentials(self) -> "Config":
        if self.TELEGRAM_ACCOUNTS_JSON:
            return self
        missing = [
            name
            for name, value in (
                ("TELEGRAM_READER_API_ID", self.TELEGRAM_READER_API_ID),
                ("TELEGRAM_READER_API_HASH", self.TELEGRAM_READER_API_HASH),
                ("TELEGRAM_READER_SESSION_NAME", self.TELEGRAM_READER_SESSION_NAME),
            )
            if value in (None, "")
        ]
        if missing:
            raise ValueError(
                "Missing required Telegram reader settings: " + ", ".join(missing)
            )
        if not self.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY is required when no accounts are defined")
        return self

    def telegram_accounts(self) -> List[TelegramAccountConfig]:
        raw = (self.TELEGRAM_ACCOUNTS_JSON or "").strip()
        if raw:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError("TELEGRAM_ACCOUNTS_JSON contains invalid JSON") from exc
            if isinstance(data, dict):
                data = [data]
            if not isinstance(data, list):
                raise ValueError("TELEGRAM_ACCOUNTS_JSON must be a list of accounts")
            accounts = [TelegramAccountConfig(**item) for item in data]
            names = [item.name for item in accounts]
            if len(set(names)) != len(names):
                raise ValueError("TELEGRAM_ACCOUNTS_JSON contains duplicate account names")
            return accounts
        return [self._build_default_account()]

    def resolve_openai_settings(
        self, account_openai: Optional[OpenAIAccountConfig]
    ) -> OpenAISettings:
        api_key = account_openai.api_key if account_openai else None
        if not api_key:
            api_key = self.OPENAI_API_KEY
        if not api_key:
            raise ValueError("OpenAI api_key is required for each account")
        system_prompt_path = (
            account_openai.system_prompt_path
            if account_openai and account_openai.system_prompt_path
            else self.OPENAI_SYSTEM_PROMPT_PATH
        )
        return OpenAISettings(
            api_key=api_key,
            system_prompt_path=system_prompt_path,
            text_model=(
                account_openai.text_model
                if account_openai and account_openai.text_model
                else self.OPENAI_TEXT_MODEL
            ),
            vision_model=(
                account_openai.vision_model
                if account_openai and account_openai.vision_model
                else self.OPENAI_VISION_MODEL
            ),
            image_model=(
                account_openai.image_model
                if account_openai and account_openai.image_model
                else self.OPENAI_IMAGE_MODEL
            ),
            text_input_price_per_1m=(
                account_openai.text_input_price_per_1m
                if account_openai and account_openai.text_input_price_per_1m is not None
                else self.OPENAI_TEXT_INPUT_PRICE_PER_1M
            ),
            text_output_price_per_1m=(
                account_openai.text_output_price_per_1m
                if account_openai and account_openai.text_output_price_per_1m is not None
                else self.OPENAI_TEXT_OUTPUT_PRICE_PER_1M
            ),
            image_price_1024_usd=(
                account_openai.image_price_1024_usd
                if account_openai and account_openai.image_price_1024_usd is not None
                else self.OPENAI_IMAGE_PRICE_1024_USD
            ),
        )

    def resolve_behavior_settings(
        self, behavior_override: Optional[BehaviorProfileConfig]
    ) -> BehaviorSettings:
        settings = BehaviorSettings(
            TELEGRAM_REQUEST_DELAY_SECONDS=self.TELEGRAM_REQUEST_DELAY_SECONDS,
            RANDOM_JITTER_SECONDS=self.RANDOM_JITTER_SECONDS,
            TELEGRAM_HISTORY_LIMIT=self.TELEGRAM_HISTORY_LIMIT,
            MAX_POSTS_PER_RUN=self.MAX_POSTS_PER_RUN,
            FLOOD_WAIT_ANTIBLOCK=self.FLOOD_WAIT_ANTIBLOCK,
            FLOOD_WAIT_MAX_SECONDS=self.FLOOD_WAIT_MAX_SECONDS,
            SOURCE_SELECTION_MODE=self.SOURCE_SELECTION_MODE,
            SKIP_POST_PROBABILITY=self.SKIP_POST_PROBABILITY,
        )
        if not behavior_override:
            return settings
        if behavior_override.simple_profile_level:
            _apply_profile(settings, _PROFILE_LEVELS[behavior_override.simple_profile_level])
        if behavior_override.group_tempo_level:
            _apply_profile(settings, _TEMPO_LEVELS[behavior_override.group_tempo_level])
        if behavior_override.group_load_level:
            _apply_profile(settings, _LOAD_LEVELS[behavior_override.group_load_level])
        if behavior_override.group_safety_level:
            _apply_profile(settings, _SAFETY_LEVELS[behavior_override.group_safety_level])
        if behavior_override.group_content_level:
            _apply_profile(settings, _CONTENT_LEVELS[behavior_override.group_content_level])
        return settings

    def _build_default_account(self) -> TelegramAccountConfig:
        if not self.TELEGRAM_READER_API_ID or not self.TELEGRAM_READER_API_HASH:
            raise ValueError("Default account requires TELEGRAM_READER_API_ID/HASH")
        reader = TelegramCredentials(
            api_id=self.TELEGRAM_READER_API_ID,
            api_hash=self.TELEGRAM_READER_API_HASH,
            session=self.TELEGRAM_READER_SESSION_NAME or "tg_post_service",
        )
        writer = None
        if self.TELEGRAM_WRITER_API_ID and self.TELEGRAM_WRITER_API_HASH:
            writer = TelegramCredentials(
                api_id=self.TELEGRAM_WRITER_API_ID,
                api_hash=self.TELEGRAM_WRITER_API_HASH,
                session=self.TELEGRAM_WRITER_SESSION_NAME
                or f"{reader.session}_writer",
            )
        behavior = BehaviorProfileConfig(
            simple_profile_level=self.SIMPLE_PROFILE_LEVEL,
            group_tempo_level=self.GROUP_TEMPO_LEVEL,
            group_load_level=self.GROUP_LOAD_LEVEL,
            group_safety_level=self.GROUP_SAFETY_LEVEL,
            group_content_level=self.GROUP_CONTENT_LEVEL,
        )
        return TelegramAccountConfig(
            name="default",
            reader=reader,
            writer=writer,
            openai=None,
            behavior=behavior,
        )

    def apply_behavior_profiles(self) -> None:
        profile_level = self.SIMPLE_PROFILE_LEVEL or 3
        _apply_profile(self, _PROFILE_LEVELS[profile_level])
        if self.GROUP_TEMPO_LEVEL:
            _apply_profile(self, _TEMPO_LEVELS[self.GROUP_TEMPO_LEVEL])
        if self.GROUP_LOAD_LEVEL:
            _apply_profile(self, _LOAD_LEVELS[self.GROUP_LOAD_LEVEL])
        if self.GROUP_SAFETY_LEVEL:
            _apply_profile(self, _SAFETY_LEVELS[self.GROUP_SAFETY_LEVEL])
        if self.GROUP_CONTENT_LEVEL:
            _apply_profile(self, _CONTENT_LEVELS[self.GROUP_CONTENT_LEVEL])


def _apply_profile(target: object, updates: dict[str, object]) -> None:
    for key, value in updates.items():
        setattr(target, key, value)


_TEMPO_LEVELS = {
    1: {"TELEGRAM_REQUEST_DELAY_SECONDS": 0.5, "RANDOM_JITTER_SECONDS": 0.0},
    2: {"TELEGRAM_REQUEST_DELAY_SECONDS": 1.0, "RANDOM_JITTER_SECONDS": 1.0},
    3: {"TELEGRAM_REQUEST_DELAY_SECONDS": 2.0, "RANDOM_JITTER_SECONDS": 2.0},
    4: {"TELEGRAM_REQUEST_DELAY_SECONDS": 4.0, "RANDOM_JITTER_SECONDS": 4.0},
    5: {"TELEGRAM_REQUEST_DELAY_SECONDS": 8.0, "RANDOM_JITTER_SECONDS": 6.0},
}
_LOAD_LEVELS = {
    1: {"TELEGRAM_HISTORY_LIMIT": 30, "MAX_POSTS_PER_RUN": 3},
    2: {"TELEGRAM_HISTORY_LIMIT": 20, "MAX_POSTS_PER_RUN": 2},
    3: {"TELEGRAM_HISTORY_LIMIT": 10, "MAX_POSTS_PER_RUN": 1},
    4: {"TELEGRAM_HISTORY_LIMIT": 10, "MAX_POSTS_PER_RUN": 1},
    5: {"TELEGRAM_HISTORY_LIMIT": 5, "MAX_POSTS_PER_RUN": 1},
}
_SAFETY_LEVELS = {
    1: {"FLOOD_WAIT_ANTIBLOCK": True, "FLOOD_WAIT_MAX_SECONDS": 300},
    2: {"FLOOD_WAIT_ANTIBLOCK": True, "FLOOD_WAIT_MAX_SECONDS": 600},
    3: {"FLOOD_WAIT_ANTIBLOCK": True, "FLOOD_WAIT_MAX_SECONDS": 900},
    4: {"FLOOD_WAIT_ANTIBLOCK": True, "FLOOD_WAIT_MAX_SECONDS": 1200},
    5: {"FLOOD_WAIT_ANTIBLOCK": True, "FLOOD_WAIT_MAX_SECONDS": 1800},
}
_CONTENT_LEVELS = {
    1: {"SOURCE_SELECTION_MODE": "ROUND_ROBIN", "SKIP_POST_PROBABILITY": 0.0},
    2: {"SOURCE_SELECTION_MODE": "ROUND_ROBIN", "SKIP_POST_PROBABILITY": 0.05},
    3: {"SOURCE_SELECTION_MODE": "ROUND_ROBIN", "SKIP_POST_PROBABILITY": 0.10},
    4: {"SOURCE_SELECTION_MODE": "RANDOM", "SKIP_POST_PROBABILITY": 0.20},
    5: {"SOURCE_SELECTION_MODE": "RANDOM", "SKIP_POST_PROBABILITY": 0.30},
}
_PROFILE_LEVELS = {
    level: {
        **_TEMPO_LEVELS[level],
        **_LOAD_LEVELS[level],
        **_SAFETY_LEVELS[level],
        **_CONTENT_LEVELS[level],
    }
    for level in range(1, 6)
}
