"""Runtime containers for account-specific settings and clients."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from telethon import TelegramClient

from project_root.config import BehaviorSettings, OpenAISettings
from project_root.openai_client import OpenAIClient



@dataclass
class AccountRuntime:
    name: str
    reader_client: TelegramClient
    writer_client: TelegramClient
    openai_client: OpenAIClient
    behavior: BehaviorSettings
    openai_settings: OpenAISettings
    user_id: int | None = None
    username: str | None = None
    flood_wait_until: datetime | None = None
    flood_wait_notified_until: datetime | None = None
    # Pipeline 2 (live replies) uses this if set; otherwise openai_client.system_prompt
    system_prompt_chat: str | None = None
