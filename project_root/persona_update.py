"""Safe persona update for a single userbot with backup for rollback."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from project_root.db import get_userbot_persona
from project_root.models import UserbotPersona

logger = logging.getLogger(__name__)

TARGET_ACCOUNT = "t9174800805"

NEW_TONE = "skeptical"
NEW_VERBOSITY = "short"
NEW_STYLE_HINT = (
    "Мягко скептический тон. Указывает на вторую сторону вопроса.\n"
    "Допускается ирония и редкий сарказм, без агрессии.\n"
    "Иногда использует черный юмор, но сдержанно.\n"
    "Может уместно цитировать классическую русскую литературу\n"
    "(одна короткая цитата или аллюзия, только если это действительно подходит по смыслу).\n"
    "Не повторяется, не морализирует, не выглядит как заготовка."
)


def _backup_dir() -> str:
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(repo_root, "personas_backup")


def _persona_to_backup_dict(persona: UserbotPersona) -> dict:
    topics = []
    if persona.persona_topics:
        try:
            raw = json.loads(persona.persona_topics)
            if isinstance(raw, list):
                topics = [str(x) for x in raw if x]
        except (TypeError, ValueError):
            pass
    return {
        "account_name": persona.account_name,
        "persona_tone": persona.persona_tone,
        "persona_verbosity": persona.persona_verbosity,
        "persona_style_hint": persona.persona_style_hint,
        "persona_topics": topics,
        "persona_topic_priority": persona.persona_topic_priority,
        "persona_offtopic_tolerance": persona.persona_offtopic_tolerance,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _backup_path_existing(base_name: str) -> str | None:
    d = _backup_dir()
    path = os.path.join(d, base_name)
    return path if os.path.isfile(path) else None


def _write_backup(persona: UserbotPersona) -> str:
    os.makedirs(_backup_dir(), exist_ok=True)
    base_name = f"{TARGET_ACCOUNT}.before.json"
    existing = _backup_path_existing(base_name)
    if existing is not None:
        safe_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        base_name = f"{TARGET_ACCOUNT}.before.{safe_ts}.json"
    path = os.path.join(_backup_dir(), base_name)
    data = _persona_to_backup_dict(persona)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def ensure_persona_t9174800805(session: Session) -> None:
    """
    Idempotent update of persona for t9174800805: backup current state to
    personas_backup/ then set tone, verbosity, style_hint. Other fields unchanged.
    """
    persona = get_userbot_persona(session, TARGET_ACCOUNT)
    if persona is None:
        return

    same = (
        (persona.persona_tone or "").strip() == NEW_TONE
        and (persona.persona_verbosity or "").strip() == NEW_VERBOSITY
        and (persona.persona_style_hint or "").strip() == NEW_STYLE_HINT.strip()
    )
    if same:
        return

    backup_path = _write_backup(persona)
    persona.persona_tone = NEW_TONE
    persona.persona_verbosity = NEW_VERBOSITY
    persona.persona_style_hint = NEW_STYLE_HINT.strip()
    session.flush()
    logger.info(
        "persona updated for account %s (backup saved to %s)",
        TARGET_ACCOUNT,
        backup_path,
    )
