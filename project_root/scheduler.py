"""Main service loop for reading, paraphrasing, and posting news."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, List

# Candidate post from channel with Telegram message_id (P0: no search by text)
PostCandidate = namedtuple("PostCandidate", ["message_id", "text", "created_at"])

from sqlalchemy import delete, select
from sqlalchemy.orm import Session
from rank_bm25 import BM25Okapi

from project_root.config import Config
from project_root.db import (
    create_discussion_reply,
    get_all_pipelines,
    get_chat_state,
    get_discussion_settings,
    get_discussion_state,
    get_pipeline_by_name,
    get_pipeline_sources,
    get_pipeline_state,
    get_session,
    get_userbot_persona,
    list_discussion_bot_weights,
    list_due_discussion_replies,
    mark_discussion_reply_cancelled,
    mark_discussion_reply_sent,
    upsert_discussion_bot_weight,
)
from project_root.pipeline_status import set_status as _set_pipeline_status
from project_root.topics import extract_topics_for_text
from project_root.models import (
    ChatState,
    DiscussionBotWeight,
    DiscussionReply,
    DiscussionSettings,
    DiscussionState,
    Pipeline,
    PipelineSource,
    PipelineState,
    PostHistory,
)
from project_root.runtime import AccountRuntime
from project_root.telegram_client import (
    download_message_photo,
    FloodWaitBlocked,
    get_available_reaction_emojis,
    get_new_messages,
    pick_album_caption_message,
    send_reply_text,
    send_image_with_caption,
    send_media_from_message,
    send_text,
    set_message_reaction,
)

logger = logging.getLogger(__name__)
UFA_TZ = ZoneInfo("Asia/Yekaterinburg")
_DISCUSSION_RECENT_TOPICS_LIMIT = 3
# Обрабатывать не больше N отложенных ответов за цикл, чтобы не блокировать скан чата
_MAX_DUE_USER_REPLIES_PER_CYCLE = 5

# Имя и пол для role_label (Pipeline 2 gender-формы: согласна/согласен). Без миграции БД.
PERSONA_PROFILE_OVERRIDES: dict[str, dict[str, str]] = {
    "t9870202433": {"display_name": "Мария Кузнецова", "gender": "female"},
    "t9876002641": {"display_name": "Виктория Сергеевна", "gender": "female"},
    "t9174800805": {"display_name": "Екатерина Волкова", "gender": "female"},
    "t9174801182": {"display_name": "Анна Романова", "gender": "female"},
    "acc1": {"display_name": "Александр Грушевский", "gender": "male"},
    "t9876001411": {"display_name": "Олег Синица", "gender": "male"},
    "t9174803110": {"display_name": "Дмитрий Орлов", "gender": "male"},
    "t9014429801": {"display_name": "Илья Морозов", "gender": "male"},
    "t9083516765": {"display_name": "Николай Лебедев", "gender": "male"},
}

# In-memory reaction throttling (Pipeline 1 channel). Resets on process restart.
_REACTION_LAST_AT: dict[tuple[str, str], datetime] = {}  # (account_name, chat_id)
_REACTION_TODAY: dict[tuple[str, str, str], int] = {}  # (account_name, chat_id, date_str)
# (chat_id, message_id) -> count: reactions placed on this post today
_REACTION_POST_REACT_COUNT: dict[tuple[str, int], int] = {}
# (chat_id, message_id, account_name) -> date_iso: bot already reacted to this post today
_REACTION_POST_REACTED_BY: dict[tuple[str, int, str], str] = {}
_REACTION_DAY: str | None = None  # YYYY-MM-DD for daily reset

# In-memory chat reaction throttling (Pipeline 2). Resets on process restart.
_CHAT_REACTION_LAST_AT: dict[tuple[str, str], datetime] = {}
_CHAT_REACTION_TODAY: dict[tuple[str, str, str], int] = {}
_CHAT_REACTION_REACTED_TODAY: dict[tuple[str, int], str] = {}
_CHAT_REACTION_DAY: str | None = None


# Keywords that suggest sensitive content (conflict/tragedy) — avoid 🔥 😎 😂
_REACTION_SENSITIVE_KEYWORDS = frozenset(
    [
        "конфликт", "войн", "трагеди", "санкц", "обстрел", "атак", "жертв",
        "погиб", "умер", "смерт", "теракт", "катастроф", "авари", "насили",
    ]
)
# Scandal/exposé/shock — prefer ⚡ 👀 🤔
_REACTION_SCANDAL_KEYWORDS = frozenset(
    [
        "разоблачен", "скандал", "шок", "внезапно", "утечк", "хак", "мошенник",
        "обман", "подделка", "фейк", "расследован",
    ]
)
# Sport/victory — prefer ✅ 🔥 😎
_REACTION_SPORT_KEYWORDS = frozenset(
    [
        "спорт", "победа", "рекорд", "гол", "матч", "чемпион", "турнир",
        "олимпиад", "медал",
    ]
)
# Boring/routine — prefer 🥱
_REACTION_BORING_KEYWORDS = frozenset(
    [
        "скучно", "опять", "рутина", "в сотый раз", "как всегда", "как обычно",
    ]
)

# P1/P2: Gender grammar fix (male/female forms).
from project_root.grammar_fix import fix_gender_grammar


def _update_pipeline_status(
    pipeline: Pipeline,
    *,
    category: str,
    state: str,
    progress_current: int | None = None,
    progress_total: int | None = None,
    next_action_at: datetime | None = None,
    message: str | None = None,
) -> None:
    _set_pipeline_status(
        pipeline_id=pipeline.id,
        pipeline_name=pipeline.name,
        pipeline_type=pipeline.pipeline_type,
        category=category,
        state=state,
        progress_current=progress_current,
        progress_total=progress_total,
        next_action_at=next_action_at,
        message=message,
    )


def _as_utc(dt: datetime | None) -> datetime | None:
    if not dt:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _should_store_post_history(
    session: Session, pipeline: Pipeline, config: Config
) -> bool:
    if config.DEDUP_ENABLED:
        return True
    row = session.execute(
        select(DiscussionSettings.id)
        .where(DiscussionSettings.source_pipeline_name == pipeline.name)
        .limit(1)
    ).scalar_one_or_none()
    return row is not None


def _post_history_is_stale(
    items: list[PostHistory], now: datetime, max_age_hours: int = 6
) -> bool:
    if not items:
        return True
    latest = max((item.created_at for item in items if item.created_at), default=None)
    latest_utc = _as_utc(latest)
    if not latest_utc:
        return True
    return (now - latest_utc).total_seconds() >= max_age_hours * 3600


async def _backfill_post_history_from_channel(
    session: Session,
    client,
    *,
    source_channel: str,
    pipeline_id: int,
    min_text_length: int,
    window_size: int,
) -> int:
    existing_texts = set(
        session.execute(
            select(PostHistory.text)
            .where(PostHistory.pipeline_id == pipeline_id)
            .order_by(PostHistory.id.desc())
            .limit(window_size)
        )
        .scalars()
        .all()
    )
    messages = []
    async for message in client.iter_messages(source_channel, limit=window_size * 2):
        text = (message.message or "").strip()
        if len(text) < min_text_length:
            continue
        if text in existing_texts:
            continue
        messages.append(text)
    if not messages:
        return 0
    # Store oldest first so ordering matches channel recency.
    for text in reversed(messages):
        _store_recent_post(session, pipeline_id, text, window_size)
    return len(messages)


async def fetch_recent_posts_from_channel(
    client,
    channel: str,
    limit: int,
    min_text_length: int,
) -> list[PostCandidate]:
    """Fetch recent posts from channel with message_id (P0: direct id, no search by text)."""
    result: list[PostCandidate] = []
    async for message in client.iter_messages(channel, limit=limit * 2):
        text = (message.message or "").strip()
        if len(text) < min_text_length:
            continue
        created = message.date
        if created and getattr(created, "tzinfo", None) is None:
            created = created.replace(tzinfo=timezone.utc)
        result.append(
            PostCandidate(message_id=message.id, text=text, created_at=created or datetime.now(timezone.utc))
        )
        if len(result) >= limit:
            break
    return result


async def _resolve_post_message_id(client, channel: str, text: str, limit: int = 50) -> int | None:
    """Find message_id in channel by matching text. Returns None if not found."""
    if not text or not text.strip():
        return None
    needle = text.strip()
    async for message in client.iter_messages(channel, limit=limit):
        msg_text = (message.message or "").strip()
        if not msg_text:
            continue
        if msg_text == needle or needle in msg_text or msg_text in needle:
            return message.id
    return None


def _pick_reaction_emoji(text: str, emojis: list[str]) -> tuple[str, bool]:
    """Pick emoji for reaction. Avoid 🔥 for sensitive content. Returns (emoji, sensitive)."""
    emoji, meta = _pick_reaction_emoji_from_candidates(text, emojis)
    return (emoji, meta.get("sensitive", False))


def _pick_reaction_emoji_from_candidates(
    text: str, candidates: list[str]
) -> tuple[str, dict]:
    """Rule-based emoji pick for Pipeline 1. Returns (emoji, meta) with meta={sensitive, rule}."""
    if not candidates:
        return ("👍", {"sensitive": False, "rule": "fallback_empty"})
    norm = (text or "").strip().lower()
    if not norm:
        return (random.choice(candidates), {"sensitive": False, "rule": "random"})
    candidates_set = set(candidates)

    # Sensitive: avoid 🔥 😎 😂, prefer 🤔 👀 ✅
    if any(kw in norm for kw in _REACTION_SENSITIVE_KEYWORDS):
        prefer = [e for e in ["🤔", "👀", "✅"] if e in candidates_set]
        avoid = {"🔥", "😎", "😂"}
        safe = [e for e in candidates if e not in avoid]
        pick = random.choice(prefer) if prefer else (random.choice(safe) if safe else candidates[0])
        return (pick, {"sensitive": True, "rule": "sensitive"})

    # Scandal/exposé: prefer ⚡ 👀 🤔
    if any(kw in norm for kw in _REACTION_SCANDAL_KEYWORDS):
        prefer = [e for e in ["⚡", "👀", "🤔"] if e in candidates_set]
        if prefer:
            return (random.choice(prefer), {"sensitive": False, "rule": "scandal"})

    # Sport/victory: prefer ✅ 🔥 😎
    if any(kw in norm for kw in _REACTION_SPORT_KEYWORDS):
        prefer = [e for e in ["✅", "🔥", "😎"] if e in candidates_set]
        if prefer:
            return (random.choice(prefer), {"sensitive": False, "rule": "sport"})

    # Boring: prefer 🥱
    if any(kw in norm for kw in _REACTION_BORING_KEYWORDS):
        if "🥱" in candidates_set:
            return ("🥱", {"sensitive": False, "rule": "boring"})

    return (random.choice(candidates), {"sensitive": False, "rule": "random"})


def _reaction_ensure_date_reset(now: datetime) -> None:
    """P1: clear daily structures when date changes (no restart)."""
    global _REACTION_DAY
    today = now.strftime("%Y-%m-%d")
    if _REACTION_DAY is not None and _REACTION_DAY != today:
        _REACTION_TODAY.clear()
        _REACTION_POST_REACT_COUNT.clear()
        _REACTION_POST_REACTED_BY.clear()
    _REACTION_DAY = today


def _filter_bots_for_reaction(
    available: list,
    chat_id: str,
    now: datetime,
    cooldown_minutes: int,
    daily_limit: int,
) -> list:
    """Filter bots by reaction-specific cooldown and daily limit (in-memory)."""
    today = now.strftime("%Y-%m-%d")
    result = []
    for item in available:
        account_name = item.account_name
        key_last = (account_name, chat_id)
        key_today = (account_name, chat_id, today)
        last_at = _REACTION_LAST_AT.get(key_last)
        if last_at:
            elapsed = (now - last_at).total_seconds() / 60
            if elapsed < cooldown_minutes:
                continue
        count = _REACTION_TODAY.get(key_today, 0)
        if count >= daily_limit:
            continue
        result.append(item)
    return result


def _update_reaction_state(account_name: str, chat_id: str, now: datetime) -> None:
    """Update in-memory reaction state after successful reaction."""
    key_last = (account_name, chat_id)
    key_today = (account_name, chat_id, now.strftime("%Y-%m-%d"))
    _REACTION_LAST_AT[key_last] = now
    _REACTION_TODAY[key_today] = _REACTION_TODAY.get(key_today, 0) + 1


def _chat_reaction_ensure_date_reset(now: datetime) -> None:
    """Reset chat reaction daily structures when date changes."""
    global _CHAT_REACTION_DAY
    today = now.strftime("%Y-%m-%d")
    if _CHAT_REACTION_DAY is not None and _CHAT_REACTION_DAY != today:
        _CHAT_REACTION_TODAY.clear()
        _CHAT_REACTION_REACTED_TODAY.clear()
    _CHAT_REACTION_DAY = today


def _can_bot_chat_react(
    account_name: str,
    chat_id: str,
    now: datetime,
    cooldown_minutes: int,
    daily_limit: int,
) -> bool:
    """Check if bot can put a chat reaction (cooldown and daily limit)."""
    today = now.strftime("%Y-%m-%d")
    key_last = (account_name, chat_id)
    key_today = (account_name, chat_id, today)
    last_at = _CHAT_REACTION_LAST_AT.get(key_last)
    if last_at:
        elapsed = (now - last_at).total_seconds() / 60
        if elapsed < cooldown_minutes:
            return False
    count = _CHAT_REACTION_TODAY.get(key_today, 0)
    return count < daily_limit


def _update_chat_reaction_state(account_name: str, chat_id: str, msg_id: int, now: datetime) -> None:
    """Update in-memory chat reaction state after successful reaction."""
    today = now.strftime("%Y-%m-%d")
    _CHAT_REACTION_LAST_AT[(account_name, chat_id)] = now
    key_today = (account_name, chat_id, today)
    _CHAT_REACTION_TODAY[key_today] = _CHAT_REACTION_TODAY.get(key_today, 0) + 1
    _CHAT_REACTION_REACTED_TODAY[(chat_id, msg_id)] = today


async def _try_set_reaction_on_chat_message(
    config: Config,
    accounts: dict[str, AccountRuntime],
    account_name: str,
    chat_id: str,
    message_id: int,
    message_text: str,
    now: datetime,
) -> None:
    """Pipeline 2: optionally set reaction on user message we replied to."""
    if not getattr(config, "CHAT_REACTIONS_ENABLED", False):
        return
    if getattr(config, "CHAT_REACTIONS_MODEL_DRIVEN", False):
        return  # reactions set at plan-time by OpenAI
    if not getattr(config, "CHAT_REACTION_ON_USER_MESSAGE", True):
        return
    _chat_reaction_ensure_date_reset(now)
    today = now.strftime("%Y-%m-%d")
    if _CHAT_REACTION_REACTED_TODAY.get((chat_id, message_id)) == today:
        logger.info(
            "chat reaction skipped reason=pipeline2_user_message chat=%s msg_id=%s why=already_reacted_today",
            chat_id,
            message_id,
        )
        return
    if random.random() >= getattr(config, "CHAT_REACTION_PROBABILITY", 0.15):
        logger.info(
            "chat reaction skipped reason=pipeline2_user_message chat=%s msg_id=%s why=probability",
            chat_id,
            message_id,
        )
        return
    cooldown = getattr(config, "CHAT_REACTION_COOLDOWN_MINUTES", 10)
    daily_limit = getattr(config, "CHAT_REACTION_DAILY_LIMIT_PER_BOT", 20)
    if not _can_bot_chat_react(account_name, chat_id, now, cooldown, daily_limit):
        logger.info(
            "chat reaction skipped reason=pipeline2_user_message chat=%s msg_id=%s why=limit",
            chat_id,
            message_id,
        )
        return
    account = accounts.get(account_name)
    if not account or not account.writer_client:
        logger.info(
            "chat reaction skipped reason=pipeline2_user_message chat=%s msg_id=%s bot=%s why=no_permission",
            chat_id,
            message_id,
            account_name,
        )
        return
    emojis = config.chat_reaction_emojis_list()
    emoji, _ = _pick_reaction_emoji(message_text, emojis)
    logger.info(
        "chat reaction attempt reason=pipeline2_user_message chat=%s msg_id=%s bot=%s emoji=%s",
        chat_id,
        message_id,
        account_name,
        emoji,
    )
    ok = await set_message_reaction(account.writer_client, chat_id, message_id, emoji)
    if ok:
        _update_chat_reaction_state(account_name, chat_id, message_id, now)
        logger.info(
            "chat reaction set reason=pipeline2_user_message chat=%s msg_id=%s bot=%s emoji=%s",
            chat_id,
            message_id,
            account_name,
            emoji,
        )
    else:
        logger.warning(
            "chat reaction skipped reason=pipeline2_user_message chat=%s msg_id=%s bot=%s why=api_error",
            chat_id,
            message_id,
            account_name,
        )


async def _try_set_reaction_on_news_post(
    config: Config,
    accounts: dict[str, AccountRuntime],
    primary_account: AccountRuntime,
    pipeline: Pipeline,
    source_channel: str,
    news_text: str,
    available_bots: list,
    selected_bots_for_replies: list,
    now: datetime,
    *,
    selected_post_message_id: int | None = None,
    selected_post_chat_id: str | None = None,
) -> None:
    """Pipeline 1: optionally set reaction(s) on the selected news post. No-op if disabled/skipped."""
    if not getattr(config, "REACTIONS_ENABLED", False):
        return
    _reaction_ensure_date_reset(now)
    chat_id = selected_post_chat_id or source_channel
    msg_id: int | None = selected_post_message_id
    if msg_id is None:
        msg_id = await _resolve_post_message_id(
            primary_account.reader_client, source_channel, news_text
        )
    if msg_id is None:
        logger.info(
            "reaction skipped reason=pipeline1_news_post chat=%s msg_id=%s why=message_id_missing",
            chat_id,
            0,
        )
        return
    today = now.strftime("%Y-%m-%d")
    max_per_post = getattr(config, "REACTION_MAX_REACTIONS_PER_POST_PER_DAY", 1)
    use_allowed = getattr(config, "REACTION_USE_ALLOWED_FROM_TELEGRAM", True)
    sample_limit = getattr(config, "REACTION_ALLOWED_SAMPLE_LIMIT", 80)
    min_bots = getattr(config, "REACTION_MIN_BOTS_PER_POST", 1)
    cooldown = getattr(config, "REACTION_COOLDOWN_MINUTES", 30)
    daily_limit = getattr(config, "REACTION_DAILY_LIMIT_PER_BOT", 10)
    reply_names = {b.account_name for b in selected_bots_for_replies}

    # Get emoji candidates: from Telegram allowed or config fallback
    if use_allowed:
        client = primary_account.reader_client
        allowed = await get_available_reaction_emojis(client, chat_id)
        if allowed:
            candidates = allowed[:sample_limit]
        else:
            candidates = config.reaction_emojis_list()
    else:
        candidates = config.reaction_emojis_list()

    post_count = _REACTION_POST_REACT_COUNT.get((chat_id, msg_id), 0)
    attempts_limit = min(max_per_post - post_count, 3)
    if attempts_limit <= 0:
        logger.info(
            "reaction skipped reason=pipeline1_news_post chat=%s msg_id=%s why=post_daily_cap",
            chat_id,
            msg_id,
        )
        return
    if random.random() >= config.REACTION_PROBABILITY:
        logger.info(
            "reaction skipped reason=pipeline1_news_post chat=%s msg_id=%s why=probability",
            chat_id,
            msg_id,
        )
        return

    for _ in range(attempts_limit):
        post_count = _REACTION_POST_REACT_COUNT.get((chat_id, msg_id), 0)
        if post_count >= max_per_post:
            logger.info(
                "reaction skipped reason=pipeline1_news_post chat=%s msg_id=%s why=post_daily_cap",
                chat_id,
                msg_id,
            )
            break
        reaction_candidates = _filter_bots_for_reaction(
            available_bots, chat_id, now, cooldown, daily_limit
        )
        # Exclude bots that already reacted to this post today
        before_filter = len(reaction_candidates)
        reaction_candidates = [
            b for b in reaction_candidates
            if _REACTION_POST_REACTED_BY.get((chat_id, msg_id, b.account_name)) != today
        ]
        if not reaction_candidates:
            why = "bot_already_reacted" if before_filter > 0 else "limit"
            logger.info(
                "reaction skipped reason=pipeline1_news_post chat=%s msg_id=%s why=%s",
                chat_id,
                msg_id,
                why,
            )
            break
        preferred = [b for b in reaction_candidates if b.account_name not in reply_names]
        bot_row = random.choice(preferred) if preferred else random.choice(reaction_candidates)
        account = accounts.get(bot_row.account_name)
        if not account or not account.writer_client:
            logger.info(
                "reaction skipped reason=pipeline1_news_post chat=%s msg_id=%s bot=%s why=no_permission",
                chat_id,
                msg_id,
                bot_row.account_name,
            )
            continue
        emoji, meta = _pick_reaction_emoji_from_candidates(news_text, candidates)
        rule = meta.get("rule", "random")
        sensitive = meta.get("sensitive", False)
        logger.info(
            "reaction attempt reason=pipeline1_news_post chat=%s msg_id=%s bot=%s emoji=%s rule=%s sensitive=%s candidates_count=%s",
            chat_id,
            msg_id,
            bot_row.account_name,
            emoji,
            rule,
            sensitive,
            len(candidates),
        )
        try:
            ok = await set_message_reaction(
                account.writer_client, chat_id, msg_id, emoji
            )
        except Exception as e:
            err_type = "reactions_not_allowed" if "REACTIONS" in str(e).upper() or "REACTION" in str(e).upper() else "api_error"
            logger.warning(
                "reaction failed reason=pipeline1_news_post chat=%s msg_id=%s bot=%s emoji=%s why=api_error err_type=%s err=%s",
                chat_id,
                msg_id,
                bot_row.account_name,
                emoji,
                err_type,
                e,
            )
            break
        if ok:
            _update_reaction_state(bot_row.account_name, chat_id, now)
            _REACTION_POST_REACT_COUNT[(chat_id, msg_id)] = post_count + 1
            _REACTION_POST_REACTED_BY[(chat_id, msg_id, bot_row.account_name)] = today
            new_count = post_count + 1
            logger.info(
                "reaction set reason=pipeline1_news_post chat=%s msg_id=%s bot=%s emoji=%s post_count=%s/%s",
                chat_id,
                msg_id,
                bot_row.account_name,
                emoji,
                new_count,
                max_per_post,
            )
            if new_count >= min(min_bots, max_per_post):
                break
        else:
            logger.warning(
                "reaction failed reason=pipeline1_news_post chat=%s msg_id=%s bot=%s emoji=%s why=api_error err_type=set_message_reaction_returned_false err=...",
                chat_id,
                msg_id,
                bot_row.account_name,
                emoji,
            )
            break


async def _try_set_admin_reaction_on_source_post(
    config: Config,
    accounts: dict[str, AccountRuntime],
    channel: str,
    message_id: int,
    reason: str,
    now: datetime,
) -> None:
    """Admin account puts reaction (e.g. 👀) on channel post when Pipeline 1 publishes question.
    Uses channel+message_id directly (from source_pipeline.destination_channel and state.last_source_post_id)."""
    if not getattr(config, "ADMIN_REACTIONS_ENABLED", False):
        return
    if not channel or not message_id:
        logger.info(
            "admin_reaction skipped reason=%s why=channel_message_id_missing channel=%s msg_id=%s",
            reason,
            channel or "(empty)",
            message_id,
        )
        return
    account_name = getattr(config, "ADMIN_REACTION_ACCOUNT_NAME", None) or ""
    if not account_name.strip():
        logger.info("admin_reaction skipped reason=%s why=admin_account_missing", reason)
        return
    admin_account = accounts.get(account_name.strip())
    if not admin_account or not admin_account.writer_client:
        logger.warning(
            "admin_reaction skipped reason=%s why=admin_no_client account=%s",
            reason,
            account_name,
        )
        return
    target_emoji = getattr(config, "ADMIN_REACTION_EMOJI", "👀") or "👀"
    fallback_emoji = getattr(config, "ADMIN_REACTION_FALLBACK_EMOJI", "👍") or "👍"
    skip_if_unavailable = getattr(config, "ADMIN_REACTION_SKIP_IF_UNAVAILABLE", False)
    allowed = await get_available_reaction_emojis(admin_account.writer_client, channel)
    allowed_set = set(allowed) if allowed else set()
    emoji: str | None = None
    if allowed:
        if target_emoji in allowed_set:
            emoji = target_emoji
        elif not skip_if_unavailable and fallback_emoji in allowed_set:
            emoji = fallback_emoji
        else:
            logger.info(
                "admin_reaction skipped reason=%s channel=%s msg_id=%s why=emoji_not_allowed target=%s allowed_count=%s",
                reason,
                channel,
                message_id,
                target_emoji,
                len(allowed),
            )
            return
    else:
        if skip_if_unavailable:
            logger.info(
                "admin_reaction skipped reason=%s channel=%s msg_id=%s why=reactions_not_allowed",
                reason,
                channel,
                message_id,
            )
            return
        emoji = fallback_emoji
    if not emoji:
        return
    logger.info(
        "admin_reaction attempt reason=%s channel=%s msg_id=%s emoji=%s allowed_count=%s",
        reason,
        channel,
        message_id,
        emoji,
        len(allowed) if allowed else 0,
    )
    try:
        ok = await set_message_reaction(
            admin_account.writer_client, channel, message_id, emoji
        )
        if ok:
            logger.info(
                "admin_reaction set reason=%s channel=%s msg_id=%s emoji=%s",
                reason,
                channel,
                message_id,
                emoji,
            )
        else:
            logger.warning(
                "admin_reaction failed reason=%s channel=%s msg_id=%s emoji=%s err_type=api_error err=set_message_reaction_returned_false",
                reason,
                channel,
                message_id,
                emoji,
            )
    except Exception as e:
        err_type = "flood_wait" if "FloodWait" in type(e).__name__ else "api_error"
        logger.warning(
            "admin_reaction failed reason=%s channel=%s msg_id=%s emoji=%s err_type=%s err=%s",
            reason,
            channel,
            message_id,
            emoji,
            err_type,
            e,
        )


def normalize_text_for_fingerprint(text: str) -> str:
    """Normalize text for fingerprint: lowercase, no URLs, @user, #tag, digits->0, collapse spaces."""
    if not text or not isinstance(text, str):
        return ""
    s = text.strip().lower()
    s = re.sub(r"https?://\S+", " ", s)
    s = re.sub(r"@\w+", " ", s)
    s = re.sub(r"#\w+", " ", s)
    s = re.sub(r"\d+", "0", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:800] if len(s) > 800 else s


def topic_fingerprint(text: str) -> str:
    """Stable hash of normalized text for anti-repeat."""
    norm = normalize_text_for_fingerprint(text)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]


def _parse_discussion_state_topics_json(raw: str | None) -> tuple[list[str], list[str]]:
    """Parse recent_topics_json. Returns (topics, fingerprints). Backward compat: list -> topics, fingerprints=[]."""
    topics: list[str] = []
    fingerprints: list[str] = []
    s = (raw or "").strip()
    if not s:
        return (topics, fingerprints)
    try:
        data = json.loads(s)
    except json.JSONDecodeError:
        return (topics, fingerprints)
    if isinstance(data, list):
        topics = [str(x).strip().lower() for x in data if str(x).strip()]
        return (topics, fingerprints)
    if isinstance(data, dict):
        raw_topics = data.get("topics")
        if isinstance(raw_topics, list):
            topics = [str(x).strip().lower() for x in raw_topics if str(x).strip()]
        raw_fps = data.get("fingerprints")
        if isinstance(raw_fps, list):
            fingerprints = [str(x).strip() for x in raw_fps if str(x).strip() and len(str(x)) <= 32]
    return (topics, fingerprints)


def _save_discussion_state_topics_json(
    topics: list[str], fingerprints: list[str], topics_limit: int = 3, fp_limit: int = 10
) -> str:
    """Save topics + fingerprints as JSON object."""
    t = [x for x in topics[:topics_limit] if x]
    f = fingerprints[-fp_limit:] if len(fingerprints) > fp_limit else fingerprints
    return json.dumps({"topics": t, "fingerprints": f}, ensure_ascii=False)


def _load_recent_topics(state: DiscussionState) -> list[str]:
    topics, _ = _parse_discussion_state_topics_json(state.recent_topics_json)
    return topics


def _load_recent_fingerprints(state: DiscussionState) -> list[str]:
    _, fps = _parse_discussion_state_topics_json(state.recent_topics_json)
    return fps


def _update_recent_topics(
    state: DiscussionState,
    topics: list[str],
    add_fingerprint: str | None = None,
    fingerprint_ring_size: int = 10,
) -> None:
    normalized = [item.strip().lower() for item in topics if item and item.strip()]
    current_topics, current_fps = _parse_discussion_state_topics_json(state.recent_topics_json)
    for topic in normalized:
        if topic in current_topics:
            current_topics.remove(topic)
        current_topics.insert(0, topic)
    topics_out = current_topics[:_DISCUSSION_RECENT_TOPICS_LIMIT]
    fps_out = list(current_fps)
    if add_fingerprint and add_fingerprint.strip():
        fps_out.append(add_fingerprint.strip())
        fps_out = fps_out[-fingerprint_ring_size:]
    if not normalized and not (add_fingerprint and add_fingerprint.strip()):
        return
    state.recent_topics_json = _save_discussion_state_topics_json(
        topics_out, fps_out, topics_limit=_DISCUSSION_RECENT_TOPICS_LIMIT, fp_limit=fingerprint_ring_size
    )


async def run_service(
    config: Config, accounts: dict[str, AccountRuntime], bot_app=None
) -> None:
    """Run the main loop indefinitely."""
    pipelines: list[Pipeline] = []
    while True:
        try:
            with get_session() as session:
                pipelines = get_all_pipelines(session)
                if not pipelines:
                    logger.warning("No pipelines configured in database")
                now = datetime.utcnow()
                due_pipelines: list[Pipeline] = []
                for pipeline in pipelines:
                    if not pipeline.is_enabled:
                        continue
                    state = get_pipeline_state(session, pipeline.id)
                    if state.last_run_at is None or (
                        now - state.last_run_at
                    ).total_seconds() >= pipeline.interval_seconds:
                        due_pipelines.append(pipeline)

                if due_pipelines:
                    processed = False
                    for pipeline in due_pipelines:
                        account_name = pipeline.account_name or "default"
                        account = accounts.get(account_name)
                        if not account:
                            logger.warning(
                                "Pipeline %s: account %s not configured",
                                pipeline.name,
                                account_name,
                            )
                            continue
                        if _is_account_in_flood_wait(account, now):
                            logger.warning(
                                "Account %s is in flood wait, skipping pipeline %s",
                                account.name,
                                pipeline.name,
                            )
                            continue
                        state = get_pipeline_state(session, pipeline.id)
                        logger.info("Pipeline %s: starting cycle", pipeline.name)
                        if pipeline.pipeline_type == "DISCUSSION":
                            posted = await _process_discussion_pipeline(
                                config,
                                accounts,
                                account,
                                session,
                                pipeline,
                                bot_app,
                            )
                        else:
                            posted = False
                            for _ in range(account.behavior.MAX_POSTS_PER_RUN):
                                posted = await _process_pipeline_once(
                                    config,
                                    account,
                                    session,
                                    pipeline,
                                    state,
                                    bot_app,
                                )
                                if not posted:
                                    break
                        state.last_run_at = now
                        session.commit()
                        processed = True
                        break
                    if not processed:
                        logger.info("No eligible pipelines to process this cycle")
                discussion_pipelines = [
                    item for item in pipelines if item.pipeline_type == "DISCUSSION" and item.is_enabled
                ]
                logger.info(
                    "Discussion pipelines this cycle: %s",
                    [p.name for p in discussion_pipelines],
                )
                for pipeline in discussion_pipelines:
                    account_name = pipeline.account_name or "default"
                    account = accounts.get(account_name)
                    if not account:
                        logger.warning(
                            "Pipeline %s: account %s not in runtime, skipping live replies",
                            pipeline.name,
                            account_name,
                        )
                        _update_pipeline_status(
                            pipeline,
                            category="pipeline2",
                            state="idle",
                            message=f"account {account_name!r} not in runtime",
                        )
                        continue
                    await _process_live_replies_pipeline(
                        config, accounts, account, session, pipeline
                    )
                    session.commit()
        except Exception:
            logger.exception("Unexpected error in main loop")
        sleep_min = config.SERVICE_SLEEP_MIN_SECONDS
        sleep_max = config.SERVICE_SLEEP_MAX_SECONDS
        if any(
            pipeline.pipeline_type == "DISCUSSION" and pipeline.is_enabled
            for pipeline in pipelines
        ):
            sleep_min = min(sleep_min, 30.0)
            sleep_max = min(sleep_max, 60.0)
        await _sleep_between_cycles(sleep_min, sleep_max)


async def _process_pipeline_once(
    config: Config,
    account: AccountRuntime,
    session: Session,
    pipeline: Pipeline,
    state: PipelineState,
    bot_app=None,
) -> bool:
    sources = get_pipeline_sources(session, pipeline.id)
    if not sources:
        logger.warning("No sources configured for pipeline %s", pipeline.name)
        return False

    if account.behavior.SOURCE_SELECTION_MODE == "RANDOM":
        index = random.randrange(len(sources))
    else:
        index = state.current_source_index % len(sources)
    source = sources[index]
    # Even in TEXT_IMAGE mode, we allow text-only posts if the source has no image.
    require_image = False
    logger.info(
        "Pipeline %s selected source %s (index %s)",
        pipeline.name,
        source.source_channel,
        index,
    )

    min_text_length = config.MIN_TEXT_LENGTH
    if pipeline.posting_mode in {"TEXT_MEDIA", "PLAGIAT"}:
        # Allow media posts with empty captions in these modes.
        min_text_length = 0

    try:
        messages = await get_new_messages(
            account.reader_client,
            source_channel=source.source_channel,
            last_message_id=source.last_message_id,
            min_text_length=min_text_length,
            require_image=require_image,
            limit=account.behavior.TELEGRAM_HISTORY_LIMIT,
            request_delay_seconds=account.behavior.TELEGRAM_REQUEST_DELAY_SECONDS,
            random_jitter_seconds=account.behavior.RANDOM_JITTER_SECONDS,
            flood_wait_antiblock=account.behavior.FLOOD_WAIT_ANTIBLOCK,
            flood_wait_max_seconds=account.behavior.FLOOD_WAIT_MAX_SECONDS,
            flood_wait_notify_after_seconds=pipeline.interval_seconds,
        )
    except FloodWaitBlocked as exc:
        await _handle_flood_wait_block(
            config,
            account,
            pipeline,
            exc.seconds,
            bot_app,
        )
        return False

    if not messages:
        logger.info(
            "No new messages in pipeline %s source %s (last_message_id=%s)",
            pipeline.name,
            source.source_channel,
            source.last_message_id,
        )
        state.current_source_index = (index + 1) % len(sources)
        return False

    # Telethon returns newest-to-oldest; we pick the newest for latest-only mode.
    message = messages[0]
    if message.grouped_id:
        message = await pick_album_caption_message(account.reader_client, message)
    original_text = (message.message or "").strip()
    if config.AD_FILTER_ENABLED and original_text:
        if _is_ad_text(
            original_text,
            threshold=config.AD_FILTER_THRESHOLD,
            custom_keywords=config.AD_FILTER_KEYWORDS,
        ):
            logger.info(
                "Skipping ad-like post in pipeline %s source %s",
                pipeline.name,
                source.source_channel,
            )
            source.last_message_id = message.id
            state.current_source_index = (index + 1) % len(sources)
            return False
    if config.DEDUP_ENABLED and original_text:
        is_similar, _ = _is_similar_news_bm25(
            session,
            pipeline.id,
            original_text,
            config.DEDUP_WINDOW_SIZE,
            config.DEDUP_BM25_THRESHOLD,
        )
        if is_similar:
            logger.info(
                "Skipping similar news in pipeline %s source %s",
                pipeline.name,
                source.source_channel,
            )
            source.last_message_id = message.id
            state.current_source_index = (index + 1) % len(sources)
            return False
    if (
        account.behavior.SKIP_POST_PROBABILITY > 0
        and random.random() < account.behavior.SKIP_POST_PROBABILITY
    ):
        logger.info(
            "Skipping post by probability in pipeline %s source %s",
            pipeline.name,
            source.source_channel,
        )
        source.last_message_id = message.id
        state.current_source_index = (index + 1) % len(sources)
        return False
    next_post_counter = state.total_posts + 1
    apply_blackbox = (
        pipeline.blackbox_every_n > 0
        and next_post_counter % pipeline.blackbox_every_n == 0
    )
    try:
        sent_msg = await _post_message(
            config,
            account,
            message,
            apply_blackbox,
            destination_channel=pipeline.destination_channel,
            posting_mode=pipeline.posting_mode,
            flood_wait_notify_after_seconds=pipeline.interval_seconds,
        )
    except FloodWaitBlocked as exc:
        await _handle_flood_wait_block(
            config,
            account,
            pipeline,
            exc.seconds,
            bot_app,
        )
        return False
    except Exception:
        logger.exception(
            "Failed to process or send message for pipeline %s source %s",
            pipeline.name,
            source.source_channel,
        )
        session.rollback()
        return False

    source.last_message_id = message.id
    channel_message_id = getattr(sent_msg, "id", None) if sent_msg else None
    destination_channel = pipeline.destination_channel
    logger.info(
        "Posted to pipeline=%s destination=%s source=%s msg_id=%s",
        pipeline.name,
        destination_channel,
        source.source_channel,
        message.id,
    )
    if channel_message_id is None and original_text:
        logger.warning(
            "post_history channel_message_id missing pipeline=%s destination=%s (sent_msg not available)",
            pipeline.name,
            destination_channel,
        )
    if original_text and _should_store_post_history(session, pipeline, config):
        _store_recent_post(
            session,
            pipeline.id,
            original_text,
            config.DEDUP_WINDOW_SIZE,
            destination_channel=destination_channel,
            channel_message_id=channel_message_id,
        )
    state.current_source_index = (index + 1) % len(sources)
    state.total_posts = next_post_counter
    return True


async def _process_discussion_pipeline(
    config: Config,
    accounts: dict[str, AccountRuntime],
    primary_account: AccountRuntime,
    session: Session,
    pipeline: Pipeline,
    bot_app=None,
) -> bool:
    settings = get_discussion_settings(session, pipeline.id)
    if not settings:
        logger.warning("Discussion settings missing for pipeline %s", pipeline.name)
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="skipped",
            message="missing discussion settings",
        )
        return False
    if not settings.target_chat:
        logger.warning("Discussion pipeline %s: target chat is empty", pipeline.name)
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="skipped",
            message="target chat is empty",
        )
        return False
    discussion_level, _ = _get_account_activity_levels(config, pipeline.account_name)
    effective_min_interval = _scale_minutes(settings.min_interval_minutes, discussion_level)
    effective_max_interval = _scale_minutes(settings.max_interval_minutes, discussion_level)
    if effective_max_interval < effective_min_interval:
        effective_max_interval = effective_min_interval
    effective_inactivity = (
        _scale_minutes(settings.inactivity_pause_minutes, discussion_level, min_value=0)
        if settings.inactivity_pause_minutes > 0
        else 0
    )
    now = datetime.now(timezone.utc)
    now_local = _localize_time(now, settings.activity_timezone)
    windows = _resolve_activity_windows(settings, now_local)
    if windows and not _is_within_windows(now_local, windows):
        logger.info(
            "discussion skipped: outside activity window (pipeline=%s)",
            pipeline.name,
        )
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="skipped",
            message="outside activity window",
        )
        return False
    state = get_discussion_state(session, pipeline.id)
    next_due_at = _as_utc(state.next_due_at)
    if next_due_at and now < next_due_at:
        minutes_left = int((next_due_at - now).total_seconds() / 60)
        logger.info(
            "next discussion in %s minutes (pipeline=%s)",
            minutes_left,
            pipeline.name,
        )
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="scheduled",
            next_action_at=next_due_at,
            message=f"next discussion in ~{minutes_left} min",
        )
        return False
    sent_any = await _send_due_discussion_replies(
        config, accounts, primary_account, session, pipeline, settings, state, now
    )
    expires_at = _as_utc(state.expires_at)
    if expires_at and now < expires_at:
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="scheduled",
            message="waiting for replies",
        )
        return sent_any
    if expires_at and now >= expires_at:
        _reset_discussion_state(state)
        session.flush()
    if effective_inactivity > 0:
        active = await _discussion_chat_active(
            primary_account.reader_client,
            settings.target_chat,
            await _collect_bot_user_ids(accounts),
            effective_inactivity,
        )
        if not active:
            logger.info(
                "discussion skipped: inactive chat (pipeline=%s)",
                pipeline.name,
            )
            _update_pipeline_status(
                pipeline,
                category="pipeline1",
                state="skipped",
                message="inactive chat",
            )
            return sent_any
    source_pipeline = get_pipeline_by_name(session, settings.source_pipeline_name)
    if not source_pipeline:
        logger.warning(
            "Discussion pipeline %s: source pipeline %s not found",
            pipeline.name,
            settings.source_pipeline_name,
        )
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="skipped",
            message="source pipeline not found",
        )
        return sent_any
    k = random.randint(settings.k_min, settings.k_max)
    source_channel = source_pipeline.destination_channel
    candidates_all = await fetch_recent_posts_from_channel(
        primary_account.reader_client,
        source_channel,
        limit=k,
        min_text_length=config.MIN_TEXT_LENGTH,
    )
    if not candidates_all:
        logger.info(
            "discussion skipped: no candidate posts in channel (pipeline=%s)",
            pipeline.name,
        )
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="waiting_for_posts",
            progress_current=0,
            progress_total=k,
            message="no candidate posts",
        )
        return sent_any
    logger.info(
        "discussion_candidates pipeline=%s source=%s total=%s",
        pipeline.name, settings.source_pipeline_name, len(candidates_all),
    )
    candidates = candidates_all
    removed_by_last_post = 0
    removed_by_topics = 0
    removed_by_fingerprint = 0
    removed_by_bm25 = 0

    if state.last_source_post_id and len(candidates_all) > 1:
        before = len(candidates)
        filtered = [
            item for item in candidates_all if item.message_id != state.last_source_post_id
        ]
        removed_ids = [c.message_id for c in candidates_all if c.message_id == state.last_source_post_id]
        candidates = filtered
        removed_by_last_post = before - len(candidates)
        if removed_ids:
            logger.info(
                "discussion_filter pipeline=%s reason=last_post_id removed=%s msg_ids=%s before=%s after=%s",
                pipeline.name, removed_by_last_post, removed_ids, before, len(candidates),
            )

    # Keep reference to newest post so we can preserve it through topic/fingerprint/bm25 filters
    # (so the just-published post can get discussion and reactions).
    newest_candidate = candidates[0] if candidates else None

    recent_topics = set(_load_recent_topics(state))
    if recent_topics and len(candidates) > 1:
        before = len(candidates)
        filtered_by_topics = [
            item for item in candidates
            if not recent_topics.intersection({t.lower() for t in extract_topics_for_text(item.text)})
        ]
        # Always keep the newest post as a candidate so the just-published post
        # can get discussion and reactions even when its topics overlap with recent ones.
        if newest_candidate and newest_candidate not in filtered_by_topics:
            filtered_by_topics = [newest_candidate] + [c for c in filtered_by_topics if c != newest_candidate]
        removed_ids = [c.message_id for c in candidates if c not in filtered_by_topics]
        candidates = filtered_by_topics
        removed_by_topics = before - len(candidates)
        if filtered_by_topics:
            logger.info(
                "discussion_filter pipeline=%s source=%s reason=recent_topics removed=%s msg_ids=%s topics=%s before=%s after=%s",
                pipeline.name, settings.source_pipeline_name, removed_by_topics, removed_ids[:5], list(recent_topics)[:5], before, len(candidates),
            )
        else:
            logger.info(
                "discussion_filter pipeline=%s source=%s reason=recent_topics removed=%s msg_ids=%s topics=%s before=%s after=0 (skip repeat)",
                pipeline.name, settings.source_pipeline_name, removed_by_topics, removed_ids[:5], list(recent_topics)[:5], before,
            )

    fp_ring_size = getattr(config, "DISCUSSION_FINGERPRINT_RING_SIZE", 10)
    seen_fps = set(_load_recent_fingerprints(state))
    if seen_fps and len(candidates) > 1:
        before = len(candidates)
        filtered_fp = [c for c in candidates if topic_fingerprint(c.text) not in seen_fps]
        if newest_candidate and newest_candidate not in filtered_fp:
            filtered_fp = [newest_candidate] + [c for c in filtered_fp if c != newest_candidate]
        removed = [c for c in candidates if c not in filtered_fp]
        removed_ids = [c.message_id for c in removed]
        removed_fps = [topic_fingerprint(c.text) for c in removed]
        candidates = filtered_fp
        if filtered_fp:
            removed_by_fingerprint = before - len(candidates)
            logger.info(
                "discussion_filter pipeline=%s reason=fingerprint_seen removed=%s msg_ids=%s fps_sample=%s before=%s after=%s",
                pipeline.name, removed_by_fingerprint, removed_ids[:5], removed_fps[:3], before, len(candidates),
            )
        else:
            removed_by_fingerprint = before
            logger.info(
                "discussion_filter pipeline=%s reason=fingerprint_seen removed=%s msg_ids=%s fps_sample=%s before=%s after=0 (skip repeat)",
                pipeline.name, removed_by_fingerprint, removed_ids[:5], removed_fps[:3], before,
            )

    bm25_window = getattr(config, "DEDUP_WINDOW_SIZE", 15)
    bm25_threshold = getattr(config, "DEDUP_BM25_THRESHOLD", 10.5)
    if bm25_window > 0 and len(candidates) > 1:
        before = len(candidates)
        filtered_bm25 = []
        removed_with_scores: list[tuple[int, float]] = []
        for c in candidates:
            is_similar, max_score = _is_similar_news_bm25(
                session, source_pipeline.id, c.text, bm25_window, bm25_threshold
            )
            if not is_similar:
                filtered_bm25.append(c)
            else:
                removed_with_scores.append((c.message_id, max_score))
        if newest_candidate and newest_candidate not in filtered_bm25:
            filtered_bm25 = [newest_candidate] + [c for c in filtered_bm25 if c != newest_candidate]
        removed = [c for c in candidates if c not in filtered_bm25]
        removed_ids = [c.message_id for c in removed]
        candidates = filtered_bm25
        if filtered_bm25:
            removed_by_bm25 = before - len(candidates)
            logger.info(
                "discussion_filter pipeline=%s reason=bm25_similar removed=%s msg_ids=%s threshold=%s before=%s after=%s scores=%s",
                pipeline.name, removed_by_bm25, removed_ids[:5], bm25_threshold, before, len(candidates),
                [(mid, round(s, 1)) for mid, s in removed_with_scores[:5]],
            )
        else:
            removed_by_bm25 = before
            logger.info(
                "discussion_filter pipeline=%s reason=bm25_similar removed=%s msg_ids=%s threshold=%s before=%s after=0 (skip repeat) scores=%s",
                pipeline.name, removed_by_bm25, removed_ids[:5], bm25_threshold, before,
                [(mid, round(s, 1)) for mid, s in removed_with_scores[:5]],
            )

    if not candidates:
        logger.info(
            "discussion skipped: all candidates filtered (already discussed) (pipeline=%s) "
            "removed_by_last_post=%s removed_by_topics=%s removed_by_fingerprint=%s removed_by_bm25=%s total_start=%s",
            pipeline.name,
            removed_by_last_post, removed_by_topics, removed_by_fingerprint, removed_by_bm25,
            len(candidates_all),
        )
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="skipped",
            message="all candidates already discussed",
        )
        return sent_any

    _update_pipeline_status(
        pipeline,
        category="pipeline1",
        state="scanning_posts",
        progress_current=len(candidates),
        progress_total=k,
        message="collecting candidate posts",
    )
    candidate_texts = [item.text for item in candidates]
    try:
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="selecting_post",
            progress_current=len(candidates),
            progress_total=k,
            message="selecting best post",
        )
        recent_topics_list = list(recent_topics)
        selected_index, in_t, out_t, total_t = await asyncio.to_thread(
            primary_account.openai_client.select_discussion_news,
            candidate_texts,
            recent_topics=recent_topics_list,
            pipeline_id=pipeline.id,
            chat_id=settings.target_chat,
            extra={"source": "pipeline1"},
        )
    except Exception:
        logger.exception("Discussion pipeline %s: OpenAI select failed", pipeline.name)
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="skipped",
            message="openai select failed",
        )
        return sent_any
    if selected_index < 1 or selected_index > len(candidate_texts):
        selected_index = 1
    selected_item = candidates[selected_index - 1]
    news_text = candidate_texts[selected_index - 1]
    sel_fp = topic_fingerprint(news_text)
    title_snippet = (news_text[:60] + "...") if len(news_text) > 60 else news_text
    logger.info(
        "discussion_selected pipeline=%s msg_id=%s fp=%s idx=%s title_snippet=%s",
        pipeline.name, selected_item.message_id, sel_fp, selected_index, title_snippet,
    )
    replies_count = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
    available_weights = _ensure_discussion_weights(
        session, pipeline.id, accounts, exclude_account=primary_account.name
    )
    available = _filter_available_bots(available_weights, now)
    if not available:
        logger.info(
            "discussion skipped: no available userbots (cooldown/limits) (pipeline=%s)",
            pipeline.name,
        )
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="skipped",
            message="no available userbots",
        )
        return sent_any
    replies_count = min(replies_count, len(available))
    try:
        message_topics = extract_topics_for_text(news_text)
        effective_weights = _build_effective_weights(
            session,
            available,
            message_topics,
            pipeline_id=pipeline.id,
            chat_id=settings.target_chat,
            message_id=selected_item.message_id,
        )
    except Exception:
        logger.warning(
            "discussion topic detect failed: fallback to base weights (pipeline=%s)",
            pipeline.name,
        )
        effective_weights = None
    selected_bots = _select_discussion_bots(
        available, replies_count, effective_weights
    )
    selected_bots = _order_bots_for_chain(session, selected_bots)
    # Persona is presentation-only and must not affect decision logic.
    roles = [
        _format_persona_for_prompt(session, primary_account.name)
    ] + [
        _format_persona_for_prompt(session, bot.account_name)
        for bot in selected_bots
    ]
    try:
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="generating_question",
            message="generating discussion question",
        )
        payload, in_t2, out_t2, total_t2 = await asyncio.to_thread(
            primary_account.openai_client.generate_discussion_messages,
            news_text,
            replies_count,
            roles,
            pipeline_id=pipeline.id,
            chat_id=settings.target_chat,
            extra={"source": "pipeline1", "post_id": selected_item.message_id},
        )
    except Exception:
        logger.exception("Discussion pipeline %s: OpenAI generate failed", pipeline.name)
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="skipped",
            message="openai generate failed",
        )
        return sent_any
    question = str(payload.get("question", "")).strip()
    replies = payload.get("replies", [])
    if not question or not isinstance(replies, list):
        logger.warning("Discussion pipeline %s: invalid OpenAI payload", pipeline.name)
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="skipped",
            message="invalid OpenAI payload",
        )
        return sent_any
    replies = [str(item).strip() for item in replies if str(item).strip()]
    if not replies:
        logger.warning("Discussion pipeline %s: empty replies", pipeline.name)
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="skipped",
            message="empty replies",
        )
        return sent_any
    replies_count = min(replies_count, len(replies))
    replies = replies[:replies_count]
    selected_bots = selected_bots[:replies_count]
    logger.info(
        "discussion allowed: within activity window (pipeline=%s)",
        pipeline.name,
    )
    reply_message = await send_reply_text(
        primary_account.writer_client,
        settings.target_chat,
        question,
        reply_to_message_id=None,
        request_delay_seconds=primary_account.behavior.TELEGRAM_REQUEST_DELAY_SECONDS,
        random_jitter_seconds=primary_account.behavior.RANDOM_JITTER_SECONDS,
        flood_wait_antiblock=primary_account.behavior.FLOOD_WAIT_ANTIBLOCK,
        flood_wait_max_seconds=primary_account.behavior.FLOOD_WAIT_MAX_SECONDS,
        flood_wait_notify_after_seconds=pipeline.interval_seconds,
    )
    question_message_id = reply_message.id
    state.question_message_id = question_message_id
    state.question_created_at = now
    state.last_source_post_id = selected_item.message_id
    state.last_source_post_at = _as_utc(selected_item.created_at) if selected_item.created_at else now
    _update_recent_topics(
        state,
        extract_topics_for_text(news_text),
        add_fingerprint=sel_fp,
        fingerprint_ring_size=fp_ring_size,
    )
    state.expires_at = now + timedelta(minutes=60)
    state.replies_planned = len(replies)
    state.replies_sent = 0
    state.last_bot_reply_at = None
    state.last_reply_parent_id = question_message_id
    delay_factor = 1.5 - (discussion_level / 100.0)
    delay_factor = max(0.5, min(1.5, delay_factor))
    for idx, reply_text in enumerate(replies, start=1):
        # Planned chain for a single question; still keep persona roles per order.
        account_name = selected_bots[idx - 1].account_name
        _, persona_meta = _build_persona_prompt_and_meta(session, account_name)
        gender = persona_meta.get("gender", "male")
        before = reply_text
        reply_text, changed = fix_gender_grammar(reply_text, gender)
        if changed and logger.isEnabledFor(logging.DEBUG):
            b = (before[:120] + "…") if len(before) > 120 else before
            a = (reply_text[:120] + "…") if len(reply_text) > 120 else reply_text
            logger.debug(
                "gender_grammar_fix applied pipeline=p1 account=%s gender=%s before=%s after=%s",
                account_name, gender, b, a,
            )
        base_delay = _reply_delay_minutes(idx)
        adjusted_delay = max(1, int(round(base_delay * delay_factor)))
        send_at = now + timedelta(minutes=adjusted_delay)
        create_discussion_reply(
            session,
            pipeline_id=pipeline.id,
            account_name=account_name,
            reply_text=reply_text,
            send_at=send_at,
            reply_to_message_id=None,
        )
    await _try_set_reaction_on_news_post(
        config,
        accounts,
        primary_account,
        pipeline,
        source_channel=source_channel,
        news_text=news_text,
        available_bots=available,
        selected_bots_for_replies=selected_bots,
        now=now,
        selected_post_message_id=selected_item.message_id,
        selected_post_chat_id=source_channel,
    )
    await _try_set_admin_reaction_on_source_post(
        config=config,
        accounts=accounts,
        channel=source_channel,
        message_id=state.last_source_post_id,
        reason="pipeline1_question_to_chat",
        now=now,
    )
    text_cost = _estimate_text_cost(
        in_t + in_t2,
        out_t + out_t2,
        primary_account.openai_settings.text_input_price_per_1m,
        primary_account.openai_settings.text_output_price_per_1m,
    )
    _log_news_usage(
        question,
        primary_account.openai_client.text_model,
        in_t + in_t2,
        out_t + out_t2,
        total_t + total_t2,
        text_cost,
        primary_account.openai_client.image_model,
        0,
        0,
        0.0,
    )
    state.next_due_at = now + timedelta(
        minutes=random.randint(
            effective_min_interval, effective_max_interval
        )
    )
    _update_pipeline_status(
        pipeline,
        category="pipeline1",
        state="scheduled",
        next_action_at=state.next_due_at,
        message=f"replies planned: {len(replies)}",
    )
    logger.info(
        "discussion allowed: within activity window (pipeline=%s)",
        pipeline.name,
    )
    logger.info(
        "next discussion in %s minutes (pipeline=%s)",
        int((state.next_due_at - now).total_seconds() / 60),
        pipeline.name,
    )
    logger.info(
        "Discussion pipeline %s: scheduled %s replies",
        pipeline.name,
        len(replies),
    )
    return True


def _localize_time(now_utc: datetime, tz_name: str) -> datetime:
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    return now_utc.replace(tzinfo=timezone.utc).astimezone(tz)


def _resolve_activity_windows(
    settings: DiscussionSettings, now_local: datetime
) -> list[tuple[datetime.time, datetime.time]]:
    is_weekend = now_local.weekday() >= 5
    raw = (
        settings.activity_windows_weekends_json
        if is_weekend
        else settings.activity_windows_weekdays_json
    )
    return _parse_activity_windows(raw)


def _parse_activity_windows(
    raw: str | None,
) -> list[tuple[datetime.time, datetime.time]]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    windows: list[tuple[datetime.time, datetime.time]] = []
    for item in data:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        start, end = item
        try:
            start_time = datetime.strptime(start, "%H:%M").time()
            end_time = datetime.strptime(end, "%H:%M").time()
        except Exception:
            continue
        windows.append((start_time, end_time))
    return windows


def _is_within_windows(
    now_local: datetime, windows: list[tuple[datetime.time, datetime.time]]
) -> bool:
    if not windows:
        return True
    now_time = now_local.time()
    for start, end in windows:
        if start <= end:
            if start <= now_time <= end:
                return True
        else:
            if now_time >= start or now_time <= end:
                return True
    return False


async def _discussion_chat_active(
    client,
    target_chat: str,
    bot_ids: set[int],
    inactivity_minutes: int,
) -> bool:
    last_human_at = None
    async for message in client.iter_messages(target_chat, limit=50):
        if message.sender_id in bot_ids:
            continue
        if message.message or message.media:
            last_human_at = message.date
            break
    if not last_human_at:
        return False
    now = datetime.now(timezone.utc)
    if last_human_at.tzinfo is None:
        last_human_at = last_human_at.replace(tzinfo=timezone.utc)
    delta = now - last_human_at
    return delta.total_seconds() <= inactivity_minutes * 60


def _reply_delay_minutes(reply_index: int) -> int:
    if reply_index == 1:
        return random.randint(5, 15)
    if reply_index == 2:
        return random.randint(5, 30)
    return random.randint(10, 45)


def _reset_discussion_state(state: DiscussionState) -> None:
    state.question_message_id = None
    state.question_created_at = None
    state.expires_at = None
    state.replies_planned = 0
    state.replies_sent = 0
    state.last_bot_reply_at = None
    state.last_reply_parent_id = None
    state.last_bot_reply_message_id = None


def _ensure_discussion_weights(
    session: Session,
    pipeline_id: int,
    accounts: dict[str, AccountRuntime],
    *,
    exclude_account: str,
) -> list[DiscussionBotWeight]:
    existing = list_discussion_bot_weights(session, pipeline_id)
    existing_names = {w.account_name for w in existing}
    for account_name in accounts:
        if account_name == exclude_account:
            continue
        if account_name not in existing_names:
            upsert_discussion_bot_weight(
                session,
                pipeline_id=pipeline_id,
                account_name=account_name,
                weight=1,
                daily_limit=5,
                cooldown_minutes=60,
            )
            existing_names.add(account_name)
    return list_discussion_bot_weights(session, pipeline_id)


def _load_persona_interest(
    session: Session, account_name: str
) -> tuple[list[str], int, int]:
    persona = get_userbot_persona(session, account_name)
    topics_raw = persona.persona_topics if persona and persona.persona_topics else None
    topics: list[str] = []
    if topics_raw:
        try:
            data = json.loads(topics_raw)
            if isinstance(data, list):
                topics = [str(item) for item in data if str(item)]
        except Exception:
            logger.warning(
                "persona topics parse failed (account=%s)", account_name
            )
    topic_priority = (
        persona.persona_topic_priority
        if persona and persona.persona_topic_priority is not None
        else 50
    )
    offtopic_tolerance = (
        persona.persona_offtopic_tolerance
        if persona and persona.persona_offtopic_tolerance is not None
        else 50
    )
    return topics, int(topic_priority), int(offtopic_tolerance)


def _topic_multiplier(
    message_topics: list[str],
    persona_topics: list[str],
    topic_priority: int,
    offtopic_tolerance: int,
) -> tuple[float, bool]:
    # Soft bias only: no hard filters, weights can be softened by tolerance.
    if not message_topics or not persona_topics:
        return 1.0, False
    overlap = len(set(message_topics) & set(persona_topics))
    if overlap == 0:
        tolerance = max(0, min(offtopic_tolerance, 100)) / 100.0
        return max(tolerance, 0.0), False
    priority = max(0, min(topic_priority, 100)) / 100.0
    boost = (overlap * priority) * 0.25
    return 1.0 + boost, True


def _build_effective_weights(
    session: Session,
    weights: list[DiscussionBotWeight],
    message_topics: list[str],
    *,
    pipeline_id: int,
    chat_id: str | None,
    message_id: int | None,
) -> dict[str, float]:
    logger.info(
        "discussion_topic_detected pipeline=%s chat=%s message_id=%s topics=%s",
        pipeline_id,
        chat_id,
        message_id,
        message_topics,
    )
    effective: dict[str, float] = {}
    for item in weights:
        persona_topics, topic_priority, offtopic_tolerance = _load_persona_interest(
            session, item.account_name
        )
        multiplier, match = _topic_multiplier(
            message_topics, persona_topics, topic_priority, offtopic_tolerance
        )
        if not message_topics or not persona_topics:
            reason = "no_topics"
        elif match:
            reason = "match"
        else:
            reason = "no_match"
        base_weight = float(item.weight)
        effective_weight = max(base_weight * multiplier, 0.0)
        effective[item.account_name] = effective_weight
        logger.info(
            "discussion_bot_selection pipeline=%s chat=%s message_id=%s bot=%s "
            "base=%s multiplier=%.2f effective=%.2f reason=%s persona_topics=%s",
            pipeline_id,
            chat_id,
            message_id,
            item.account_name,
            base_weight,
            multiplier,
            effective_weight,
            reason,
            persona_topics,
        )
    return effective


def _activity_percent(value: int | None) -> int:
    if value is None:
        return 50
    return max(0, min(int(value), 100))


def _activity_factor(percent: int) -> float:
    # 0 -> 0.5, 50 -> 1.0, 100 -> 1.5
    return 0.5 + (percent / 100.0)


def _scale_minutes(base_minutes: int, percent: int, min_value: int = 5) -> int:
    factor = 1.5 - (percent / 100.0)  # 0 -> 1.5, 50 -> 1.0, 100 -> 0.5
    factor = max(0.5, min(1.5, factor))
    return max(min_value, int(round(base_minutes * factor)))


def _get_account_activity_levels(
    config: Config, account_name: str
) -> tuple[int, int]:
    discussion = 50
    replies = 50
    for account in config.telegram_accounts():
        if account.name == account_name:
            discussion = _activity_percent(account.discussion_activity_percent)
            replies = _activity_percent(account.user_reply_activity_percent)
            break
    return discussion, replies


def _filter_available_bots(
    weights: list[DiscussionBotWeight], now: datetime
) -> list[DiscussionBotWeight]:
    available: list[DiscussionBotWeight] = []
    today = now.strftime("%Y-%m-%d")
    for item in weights:
        if item.used_today_date != today:
            item.used_today = 0
            item.used_today_date = today
        if item.used_today >= item.daily_limit:
            continue
        if item.last_used_at is not None:
            last_used = _as_utc(item.last_used_at)
            elapsed = (now - last_used).total_seconds() / 60 if last_used else 0
            if elapsed < item.cooldown_minutes:
                continue
        if item.weight <= 0:
            continue
        available.append(item)
    return available


def _select_discussion_bots(
    weights: list[DiscussionBotWeight],
    count: int,
    effective_weights: dict[str, float] | None = None,
) -> list[DiscussionBotWeight]:
    if count <= 0:
        return []
    if count == 1:
        return [_weighted_choice_with_map(weights, effective_weights)]
    if count == 2:
        first = _weighted_choice_with_map(weights, effective_weights)
        remaining = [item for item in weights if item != first]
        second = (
            _weighted_choice_with_map(remaining, effective_weights)
            if remaining
            else first
        )
        return [first, second]
    ordered = sorted(weights, key=lambda item: item.account_name)
    start = random.randint(0, len(ordered) - 1)
    selected = []
    for offset in range(count):
        selected.append(ordered[(start + offset) % len(ordered)])
    return selected


def _weighted_choice_with_map(
    weights: list[DiscussionBotWeight], effective_weights: dict[str, float] | None
) -> DiscussionBotWeight:
    if effective_weights is None:
        total = sum(item.weight for item in weights)
    else:
        total = sum(effective_weights.get(item.account_name, item.weight) for item in weights)
    if total <= 0:
        return random.choice(weights)
    pick = random.uniform(0, total)
    cumulative = 0.0
    for item in weights:
        if effective_weights is None:
            cumulative += item.weight
        else:
            cumulative += effective_weights.get(item.account_name, item.weight)
        if pick <= cumulative:
            return item
    return weights[-1]


def _build_persona_prompt_and_meta(
    session: Session, account_name: str
) -> tuple[str, dict[str, Any]]:
    """Строит человекочитаемый role_label и структурированные метаданные персоны.
    Возвращает (role_label, persona_meta). Без META-строки в role_label."""
    persona = get_userbot_persona(session, account_name)
    tone = persona.persona_tone if persona and persona.persona_tone else "neutral"
    verbosity = (
        persona.persona_verbosity if persona and persona.persona_verbosity else "short"
    )
    style_hint = (
        persona.persona_style_hint if persona and persona.persona_style_hint else None
    )
    profile = PERSONA_PROFILE_OVERRIDES.get(account_name, {})
    display_name = profile.get("display_name", account_name)
    gender_raw = profile.get("gender", "male")
    gender = str(gender_raw).strip().lower() if gender_raw else "male"
    if gender not in {"male", "female"}:
        logger.warning(
            "persona gender invalid account=%s raw=%r skipping grammar fix",
            account_name,
            gender_raw,
        )
        gender = "unknown"
    if gender == "female":
        grammar = (
            "пиши от первого лица в женском роде. "
            "Всегда: согласна, не согласна, уверена, не уверена, удивлена, не удивлена, готова, права. "
            "Никогда: согласен, уверен, удивлён, готов, прав. "
            "Примеры вводных (разнообразь, не только эти): «Согласна…», «Не уверена…», «Мне кажется…», «Не совсем согласна…», «Похоже на то», «Тут есть нюанс», «Сомневаюсь», или начало сразу по делу."
        )
    else:
        grammar = (
            "пиши согласен/не согласен, уверен/не уверен. "
            "Разнообразь вводные (не только «Согласен», «Не уверен»): можно «Скорее всего», «Тут нюанс», «Похоже на то», или сразу по делу."
        )
    instructions = [
        f"Имя: {display_name}. Пол: {grammar}.",
    ]
    if gender == "female":
        instructions.append(
            "Чаще формулируй через нюанс, уточнение или мягкую позицию."
        )
    else:
        instructions.append(
            "Формулировки могут быть чуть более прямыми и уверенными, без смягчающих оборотов."
        )
    if tone == "analytical":
        instructions.append("тон: спокойный, взвешенный")
    elif tone == "emotional":
        instructions.append("тон: мягко эмоциональный")
    elif tone == "ironic":
        instructions.append("тон: легкая ирония без сарказма")
    elif tone == "skeptical":
        instructions.append("тон: умеренный скепсис без агрессии")
    else:
        instructions.append("тон: нейтральный")
    if verbosity == "medium":
        instructions.append("длина: 1–2 предложения")
    elif verbosity == "long":
        instructions.append("длина: 2–3 предложения")
    else:
        instructions.append("длина: 1 короткое предложение")
    if style_hint:
        instructions.append(f"стиль: {style_hint}")
    topics, topic_priority, offtopic_tolerance = _load_persona_interest(
        session, account_name
    )
    if topics:
        instructions.append("темы: " + ", ".join(topics))
    instructions.append(
        "ограничения: без сленга, без эмодзи, без капса, без упоминания бота/ИИ; пунктуация естественная, не обязательно точка в конце; никогда не используй длинное тире (—)."
    )
    role_label = " | ".join(instructions)
    persona_meta: dict[str, Any] = {
        "display_name": display_name,
        "gender": gender,
        "tone": tone,
        "verbosity": verbosity,
        "topics": topics,
        "topic_priority": topic_priority,
        "offtopic_tolerance": offtopic_tolerance,
    }
    return role_label, persona_meta


def _format_persona_for_prompt(session: Session, account_name: str) -> str:
    """Человекочитаемый role_label для Pipeline 1 и Pipeline 2 (без META)."""
    role_label, _ = _build_persona_prompt_and_meta(session, account_name)
    return role_label


def _persona_role_rank(session: Session, account_name: str) -> int:
    persona = get_userbot_persona(session, account_name)
    tone = persona.persona_tone if persona and persona.persona_tone else "neutral"
    order = {
        "analytical": 0,
        "neutral": 1,
        "skeptical": 2,
        "ironic": 3,
        "emotional": 4,
    }
    return order.get(tone, 1)


def _order_bots_for_chain(
    session: Session, bots: list[DiscussionBotWeight]
) -> list[DiscussionBotWeight]:
    # Only reorder planned replies; selection probability is unchanged.
    return sorted(bots, key=lambda item: _persona_role_rank(session, item.account_name))


async def _send_due_discussion_replies(
    config: Config,
    accounts: dict[str, AccountRuntime],
    primary_account: AccountRuntime,
    session: Session,
    pipeline: Pipeline,
    settings: DiscussionSettings,
    state: DiscussionState,
    now: datetime,
) -> bool:
    due_replies = list_due_discussion_replies(
        session, pipeline.id, now, kind="discussion"
    )
    if not due_replies:
        return False
    bot_ids = await _collect_bot_user_ids(accounts)
    sent_any = False
    for reply in due_replies:
        if not state.question_message_id or not state.question_created_at:
            mark_discussion_reply_cancelled(session, reply, "no_question")
            _update_pipeline_status(
                pipeline,
                category="pipeline1",
                state="cancelled",
                message=f"reply {reply.id}: no question",
            )
            continue
        expires_at = _as_utc(state.expires_at)
        if expires_at and now >= expires_at:
            mark_discussion_reply_cancelled(session, reply, "expired")
            _update_pipeline_status(
                pipeline,
                category="pipeline1",
                state="cancelled",
                message=f"reply {reply.id}: expired",
            )
            continue
        if not await _discussion_still_valid(
            primary_account.reader_client,
            settings.target_chat,
            state.question_message_id,
            bot_ids,
        ):
            mark_discussion_reply_cancelled(session, reply, "topic_moved")
            _update_pipeline_status(
                pipeline,
                category="pipeline1",
                state="cancelled",
                message=f"reply {reply.id}: topic moved",
            )
            continue
        account = accounts.get(reply.account_name)
        if not account:
            mark_discussion_reply_cancelled(session, reply, "account_missing")
            _update_pipeline_status(
                pipeline,
                category="pipeline1",
                state="cancelled",
                message=f"reply {reply.id}: account missing",
            )
            continue
        reply_to_id = _pick_reply_parent(state, settings, reply)
        try:
            sent_message = await send_reply_text(
                account.writer_client,
                settings.target_chat,
                reply.reply_text,
                reply_to_message_id=reply_to_id,
                request_delay_seconds=account.behavior.TELEGRAM_REQUEST_DELAY_SECONDS,
                random_jitter_seconds=account.behavior.RANDOM_JITTER_SECONDS,
                flood_wait_antiblock=account.behavior.FLOOD_WAIT_ANTIBLOCK,
                flood_wait_max_seconds=account.behavior.FLOOD_WAIT_MAX_SECONDS,
                flood_wait_notify_after_seconds=pipeline.interval_seconds,
            )
        except Exception:
            logger.exception(
                "Discussion pipeline %s: failed to send reply", pipeline.name
            )
            _update_pipeline_status(
                pipeline,
                category="pipeline1",
                state="cancelled",
                message=f"reply {reply.id}: send failed",
            )
            continue
        mark_discussion_reply_sent(session, reply, now)
        state.replies_sent += 1
        state.last_bot_reply_at = now
        state.last_reply_parent_id = reply_to_id
        state.last_bot_reply_message_id = getattr(sent_message, "id", None)
        _update_bot_usage(session, pipeline.id, reply.account_name, now)
        _update_pipeline_status(
            pipeline,
            category="pipeline1",
            state="sent",
            message=f"bot {reply.account_name} -> {getattr(sent_message, 'id', None)}",
        )
        sent_any = True
    return sent_any


def _pick_reply_parent(
    state: DiscussionState,
    settings: DiscussionSettings,
    reply: DiscussionReply,
) -> int:
    question_id = state.question_message_id or 0
    if not question_id:
        return question_id
    probability = settings.reply_to_reply_probability / 100.0
    if state.last_reply_parent_id and state.last_reply_parent_id != question_id:
        return question_id
    if random.random() < probability and state.last_bot_reply_message_id:
        return state.last_bot_reply_message_id
    return question_id


def _update_bot_usage(
    session: Session, pipeline_id: int, account_name: str, now: datetime
) -> None:
    today = now.strftime("%Y-%m-%d")
    row = session.execute(
        select(DiscussionBotWeight).where(
            DiscussionBotWeight.pipeline_id == pipeline_id,
            DiscussionBotWeight.account_name == account_name,
        )
    ).scalar_one_or_none()
    if row is None:
        return
    if row.used_today_date != today:
        row.used_today = 0
        row.used_today_date = today
    row.used_today += 1
    row.last_used_at = now


async def _collect_bot_user_ids(
    accounts: dict[str, AccountRuntime]
) -> set[int]:
    bot_ids: set[int] = set()
    for runtime in accounts.values():
        if runtime.user_id:
            bot_ids.add(runtime.user_id)
            continue
        try:
            me = await runtime.reader_client.get_me()
            if me and getattr(me, "id", None) is not None:
                runtime.user_id = int(me.id)
                bot_ids.add(runtime.user_id)
        except Exception:
            continue
    return bot_ids


async def _discussion_still_valid(
    client,
    target_chat: str,
    question_message_id: int,
    bot_ids: set[int],
) -> bool:
    messages = []
    async for message in client.iter_messages(
        target_chat, min_id=question_message_id, limit=10
    ):
        messages.append(message)
    if not messages:
        return True
    consecutive_bots = 0
    for message in messages:
        if message.sender_id in bot_ids:
            consecutive_bots += 1
        else:
            break
    if consecutive_bots >= 2:
        return False
    human_offtopic = 0
    for message in messages:
        if message.sender_id in bot_ids:
            continue
        if message.reply_to_msg_id != question_message_id:
            human_offtopic += 1
    if human_offtopic >= 3:
        return False
    return True


async def _process_live_replies_pipeline(
    config: Config,
    accounts: dict[str, AccountRuntime],
    primary_account: AccountRuntime,
    session: Session,
    pipeline: Pipeline,
) -> None:
    _update_pipeline_status(
        pipeline,
        category="pipeline2",
        state="processing",
        message="Pipeline 2 started",
    )
    settings = get_discussion_settings(session, pipeline.id)
    if not settings or not settings.target_chat:
        _update_pipeline_status(
            pipeline,
            category="pipeline2",
            state="idle",
            message="discussion settings missing",
        )
        return
    now = datetime.now(timezone.utc)
    now_local = _localize_time(now, settings.activity_timezone)
    windows = _resolve_activity_windows(settings, now_local)
    if windows and not _is_within_windows(now_local, windows):
        logger.info(
            "user reply skipped: outside activity window (pipeline=%s)",
            pipeline.name,
        )
        _update_pipeline_status(
            pipeline,
            category="pipeline2",
            state="skipped",
            message="outside activity window",
        )
        await _send_due_user_replies(
            config,
            accounts,
            primary_account,
            session,
            pipeline,
            settings,
            now,
            allow_send=False,
        )
        return
    _update_pipeline_status(
        pipeline,
        category="pipeline2",
        state="processing",
        message="checking due replies",
    )
    await _send_due_user_replies(
        config,
        accounts,
        primary_account,
        session,
        pipeline,
        settings,
        now,
        allow_send=True,
    )
    chat_state = get_chat_state(session, pipeline.id, settings.target_chat)
    next_scan_at = _as_utc(chat_state.next_scan_at)
    if next_scan_at and now < next_scan_at:
        minutes_left = int((next_scan_at - now).total_seconds() / 60)
        _update_pipeline_status(
            pipeline,
            category="pipeline2",
            state="waiting",
            next_action_at=next_scan_at,
            message=f"next scan in ~{minutes_left} min",
        )
        return
    _update_pipeline_status(
        pipeline,
        category="pipeline2",
        state="processing",
        message="scanning chat",
    )
    logger.info("Pipeline 2 %s: scanning chat %s", pipeline.name, settings.target_chat)
    candidates, max_id_seen = await _scan_chat_for_candidates(
        accounts,
        primary_account,
        chat_state,
        settings.target_chat,
    )
    chat_state.next_scan_at = now + timedelta(seconds=random.randint(30, 60))
    logger.info(
        "Pipeline 2 %s: scan done, candidates=%s (last_seen_message_id=%s)",
        pipeline.name,
        len(candidates),
        chat_state.last_seen_message_id,
    )
    if not candidates:
        if max_id_seen is not None:
            chat_state.last_seen_message_id = max_id_seen
        _update_pipeline_status(
            pipeline,
            category="pipeline2",
            state="waiting",
            next_action_at=chat_state.next_scan_at,
            message="no candidates",
        )
        return
    logger.info("user reply candidates: %s", len(candidates))
    _update_pipeline_status(
        pipeline,
        category="pipeline2",
        state="processing",
        progress_current=0,
        progress_total=len(candidates),
        message="processing candidates",
    )
    replies_created = 0
    last_selected_bot_account: str | None = None
    for candidate in candidates:
        created, last_used = await _plan_user_reply_for_candidate(
            config,
            accounts,
            primary_account,
            session,
            pipeline,
            settings,
            chat_state,
            candidate,
            last_selected_bot_account=last_selected_bot_account,
        )
        if created:
            replies_created += 1
        if last_used:
            last_selected_bot_account = last_used
    if max_id_seen is not None and replies_created > 0:
        chat_state.last_seen_message_id = max_id_seen
    elif replies_created == 0 and candidates:
        logger.info(
            "Pipeline 2 %s: no replies created for %s candidates, keeping last_seen=%s for retry",
            pipeline.name,
            len(candidates),
            chat_state.last_seen_message_id,
        )


async def _scan_chat_for_candidates(
    accounts: dict[str, AccountRuntime],
    primary_account: AccountRuntime,
    chat_state: ChatState,
    chat_id: str,
) -> tuple[list[dict], int | None]:
    """Scan chat for candidate messages to reply to.
    Returns (candidates, max_id_seen). max_id_seen is None if no messages fetched.
    Does NOT update chat_state.last_seen_message_id — caller must do that
    only when appropriate (see _process_live_replies_pipeline).
    """
    bot_ids = await _collect_bot_user_ids(accounts)
    last_seen = chat_state.last_seen_message_id or 0
    messages = []
    async for message in primary_account.reader_client.iter_messages(
        chat_id, min_id=last_seen, limit=50
    ):
        messages.append(message)
    if not messages:
        logger.debug(
            "Pipeline 2 scan: no messages (chat=%s, last_seen=%s)",
            chat_id, last_seen,
        )
        return [], None
    messages = sorted(messages, key=lambda item: item.id)
    logger.info(
        "Pipeline 2 scan: chat=%s last_seen=%s fetched=%s msg_ids=%s..%s",
        chat_id, last_seen, len(messages), messages[0].id, messages[-1].id,
    )
    candidates: list[dict] = []
    max_id = last_seen
    for message in messages:
        max_id = max(max_id, message.id)
        if getattr(message, "out", False):
            continue
        if getattr(message, "action", None) is not None:
            continue
        sender = getattr(message, "sender", None)
        if sender is not None and getattr(sender, "bot", False):
            continue
        if message.sender_id in bot_ids:
            continue
        text = (message.message or "").strip()
        if not text and not message.media:
            continue
        chat_state.last_human_message_at = message.date
        is_reply_to_bot = False
        if message.reply_to_msg_id:
            try:
                replied = await primary_account.reader_client.get_messages(
                    chat_id, ids=message.reply_to_msg_id
                )
            except Exception:
                replied = None
            if replied and getattr(replied, "sender_id", None) in bot_ids:
                is_reply_to_bot = True
        if _is_candidate_for_reply(text, is_reply_to_bot):
            candidates.append(
                {
                    "message_id": message.id,
                    "chat_id": chat_id,
                    "author_id": message.sender_id,
                    "text": text,
                    "is_reply_to_bot": is_reply_to_bot,
                    "created_at": message.date,
                }
            )
    if messages and not candidates:
        logger.info(
            "Pipeline 2 scan: 0 candidates from %s messages (chat=%s). "
            "Candidate = has '?' or trigger phrase or reply to our bot; human, not out, no action.",
            len(messages), chat_id,
        )
    return candidates, max_id


def _is_candidate_for_reply(text: str, is_reply_to_bot: bool) -> bool:
    if is_reply_to_bot:
        return True
    lowered = text.lower()
    if "?" in lowered:
        return True
    triggers = [
        "как думаете",
        "что скажете",
        "есть инфа",
        "а это как работает",
    ]
    return any(trigger in lowered for trigger in triggers)


async def _plan_user_reply_for_candidate(
    config: Config,
    accounts: dict[str, AccountRuntime],
    primary_account: AccountRuntime,
    session: Session,
    pipeline: Pipeline,
    settings: DiscussionSettings,
    chat_state: ChatState,
    candidate: dict,
    *,
    last_selected_bot_account: str | None = None,
) -> tuple[bool, str | None]:
    now = datetime.now(timezone.utc)
    _update_pipeline_status(
        pipeline,
        category="pipeline2",
        state="processing",
        message=f"message {candidate.get('message_id')}",
    )
    today = now.strftime("%Y-%m-%d")
    _, reply_level = _get_account_activity_levels(config, pipeline.account_name)
    reply_factor = _activity_factor(reply_level)
    if chat_state.replies_today_date != today:
        chat_state.replies_today = 0
        chat_state.replies_today_date = today
    max_replies = settings.max_auto_replies_per_chat_per_day
    if max_replies > 0:
        max_replies = max(1, int(round(max_replies * reply_factor)))
    if max_replies > 0 and chat_state.replies_today >= max_replies:
        logger.info("user reply skipped: global limit reached")
        _update_pipeline_status(
            pipeline,
            category="pipeline2",
            state="skipped",
            message=f"message {candidate.get('message_id')}: global limit",
        )
        return False, None
    base_prob = random.uniform(0.4, 0.6)
    boosted = candidate["is_reply_to_bot"] or "?" in candidate["text"]
    decision_prob = 0.8 if boosted else base_prob
    decision_prob = min(0.95, decision_prob * reply_factor)
    if random.random() > decision_prob:
        logger.info("user reply skipped: probability")
        _update_pipeline_status(
            pipeline,
            category="pipeline2",
            state="skipped",
            message=f"message {candidate.get('message_id')}: probability",
        )
        return False, None
    if settings.user_reply_max_age_minutes > 0:
        candidate_time = candidate["created_at"]
        if candidate_time.tzinfo is None:
            candidate_time = candidate_time.replace(tzinfo=timezone.utc)
        age_minutes = (now - candidate_time).total_seconds() / 60
        if age_minutes > settings.user_reply_max_age_minutes:
            logger.info("user reply skipped: message too old")
            _update_pipeline_status(
                pipeline,
                category="pipeline2",
                state="skipped",
                message=f"message {candidate.get('message_id')}: too old",
            )
            return False, None
    effective_inactivity = (
        _scale_minutes(settings.inactivity_pause_minutes, reply_level, min_value=0)
        if settings.inactivity_pause_minutes > 0
        else 0
    )
    if effective_inactivity > 0 and chat_state.last_human_message_at:
        last_human = chat_state.last_human_message_at
        if last_human.tzinfo is None:
            last_human = last_human.replace(tzinfo=timezone.utc)
        delta = now - last_human
        if delta.total_seconds() > effective_inactivity * 60:
            logger.info("user reply skipped: inactive chat")
            _update_pipeline_status(
                pipeline,
                category="pipeline2",
                state="skipped",
                message=f"message {candidate.get('message_id')}: inactive chat",
            )
            return False, None
    replies_count = random.choices([1, 2], weights=[80, 20])[0]
    available_weights = _ensure_discussion_weights(
        session, pipeline.id, accounts, exclude_account=primary_account.name
    )
    available = _filter_available_bots(available_weights, now)
    if not available:
        logger.info("user reply skipped: no available userbots")
        _update_pipeline_status(
            pipeline,
            category="pipeline2",
            state="skipped",
            message=f"message {candidate.get('message_id')}: no available userbots",
        )
        return False, None
    # Anti-repeat: не выбирать того же бота, который отвечал последним в этом скане
    if last_selected_bot_account and len(available) > 1:
        available = [a for a in available if a.account_name != last_selected_bot_account]
    try:
        message_topics = extract_topics_for_text(candidate["text"])
        effective_weights = _build_effective_weights(
            session,
            available,
            message_topics,
            pipeline_id=pipeline.id,
            chat_id=candidate["chat_id"],
            message_id=candidate["message_id"],
        )
    except Exception:
        logger.warning(
            "user reply topic detect failed: fallback to base weights (pipeline=%s)",
            pipeline.name,
        )
        effective_weights = None
    selected_bots = _select_user_reply_bots(
        available, replies_count, effective_weights
    )
    context_messages = await _fetch_recent_chat_context(
        primary_account.reader_client, candidate["chat_id"], limit=8
    )
    base_time = candidate["created_at"]
    if base_time.tzinfo is None:
        base_time = base_time.replace(tzinfo=timezone.utc)
    first_send_at = base_time + timedelta(minutes=random.randint(2, 10))
    second_send_at = first_send_at + timedelta(minutes=random.randint(3, 15))
    created_any = False
    last_used: str | None = None
    model_driven = getattr(config, "CHAT_REACTIONS_MODEL_DRIVEN", False)
    allowed_reactions: list[str] = []
    if model_driven and getattr(config, "CHAT_REACTIONS_ENABLED", False):
        allowed_reactions = await get_available_reaction_emojis(
            primary_account.reader_client, candidate["chat_id"]
        )
        if not allowed_reactions:
            allowed_reactions = config.chat_reaction_emojis_list()
    null_rate = getattr(config, "CHAT_REACTIONS_MODEL_NULL_RATE", 0.65)
    for idx, bot_weight in enumerate(selected_bots, start=1):
        account = accounts.get(bot_weight.account_name)
        if not account:
            continue
        try:
            role_label, persona_meta = _build_persona_prompt_and_meta(
                session, bot_weight.account_name
            )
            reply_text, reaction_emoji, _, _, _, gen_info = await asyncio.to_thread(
                account.openai_client.generate_user_reply,
                source_text=candidate["text"],
                context_messages=context_messages,
                role_label=role_label,
                persona_meta=persona_meta,
                pipeline_id=pipeline.id,
                chat_id=candidate["chat_id"],
                extra={
                    "source": "pipeline2",
                    "reply_to_message_id": candidate["message_id"],
                    "account_name": bot_weight.account_name,
                },
                system_prompt_override=getattr(account, "system_prompt_chat", None),
                allowed_reactions=allowed_reactions if model_driven else None,
                model_driven_reaction=model_driven,
                reaction_null_rate=null_rate,
            )
        except Exception:
            logger.exception("user reply skipped: openai error")
            _update_pipeline_status(
                pipeline,
                category="pipeline2",
                state="skipped",
                message=f"message {candidate.get('message_id')}: openai error",
            )
            continue
        if not reply_text:
            continue
        gender = persona_meta.get("gender", "male")
        before = reply_text
        reply_text, changed = fix_gender_grammar(reply_text, gender)
        if changed and logger.isEnabledFor(logging.DEBUG):
            b = (before[:120] + "…") if len(before) > 120 else before
            a = (reply_text[:120] + "…") if len(reply_text) > 120 else reply_text
            logger.debug(
                "gender_grammar_fix applied pipeline=p2 account=%s gender=%s before=%s after=%s",
                bot_weight.account_name, gender, b, a,
            )
        send_at = first_send_at if idx == 1 else second_send_at
        create_discussion_reply(
            session,
            pipeline_id=pipeline.id,
            kind="user_reply",
            chat_id=candidate["chat_id"],
            account_name=bot_weight.account_name,
            reply_text=reply_text,
            send_at=send_at,
            reply_to_message_id=candidate["message_id"],
            source_message_at=base_time,
        )
        created_any = True
        last_used = bot_weight.account_name
        if (
            model_driven
            and reaction_emoji
            and getattr(config, "CHAT_REACTIONS_ENABLED", False)
            and account.writer_client
        ):
            _chat_reaction_ensure_date_reset(now)
            logger.info(
                "chat reaction model candidate chat=%s msg_id=%s emoji=%s allowed_count=%s source=model",
                candidate["chat_id"],
                candidate["message_id"],
                reaction_emoji,
                len(allowed_reactions),
            )
            ok = await set_message_reaction(
                account.writer_client,
                candidate["chat_id"],
                candidate["message_id"],
                reaction_emoji,
            )
            if ok:
                _update_chat_reaction_state(
                    bot_weight.account_name,
                    candidate["chat_id"],
                    candidate["message_id"],
                    now,
                )
                logger.info(
                    "chat reaction set reason=pipeline2_user_message chat=%s msg_id=%s bot=%s emoji=%s source=model",
                    candidate["chat_id"],
                    candidate["message_id"],
                    bot_weight.account_name,
                    reaction_emoji,
                )
            else:
                logger.info(
                    "chat reaction skipped reason=pipeline2_user_message chat=%s msg_id=%s bot=%s why=api_error source=model",
                    candidate["chat_id"],
                    candidate["message_id"],
                    bot_weight.account_name,
                )
        _update_pipeline_status(
            pipeline,
            category="pipeline2",
            state="scheduled",
            next_action_at=send_at,
            message=(
                f"message {candidate.get('message_id')}: "
                f"bot {bot_weight.account_name}"
            ),
        )
        chat_state.replies_today += 1
        preset_str = gen_info.get("preset_idx", "n/a") if gen_info else "n/a"
        length_str = gen_info.get("length_hint", "n/a") if gen_info else "n/a"
        logger.info(
            "user_reply scheduled bot=%s tone=%s verbosity=%s gender=%s preset=%s length=%s at=%s",
            bot_weight.account_name,
            persona_meta.get("tone", "n/a"),
            persona_meta.get("verbosity", "n/a"),
            persona_meta.get("gender", "n/a"),
            preset_str,
            length_str,
            send_at.isoformat(),
        )
    return created_any, last_used


def _select_user_reply_bots(
    weights: list[DiscussionBotWeight],
    count: int,
    effective_weights: dict[str, float] | None = None,
) -> list[DiscussionBotWeight]:
    if count <= 0:
        return []
    if count == 1:
        return [_weighted_choice_with_map(weights, effective_weights)]
    first = _weighted_choice_with_map(weights, effective_weights)
    remaining = [item for item in weights if item != first]
    second = (
        _weighted_choice_with_map(remaining, effective_weights) if remaining else first
    )
    return [first, second]


async def _fetch_recent_chat_context(client, chat_id: str, limit: int = 8) -> list[str]:
    messages = []
    async for message in client.iter_messages(chat_id, limit=limit):
        text = (message.message or "").strip()
        if text:
            messages.append(text)
    return list(reversed(messages))


async def _send_due_user_replies(
    config: Config,
    accounts: dict[str, AccountRuntime],
    primary_account: AccountRuntime,
    session: Session,
    pipeline: Pipeline,
    settings: DiscussionSettings,
    now: datetime,
    *,
    allow_send: bool,
) -> None:
    due_replies = list_due_discussion_replies(
        session, pipeline.id, now, kind="user_reply"
    )
    if not due_replies:
        return
    total_due = len(due_replies)
    due_replies = due_replies[:_MAX_DUE_USER_REPLIES_PER_CYCLE]
    logger.info(
        "Pipeline 2 %s: %s due replies in queue, processing %s this cycle",
        pipeline.name,
        total_due,
        len(due_replies),
    )
    _update_pipeline_status(
        pipeline,
        category="pipeline2",
        state="processing",
        message=f"sending {len(due_replies)} of {total_due} due replies",
    )
    if not allow_send:
        for reply in due_replies:
            mark_discussion_reply_cancelled(session, reply, "outside activity window")
            _update_pipeline_status(
                pipeline,
                category="pipeline2",
                state="cancelled",
                message=f"reply {reply.id}: outside activity window",
            )
        return
    chat_state = get_chat_state(session, pipeline.id, settings.target_chat)
    _, reply_level = _get_account_activity_levels(config, pipeline.account_name)
    effective_inactivity = (
        _scale_minutes(settings.inactivity_pause_minutes, reply_level, min_value=0)
        if settings.inactivity_pause_minutes > 0
        else 0
    )
    if effective_inactivity > 0 and chat_state.last_human_message_at:
        delta = now - chat_state.last_human_message_at.replace(tzinfo=timezone.utc)
        if delta.total_seconds() > effective_inactivity * 60:
            for reply in due_replies:
                mark_discussion_reply_cancelled(session, reply, "inactive chat")
                logger.info("user reply cancelled: inactive chat")
                _update_pipeline_status(
                    pipeline,
                    category="pipeline2",
                    state="cancelled",
                    message=f"reply {reply.id}: inactive chat",
                )
            return
    for reply in due_replies:
        if reply.reply_to_message_id is None:
            mark_discussion_reply_cancelled(session, reply, "missing reply_to")
            _update_pipeline_status(
                pipeline,
                category="pipeline2",
                state="cancelled",
                message=f"reply {reply.id}: missing reply_to",
            )
            continue
        if settings.user_reply_max_age_minutes > 0:
            if reply.source_message_at:
                source_time = reply.source_message_at
                if source_time.tzinfo is None:
                    source_time = source_time.replace(tzinfo=timezone.utc)
                age = (now - source_time).total_seconds() / 60
            else:
                send_at = reply.send_at
                if send_at and send_at.tzinfo is None:
                    send_at = send_at.replace(tzinfo=timezone.utc)
                age = (now - send_at).total_seconds() / 60 if send_at else 0
            if age > settings.user_reply_max_age_minutes:
                mark_discussion_reply_cancelled(session, reply, "message too old")
                ref_ts = source_time if reply.source_message_at else reply.send_at
                logger.info(
                    "user reply cancelled: message too old (reply %s: age=%.1f min, limit=%s min, now_utc=%s, ref_utc=%s)",
                    reply.id,
                    age,
                    settings.user_reply_max_age_minutes,
                    now.isoformat(),
                    ref_ts.isoformat() if ref_ts else str(ref_ts),
                )
                _update_pipeline_status(
                    pipeline,
                    category="pipeline2",
                    state="cancelled",
                    message=f"reply {reply.id}: message too old",
                )
                continue
        account = accounts.get(reply.account_name)
        if not account:
            mark_discussion_reply_cancelled(session, reply, "account_missing")
            _update_pipeline_status(
                pipeline,
                category="pipeline2",
                state="cancelled",
                message=f"reply {reply.id}: account missing",
            )
            continue
        if not _can_use_bot_for_reply(session, pipeline.id, reply.account_name, now):
            mark_discussion_reply_cancelled(session, reply, "cooldown/limit")
            logger.info("user reply cancelled: cooldown/limits")
            _update_pipeline_status(
                pipeline,
                category="pipeline2",
                state="cancelled",
                message=f"reply {reply.id}: cooldown/limits",
            )
            continue
        _update_pipeline_status(
            pipeline,
            category="pipeline2",
            state="processing",
            message=f"sending reply {reply.id}",
        )
        try:
            sent = await send_reply_text(
                account.writer_client,
                reply.chat_id or settings.target_chat,
                reply.reply_text,
                reply_to_message_id=reply.reply_to_message_id,
                request_delay_seconds=account.behavior.TELEGRAM_REQUEST_DELAY_SECONDS,
                random_jitter_seconds=account.behavior.RANDOM_JITTER_SECONDS,
                flood_wait_antiblock=account.behavior.FLOOD_WAIT_ANTIBLOCK,
                flood_wait_max_seconds=account.behavior.FLOOD_WAIT_MAX_SECONDS,
                flood_wait_notify_after_seconds=pipeline.interval_seconds,
            )
        except Exception as exc:
            logger.exception("user reply cancelled: send failed")
            mark_discussion_reply_cancelled(
                session,
                reply,
                f"send failed: {exc.__class__.__name__}",
            )
            _update_pipeline_status(
                pipeline,
                category="pipeline2",
                state="cancelled",
                message=f"reply {reply.id}: send failed",
            )
            continue
        mark_discussion_reply_sent(session, reply, now)
        _update_bot_usage(session, pipeline.id, reply.account_name, now)
        logger.info(
            "user reply sent: bot %s -> %s",
            reply.account_name,
            getattr(sent, "id", None),
        )
        _update_pipeline_status(
            pipeline,
            category="pipeline2",
            state="sent",
            message=f"bot {reply.account_name} -> {getattr(sent, 'id', None)}",
        )
        await _try_set_reaction_on_chat_message(
            config,
            accounts,
            reply.account_name,
            reply.chat_id or settings.target_chat,
            reply.reply_to_message_id,
            "",  # message_text for sensitive check; empty is ok
            now,
        )


def _can_use_bot_for_reply(
    session: Session, pipeline_id: int, account_name: str, now: datetime
) -> bool:
    row = session.execute(
        select(DiscussionBotWeight).where(
            DiscussionBotWeight.pipeline_id == pipeline_id,
            DiscussionBotWeight.account_name == account_name,
        )
    ).scalar_one_or_none()
    if row is None:
        return False
    today = now.strftime("%Y-%m-%d")
    if row.used_today_date != today:
        row.used_today = 0
        row.used_today_date = today
    if row.used_today >= row.daily_limit:
        return False
    if row.last_used_at is not None:
        last_used = row.last_used_at
        if last_used.tzinfo is None:
            last_used = last_used.replace(tzinfo=timezone.utc)
        elapsed = (now - last_used).total_seconds() / 60
        if elapsed < row.cooldown_minutes:
            return False
    return True


async def _post_message(
    config: Config,
    account: AccountRuntime,
    message,
    apply_blackbox: bool,
    destination_channel: str,
    posting_mode: str,
    flood_wait_notify_after_seconds: int | None = None,
):
    """Post message to destination channel. Returns the sent Message or None if no single message was sent."""
    reader_client = account.reader_client
    writer_client = account.writer_client
    openai_client = account.openai_client
    footer_handle = destination_channel
    original_text = (message.message or "").strip()
    sent_msg = None
    if posting_mode == "PLAGIAT":
        final_text = _append_footer(original_text, footer_handle)
        if message.media:
            sent_msg = await send_media_from_message(
                reader_client,
                writer_client,
                destination_channel,
                message,
                final_text,
                request_delay_seconds=account.behavior.TELEGRAM_REQUEST_DELAY_SECONDS,
                random_jitter_seconds=account.behavior.RANDOM_JITTER_SECONDS,
                flood_wait_antiblock=account.behavior.FLOOD_WAIT_ANTIBLOCK,
                flood_wait_max_seconds=account.behavior.FLOOD_WAIT_MAX_SECONDS,
                flood_wait_notify_after_seconds=flood_wait_notify_after_seconds,
            )
        else:
            sent_msg = await send_text(
                writer_client,
                destination_channel,
                final_text,
                request_delay_seconds=account.behavior.TELEGRAM_REQUEST_DELAY_SECONDS,
                random_jitter_seconds=account.behavior.RANDOM_JITTER_SECONDS,
                flood_wait_antiblock=account.behavior.FLOOD_WAIT_ANTIBLOCK,
                flood_wait_max_seconds=account.behavior.FLOOD_WAIT_MAX_SECONDS,
                flood_wait_notify_after_seconds=flood_wait_notify_after_seconds,
            )
        _log_news_usage(
            final_text,
            openai_client.text_model,
            0,
            0,
            0,
            0.0,
            openai_client.image_model,
            0,
            0,
            0.0,
        )
        return sent_msg

    if posting_mode == "TEXT_MEDIA":
        if not original_text:
            final_text = _append_footer("", footer_handle)
            if message.media:
                sent_msg = await send_media_from_message(
                    reader_client,
                    writer_client,
                    destination_channel,
                    message,
                    final_text,
                    request_delay_seconds=account.behavior.TELEGRAM_REQUEST_DELAY_SECONDS,
                    random_jitter_seconds=account.behavior.RANDOM_JITTER_SECONDS,
                    flood_wait_antiblock=account.behavior.FLOOD_WAIT_ANTIBLOCK,
                    flood_wait_max_seconds=account.behavior.FLOOD_WAIT_MAX_SECONDS,
                    flood_wait_notify_after_seconds=flood_wait_notify_after_seconds,
                )
            else:
                sent_msg = await send_text(
                    writer_client,
                    destination_channel,
                    final_text,
                    request_delay_seconds=account.behavior.TELEGRAM_REQUEST_DELAY_SECONDS,
                    random_jitter_seconds=account.behavior.RANDOM_JITTER_SECONDS,
                    flood_wait_antiblock=account.behavior.FLOOD_WAIT_ANTIBLOCK,
                    flood_wait_max_seconds=account.behavior.FLOOD_WAIT_MAX_SECONDS,
                    flood_wait_notify_after_seconds=flood_wait_notify_after_seconds,
                )
            _log_news_usage(
                final_text,
                openai_client.text_model,
                0,
                0,
                0,
                0.0,
                openai_client.image_model,
                0,
                0,
                0.0,
            )
            return sent_msg
        text = original_text
        if apply_blackbox:
            text = f"[BLACKBOX]\n{text}"
        paraphrased, in_tokens, out_tokens, total_tokens = await asyncio.to_thread(
            openai_client.paraphrase_news, text
        )
        if apply_blackbox:
            paraphrased = _apply_blackbox_effect(
                paraphrased,
                ratio=config.BLACKBOX_WORD_RATIO,
                min_word_len=config.BLACKBOX_MIN_WORD_LEN,
                distort_min=config.BLACKBOX_DISTORT_MIN,
                distort_max=config.BLACKBOX_DISTORT_MAX,
            )
        final_text = _append_footer(paraphrased, footer_handle)
        if message.media:
            sent_msg = await send_media_from_message(
                reader_client,
                writer_client,
                destination_channel,
                message,
                final_text,
                request_delay_seconds=account.behavior.TELEGRAM_REQUEST_DELAY_SECONDS,
                random_jitter_seconds=account.behavior.RANDOM_JITTER_SECONDS,
                flood_wait_antiblock=account.behavior.FLOOD_WAIT_ANTIBLOCK,
                flood_wait_max_seconds=account.behavior.FLOOD_WAIT_MAX_SECONDS,
                flood_wait_notify_after_seconds=flood_wait_notify_after_seconds,
            )
        else:
            sent_msg = await send_text(
                writer_client,
                destination_channel,
                final_text,
                request_delay_seconds=account.behavior.TELEGRAM_REQUEST_DELAY_SECONDS,
                random_jitter_seconds=account.behavior.RANDOM_JITTER_SECONDS,
                flood_wait_antiblock=account.behavior.FLOOD_WAIT_ANTIBLOCK,
                flood_wait_max_seconds=account.behavior.FLOOD_WAIT_MAX_SECONDS,
                flood_wait_notify_after_seconds=flood_wait_notify_after_seconds,
            )
        text_cost = _estimate_text_cost(
            in_tokens,
            out_tokens,
            account.openai_settings.text_input_price_per_1m,
            account.openai_settings.text_output_price_per_1m,
        )
        _log_news_usage(
            final_text,
            openai_client.text_model,
            in_tokens,
            out_tokens,
            total_tokens,
            text_cost,
            openai_client.image_model,
            0,
            0,
            0.0,
        )
        return sent_msg

    text = original_text
    if apply_blackbox:
        text = f"[BLACKBOX]\n{text}"
    paraphrased, in_tokens, out_tokens, total_tokens = await asyncio.to_thread(
        openai_client.paraphrase_news, text
    )
    if apply_blackbox:
        paraphrased = _apply_blackbox_effect(
            paraphrased,
            ratio=config.BLACKBOX_WORD_RATIO,
            min_word_len=config.BLACKBOX_MIN_WORD_LEN,
            distort_min=config.BLACKBOX_DISTORT_MIN,
            distort_max=config.BLACKBOX_DISTORT_MAX,
        )
    paraphrased = _append_footer(paraphrased, footer_handle)
    image_tokens = 0
    image_count = 0
    text_cost = _estimate_text_cost(
        in_tokens,
        out_tokens,
        account.openai_settings.text_input_price_per_1m,
        account.openai_settings.text_output_price_per_1m,
    )
    image_cost = 0.0

    if posting_mode == "TEXT":
        sent_msg = await send_text(
            writer_client,
            destination_channel,
            paraphrased,
            request_delay_seconds=account.behavior.TELEGRAM_REQUEST_DELAY_SECONDS,
            random_jitter_seconds=account.behavior.RANDOM_JITTER_SECONDS,
            flood_wait_antiblock=account.behavior.FLOOD_WAIT_ANTIBLOCK,
            flood_wait_max_seconds=account.behavior.FLOOD_WAIT_MAX_SECONDS,
            flood_wait_notify_after_seconds=flood_wait_notify_after_seconds,
        )
        _log_news_usage(
            paraphrased,
            openai_client.text_model,
            in_tokens,
            out_tokens,
            total_tokens,
            text_cost,
            openai_client.image_model,
            image_tokens,
            image_count,
            image_cost,
        )
        return sent_msg

    if not message.photo:
        # In TEXT_IMAGE mode we fall back to text-only if the source has no image.
        sent_msg = await send_text(
            writer_client,
            destination_channel,
            paraphrased,
            request_delay_seconds=account.behavior.TELEGRAM_REQUEST_DELAY_SECONDS,
            random_jitter_seconds=account.behavior.RANDOM_JITTER_SECONDS,
            flood_wait_antiblock=account.behavior.FLOOD_WAIT_ANTIBLOCK,
            flood_wait_max_seconds=account.behavior.FLOOD_WAIT_MAX_SECONDS,
            flood_wait_notify_after_seconds=flood_wait_notify_after_seconds,
        )
        _log_news_usage(
            paraphrased,
            openai_client.text_model,
            in_tokens,
            out_tokens,
            total_tokens,
            text_cost,
            openai_client.image_model,
            image_tokens,
            image_count,
            image_cost,
        )
        return sent_msg

    image_bytes = await download_message_photo(reader_client, message)
    description = await asyncio.to_thread(
        openai_client.describe_image_for_news, image_bytes
    )
    generated_bytes, image_tokens = await asyncio.to_thread(
        openai_client.generate_image_from_description, description
    )
    image_count = 1
    image_cost = account.openai_settings.image_price_1024_usd
    sent_msg = await send_image_with_caption(
        writer_client,
        destination_channel,
        generated_bytes,
        paraphrased,
        request_delay_seconds=account.behavior.TELEGRAM_REQUEST_DELAY_SECONDS,
        random_jitter_seconds=account.behavior.RANDOM_JITTER_SECONDS,
        flood_wait_antiblock=account.behavior.FLOOD_WAIT_ANTIBLOCK,
        flood_wait_max_seconds=account.behavior.FLOOD_WAIT_MAX_SECONDS,
        flood_wait_notify_after_seconds=flood_wait_notify_after_seconds,
    )
    _log_news_usage(
        paraphrased,
        openai_client.text_model,
        in_tokens,
        out_tokens,
        total_tokens,
        text_cost,
        openai_client.image_model,
        image_tokens,
        image_count,
        image_cost,
    )
    return sent_msg


def _load_channels(session: Session) -> List[PipelineSource]:
    return (
        session.execute(select(PipelineSource).order_by(PipelineSource.id))
        .scalars()
        .all()
    )


def _append_footer(text: str, handle: str) -> str:
    normalized = text.strip()
    if not normalized:
        return handle
    if handle.lower() in normalized.lower():
        return normalized
    return f"{normalized} {handle}"


def _log_news_usage(
    text: str,
    text_model: str,
    input_tokens: int,
    output_tokens: int,
    total_tokens: int,
    text_cost_usd: float,
    image_model: str,
    image_tokens: int,
    image_count: int,
    image_cost_usd: float,
    path: str = "logs/news_usage.log",
) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        header = (
            "timestamp\ttext_model\tinput_tokens\toutput_tokens\ttotal_tokens\t"
            "text_cost_usd\timage_model\timage_tokens\timage_count\timage_cost_usd\t"
            "post_text\n"
        )
        with open(path, "a", encoding="utf-8") as file_handle:
            file_handle.write(header)
    timestamp = datetime.now(UFA_TZ).strftime("%Y-%m-%d %H:%M:%S")
    line = (
        f"{timestamp}\t{text_model}\t{input_tokens}\t{output_tokens}\t"
        f"{total_tokens}\t{text_cost_usd:.6f}\t"
        f"{image_model}\t{image_tokens}\t{image_count}\t{image_cost_usd:.6f}\t"
        f"{text}\n"
    )
    with open(path, "a", encoding="utf-8") as file_handle:
        file_handle.write(line)


def _estimate_text_cost(
    input_tokens: int, output_tokens: int, input_price: float, output_price: float
) -> float:
    return (input_tokens / 1_000_000) * input_price + (
        output_tokens / 1_000_000
    ) * output_price


def _apply_blackbox_effect(
    text: str,
    ratio: float,
    min_word_len: int,
    distort_min: int,
    distort_max: int,
) -> str:
    word_pattern = re.compile(rf"[A-Za-zА-Яа-яЁё]{{{min_word_len},}}")
    matches = list(word_pattern.finditer(text))
    if not matches:
        return text

    total_words = len(matches)
    target_count = max(1, int(total_words * ratio))
    candidate_indices = list(range(total_words))

    rng = random.Random()
    rng.shuffle(candidate_indices)

    selected = []
    blocked = set()
    for idx in candidate_indices:
        if idx in blocked:
            continue
        selected.append(idx)
        blocked.add(idx - 1)
        blocked.add(idx + 1)
        if len(selected) >= target_count:
            break

    if not selected:
        return text

    result = []
    last_end = 0
    for word_index, match in enumerate(matches):
        start, end = match.span()
        result.append(text[last_end:start])
        word = match.group(0)
        if word_index in selected:
            result.append(_distort_word(word, rng, distort_min, distort_max))
        else:
            result.append(word)
        last_end = end
    result.append(text[last_end:])
    return "".join(result)


def _distort_word(
    word: str, rng: random.Random, distort_min: int, distort_max: int
) -> str:
    positions = [i for i, char in enumerate(word) if char.isalpha()]
    if len(positions) < 2:
        return word
    distort_count = min(distort_max, max(distort_min, len(positions) // 2))
    rng.shuffle(positions)
    chosen = set(positions[:distort_count])
    chars = list(word)
    for idx in chosen:
        char = chars[idx]
        if char.islower():
            chars[idx] = char.upper()
        elif char.isupper():
            chars[idx] = char.lower()
    return "".join(chars)


_STOPWORDS_RU = {
    "и",
    "в",
    "во",
    "не",
    "что",
    "он",
    "на",
    "я",
    "с",
    "со",
    "как",
    "а",
    "то",
    "все",
    "она",
    "так",
    "его",
    "но",
    "да",
    "ты",
    "к",
    "у",
    "же",
    "вы",
    "за",
    "бы",
    "по",
    "ее",
    "мне",
    "было",
    "вот",
    "от",
    "меня",
    "еще",
    "нет",
    "о",
    "из",
    "ему",
    "теперь",
    "когда",
    "даже",
    "ну",
    "вдруг",
    "ли",
    "если",
    "уже",
    "или",
    "ни",
    "быть",
    "был",
    "него",
    "до",
    "вас",
    "нибудь",
    "опять",
    "уж",
    "вам",
    "ведь",
    "там",
    "потом",
    "себя",
    "ничего",
    "ей",
    "может",
    "они",
    "тут",
    "где",
    "есть",
    "надо",
    "ней",
    "для",
    "мы",
    "тебя",
    "их",
    "чем",
    "была",
    "сам",
    "чтоб",
    "без",
    "будто",
    "чего",
    "раз",
    "тоже",
    "себе",
    "под",
    "будет",
    "ж",
    "тогда",
    "кто",
    "этот",
    "того",
    "потому",
    "этого",
    "какой",
    "совсем",
    "ним",
    "здесь",
    "этом",
    "один",
    "почти",
    "мой",
    "тем",
    "чтобы",
    "нее",
    "сейчас",
    "были",
    "куда",
    "зачем",
    "всех",
    "никогда",
    "можно",
    "при",
    "наконец",
    "два",
    "об",
    "другой",
    "хоть",
    "после",
    "над",
    "больше",
    "тот",
    "через",
    "эти",
    "нас",
    "про",
    "всего",
    "них",
    "какая",
    "много",
    "разве",
    "три",
    "эту",
    "моя",
    "впрочем",
    "хорошо",
    "свою",
    "этой",
    "перед",
    "иногда",
    "лучше",
    "чуть",
    "том",
    "нельзя",
    "такой",
    "им",
    "более",
    "всегда",
    "конечно",
    "всю",
    "между",
}


def _is_similar_news_bm25(
    session: Session,
    pipeline_id: int,
    text: str,
    window_size: int,
    threshold: float,
) -> tuple[bool, float]:
    """Check if text is similar to recent posts in post_history (BM25).
    Excludes the candidate text itself from corpus to avoid self-match inflation.
    Returns (is_similar, max_score) for logging.
    """
    if window_size <= 0:
        return (False, 0.0)
    recent_texts = (
        session.execute(
            select(PostHistory.text)
            .where(PostHistory.pipeline_id == pipeline_id)
            .order_by(PostHistory.id.desc())
            .limit(window_size)
        )
        .scalars()
        .all()
    )
    if not recent_texts:
        return (False, 0.0)
    text_stripped = text.strip()
    recent_texts = [t for t in recent_texts if (t or "").strip() != text_stripped]
    if not recent_texts:
        return (False, 0.0)
    query_tokens = _tokenize(text)
    if not query_tokens:
        return (False, 0.0)
    corpus_tokens = []
    for item in recent_texts:
        tokens = _tokenize(item)
        if tokens:
            corpus_tokens.append(tokens)
    if not corpus_tokens:
        return (False, 0.0)
    bm25 = BM25Okapi(corpus_tokens)
    scores = bm25.get_scores(query_tokens)
    max_score = max(scores) if len(scores) > 0 else 0.0
    return (max_score >= threshold, max_score)


def _store_recent_post(
    session: Session,
    pipeline_id: int,
    text: str,
    window_size: int,
    *,
    destination_channel: str | None = None,
    channel_message_id: int | None = None,
) -> None:
    session.add(
        PostHistory(
            pipeline_id=pipeline_id,
            text=text,
            created_at=datetime.utcnow(),
            destination_channel=destination_channel,
            channel_message_id=channel_message_id,
        )
    )
    session.flush()
    if window_size <= 0:
        return
    excess_ids = (
        session.execute(
            select(PostHistory.id)
            .where(PostHistory.pipeline_id == pipeline_id)
            .order_by(PostHistory.id.desc())
            .offset(window_size)
        )
        .scalars()
        .all()
    )
    if excess_ids:
        session.execute(delete(PostHistory).where(PostHistory.id.in_(excess_ids)))


def _tokenize(text: str) -> list[str]:
    words = re.findall(r"[A-Za-zА-Яа-яЁё]+", text.lower())
    return [word for word in words if word not in _STOPWORDS_RU and len(word) > 3]


async def _sleep_between_cycles(min_seconds: float, max_seconds: float) -> None:
    if max_seconds <= 0:
        return
    if min_seconds <= 0:
        min_seconds = 0.0
    if max_seconds < min_seconds:
        max_seconds = min_seconds
    delay = random.uniform(min_seconds, max_seconds)
    if delay > 0:
        await asyncio.sleep(delay)


def _is_account_in_flood_wait(account: AccountRuntime, now: datetime) -> bool:
    if account.flood_wait_until is None:
        return False
    return now < account.flood_wait_until


async def _handle_flood_wait_block(
    config: Config,
    account: AccountRuntime,
    pipeline: Pipeline,
    seconds: int,
    bot_app,
) -> None:
    now = datetime.now(timezone.utc)
    until = now + timedelta(seconds=seconds)
    if account.flood_wait_until and until <= account.flood_wait_until:
        return
    account.flood_wait_until = until
    if account.flood_wait_notified_until and until <= account.flood_wait_notified_until:
        return
    account.flood_wait_notified_until = until
    await _notify_flood_wait(config, bot_app, account.name, pipeline.name, until, seconds)


async def _notify_flood_wait(
    config: Config,
    bot_app,
    account_name: str,
    pipeline_name: str,
    until: datetime,
    seconds: int,
) -> None:
    if bot_app is None:
        logger.warning(
            "Flood wait for account %s (%s), but bot is not running",
            account_name,
            pipeline_name,
        )
        return
    owners = [
        admin_id
        for admin_id, info in config.bot_admins().items()
        if str(info.get("role", "")).lower() == "owner"
    ]
    if not owners:
        logger.warning("Flood wait for account %s, no owner admins configured", account_name)
        return
    duration = _format_duration(seconds)
    until_local = until.astimezone(UFA_TZ).strftime("%Y-%m-%d %H:%M:%S")
    text = (
        "⚠️ FloodWait\n"
        f"Аккаунт: {account_name}\n"
        f"Пайплайн: {pipeline_name}\n"
        f"Срок: {duration}\n"
        f"До: {until_local} (UFA)\n"
        "Запросы приостановлены до окончания FloodWait."
    )
    for owner_id in owners:
        try:
            await bot_app.bot.send_message(chat_id=owner_id, text=text)
        except Exception:
            logger.exception("Failed to notify owner %s about flood wait", owner_id)


def _format_duration(total_seconds: int) -> str:
    remaining = max(0, int(total_seconds))
    days, rem = divmod(remaining, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}д")
    if hours:
        parts.append(f"{hours}ч")
    if minutes or not parts:
        parts.append(f"{minutes}м")
    return " ".join(parts)


_DEFAULT_AD_KEYWORDS = [
    "реклама",
    "акция",
    "скидк",
    "промокод",
    "по ссылке",
    "оформ",
    "подписк",
    "подарок",
    "кэшбэк",
    "кешбек",
    "рассрочк",
    "кредит",
    "банк",
    "карта",
    "выгодн",
    "партнер",
    "бесплатн",
    "услови",
    "доход",
]


def _is_ad_text(text: str, threshold: int, custom_keywords: str | None) -> bool:
    lowered = text.lower()
    keywords = _DEFAULT_AD_KEYWORDS
    if custom_keywords:
        parsed = [item.strip().lower() for item in custom_keywords.split(",")]
        keywords = [item for item in parsed if item]
    score = 0
    for keyword in keywords:
        if keyword and keyword in lowered:
            score += 1

    if re.search(r"https?://|t\\.me/|bit\\.ly|tinyurl\\.com", lowered):
        score += 2
    if re.search(r"\\b\\d+\\s*(₽|руб|руб\\.|р\\.)", lowered):
        score += 1
    if re.search(r"\\b\\d+%\\b", lowered):
        score += 1
    if re.search(r"\\bдо\\s+\\d+\\b", lowered):
        score += 1
    return score >= max(1, threshold)
