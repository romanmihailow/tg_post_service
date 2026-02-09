"""Main service loop for reading, paraphrasing, and posting news."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List

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
    get_recent_post_history,
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
    get_new_messages,
    pick_album_caption_message,
    send_reply_text,
    send_image_with_caption,
    send_media_from_message,
    send_text,
)

logger = logging.getLogger(__name__)
UFA_TZ = ZoneInfo("Asia/Yekaterinburg")


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
                for pipeline in discussion_pipelines:
                    account_name = pipeline.account_name or "default"
                    account = accounts.get(account_name)
                    if not account:
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
            "No new messages in pipeline %s source %s",
            pipeline.name,
            source.source_channel,
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
        if _is_similar_news_bm25(
            session,
            pipeline.id,
            original_text,
            config.DEDUP_WINDOW_SIZE,
            config.DEDUP_BM25_THRESHOLD,
        ):
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
        await _post_message(
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
    if config.DEDUP_ENABLED and original_text:
        _store_recent_post(session, pipeline.id, original_text, config.DEDUP_WINDOW_SIZE)
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
    candidates_all = get_recent_post_history(session, source_pipeline.id, k)
    if not candidates_all:
        logger.info(
            "discussion skipped: no candidate posts in post_history (pipeline=%s)",
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
    candidates = candidates_all
    if state.last_source_post_id and len(candidates_all) > 1:
        filtered = [
            item for item in candidates_all if item.id != state.last_source_post_id
        ]
        if filtered:
            candidates = filtered
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
        selected_index, in_t, out_t, total_t = await asyncio.to_thread(
            primary_account.openai_client.select_discussion_news,
            candidate_texts,
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
            message_id=selected_item.id,
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
            extra={"source": "pipeline1", "post_id": selected_item.id},
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
    state.last_source_post_id = selected_item.id
    state.last_source_post_at = selected_item.created_at
    state.expires_at = now + timedelta(minutes=60)
    state.replies_planned = len(replies)
    state.replies_sent = 0
    state.last_bot_reply_at = None
    state.last_reply_parent_id = question_message_id
    delay_factor = 1.5 - (discussion_level / 100.0)
    delay_factor = max(0.5, min(1.5, delay_factor))
    for idx, reply_text in enumerate(replies, start=1):
        # Planned chain for a single question; still keep persona roles per order.
        base_delay = _reply_delay_minutes(idx)
        adjusted_delay = max(1, int(round(base_delay * delay_factor)))
        send_at = now + timedelta(minutes=adjusted_delay)
        create_discussion_reply(
            session,
            pipeline_id=pipeline.id,
            account_name=selected_bots[idx - 1].account_name,
            reply_text=reply_text,
            send_at=send_at,
            reply_to_message_id=None,
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
    if existing:
        return existing
    for account_name in accounts:
        if account_name == exclude_account:
            continue
        upsert_discussion_bot_weight(
            session,
            pipeline_id=pipeline_id,
            account_name=account_name,
            weight=1,
            daily_limit=5,
            cooldown_minutes=60,
        )
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


def _format_persona_for_prompt(session: Session, account_name: str) -> str:
    persona = get_userbot_persona(session, account_name)
    tone = (persona.persona_tone if persona and persona.persona_tone else "neutral")
    verbosity = (
        persona.persona_verbosity if persona and persona.persona_verbosity else "short"
    )
    style_hint = (
        persona.persona_style_hint if persona and persona.persona_style_hint else None
    )
    instructions = [f"userbot {account_name}"]
    if tone == "analytical":
        instructions.append("tone: спокойный, взвешенный, без эмоций")
    elif tone == "emotional":
        instructions.append("tone: мягко эмоциональный, без сленга и восклицаний")
    elif tone == "ironic":
        instructions.append("tone: легкая ирония без сарказма")
    elif tone == "skeptical":
        instructions.append("tone: умеренный скепсис без агрессии")
    else:
        instructions.append("tone: нейтральный")
    if verbosity == "medium":
        instructions.append("verbosity: 1-2 предложения без воды")
    else:
        instructions.append("verbosity: 1 короткое предложение")
    instructions.append(
        "ограничения: без сленга, без эмодзи, без капса, без упоминания бота/ИИ"
    )
    if style_hint:
        instructions.append(f"style_hint: {style_hint}")
    topics, _, _ = _load_persona_interest(session, account_name)
    if topics:
        instructions.append(
            "interests: "
            + ", ".join(topics)
            + " (мягкое предпочтение, без жесткой привязки)"
        )
    return " | ".join(instructions)


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
    if chat_state.next_scan_at and now < chat_state.next_scan_at:
        minutes_left = int((chat_state.next_scan_at - now).total_seconds() / 60)
        _update_pipeline_status(
            pipeline,
            category="pipeline2",
            state="waiting",
            next_action_at=chat_state.next_scan_at,
            message=f"next scan in ~{minutes_left} min",
        )
        return
    candidates = await _scan_chat_for_candidates(
        accounts,
        primary_account,
        chat_state,
        settings.target_chat,
    )
    chat_state.next_scan_at = now + timedelta(seconds=random.randint(30, 60))
    if not candidates:
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
    for candidate in candidates:
        await _plan_user_reply_for_candidate(
            config,
            accounts,
            primary_account,
            session,
            pipeline,
            settings,
            chat_state,
            candidate,
        )


async def _scan_chat_for_candidates(
    accounts: dict[str, AccountRuntime],
    primary_account: AccountRuntime,
    chat_state: ChatState,
    chat_id: str,
) -> list[dict]:
    bot_ids = await _collect_bot_user_ids(accounts)
    last_seen = chat_state.last_seen_message_id or 0
    messages = []
    async for message in primary_account.reader_client.iter_messages(
        chat_id, min_id=last_seen, limit=50
    ):
        messages.append(message)
    if not messages:
        return []
    messages = sorted(messages, key=lambda item: item.id)
    candidates = []
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
    chat_state.last_seen_message_id = max_id
    return candidates


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
) -> None:
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
        return
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
        return
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
            return
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
            return
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
        return
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
    for idx, bot_weight in enumerate(selected_bots, start=1):
        account = accounts.get(bot_weight.account_name)
        if not account:
            continue
        try:
            reply_text, _, _, _ = await asyncio.to_thread(
                account.openai_client.generate_user_reply,
                source_text=candidate["text"],
                context_messages=context_messages,
                role_label=_format_persona_for_prompt(session, bot_weight.account_name),
                pipeline_id=pipeline.id,
                chat_id=candidate["chat_id"],
                extra={"source": "pipeline2", "reply_to_message_id": candidate["message_id"]},
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
        logger.info(
            "user reply scheduled: bot %s at %s",
            bot_weight.account_name,
            send_at.isoformat(),
        )


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
                age = (now - reply.send_at).total_seconds() / 60
            if age > settings.user_reply_max_age_minutes:
                mark_discussion_reply_cancelled(session, reply, "message too old")
                logger.info("user reply cancelled: message too old")
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
        except Exception:
            logger.exception("user reply cancelled: send failed")
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
        elapsed = (now - row.last_used_at).total_seconds() / 60
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
) -> None:
    reader_client = account.reader_client
    writer_client = account.writer_client
    openai_client = account.openai_client
    footer_handle = destination_channel
    original_text = (message.message or "").strip()
    if posting_mode == "PLAGIAT":
        final_text = _append_footer(original_text, footer_handle)
        if message.media:
            await send_media_from_message(
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
            await send_text(
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
        return

    if posting_mode == "TEXT_MEDIA":
        if not original_text:
            final_text = _append_footer("", footer_handle)
            if message.media:
                await send_media_from_message(
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
                await send_text(
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
            return
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
            await send_media_from_message(
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
            await send_text(
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
        return

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
        await send_text(
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
        return

    if not message.photo:
        # In TEXT_IMAGE mode we fall back to text-only if the source has no image.
        await send_text(
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
        return

    image_bytes = await download_message_photo(reader_client, message)
    description = await asyncio.to_thread(
        openai_client.describe_image_for_news, image_bytes
    )
    generated_bytes, image_tokens = await asyncio.to_thread(
        openai_client.generate_image_from_description, description
    )
    image_count = 1
    image_cost = account.openai_settings.image_price_1024_usd
    await send_image_with_caption(
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
) -> bool:
    if window_size <= 0:
        return False
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
        return False
    query_tokens = _tokenize(text)
    if not query_tokens:
        return False
    corpus_tokens = []
    for item in recent_texts:
        tokens = _tokenize(item)
        if tokens:
            corpus_tokens.append(tokens)
    if not corpus_tokens:
        return False
    bm25 = BM25Okapi(corpus_tokens)
    scores = bm25.get_scores(query_tokens)
    max_score = max(scores) if len(scores) > 0 else 0.0
    return max_score >= threshold


def _store_recent_post(
    session: Session, pipeline_id: int, text: str, window_size: int
) -> None:
    session.add(
        PostHistory(
            pipeline_id=pipeline_id,
            text=text,
            created_at=datetime.utcnow(),
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
