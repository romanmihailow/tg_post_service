"""Main service loop for reading, paraphrasing, and posting news."""

from __future__ import annotations

import asyncio
import logging
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session

from project_root.config import Config
from project_root.db import get_session
from project_root.models import GlobalState, SourceChannel
from project_root.openai_client import OpenAIClient
from project_root.telegram_client import (
    download_message_photo,
    get_new_messages,
    send_image_with_caption,
    send_text,
)

logger = logging.getLogger(__name__)


async def run_service(config: Config, tg_client, openai_client: OpenAIClient) -> None:
    """Run the main loop indefinitely."""
    while True:
        try:
            logger.info("Starting processing cycle")
            for _ in range(config.MAX_POSTS_PER_RUN):
                posted = await _process_once(config, tg_client, openai_client)
                if not posted:
                    break
        except Exception:
            logger.exception("Unexpected error in main loop")
        logger.info(
            "Cycle complete, sleeping for %s seconds", config.POSTING_INTERVAL_SECONDS
        )
        await asyncio.sleep(config.POSTING_INTERVAL_SECONDS)


async def _process_once(
    config: Config, tg_client, openai_client: OpenAIClient
) -> bool:
    with get_session() as session:
        channels = _load_channels(session)
        if not channels:
            logger.warning("No source channels configured in database")
            return False

        state = session.get(GlobalState, 1)
        if state is None:
            logger.error("Global state missing, re-initializing")
            state = GlobalState(id=1, current_channel_index=0)
            session.add(state)
            session.commit()

        index = state.current_channel_index % len(channels)
        source = channels[index]
        # Even in TEXT_IMAGE mode, we allow text-only posts if the source has no image.
        require_image = False
        logger.info(
            "Selected source channel %s (index %s)", source.channel_identifier, index
        )

        messages = await get_new_messages(
            tg_client,
            source_channel=source,
            min_text_length=config.MIN_TEXT_LENGTH,
            require_image=require_image,
            limit=config.TELEGRAM_HISTORY_LIMIT,
            request_delay_seconds=config.TELEGRAM_REQUEST_DELAY_SECONDS,
            flood_wait_antiblock=config.FLOOD_WAIT_ANTIBLOCK,
            flood_wait_max_seconds=config.FLOOD_WAIT_MAX_SECONDS,
        )

        if not messages:
            # We do not advance the round-robin index if there are no suitable messages.
            logger.info("No new messages in channel %s", source.channel_identifier)
            return False

        # Telethon returns newest-to-oldest; we pick the oldest to preserve order.
        message = messages[-1]
        next_post_counter = state.post_counter + 1
        apply_blackbox = (
            config.BLACKBOX_EVERY_N_POSTS > 0
            and next_post_counter % config.BLACKBOX_EVERY_N_POSTS == 0
        )
        try:
            await _post_message(config, tg_client, openai_client, message, apply_blackbox)
        except Exception:
            logger.exception("Failed to process or send message for channel %s", source.channel_identifier)
            session.rollback()
            return False

        source.last_message_id = message.id
        state.current_channel_index = (index + 1) % len(channels)
        state.post_counter = next_post_counter
        session.commit()
        return True


async def _post_message(
    config: Config,
    tg_client,
    openai_client: OpenAIClient,
    message,
    apply_blackbox: bool,
) -> None:
    text = (message.message or "").strip()
    if apply_blackbox:
        text = f"[BLACKBOX]\n{text}"
    paraphrased = await asyncio.to_thread(openai_client.paraphrase_news, text)

    if config.POSTING_MODE == "TEXT":
        await send_text(
            tg_client,
            config.DEST_CHANNEL,
            paraphrased,
            request_delay_seconds=config.TELEGRAM_REQUEST_DELAY_SECONDS,
            flood_wait_antiblock=config.FLOOD_WAIT_ANTIBLOCK,
            flood_wait_max_seconds=config.FLOOD_WAIT_MAX_SECONDS,
        )
        return

    if not message.photo:
        # In TEXT_IMAGE mode we fall back to text-only if the source has no image.
        await send_text(
            tg_client,
            config.DEST_CHANNEL,
            paraphrased,
            request_delay_seconds=config.TELEGRAM_REQUEST_DELAY_SECONDS,
            flood_wait_antiblock=config.FLOOD_WAIT_ANTIBLOCK,
            flood_wait_max_seconds=config.FLOOD_WAIT_MAX_SECONDS,
        )
        return

    image_bytes = await download_message_photo(tg_client, message)
    description = await asyncio.to_thread(
        openai_client.describe_image_for_news, image_bytes
    )
    generated_bytes = await asyncio.to_thread(
        openai_client.generate_image_from_description, description
    )
    await send_image_with_caption(
        tg_client,
        config.DEST_CHANNEL,
        generated_bytes,
        paraphrased,
        request_delay_seconds=config.TELEGRAM_REQUEST_DELAY_SECONDS,
        flood_wait_antiblock=config.FLOOD_WAIT_ANTIBLOCK,
        flood_wait_max_seconds=config.FLOOD_WAIT_MAX_SECONDS,
    )


def _load_channels(session: Session) -> List[SourceChannel]:
    return session.execute(select(SourceChannel).order_by(SourceChannel.id)).scalars().all()
