"""Telegram client helpers using Telethon."""

from __future__ import annotations

import asyncio
import io
import logging
from typing import List

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.custom.message import Message

from project_root.models import SourceChannel

logger = logging.getLogger(__name__)


async def create_client(api_id: int, api_hash: str, session_name: str) -> TelegramClient:
    """Create and start a Telethon client."""
    client = TelegramClient(session_name, api_id, api_hash)
    await client.start()
    return client


async def get_new_messages(
    client: TelegramClient,
    source_channel: SourceChannel,
    min_text_length: int,
    require_image: bool,
    limit: int = 50,
    request_delay_seconds: float = 0.0,
    flood_wait_antiblock: bool = True,
    flood_wait_max_seconds: int = 300,
) -> List[Message]:
    """Fetch new messages that satisfy text length and media requirements."""
    min_id = source_channel.last_message_id or 0
    messages: List[Message] = []
    try:
        await _sleep_if_needed(request_delay_seconds)
        async for message in client.iter_messages(
            source_channel.channel_identifier, min_id=min_id, limit=limit
        ):
            text = message.message or ""
            if len(text.strip()) < min_text_length:
                continue
            if require_image and not message.photo:
                continue
            messages.append(message)
    except FloodWaitError as exc:
        if not flood_wait_antiblock:
            raise
        wait_for = min(exc.seconds, flood_wait_max_seconds)
        logger.warning("Flood wait from Telegram: sleeping %s seconds", wait_for)
        await client.sleep(wait_for)
        # We return empty list to skip this cycle after a flood wait.
        return []
    return messages


async def download_message_photo(client: TelegramClient, message: Message) -> bytes:
    """Download a message photo into memory and return bytes."""
    buffer = io.BytesIO()
    await client.download_media(message, file=buffer)
    return buffer.getvalue()


async def send_text(
    client: TelegramClient,
    dest_channel: str,
    text: str,
    request_delay_seconds: float = 0.0,
    flood_wait_antiblock: bool = True,
    flood_wait_max_seconds: int = 300,
) -> None:
    """Send a text message to the destination channel."""
    await _sleep_if_needed(request_delay_seconds)
    await _send_with_flood_wait(
        client,
        lambda: client.send_message(dest_channel, text),
        flood_wait_antiblock,
        flood_wait_max_seconds,
    )


async def send_image_with_caption(
    client: TelegramClient,
    dest_channel: str,
    image_bytes: bytes,
    caption: str,
    request_delay_seconds: float = 0.0,
    flood_wait_antiblock: bool = True,
    flood_wait_max_seconds: int = 300,
) -> None:
    """Send an image with caption to the destination channel."""
    await _sleep_if_needed(request_delay_seconds)
    buffer = io.BytesIO(image_bytes)
    buffer.name = "image.png"
    await _send_with_flood_wait(
        client,
        lambda: client.send_file(dest_channel, file=buffer, caption=caption),
        flood_wait_antiblock,
        flood_wait_max_seconds,
    )


async def _send_with_flood_wait(
    client: TelegramClient,
    action,
    flood_wait_antiblock: bool,
    flood_wait_max_seconds: int,
) -> None:
    for attempt in range(2):
        try:
            await action()
            return
        except FloodWaitError as exc:
            if not flood_wait_antiblock:
                raise
            wait_for = min(exc.seconds, flood_wait_max_seconds)
            logger.warning("Flood wait on send: sleeping %s seconds", wait_for)
            await client.sleep(wait_for)
            if attempt == 1:
                raise


async def _sleep_if_needed(delay_seconds: float) -> None:
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
