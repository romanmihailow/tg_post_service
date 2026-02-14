"""Telegram client helpers using Telethon."""

from __future__ import annotations

import asyncio
import io
import logging
import random
from typing import List

from telethon import TelegramClient, functions, types
from telethon.errors import FloodWaitError, ChannelPrivateError, ChatWriteForbiddenError
from telethon.tl.custom.message import Message

from project_root.config import resolve_session_path

logger = logging.getLogger(__name__)


async def set_message_reaction(
    client: TelegramClient,
    chat_id: str,
    message_id: int,
    emoji: str,
) -> bool:
    """Set reaction (emoji) on a message. Returns True on success, False on permission/API error."""
    if not emoji or not emoji.strip():
        return False
    try:
        await client(
            functions.messages.SendReactionRequest(
                peer=chat_id,
                msg_id=message_id,
                reaction=[types.ReactionEmoji(emoticon=emoji.strip())],
            )
        )
        return True
    except (ChannelPrivateError, ChatWriteForbiddenError, ValueError) as e:
        why = "reactions_not_allowed" if isinstance(e, ChatWriteForbiddenError) else "no_permission"
        logger.warning(
            "reaction failed why=%s chat=%s msg_id=%s emoji=%s err_type=%s err=%s",
            why,
            chat_id,
            message_id,
            emoji,
            type(e).__name__,
            e,
        )
        return False
    except FloodWaitError as exc:
        logger.warning(
            "reaction flood wait: chat=%s msg_id=%s sleep=%s",
            chat_id,
            message_id,
            exc.seconds,
        )
        return False
    except Exception as e:
        logger.warning(
            "reaction failed why=api_error chat=%s msg_id=%s emoji=%s err_type=%s err=%s",
            chat_id,
            message_id,
            emoji,
            type(e).__name__,
            e,
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.exception("reaction exception detail")
        return False


class FloodWaitBlocked(Exception):
    def __init__(self, seconds: int) -> None:
        super().__init__(f"Flood wait blocked for {seconds} seconds")
        self.seconds = seconds


async def create_client(
    api_id: int,
    api_hash: str,
    session_name: str,
    *,
    start: bool = True,
) -> TelegramClient:
    """Create a Telethon client. Session path без директории ведёт в sessions/ (volume в контейнере)."""
    path = resolve_session_path(session_name)
    client = TelegramClient(path, api_id, api_hash)
    if start:
        await client.start()
    else:
        await client.connect()
    return client


async def get_new_messages(
    client: TelegramClient,
    source_channel: str,
    last_message_id: int | None,
    min_text_length: int,
    require_image: bool,
    limit: int = 50,
    request_delay_seconds: float = 0.0,
    random_jitter_seconds: float = 0.0,
    flood_wait_antiblock: bool = True,
    flood_wait_max_seconds: int = 300,
    flood_wait_notify_after_seconds: int | None = None,
) -> List[Message]:
    """Fetch new messages that satisfy text length and media requirements."""
    min_id = last_message_id or 0
    messages: List[Message] = []
    try:
        await _sleep_if_needed(request_delay_seconds, random_jitter_seconds)
        async for message in client.iter_messages(
            source_channel, min_id=min_id, limit=limit
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
        if (
            flood_wait_notify_after_seconds is not None
            and exc.seconds >= flood_wait_notify_after_seconds
        ):
            raise FloodWaitBlocked(exc.seconds) from exc
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
    random_jitter_seconds: float = 0.0,
    flood_wait_antiblock: bool = True,
    flood_wait_max_seconds: int = 300,
    flood_wait_notify_after_seconds: int | None = None,
) -> None:
    """Send a text message to the destination channel."""
    await _sleep_if_needed(request_delay_seconds, random_jitter_seconds)
    await _send_with_flood_wait(
        client,
        lambda: client.send_message(dest_channel, text),
        flood_wait_antiblock,
        flood_wait_max_seconds,
        flood_wait_notify_after_seconds,
    )


async def send_reply_text(
    client: TelegramClient,
    dest_channel: str,
    text: str,
    reply_to_message_id: int | None,
    request_delay_seconds: float = 0.0,
    random_jitter_seconds: float = 0.0,
    flood_wait_antiblock: bool = True,
    flood_wait_max_seconds: int = 300,
    flood_wait_notify_after_seconds: int | None = None,
) -> Message:
    """Send a text message with an optional reply_to and return Message."""
    await _sleep_if_needed(request_delay_seconds, random_jitter_seconds)
    return await _send_with_flood_wait(
        client,
        lambda: client.send_message(dest_channel, text, reply_to=reply_to_message_id),
        flood_wait_antiblock,
        flood_wait_max_seconds,
        flood_wait_notify_after_seconds,
    )


async def send_image_with_caption(
    client: TelegramClient,
    dest_channel: str,
    image_bytes: bytes,
    caption: str,
    request_delay_seconds: float = 0.0,
    random_jitter_seconds: float = 0.0,
    flood_wait_antiblock: bool = True,
    flood_wait_max_seconds: int = 300,
    flood_wait_notify_after_seconds: int | None = None,
) -> None:
    """Send an image with caption to the destination channel."""
    await _sleep_if_needed(request_delay_seconds, random_jitter_seconds)
    buffer = io.BytesIO(image_bytes)
    buffer.name = "image.png"
    await _send_with_flood_wait(
        client,
        lambda: client.send_file(dest_channel, file=buffer, caption=caption),
        flood_wait_antiblock,
        flood_wait_max_seconds,
        flood_wait_notify_after_seconds,
    )


async def send_media_from_message(
    reader_client: TelegramClient,
    writer_client: TelegramClient,
    dest_channel: str,
    message: Message,
    caption: str,
    request_delay_seconds: float = 0.0,
    random_jitter_seconds: float = 0.0,
    flood_wait_antiblock: bool = True,
    flood_wait_max_seconds: int = 300,
    flood_wait_notify_after_seconds: int | None = None,
) -> None:
    """Send media from an existing message with a caption."""
    await _sleep_if_needed(request_delay_seconds, random_jitter_seconds)
    album_messages = await _collect_album_messages(reader_client, message)
    if len(album_messages) > 1:
        files = [item.media for item in album_messages if item.media]
        if not files:
            return
        await _send_with_flood_wait(
            writer_client,
            lambda: writer_client.send_file(dest_channel, file=files, caption=caption),
            flood_wait_antiblock,
            flood_wait_max_seconds,
            flood_wait_notify_after_seconds,
        )
        return
    await _send_with_flood_wait(
        writer_client,
        lambda: writer_client.send_file(dest_channel, file=message.media, caption=caption),
        flood_wait_antiblock,
        flood_wait_max_seconds,
        flood_wait_notify_after_seconds,
    )


async def pick_album_caption_message(
    client: TelegramClient, message: Message, limit: int = 20
) -> Message:
    """Pick a message with caption text from a media album."""
    if not message.grouped_id:
        return message
    album_messages = await _collect_album_messages(client, message, limit=limit)
    for item in album_messages:
        text = (item.message or "").strip()
        if text:
            return item
    return message


async def _send_with_flood_wait(
    client: TelegramClient,
    action,
    flood_wait_antiblock: bool,
    flood_wait_max_seconds: int,
    flood_wait_notify_after_seconds: int | None,
) -> object:
    for attempt in range(2):
        try:
            return await action()
        except FloodWaitError as exc:
            if not flood_wait_antiblock:
                raise
            if (
                flood_wait_notify_after_seconds is not None
                and exc.seconds >= flood_wait_notify_after_seconds
            ):
                raise FloodWaitBlocked(exc.seconds) from exc
            wait_for = min(exc.seconds, flood_wait_max_seconds)
            logger.warning("Flood wait on send: sleeping %s seconds", wait_for)
            await client.sleep(wait_for)
            if attempt == 1:
                raise
    raise RuntimeError("Failed to send message after flood wait")


async def _collect_album_messages(
    client: TelegramClient, message: Message, limit: int = 20
) -> List[Message]:
    if not message.grouped_id:
        return [message]
    min_id = max(0, message.id - limit)
    max_id = message.id + limit
    collected: List[Message] = []
    async for msg in client.iter_messages(
        message.peer_id, min_id=min_id, max_id=max_id, reverse=True
    ):
        if msg.grouped_id == message.grouped_id and msg.media:
            collected.append(msg)
    if not collected:
        return [message]
    return sorted(collected, key=lambda item: item.id)


async def _sleep_if_needed(delay_seconds: float, jitter_seconds: float = 0.0) -> None:
    if delay_seconds <= 0 and jitter_seconds <= 0:
        return
    jitter = random.uniform(0, jitter_seconds) if jitter_seconds > 0 else 0.0
    await asyncio.sleep(delay_seconds + jitter)
