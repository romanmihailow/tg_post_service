"""Service entry point."""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from project_root.config import Config
from project_root.db import init_db
from project_root.openai_client import OpenAIClient
from project_root.scheduler import run_service
from project_root.telegram_client import create_client
from project_root.runtime import AccountRuntime
from project_root.bot_service import start_bot, stop_bot


def setup_logging() -> None:
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "service.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )


async def main_async() -> None:
    setup_logging()
    config = Config()
    config.apply_behavior_profiles()
    logging.getLogger(__name__).info("Configuration loaded, starting service")
    init_db(config)
    accounts = await _build_account_runtimes(config)
    bot_app = None
    if config.TG_BOT_TOKEN:
        bot_app = await start_bot(config, accounts)

    try:
        await run_service(config, accounts, bot_app)
    finally:
        await stop_bot(bot_app)
        await _disconnect_accounts(accounts)


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        sys.stdout.write("Service остановлен пользователем.\n")


def _load_system_prompt(path: str) -> str:
    if not os.path.exists(path):
        raise FileNotFoundError(f"System prompt file not found: {path}")
    with open(path, "r", encoding="utf-8") as file_handle:
        content = file_handle.read().strip()
    if not content:
        raise ValueError(f"System prompt file is empty: {path}")
    return content


async def _build_account_runtimes(config: Config) -> dict[str, AccountRuntime]:
    runtimes: dict[str, AccountRuntime] = {}
    for account in config.telegram_accounts():
        behavior = config.resolve_behavior_settings(account.behavior)
        openai_settings = config.resolve_openai_settings(account.openai)
        system_prompt = _load_system_prompt(openai_settings.system_prompt_path)
        openai_client = OpenAIClient(
            api_key=openai_settings.api_key,
            system_prompt=system_prompt,
            text_model=openai_settings.text_model,
            vision_model=openai_settings.vision_model,
            image_model=openai_settings.image_model,
        )
        # Pipeline 2 (live replies): optional separate system prompt for chat
        system_prompt_chat = None
        chat_path = getattr(config, "OPENAI_SYSTEM_PROMPT_CHAT_PATH", None) or "prompts/system_prompt_chat.txt"
        if chat_path and os.path.exists(chat_path):
            try:
                system_prompt_chat = _load_system_prompt(chat_path)
            except Exception as load_err:
                logging.getLogger(__name__).warning(
                    "Account %s: could not load chat system prompt from %s: %s",
                    account.name, chat_path, load_err,
                )
        reader_client = await create_client(
            api_id=account.reader.api_id,
            api_hash=account.reader.api_hash,
            session_name=account.reader.session,
            start=False,
        )
        if not await reader_client.is_user_authorized():
            logging.getLogger(__name__).warning(
                "Account %s: reader session not authorized, skipping",
                account.name,
            )
            await reader_client.disconnect()
            continue
        try:
            me = await reader_client.get_me()
            user_id = int(me.id) if me and getattr(me, "id", None) is not None else None
            username = str(getattr(me, "username", None)) if me else None
        except Exception:
            user_id = None
            username = None
        if account.writer:
            writer_client = await create_client(
                api_id=account.writer.api_id,
                api_hash=account.writer.api_hash,
                session_name=account.writer.session,
                start=False,
            )
            if not await writer_client.is_user_authorized():
                logging.getLogger(__name__).warning(
                    "Account %s: writer session not authorized, using reader",
                    account.name,
                )
                await writer_client.disconnect()
                writer_client = reader_client
        else:
            writer_client = reader_client
        runtimes[account.name] = AccountRuntime(
            name=account.name,
            reader_client=reader_client,
            writer_client=writer_client,
            openai_client=openai_client,
            behavior=behavior,
            openai_settings=openai_settings,
            user_id=user_id,
            username=username,
            system_prompt_chat=system_prompt_chat,
        )
    return runtimes


async def _disconnect_accounts(accounts: dict[str, AccountRuntime]) -> None:
    disconnected = set()
    for runtime in accounts.values():
        for client in (runtime.reader_client, runtime.writer_client):
            if client in disconnected:
                continue
            disconnected.add(client)
            await client.disconnect()


if __name__ == "__main__":
    main()
