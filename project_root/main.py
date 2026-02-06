"""Service entry point."""

from __future__ import annotations

import asyncio
import logging
import sys

from project_root.config import Config
from project_root.db import init_db
from project_root.openai_client import OpenAIClient
from project_root.scheduler import run_service
from project_root.telegram_client import create_client


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


async def main_async() -> None:
    setup_logging()
    config = Config()
    logging.getLogger(__name__).info("Configuration loaded, starting service")
    init_db(config)

    tg_client = await create_client(
        api_id=config.TELEGRAM_API_ID,
        api_hash=config.TELEGRAM_API_HASH,
        session_name=config.TELEGRAM_SESSION_NAME,
    )
    openai_client = OpenAIClient(api_key=config.OPENAI_API_KEY)

    try:
        await run_service(config, tg_client, openai_client)
    finally:
        await tg_client.disconnect()


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        sys.stdout.write("Service остановлен пользователем.\n")


if __name__ == "__main__":
    main()
