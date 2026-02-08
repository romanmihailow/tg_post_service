#!/usr/bin/env python3
"""Authorize a Telegram account using the configured session path."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from telethon.sync import TelegramClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from project_root.config import Config


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Authorize a Telegram account using .env config",
    )
    parser.add_argument(
        "--account",
        help="Account name from TELEGRAM_ACCOUNTS_JSON (e.g. t9083516765)",
    )
    parser.add_argument(
        "--role",
        choices=("reader", "writer"),
        default="reader",
        help="Which credentials to use (default: reader)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available accounts and session paths",
    )
    return parser.parse_args()


def _list_accounts(config: Config) -> None:
    accounts = config.telegram_accounts()
    print("Accounts:")
    for account in accounts:
        writer_session = (
            account.writer.session if account.writer else "<not set>"
        )
        print(
            f"- {account.name}: reader={account.reader.session} writer={writer_session}"
        )


def _select_account(config: Config, name: str):
    for account in config.telegram_accounts():
        if account.name == name:
            return account
    return None


def main() -> int:
    args = _parse_args()
    config = Config()

    if args.list:
        _list_accounts(config)
        return 0

    if not args.account:
        print("Error: --account is required unless --list is used.")
        return 2

    account = _select_account(config, args.account)
    if not account:
        print(f"Error: account '{args.account}' not found in TELEGRAM_ACCOUNTS_JSON.")
        return 2

    if args.role == "writer":
        if not account.writer:
            print(f"Error: account '{account.name}' has no writer credentials.")
            return 2
        credentials = account.writer
    else:
        credentials = account.reader

    session_path = credentials.session
    print(f"Using session: {session_path}")
    client = TelegramClient(session_path, credentials.api_id, credentials.api_hash)
    client.start()
    print("OK: session created and authorized")
    client.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
