"""One-off script: print account names from config and pipelines from DB.
Run from repo root: python -m project_root.check_accounts_and_pipelines
"""
from __future__ import annotations

import os
import sys

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from project_root.config import Config
from project_root.db import get_all_pipelines, init_db, get_session


def main() -> None:
    config = Config()
    account_names = [a.name for a in config.telegram_accounts()]
    print("Account names from config.telegram_accounts():", account_names)

    init_db(config)
    with get_session() as session:
        pipelines = get_all_pipelines(session)
        print("Pipelines from DB (id, name, pipeline_type, account_name):")
        for p in pipelines:
            print(f"  {p.id} | {p.name!r} | {p.pipeline_type!r} | {p.account_name!r}")

    # Check discuss_news_blackbox
    with get_session() as session:
        pipelines = get_all_pipelines(session)
        discuss = next((p for p in pipelines if p.name == "discuss_news_blackbox"), None)
        if discuss:
            in_config = discuss.account_name in account_names
            print(f"\ndiscuss_news_blackbox: account_name={discuss.account_name!r}, exists in config: {in_config}")
        else:
            print("\ndiscuss_news_blackbox: not found in DB")


if __name__ == "__main__":
    main()
