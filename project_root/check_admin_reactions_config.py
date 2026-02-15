#!/usr/bin/env python3
"""Self-check: print ADMIN_* reaction config and warn if misconfigured."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from project_root.config import Config


def main() -> None:
    config = Config()
    print("Admin reactions (on channel post when Pipeline 1 publishes question) — current config")
    print("  ADMIN_REACTIONS_ENABLED:", getattr(config, "ADMIN_REACTIONS_ENABLED", False))
    print("  ADMIN_REACTION_ACCOUNT_NAME:", repr(getattr(config, "ADMIN_REACTION_ACCOUNT_NAME", None)))
    print("  ADMIN_REACTION_EMOJI:", getattr(config, "ADMIN_REACTION_EMOJI", "👀"))
    print("  ADMIN_REACTION_FALLBACK_EMOJI:", getattr(config, "ADMIN_REACTION_FALLBACK_EMOJI", "👍"))
    print("  ADMIN_REACTION_SKIP_IF_UNAVAILABLE:", getattr(config, "ADMIN_REACTION_SKIP_IF_UNAVAILABLE", False))
    print()
    if getattr(config, "ADMIN_REACTIONS_ENABLED", False):
        account = getattr(config, "ADMIN_REACTION_ACCOUNT_NAME", None) or ""
        if not account.strip():
            print("  WARNING: ADMIN_REACTIONS_ENABLED=true but ADMIN_REACTION_ACCOUNT_NAME is empty or None.")
            print("  Admin reaction will be skipped (why=admin_account_missing).")
        else:
            names = [a.name for a in config.telegram_accounts()]
            if account.strip() not in names:
                print(f"  WARNING: ADMIN_REACTION_ACCOUNT_NAME={account!r} not in telegram_accounts: {names}")


if __name__ == "__main__":
    main()
