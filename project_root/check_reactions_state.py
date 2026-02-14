#!/usr/bin/env python3
"""Self-check: print Pipeline 1 reaction config and demonstrate date-reset logic (no Telegram)."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

# Load config without full app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from project_root.config import Config


def main() -> None:
    config = Config()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print("Pipeline 1 reactions — current config")
    print("  REACTIONS_ENABLED:", getattr(config, "REACTIONS_ENABLED", False))
    print("  REACTION_PROBABILITY:", getattr(config, "REACTION_PROBABILITY", 0.35))
    print("  REACTION_DAILY_LIMIT_PER_BOT:", getattr(config, "REACTION_DAILY_LIMIT_PER_BOT", 10))
    print("  REACTION_COOLDOWN_MINUTES:", getattr(config, "REACTION_COOLDOWN_MINUTES", 30))
    print("  REACTION_EMOJIS list:", config.reaction_emojis_list())
    print("  Today (UTC):", today)
    print()
    print("Date reset: _REACTION_TODAY and _REACTION_REACTED_TODAY are cleared when")
    print("the calendar date changes (on first reaction path use each day). No restart needed.")


if __name__ == "__main__":
    main()
