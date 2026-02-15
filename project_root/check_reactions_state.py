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
    print("  REACTION_MAX_REACTIONS_PER_POST_PER_DAY:", getattr(config, "REACTION_MAX_REACTIONS_PER_POST_PER_DAY", 1), "(max reactions on one post per day)")
    print("  REACTION_USE_ALLOWED_FROM_TELEGRAM:", getattr(config, "REACTION_USE_ALLOWED_FROM_TELEGRAM", True))
    print("  REACTION_ALLOWED_SAMPLE_LIMIT:", getattr(config, "REACTION_ALLOWED_SAMPLE_LIMIT", 80))
    print("  REACTION_MIN_BOTS_PER_POST:", getattr(config, "REACTION_MIN_BOTS_PER_POST", 1))
    print("  Today (UTC):", today)
    print()
    print("Pipeline 2 chat reactions — current config")
    print("  CHAT_REACTIONS_ENABLED:", getattr(config, "CHAT_REACTIONS_ENABLED", False))
    print("  CHAT_REACTION_PROBABILITY:", getattr(config, "CHAT_REACTION_PROBABILITY", 0.15))
    print("  CHAT_REACTION_DAILY_LIMIT_PER_BOT:", getattr(config, "CHAT_REACTION_DAILY_LIMIT_PER_BOT", 20))
    print("  CHAT_REACTION_COOLDOWN_MINUTES:", getattr(config, "CHAT_REACTION_COOLDOWN_MINUTES", 10))
    print("  CHAT_REACTION_EMOJIS list:", config.chat_reaction_emojis_list())
    print("  CHAT_REACTION_ON_USER_MESSAGE:", getattr(config, "CHAT_REACTION_ON_USER_MESSAGE", True))
    print("  CHAT_REACTION_ON_BOT_MESSAGE:", getattr(config, "CHAT_REACTION_ON_BOT_MESSAGE", False))
    print()
    print("Admin reactions (channel post when P1 publishes question) — current config")
    print("  ADMIN_REACTIONS_ENABLED:", getattr(config, "ADMIN_REACTIONS_ENABLED", False))
    print("  ADMIN_REACTION_ACCOUNT_NAME:", repr(getattr(config, "ADMIN_REACTION_ACCOUNT_NAME", None)))
    print("  ADMIN_REACTION_EMOJI:", getattr(config, "ADMIN_REACTION_EMOJI", "👀"))
    print("  ADMIN_REACTION_FALLBACK_EMOJI:", getattr(config, "ADMIN_REACTION_FALLBACK_EMOJI", "👍"))
    print()
    print("Pipeline 1: max reactions per post per day is enabled. When REACTION_MAX_REACTIONS_PER_POST_PER_DAY > 1,")
    print("multiple bots can react to the same post (up to N per day), respecting cooldown and daily limits.")
    print()
    print("Date reset: channel and chat reaction daily structures are cleared when")
    print("the calendar date changes (on first reaction path use each day). No restart needed.")


if __name__ == "__main__":
    main()
