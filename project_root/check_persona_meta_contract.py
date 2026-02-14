"""Self-check: контракт persona_meta и валидация.
Запуск из корня: python -m project_root.check_persona_meta_contract
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from project_root.config import Config
from project_root.db import init_db, get_session
from project_root.openai_client import VALID_TONES, VALID_VERBOSITY, VALID_GENDER
from project_root.scheduler import _build_persona_prompt_and_meta


ACCOUNTS_TO_CHECK = ["acc1", "t9174800805", "t9870202433", "unknown_account"]


def main() -> None:
    config = Config()
    init_db(config)

    ok_count = 0
    fail_count = 0

    with get_session() as session:
        for account_name in ACCOUNTS_TO_CHECK:
            try:
                role_label, persona_meta = _build_persona_prompt_and_meta(
                    session, account_name
                )

                failed = False
                if "META:" in role_label:
                    print(f"FAIL {account_name}: META found in role_label")
                    failed = True
                if "tone" not in persona_meta or "verbosity" not in persona_meta or "gender" not in persona_meta:
                    print(f"FAIL {account_name}: missing required keys in persona_meta")
                    failed = True
                if persona_meta.get("tone") not in VALID_TONES:
                    print(f"FAIL {account_name}: tone={persona_meta.get('tone')!r} not in VALID_TONES")
                    failed = True
                if persona_meta.get("verbosity") not in VALID_VERBOSITY:
                    print(f"FAIL {account_name}: verbosity={persona_meta.get('verbosity')!r} not in VALID_VERBOSITY")
                    failed = True
                if persona_meta.get("gender") not in VALID_GENDER:
                    print(f"FAIL {account_name}: gender={persona_meta.get('gender')!r} not in VALID_GENDER")
                    failed = True

                if not failed:
                    print(f"OK {account_name}: tone={persona_meta['tone']} verbosity={persona_meta['verbosity']} gender={persona_meta['gender']}")
                    ok_count += 1
                else:
                    fail_count += 1
            except Exception as exc:
                print(f"FAIL {account_name}: {exc}")
                fail_count += 1

    print(f"\nTotal: {ok_count} OK, {fail_count} FAIL")
    sys.exit(1 if fail_count > 0 else 0)


if __name__ == "__main__":
    main()
