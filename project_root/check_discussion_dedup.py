"""Self-check for Discussion dedup: normalize_text_for_fingerprint, topic_fingerprint, recent_topics_json parsing.
No Telegram, no DB. Run: python -m project_root.check_discussion_dedup
"""

from __future__ import annotations

import json
import re
import hashlib


def normalize_text_for_fingerprint(text: str) -> str:
    """Normalize text for fingerprint: lowercase, no URLs, @user, #tag, digits->0, collapse spaces."""
    if not text or not isinstance(text, str):
        return ""
    s = text.strip().lower()
    s = re.sub(r"https?://\S+", " ", s)
    s = re.sub(r"@\w+", " ", s)
    s = re.sub(r"#\w+", " ", s)
    s = re.sub(r"\d+", "0", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:800] if len(s) > 800 else s


def topic_fingerprint(text: str) -> str:
    """Stable hash of normalized text for anti-repeat."""
    norm = normalize_text_for_fingerprint(text)
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()[:16]


def parse_recent_topics_json(raw: str | None) -> tuple[list[str], list[str]]:
    """Parse recent_topics_json. Returns (topics, fingerprints). Backward compat: list -> topics, fingerprints=[]."""
    topics: list[str] = []
    fingerprints: list[str] = []
    s = (raw or "").strip()
    if not s:
        return (topics, fingerprints)
    try:
        data = json.loads(s)
    except json.JSONDecodeError:
        return (topics, fingerprints)
    if isinstance(data, list):
        topics = [str(x).strip().lower() for x in data if str(x).strip()]
        return (topics, fingerprints)
    if isinstance(data, dict):
        raw_topics = data.get("topics")
        if isinstance(raw_topics, list):
            topics = [str(x).strip().lower() for x in raw_topics if str(x).strip()]
        raw_fps = data.get("fingerprints")
        if isinstance(raw_fps, list):
            fingerprints = [str(x).strip() for x in raw_fps if str(x).strip() and len(str(x)) <= 32]
    return (topics, fingerprints)


def main() -> None:
    # 1) normalize_text_for_fingerprint
    t1 = "Политика: Зеленский объявил референдум 2025. https://t.me/abc @user #news"
    n1 = normalize_text_for_fingerprint(t1)
    assert "https" not in n1
    assert "@" not in n1
    assert "#" not in n1
    assert "0" in n1
    assert "зеленский" in n1

    t2 = "   Много   пробелов   123   "
    n2 = normalize_text_for_fingerprint(t2)
    assert "  " not in n2
    assert "0" in n2

    t3 = "a" * 1000
    n3 = normalize_text_for_fingerprint(t3)
    assert len(n3) == 800

    # 2) topic_fingerprint
    fp1 = topic_fingerprint("Новость о Зеленском")
    fp2 = topic_fingerprint("Новость о Зеленском")
    assert fp1 == fp2
    fp3 = topic_fingerprint("Другая новость")
    assert fp1 != fp3
    assert len(fp1) == 16
    assert all(c in "0123456789abcdef" for c in fp1)

    # 3) parse recent_topics_json — old format (list)
    topics_old, fps_old = parse_recent_topics_json('["политика", "экономика"]')
    assert topics_old == ["политика", "экономика"]
    assert fps_old == []

    topics_empty, fps_empty = parse_recent_topics_json("")
    assert topics_empty == [] and fps_empty == []

    # 4) parse recent_topics_json — new format (object)
    raw_new = '{"topics": ["политика"], "fingerprints": ["a1b2c3d4e5f67890"]}'
    topics_new, fps_new = parse_recent_topics_json(raw_new)
    assert topics_new == ["политика"]
    assert fps_new == ["a1b2c3d4e5f67890"]

    topics_invalid, fps_invalid = parse_recent_topics_json("not json")
    assert topics_invalid == [] and fps_invalid == []

    print("check_discussion_dedup: all asserts passed")



if __name__ == "__main__":
    main()
