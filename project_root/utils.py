"""Utility helpers."""

from __future__ import annotations


def is_text_long_enough(text: str, min_length: int) -> bool:
    """Check if text length meets the minimum requirement."""
    return len(text.strip()) >= min_length
