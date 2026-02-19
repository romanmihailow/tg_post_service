"""Female grammar fix: replace male forms (согласен, уверен, etc.) with female forms.
Used by Pipeline 1 and Pipeline 2. No DB/Telegram dependencies."""
from __future__ import annotations

import re

# Order: "не …" and "бы …" FIRST (before single words). \s+ for multi-space/newline.
_FEMALE_GRAMMAR_REPLACEMENTS: list[tuple[re.Pattern[str], str]] = [
    # не + прилагательное/краткое
    (re.compile(r"\bне\s+согласен\b"), "не согласна"),
    (re.compile(r"\bНе\s+согласен\b"), "Не согласна"),
    (re.compile(r"\bне\s+уверен\b"), "не уверена"),
    (re.compile(r"\bНе\s+уверен\b"), "Не уверена"),
    (re.compile(r"\bне\s+удивлён\b"), "не удивлена"),
    (re.compile(r"\bНе\s+удивлён\b"), "Не удивлена"),
    (re.compile(r"\bне\s+удивлен\b"), "не удивлена"),
    (re.compile(r"\bНе\s+удивлен\b"), "Не удивлена"),
    # бы + глагол (Я бы сказал → Я бы сказала)
    (re.compile(r"\bбы\s+сказал\b"), "бы сказала"),
    (re.compile(r"\bБы\s+сказал\b"), "Бы сказала"),
    (re.compile(r"\bбы\s+уточнил\b"), "бы уточнила"),
    (re.compile(r"\bбы\s+поспорил\b"), "бы поспорила"),
    (re.compile(r"\bбы\s+добавил\b"), "бы добавила"),
    (re.compile(r"\bбы\s+отметил\b"), "бы отметила"),
    (re.compile(r"\bбы\s+подумал\b"), "бы подумала"),
    (re.compile(r"\bбы\s+считал\b"), "бы считала"),
    (re.compile(r"\bбы\s+хотел\b"), "бы хотела"),
    (re.compile(r"\bбы\s+сделал\b"), "бы сделала"),
    (re.compile(r"\bбы\s+решил\b"), "бы решила"),
    (re.compile(r"\bбы\s+написал\b"), "бы написала"),
    (re.compile(r"\bбы\s+ответил\b"), "бы ответила"),
    (re.compile(r"\bбы\s+согласился\b"), "бы согласилась"),
    (re.compile(r"\bбы\s+думал\b"), "бы думала"),
    # глагол + бы (Сказал бы → Сказала бы)
    (re.compile(r"\bСказал\s+бы\b"), "Сказала бы"),
    (re.compile(r"\bсказал\s+бы\b"), "сказала бы"),
    (re.compile(r"\bУточнил\s+бы\b"), "Уточнила бы"),
    (re.compile(r"\bПоспорил\s+бы\b"), "Поспорила бы"),
    (re.compile(r"\bДобавил\s+бы\b"), "Добавила бы"),
    (re.compile(r"\bПодумал\s+бы\b"), "Подумала бы"),
    # прилагательные/краткие
    (re.compile(r"\bсогласен\b"), "согласна"),
    (re.compile(r"\bСогласен\b"), "Согласна"),
    (re.compile(r"\bуверен\b"), "уверена"),
    (re.compile(r"\bУверен\b"), "Уверена"),
    (re.compile(r"\bудивлён\b"), "удивлена"),
    (re.compile(r"\bУдивлён\b"), "Удивлена"),
    (re.compile(r"\bудивлен\b"), "удивлена"),
    (re.compile(r"\bУдивлен\b"), "Удивлена"),
    (re.compile(r"\bготов\b"), "готова"),
    (re.compile(r"\bГотов\b"), "Готова"),
    (re.compile(r"\bправ\b"), "права"),
    (re.compile(r"\bПрав\b"), "Права"),
]

FEMALE_GRAMMAR_FIX_PREFIX_CHARS = 80


def fix_female_grammar_in_reply(text: str, prefix_chars: int = FEMALE_GRAMMAR_FIX_PREFIX_CHARS) -> str:
    """Replace male forms with female forms (согласен→согласна, etc.).
    Applies only to the first prefix_chars to avoid corrupting quotes later.
    Uses word boundaries to avoid corrupting parts of words (e.g. 'прав' in 'справедливо')."""
    if not text or not text.strip():
        return text
    n = prefix_chars
    if len(text) <= n:
        prefix, rest = text, ""
    else:
        prefix, rest = text[:n], text[n:]
    result_prefix = prefix
    for pattern, replacement in _FEMALE_GRAMMAR_REPLACEMENTS:
        result_prefix = pattern.sub(replacement, result_prefix)
    return result_prefix + rest
