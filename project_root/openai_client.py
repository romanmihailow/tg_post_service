"""OpenAI client helpers for paraphrasing and image generation."""

from __future__ import annotations

import base64
import logging
import time
from typing import Callable, Optional, TypeVar

from openai import OpenAI

BLACKBOX_SYSTEM_PROMPT = """
Ты — нейросеть, которая перефразирует новости для телеграм-канала «Чёрный ящик».

ТВОЯ ЗАДАЧА:
— Получать на вход текст новостного сообщения.
— Переписывать его своими словами.
— Сохранять тот же смысл, факты, даты, имена, цифры и хронологию событий.
— Делать текст УНИКАЛЬНЫМ, но без искажения фактов и подтасовок.

СТИЛЬ:
— Нейтральный, новостной, информативный.
— Без эмоций, без оценочных суждений.
— Без кликбейта и провокационных формулировок.
— Язык: строго русский.
— Тон: спокойный, аналитический.
— Запрещены обращения к читателю: «мы», «вы», «наш канал», «подписывайтесь» и т.п.

ФОРМАТ:
— Сохраняй структуру исходного текста:
если во входе несколько абзацев — делай несколько абзацев.
— Если в тексте есть контекст, причины или последствия — сохраняй их, но формулируй по-другому.
— Допускается лёгкое уплотнение текста, НО без потери смысла.
— Не добавляй заголовки, если их не было во входном тексте.

ЖЁСТКИЕ ПРАВИЛА (КРИТИЧНО):
— НЕЛЬЗЯ придумывать факты, которых нет во входном тексте.
— НЕЛЬЗЯ менять смысл или делать выводы от себя.
— Все числа, суммы, даты, имена, названия компаний, стран, городов, каналов — сохраняй ТОЧНО.
— Если во входном тексте есть цитата:
— её можно перефразировать по смыслу,
— но нельзя приписывать человеку слова, которых он не говорил.
— Не добавляй ссылки, эмодзи, хэштеги или упоминания источников, если их нет во входном тексте.

ВЫХОД:
— Отвечай ТОЛЬКО итоговым перефразированным текстом.
— Без комментариев, пояснений, вступлений.
— Не пиши «перефразированный текст», «итог» и т.п.

================================
ДОПОЛНИТЕЛЬНЫЙ РЕЖИМ «BLACKBOX»

— Если входной текст начинается с тега [BLACKBOX],
то после перефразирования примени эффект «восстановления данных из чёрного ящика».

ПРАВИЛА ЭФФЕКТА:
— Частично искажай РЕГИСТР букв:
— некоторые буквы делай заглавными, некоторые строчными.
— Искажения должны быть:
— редкими,
— нерегулярными,
— выглядеть как следы восстановления данных.
— Не более 10–15% символов во всём тексте.
— 85–90% текста ОБЯЗАТЕЛЬНО должны быть полностью читаемыми.
— НЕЛЬЗЯ:
— ломать слова полностью,
— добавлять лишние символы,
— использовать спецсимволы или «шум».
— Эффект должен быть заметен, но не мешать чтению.

— Тег [BLACKBOX] из итогового текста УДАЛЯЙ полностью.

ЕСЛИ тега [BLACKBOX] НЕТ:
— Работай в обычном режиме,
— Без каких-либо визуальных эффектов.
""".strip()

logger = logging.getLogger(__name__)

T = TypeVar("T")


class OpenAIClient:
    """Wrapper around OpenAI SDK with retry logic for key operations."""

    def __init__(self, api_key: str) -> None:
        self.client = OpenAI(api_key=api_key)
        self.text_model = "gpt-4.1-mini"
        self.vision_model = "gpt-4.1-mini"
        self.image_model = "gpt-image-1"

    def paraphrase_news(self, text: str) -> str:
        """Paraphrase a news text in Russian with a neutral style."""
        return self._with_retries(lambda: self._responses_text(BLACKBOX_SYSTEM_PROMPT, text))

    def describe_image_for_news(self, image_bytes: bytes) -> str:
        """Describe the image in a short neutral news style."""
        prompt = (
            "Кратко опиши изображение (1–2 предложения) в нейтральном "
            "новостном стиле."
        )
        return self._with_retries(lambda: self._responses_vision(prompt, image_bytes))

    def generate_image_from_description(self, description: str) -> bytes:
        """Generate a neutral news illustration image from a description."""
        prompt = (
            "Сгенерируй нейтральную новостную иллюстрацию по описанию. "
            "Без логотипов, без текста на изображении, без копирования "
            "уникального дизайна. Описание: "
            f"{description}"
        )
        return self._with_retries(lambda: self._generate_image(prompt))

    def _responses_text(self, system_prompt: str, user_text: str) -> str:
        response = self.client.responses.create(
            model=self.text_model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_text},
            ],
        )
        return self._extract_text(response)

    def _responses_vision(self, prompt: str, image_bytes: bytes) -> str:
        image_b64 = base64.b64encode(image_bytes).decode("ascii")
        response = self.client.responses.create(
            model=self.vision_model,
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt},
                        {"type": "input_image", "image_base64": image_b64},
                    ],
                }
            ],
        )
        return self._extract_text(response)

    def _generate_image(self, prompt: str) -> bytes:
        response = self.client.images.generate(
            model=self.image_model,
            prompt=prompt,
            size="1024x1024",
        )
        image_b64 = response.data[0].b64_json
        return base64.b64decode(image_b64)

    def _extract_text(self, response: object) -> str:
        text = getattr(response, "output_text", None)
        if isinstance(text, str) and text.strip():
            return text.strip()
        output = getattr(response, "output", None)
        if output:
            for item in output:
                content = getattr(item, "content", [])
                for part in content:
                    part_text = getattr(part, "text", None)
                    if isinstance(part_text, str) and part_text.strip():
                        return part_text.strip()
        raise RuntimeError("OpenAI response did not contain text output")

    def _with_retries(self, func: Callable[[], T], retries: int = 2) -> T:
        last_error: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                return func()
            except Exception as exc:  # noqa: BLE001 - log and retry on any SDK error
                last_error = exc
                logger.exception("OpenAI request failed on attempt %s", attempt + 1)
                if attempt < retries:
                    time.sleep(2**attempt)
        raise RuntimeError("OpenAI request failed after retries") from last_error
