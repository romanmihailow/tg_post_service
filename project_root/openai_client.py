"""OpenAI client helpers for paraphrasing and image generation."""

from __future__ import annotations

import base64
import json
import logging
import random
import time
from typing import Any, Callable, Optional, Tuple, TypeVar

from openai import OpenAI

logger = logging.getLogger(__name__)

T = TypeVar("T")


class OpenAIClient:
    """Wrapper around OpenAI SDK with retry logic for key operations."""

    def __init__(
        self,
        api_key: str,
        system_prompt: str,
        text_model: Optional[str] = None,
        vision_model: Optional[str] = None,
        image_model: Optional[str] = None,
    ) -> None:
        if not system_prompt.strip():
            raise ValueError("System prompt is empty")
        self.client = OpenAI(api_key=api_key)
        self.system_prompt = system_prompt
        self.text_model = text_model or "gpt-4.1-mini"
        self.vision_model = vision_model or "gpt-4.1-mini"
        self.image_model = image_model or "gpt-image-1"

    def paraphrase_news(self, text: str) -> Tuple[str, int, int, int]:
        """Paraphrase a news text in Russian with a neutral style."""
        return self._with_retries(
            lambda: self._responses_text(self.system_prompt, text)
        )

    def describe_image_for_news(self, image_bytes: bytes) -> str:
        """Describe the image in a short neutral news style."""
        prompt = (
            "Кратко опиши изображение (1–2 предложения) в нейтральном "
            "новостном стиле."
        )
        return self._with_retries(lambda: self._responses_vision(prompt, image_bytes))

    def generate_image_from_description(self, description: str) -> Tuple[bytes, int]:
        """Generate a neutral news illustration image from a description."""
        prompt = (
            "Сгенерируй нейтральную новостную иллюстрацию по описанию. "
            "Без логотипов, без текста на изображении, без копирования "
            "уникального дизайна. Описание: "
            f"{description}"
        )
        return self._with_retries(lambda: self._generate_image(prompt))

    def select_discussion_news(
        self,
        candidates: list[str],
        *,
        pipeline_id: int | None = None,
        chat_id: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> Tuple[int, int, int, int]:
        """Select a single news index from candidates (1-based)."""
        if not candidates:
            raise ValueError("Candidates list is empty")
        enumerated = "\n".join(
            f"{idx + 1}. {text}" for idx, text in enumerate(candidates)
        )
        prompt = (
            "Выбери одну новость, которая лучше всего подходит для обсуждения.\n"
            "Верни JSON строго такого вида: {\"index\": N}\n"
            "Где N — номер новости в списке (1-based).\n\n"
            f"{enumerated}"
        )
        text, in_tokens, out_tokens, total_tokens = self._with_retries(
            lambda: self._responses_text(self.system_prompt, prompt)
        )
        _log_openai_usage(
            kind="discussion_select",
            model=self.text_model,
            pipeline_id=pipeline_id,
            chat_id=chat_id,
            input_tokens=in_tokens,
            output_tokens=out_tokens,
            total_tokens=total_tokens,
            extra=extra or {},
        )
        try:
            data = json.loads(text)
            index = int(data.get("index"))
        except Exception as exc:
            raise RuntimeError("OpenAI returned invalid JSON for selection") from exc
        if index < 1 or index > len(candidates):
            raise RuntimeError("OpenAI returned out-of-range index")
        return index, in_tokens, out_tokens, total_tokens

    def generate_discussion_messages(
        self,
        news_text: str,
        replies_count: int,
        roles: list[str],
        *,
        pipeline_id: int | None = None,
        chat_id: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> Tuple[dict, int, int, int]:
        """Generate question and replies for discussion."""
        # Persona is presentation-only and must not affect decision logic.
        roles_text = "\n".join(f"- {role}" for role in roles) or "- userbot"
        prompt = (
            "Сгенерируй живое обсуждение новости для Telegram-чата.\n"
            "Верни JSON строго вида:\n"
            "{\"question\": \"...\", \"replies\": [\"...\", ...]}\n\n"

            f"Количество ответов: {replies_count}\n\n"

            "Требования к стилю:\n"
            "- Диалог должен выглядеть как реальная беседа людей, а не как ответы на экзамене.\n"
            "- Не использовать формулировки типа: «Это может», «Это может привести», «Это может повлиять».\n"
            "- Избегать канцелярита и журналистского стиля.\n"
            "- Ответы должны различаться по длине.\n"
            "- Допускается лёгкое несогласие между участниками.\n"
            "- Можно реагировать на предыдущие ответы (соглашаться, спорить, уточнять).\n"
            "- Не повторять формулировки друг друга.\n"
            "- Не использовать абстрактные конструкции вроде «общественное восприятие», «социальный эффект», «некоторые могут считать».\n"
            "- Иногда можно начинать с живых вводных: «Честно говоря…», «Скорее всего…», «Не уверен…», «Ну тут спорно…».\n"
            "- Не все участники должны быть аналитиками — допускается бытовой язык.\n\n"

            "Роли участников (каждый строго следует своему стилю):\n"
            f"{roles_text}\n\n"

            "Новость:\n"
            f"{news_text}"
        )

        text, in_tokens, out_tokens, total_tokens = self._with_retries(
            lambda: self._responses_text(self.system_prompt, prompt)
        )
        _log_openai_usage(
            kind="discussion_qna",
            model=self.text_model,
            pipeline_id=pipeline_id,
            chat_id=chat_id,
            input_tokens=in_tokens,
            output_tokens=out_tokens,
            total_tokens=total_tokens,
            extra=extra or {},
        )
        try:
            data = json.loads(text)
        except Exception as exc:
            raise RuntimeError("OpenAI returned invalid JSON for discussion") from exc
        if not isinstance(data, dict) or "question" not in data or "replies" not in data:
            raise RuntimeError("OpenAI response missing question/replies")
        if not isinstance(data["replies"], list):
            raise RuntimeError("OpenAI replies must be a list")
        return data, in_tokens, out_tokens, total_tokens

    def generate_user_reply(
        self,
        *,
        source_text: str,
        context_messages: list[str],
        role_label: str,
        pipeline_id: int | None = None,
        chat_id: str | None = None,
        extra: dict[str, Any] | None = None,
        system_prompt_override: str | None = None,
    ) -> Tuple[str, int, int, int]:
        """Generate a short reply to a user message (Pipeline 2 live replies)."""
        context_block = "\n".join(
            f"- {text}" for text in context_messages if text.strip()
        )

        # Общие правила (всегда в промпте)
        common_rules = (
            "Ты участник живого Telegram-чата. Пиши как живой человек, не как эксперт и не как статья.\n\n"
            "Обязательно:\n"
            "- Выбери одну фразу или деталь из сообщения пользователя и отвечай именно на неё; не пересказывай весь вопрос.\n"
            "- Не копируй формулировки пользователя дословно — перефразируй своими словами.\n"
            "- Не начинай со слов: «Это может», «Это может привести», «Это может повлиять».\n"
            "- Избегай канцелярита и журналистского тона.\n"
            "- Иногда отвечай сразу по делу, не всегда с вводных («Скорее всего», «Честно говоря» и т.п.).\n"
            "- Согласие, сомнение и лёгкое несогласие равнозначны — не злоупотребляй одним типом.\n"
            "- Без ссылок, без призывов подписаться, без «я бот».\n"
            "- Если уместно, оттолкнись от последнего сообщения в контексте: согласись, оспорь или уточни одной фразой.\n"
            "- Иногда допустимо мягко не согласиться с предыдущим сообщением, если это уместно.\n"
        )

        # Пресеты манеры ответа (веса: сумма 100; ультра-короткий 15%)
        presets = [
            "Формат: одно короткое предложение. Чётко займи позицию: согласие или сомнение.",
            "Формат: 1–2 предложения. Добавь уточнение или нюанс: можно начать с «Тут есть нюанс», «Не совсем так», «Я бы уточнил» — затем кратко поясни. Без жёсткого конфликта.",
            "Формат: два предложения. Реагируй на сообщение и приведи один конкретный пример или последствие.",
            "Формат: 1–2 предложения. В конце задай короткий встречный вопрос по теме сообщения.",
            "Формат: ультра-короткая реплика — 5–10 слов. Живая реакция без пересказа и аналитики. По тону в духе: сомнение («Сомневаюсь, если честно»), удивление («Ну это звучит странно»), неясность («Как-то всё мутно»), скепсис («Не выглядит убедительно»). Без эмодзи, без вопроса по умолчанию, без «Это может».",
            "Формат: 1–2 предложения. Начни с мягкого несогласия или сомнения: «Не совсем согласен…», «Я бы поспорил…», «Не уверен, что всё так просто…», «Тут есть другой момент…» — затем одно короткое пояснение или один нюанс. Без агрессии, без морализаторства, без «Это может».",
        ]
        weights = [22, 20, 18, 10, 15, 15]  # 1 позиция 22%; 2 нюанс 20%; 3 пример 18%; 4 вопрос 10%; 5 ультра 15%; 6 мягк.несогл. 15%
        preset = random.choices(presets, weights=weights, k=1)[0]

        prompt = (
            f"{common_rules}"
            f"Сейчас:\n{preset}\n\n"
            f"Твоя роль в этом чате:\n{role_label}\n\n"
            "Последние сообщения чата:\n"
            f"{context_block}\n\n"
            "Сообщение пользователя, на которое нужно ответить:\n"
            f"{source_text}\n\n"
            "Ответ:"
        )

        system_for_call = system_prompt_override if system_prompt_override else self.system_prompt
        text, in_tokens, out_tokens, total_tokens = self._with_retries(
            lambda: self._responses_text(system_for_call, prompt)
        )
        _log_openai_usage(
            kind="user_reply",
            model=self.text_model,
            pipeline_id=pipeline_id,
            chat_id=chat_id,
            input_tokens=in_tokens,
            output_tokens=out_tokens,
            total_tokens=total_tokens,
            extra=extra or {},
        )
        return text.strip(), in_tokens, out_tokens, total_tokens

    def _responses_text(
        self, system_prompt: str, user_text: str
    ) -> Tuple[str, int, int, int]:
        response = self.client.responses.create(
            model=self.text_model,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_text},
            ],
        )
        return self._extract_text_and_tokens(response)

    def _responses_vision(self, prompt: str, image_bytes: bytes) -> str:
        image_b64 = base64.b64encode(image_bytes).decode("ascii")
        response = self.client.responses.create(
            model=self.vision_model,
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt},
                        {
                            "type": "input_image",
                            "image_url": f"data:image/png;base64,{image_b64}",
                        },
                    ],
                }
            ],
        )
        return self._extract_text(response)

    def _generate_image(self, prompt: str) -> Tuple[bytes, int]:
        response = self.client.images.generate(
            model=self.image_model,
            prompt=prompt,
            size="1024x1024",
        )
        image_b64 = response.data[0].b64_json
        return base64.b64decode(image_b64), 0

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

    def _extract_text_and_tokens(self, response: object) -> Tuple[str, int, int, int]:
        text = self._extract_text(response)
        input_tokens = 0
        output_tokens = 0
        total_tokens = 0
        usage = getattr(response, "usage", None)
        if usage:
            total = getattr(usage, "total_tokens", None)
            if isinstance(total, int):
                total_tokens = total
            input_total = getattr(usage, "input_tokens", None)
            if isinstance(input_total, int):
                input_tokens = input_total
            output_total = getattr(usage, "output_tokens", None)
            if isinstance(output_total, int):
                output_tokens = output_total
        if total_tokens == 0 and (input_tokens or output_tokens):
            total_tokens = input_tokens + output_tokens
        return text, input_tokens, output_tokens, total_tokens

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


def _log_openai_usage(
    *,
    kind: str,
    model: str,
    pipeline_id: int | None,
    chat_id: str | None,
    input_tokens: int,
    output_tokens: int,
    total_tokens: int,
    extra: dict[str, Any],
) -> None:
    if input_tokens == 0 and output_tokens == 0 and total_tokens == 0:
        logger.warning(
            "openai_usage missing usage kind=%s model=%s pipeline=%s chat=%s extra=%s",
            kind,
            model,
            pipeline_id,
            chat_id,
            extra,
        )
        return
    logger.info(
        "openai_usage kind=%s model=%s pipeline=%s chat=%s input=%d output=%d total=%d extra=%s",
        kind,
        model,
        pipeline_id,
        chat_id,
        input_tokens,
        output_tokens,
        total_tokens,
        extra,
    )
