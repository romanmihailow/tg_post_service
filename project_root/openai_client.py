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

# Контракт persona_meta: допустимые значения (P1 устойчивость)
VALID_TONES = {"neutral", "analytical", "emotional", "ironic", "skeptical"}
VALID_VERBOSITY = {"short", "medium", "long"}
VALID_GENDER = {"male", "female"}

# Большой пул вводных фраз для вариативности ответов (модель получает случайную выборку каждый раз)
REPLY_OPENING_POOL = [
    "Согласен", "Согласна", "Не совсем согласен", "Не совсем согласна",
    "Не уверен", "Не уверена", "Мне кажется", "Честно говоря", "Если честно",
    "Скорее всего", "Ну тут спорно", "Тут есть нюанс", "Я бы уточнил", "Я бы уточнила",
    "Да, но", "Вроде да", "Хм", "Не факт", "Скорее нет", "Зависит",
    "С другой стороны", "Отчасти да", "Так и есть", "Логично", "Похоже на то",
    "Сомневаюсь", "Ну это звучит странно", "Как-то всё мутно", "Не выглядит убедительно",
    "Интересно", "Любопытно", "Тут другой момент", "Я бы поспорил", "Я бы поспорила",
    "Не думаю", "Вряд ли", "Возможно", "В какой-то степени", "Сложно сказать",
    "Обычно да", "Чаще всего", "Бывает по-разному", "Тут как посмотреть",
]

# Сколько вводных показывать модели в одном запросе (случайная выборка)
REPLY_OPENING_SAMPLE_SIZE = 12

# Вариативность формулировок вопроса ведущего. Типы по смыслу новости (5–7 на группу).
ADMIN_QUESTION_TAXONOMY: dict[str, list[str]] = {
    "конфликты_геополитика_инциденты": [
        "Как вы думаете, кто за этим стоит?",
        "Кому это выгодно?",
        "Как вы думаете, что будет дальше?",
        "Как вы это восприняли?",
        "Что здесь важнее — факт или последствия?",
        "Какие у вас мысли по этому поводу?",
        "Это меняет ваше отношение к теме?",
    ],
    "товары_цены_покупки_авто": [
        "Вы бы купили?",
        "Какая у вас машина или техника?",
        "Какую марку рассматриваете для покупки?",
        "Ожидаемо или неожиданно для вас?",
        "Насколько справедлива такая цена, по-вашему?",
        "Вы бы так поступили?",
        "Что здесь важнее — цена или качество?",
    ],
    "природа_здоровье_экология_быт": [
        "Готовы ли вы к такому повороту?",
        "Как это повлияет на вас лично?",
        "Вы замечали подобное?",
        "Это вообще тренд или разовый кейс?",
        "Стоит ли этому удивляться?",
        "Как вы к этому относитесь?",
        "Как бы вы подготовились к такому?",
    ],
    "культура_медиа_тренды": [
        "Как думаете, что это за тренд?",
        "Ваши мысли?",
        "Как вам такая история?",
        "Кто как к этому относится?",
        "Согласны с таким развитием или нет?",
        "Где тут подвох, по-вашему?",
        "Это ожидаемо или сюрприз?",
    ],
    "экономика_общество_общее": [
        "Что вы об этом думаете?",
        "Как считаете, это действительно так?",
        "Что бы вы сделали на месте героя?",
        "Как бы вы объяснили такое?",
        "Насколько это типичная ситуация?",
        "Как бы вы отреагировали в такой ситуации?",
        "Ваши мысли?",
    ],
}

# Объединённый пул для случайной выборки (все из таксономии)
ADMIN_QUESTION_PHRASING_POOL: list[str] = []
for _qlist in ADMIN_QUESTION_TAXONOMY.values():
    ADMIN_QUESTION_PHRASING_POOL.extend(_qlist)
ADMIN_QUESTION_PHRASING_POOL = list(dict.fromkeys(ADMIN_QUESTION_PHRASING_POOL))
ADMIN_QUESTION_SAMPLE_SIZE = 12

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
        recent_topics: list[str] | None = None,
        recent_fingerprints: list[str] | None = None,
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
        avoid_hint = ""
        if recent_topics:
            topics_str = ", ".join(recent_topics[:10])
            avoid_hint = (
                f"\nИзбегай тем, которые уже обсуждали недавно: {topics_str}. "
                "Выбирай максимально отличающуюся тему среди кандидатов.\n"
            )
        prompt = (
            "Выбери одну новость, которая лучше всего подходит для обсуждения.\n"
            "Верни JSON строго такого вида: {\"index\": N}\n"
            "Где N — номер новости в списке (1-based).\n"
            f"{avoid_hint}\n"
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
        last_questions: list[str] | None = None,
        pipeline_id: int | None = None,
        chat_id: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> Tuple[dict, int, int, int]:
        """Generate question and replies for discussion."""
        # Persona is presentation-only and must not affect decision logic.
        roles_text = "\n".join(f"- {role}" for role in roles) or "- userbot"
        # Таксономия: подбирай тип вопроса по смыслу новости (конфликт/геополитика, товары/цены, природа/здоровье, культура/медиа, экономика/общество)
        taxonomy_lines = []
        for group_name, questions in ADMIN_QUESTION_TAXONOMY.items():
            taxonomy_lines.append(f"- {group_name}: " + " | ".join(questions))
        taxonomy_block = "Типы вопросов по смыслу новости (выбери группу и один из вариантов или свой в том же духе):\n" + "\n".join(taxonomy_lines)
        avoid_block = ""
        if last_questions:
            avoid_block = (
                "\n\nПоследние 5 вопросов (НЕ повторяй эти формулировки, выбери другой тип/группу): "
                + " | ".join(f"«{q[:60]}{'…' if len(q) > 60 else ''}»" for q in last_questions[:5])
                + "\n\n"
            )
        question_variety_hint = (
            "Вопрос ведущего (поле question) обязательно должен состоять из двух частей: (1) кратко суть новости (1–2 предложения), "
            "чтобы в чате было понятно, о чём речь; (2) вопрос к аудитории. "
            "Подбирай формулировку под смысл: для конфликтов/инцидентов — «кто за этим?», «кому выгодно?»; "
            "для товаров/цен — «вы бы купили?», «какая у вас машина?»; "
            "для природы/здоровья — «готовы ли к такому?», «как повлияет на вас?»; "
            "для культуры/трендов — «как вам история?», «согласны с развитием?». "
            "Нельзя выводить в question только короткую фразу без контекста.\n\n"
            + taxonomy_block
            + avoid_block
            + "\n"
        )
        prompt = (
            "Сгенерируй живое обсуждение новости для Telegram-чата.\n"
            "Верни JSON строго вида:\n"
            "{\"question\": \"...\", \"replies\": [\"...\", ...]}\n\n"

            f"Количество ответов: {replies_count}\n\n"

            + question_variety_hint
            + "Требования к стилю:\n"
            "- В question всегда сначала изложи суть новости, затем задай вопрос к чату. Короткий вопрос без контекста (только «Как вы это восприняли?» и т.п.) запрещён.\n"
            "- Диалог должен выглядеть как реальная беседа людей, а не как ответы на экзамене.\n"
            "- Не использовать формулировки типа: «Это может», «Это может привести», «Это может повлиять».\n"
            "- Избегать канцелярита и журналистского стиля.\n"
            "- Ответы должны различаться по длине.\n"
            "- Допускается лёгкое несогласие между участниками.\n"
            "- Можно реагировать на предыдущие ответы (соглашаться, спорить, уточнять).\n"
            "- Не повторять формулировки друг друга.\n"
            "- Не использовать абстрактные конструкции вроде «общественное восприятие», «социальный эффект», «некоторые могут считать».\n"
            "- Разнообразь вводные: не все ответы должны начинаться с «Согласна», «Не уверен», «Если честно», «Интересно». Используй разные начала: «Скорее всего», «Тут есть нюанс», «Ну тут спорно», «Логично», «С другой стороны», «Зависит», «Похоже на то», «Вряд ли», «Хм», или начинай сразу по делу.\n"
            "- Не все участники должны быть аналитиками — допускается бытовой язык.\n"
            "- Пунктуация: естественная. Не обязательно всегда ставить точку в конце. Запятые — где уместно. Никогда не используй длинное тире (—).\n\n"

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
        persona_meta: dict[str, Any] | None = None,
        pipeline_id: int | None = None,
        chat_id: str | None = None,
        extra: dict[str, Any] | None = None,
        system_prompt_override: str | None = None,
        allowed_reactions: list[str] | None = None,
        model_driven_reaction: bool = False,
        reaction_null_rate: float = 0.65,
    ) -> Tuple[str, Optional[str], int, int, int, dict[str, Any]]:
        """Generate a short reply to a user message (Pipeline 2 live replies).
        Returns (reply_text, reaction_emoji, in_tokens, out_tokens, total_tokens, gen_info).
        reaction_emoji: str | None — emoji to put on user's message (when model_driven_reaction).
        gen_info: {preset_idx, length_hint, reaction_emoji} for observability."""
        context_block = "\n".join(
            f"- {text}" for text in context_messages if text.strip()
        )

        if persona_meta is None:
            logger.warning(
                "generate_user_reply: persona_meta is None, using defaults (tone=neutral verbosity=short)"
            )
        meta = persona_meta or {}

        raw_tone = meta.get("tone", "neutral")
        tone = raw_tone if raw_tone in VALID_TONES else "neutral"
        if raw_tone != tone:
            logger.warning(
                "generate_user_reply: invalid tone=%r, using default neutral",
                raw_tone,
            )

        raw_verbosity = meta.get("verbosity", "short")
        verbosity = raw_verbosity if raw_verbosity in VALID_VERBOSITY else "short"
        if raw_verbosity != verbosity:
            logger.warning(
                "generate_user_reply: invalid verbosity=%r, using default short",
                raw_verbosity,
            )

        raw_gender = meta.get("gender", "male")
        gender = raw_gender if raw_gender in VALID_GENDER else "male"
        if raw_gender != gender:
            logger.warning(
                "generate_user_reply: invalid gender=%r, using default male",
                raw_gender,
            )

        # Общие правила (всегда в промпте)
        common_rules_parts = [
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
            "- Не обязательно поддерживать общий тон беседы — допускается лёгкий контраст или альтернативная точка зрения.\n"
            "- Пунктуация: естественная. Не обязательно всегда ставить точку в конце. Запятые — где уместно, можно опускать. Никогда не используй длинное тире (—).\n",
        ]
        if tone == "emotional":
            common_rules_parts.append(
                "- Допустима более резкая или эмоциональная формулировка, если это уместно.\n"
            )
        common_rules = "".join(common_rules_parts)

        # Случайная выборка вводных фраз для вариативности (каждый запрос — разный набор)
        opening_sample = random.sample(
            REPLY_OPENING_POOL,
            min(REPLY_OPENING_SAMPLE_SIZE, len(REPLY_OPENING_POOL)),
        )
        opening_hint = (
            "Варианты начала реплики (выбери один или свой, не повторяй одни и те же подряд): "
            + ", ".join(f"«{x}»" for x in opening_sample)
            + ". Можно начать сразу по делу без вводной.\n\n"
        )
        common_rules = common_rules + opening_hint

        # Часть 1: микрослучайная длина по verbosity
        r = random.random()
        if verbosity == "short":
            length_hint = "Длина: одно предложение." if r < 0.7 else "Длина: 1–2 предложения."
        elif verbosity == "medium":
            length_hint = "Длина: одно предложение." if r < 0.4 else "Длина: 1–2 предложения."
        elif verbosity == "long":
            if r < 0.15:
                length_hint = "Длина: одно предложение."
            elif r < 0.70:
                length_hint = "Длина: 1–2 предложения."
            else:
                length_hint = "Длина: 2–3 предложения."
        else:
            length_hint = "Длина: одно предложение." if r < 0.7 else "Длина: 1–2 предложения."

        # Часть 2: эмоциональный коэффициент (25% для emotional)
        emotional_boost = ""
        if tone == "emotional" and random.random() < 0.25:
            emotional_boost = "\nМожно использовать более живую или резкую интонацию."

        # Часть 3: микро-несогласие (20%)
        contrast_hint = ""
        if random.random() < 0.20:
            contrast_hint = "\nМожно занять слегка отличающуюся позицию от предыдущего сообщения, если это логично."

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
        preset_idx = random.choices(range(len(presets)), weights=weights, k=1)[0]
        preset = presets[preset_idx]
        # Для ультра-короткого пресета (индекс 4) не добавляем length_hint — он уже задан
        preset_block = f"Сейчас:\n{preset}\n"
        if preset_idx != 4:
            preset_block += f"\n{length_hint}\n"
        preset_block += f"{emotional_boost}{contrast_hint}\n\n"

        gen_info: dict[str, Any] = {
            "preset_idx": preset_idx,
            "length_hint": length_hint,
        }
        if logger.isEnabledFor(logging.DEBUG):
            account_name = (extra or {}).get("account_name", "?")
            logger.debug(
                "user_reply persona: account=%s tone=%s verbosity=%s gender=%s preset_idx=%s length_hint=%s",
                account_name,
                tone,
                verbosity,
                gender,
                preset_idx,
                length_hint,
            )

        json_block = ""
        if model_driven_reaction and allowed_reactions:
            allowed_str = json.dumps(allowed_reactions, ensure_ascii=False)
            null_pct = int(reaction_null_rate * 100)
            json_block = (
                "\n\nФОРМАТ ОТВЕТА — строго JSON:\n"
                '{"reply_text":"...","reaction_emoji":"👍"}\n'
                'или {"reply_text":"...","reaction_emoji":null}\n'
                f"- reply_text: 1–2 предложения по правилам выше, без эмодзи в тексте.\n"
                f"- reaction_emoji: null примерно в {null_pct}% случаев; иначе ОДИН эмодзи ТОЛЬКО из списка: {allowed_str}\n"
                "- НЕ добавляй эмодзи в reply_text — они передаются отдельно.\n"
                "- Если сообщение токсичное/конфликтное — предпочитай нейтральные (🤔/😅), избегай 🔥.\n"
            )
            if logger.isEnabledFor(logging.DEBUG):
                preview = allowed_reactions[:50] if len(allowed_reactions) <= 50 else allowed_reactions[:50] + ["…"]
                logger.debug("allowed_reactions (first 50): %s", preview)

        answer_label = "Ответ (JSON):" if (model_driven_reaction and allowed_reactions) else "Ответ:"
        prompt = (
            f"{common_rules}"
            f"{preset_block}"
            f"Твоя роль в этом чате:\n{role_label}\n\n"
            "Последние сообщения чата:\n"
            f"{context_block}\n\n"
            "Сообщение пользователя, на которое нужно ответить:\n"
            f"{source_text}\n\n"
            f"{json_block}\n{answer_label}"
        )

        system_for_call = system_prompt_override if system_prompt_override else self.system_prompt
        raw_text, in_tokens, out_tokens, total_tokens = self._with_retries(
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

        reply_text = raw_text.strip()
        reaction_emoji: Optional[str] = None
        if model_driven_reaction and allowed_reactions:
            try:
                data = json.loads(reply_text)
                if isinstance(data, dict):
                    reply_text = (data.get("reply_text") or "").strip()
                    raw_emoji = data.get("reaction_emoji")
                    if raw_emoji is not None and str(raw_emoji).strip():
                        e = str(raw_emoji).strip()
                        if e in allowed_reactions:
                            reaction_emoji = e
                        else:
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug("reaction_emoji not in allowed: %r raw_json=%s", e, raw_text[:200])
            except json.JSONDecodeError as exc:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("generate_user_reply JSON parse failed: %s raw=%s", exc, raw_text[:200])
                reply_text = raw_text.strip()
                reaction_emoji = None
            gen_info["reaction_emoji"] = reaction_emoji

        if not model_driven_reaction or not allowed_reactions:
            return reply_text, None, in_tokens, out_tokens, total_tokens, gen_info
        return reply_text, reaction_emoji, in_tokens, out_tokens, total_tokens, gen_info

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
