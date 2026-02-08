# Telegram News Reposter_____

Сервис читает новости из указанных Telegram-каналов, перефразирует их через OpenAI и публикует в ваш канал текст или текст + изображение.

## Переменные окружения

Создайте файл `.env` в корне проекта:

```
TELEGRAM_READER_API_ID=
TELEGRAM_READER_API_HASH=
TELEGRAM_READER_SESSION_NAME=
TELEGRAM_WRITER_API_ID=
TELEGRAM_WRITER_API_HASH=
TELEGRAM_WRITER_SESSION_NAME=
TELEGRAM_ACCOUNTS_JSON=
TG_BOT_TOKEN=
TG_BOT_ADMINS_JSON=
PIPELINES_JSON=[{"name":"main","account":"acc1","destination":"@your_destination_channel","sources":["@source_a","@source_b"],"mode":"TEXT","interval_seconds":3600,"blackbox_every_n":3}]
OPENAI_API_KEY=sk-...
OPENAI_SYSTEM_PROMPT_PATH=openai_system_prompt.txt
OPENAI_TEXT_MODEL=gpt-4o-mini
OPENAI_VISION_MODEL=gpt-4o-mini
OPENAI_IMAGE_MODEL=gpt-image-1
OPENAI_TEXT_INPUT_PRICE_PER_1M=0.15
OPENAI_TEXT_OUTPUT_PRICE_PER_1M=0.60
OPENAI_IMAGE_PRICE_1024_USD=0.042
MIN_TEXT_LENGTH=100
MAX_POSTS_PER_RUN=1
TELEGRAM_REQUEST_DELAY_SECONDS=1.0
TELEGRAM_HISTORY_LIMIT=30
FLOOD_WAIT_ANTIBLOCK=true
FLOOD_WAIT_MAX_SECONDS=300
SOURCE_SELECTION_MODE=ROUND_ROBIN
SKIP_POST_PROBABILITY=0.0
RANDOM_JITTER_SECONDS=0.0
SIMPLE_PROFILE_LEVEL=5
GROUP_TEMPO_LEVEL=
GROUP_LOAD_LEVEL=
GROUP_SAFETY_LEVEL=
GROUP_CONTENT_LEVEL=
BLACKBOX_EVERY_N_POSTS=5
DEDUP_ENABLED=true
DEDUP_WINDOW_SIZE=200
AD_FILTER_ENABLED=true
AD_FILTER_THRESHOLD=3
AD_FILTER_KEYWORDS=
```

`PIPELINES_JSON` — JSON-список конвейеров. Каждый конвейер задаёт источники,
назначение, режим, интервал и частоту BLACKBOX. Также поддерживаются
discussion-пайплайны (см. ниже).

`TELEGRAM_ACCOUNTS_JSON` — опциональный JSON-список аккаунтов (reader/writer).
Если задан, глобальные `TELEGRAM_READER_*`/`TELEGRAM_WRITER_*` не обязательны.

Пример структуры (сокращённо):
```
TELEGRAM_ACCOUNTS_JSON='[
  {
    "name": "acc1",
    "reader": {"api_id": 111, "api_hash": "hash", "session": "acc1_reader"},
    "writer": {"api_id": 112, "api_hash": "hash", "session": "acc1_writer"},
    "openai": {"api_key": "sk-...", "system_prompt_path": "openai_prompt_acc1.txt"},
    "behavior": {"simple_profile_level": 3, "group_safety_level": 4}
  }
]'
```

### Админы бота

Формат допускает роли и ограничение по аккаунтам:
```
TG_BOT_ADMINS_JSON=[
  {"id":388247897,"role":"owner","accounts":["*"]},
  {"id":123456789,"role":"editor","accounts":["acc2"]}
]
```
`accounts=["*"]` — доступ ко всем аккаунтам.

### Простые и расширенные настройки поведения

- `SIMPLE_PROFILE_LEVEL` (1–5) — глобальный профиль поведения.
- `GROUP_*_LEVEL` — уровни по группам (темп, нагрузка, безопасность, контент).
- Если `GROUP_*_LEVEL` пустой — используется значение из `SIMPLE_PROFILE_LEVEL`.
- Профили применяются в памяти при запуске, `.env` не переписывается.
- По умолчанию установлен уровень 5 (максимальная осторожность).

Группы соответствуют параметрам:
- Темп и паузы: `TELEGRAM_REQUEST_DELAY_SECONDS`, `RANDOM_JITTER_SECONDS`
- Нагрузка/объём: `TELEGRAM_HISTORY_LIMIT`, `MAX_POSTS_PER_RUN`
- Осторожность: `FLOOD_WAIT_ANTIBLOCK`, `FLOOD_WAIT_MAX_SECONDS`
- Поведение контента: `SOURCE_SELECTION_MODE`, `SKIP_POST_PROBABILITY`

## Discussion-пайплайны

### Типы
- `pipeline_type=STANDARD` — обычный репост новостей.
- `pipeline_type=DISCUSSION` — обсуждения в чате (Pipeline 1 и Pipeline 2).

### Pipeline 1: обсуждение одной новости
- Берёт последние K постов из `post_history` выбранного источника.
- Одним запросом выбирает новость, вторым — генерирует вопрос и ответы.
- Ответы идут с задержками и только в reply.

### Pipeline 2: ответы на живые сообщения
- Периодически читает чат, ищет кандидатов и планирует 1–2 ответа.
- Все проверки (окна активности, пауза, лимиты, доступные userbot’ы, вероятность)
  выполняются до OpenAI.

### Настройки activity windows (хранятся в БД)
- `activity_windows_weekdays_json`, `activity_windows_weekends_json` — список пар `["HH:MM","HH:MM"]`.
- `activity_timezone` — часовой пояс (например `Europe/Moscow`).
- `min_interval_minutes`, `max_interval_minutes` — интервал запусков внутри окна.
- `inactivity_pause_minutes` — пауза, если чат молчит.
- `max_auto_replies_per_chat_per_day`, `user_reply_max_age_minutes` — лимиты Pipeline 2.

Окна не заданы — работает 24/7.

### Persona userbot’ов
Persona влияет только на стиль текста OpenAI в обсуждениях.
Хранится в таблице `userbot_persona`:
- `persona_tone`: `neutral|analytical|emotional|ironic|skeptical`
- `persona_verbosity`: `short|medium`
- `persona_style_hint`: произвольная подсказка

Если persona не задана — используется нейтральный короткий стиль.

### Логи OpenAI для обсуждений
В `logs/service.log` пишутся строки:
`openai_usage kind=... model=... pipeline=... chat=... input=... output=... total=... extra=...`
где `kind` — `discussion_select`, `discussion_qna`, `user_reply`.

## Нюансы проекта (для переноса)

- OpenAI: требуется рабочий доступ к API из региона сервера; ключ хранится в `OPENAI_API_KEY`.
- Telethon: при первом запуске запрашивает телефон и код; создаёт файл сессии `*.session` (лежит рядом с `TELEGRAM_SESSION_NAME`), без него будет повторная авторизация.
- БД: используется `./tg_post_service.db`; при переносе можно взять с собой, чтобы сохранить `last_message_id` и счётчик постов.
- Round‑robin: индекс канала и `post_counter` хранятся в БД; `BLACKBOX_EVERY_N_POSTS` влияет на каждый N‑й успешный пост.
- Режим `TEXT_IMAGE`: если в исходном посте нет фото, сервис публикует только текст.
- Альбомы (несколько фото): подпись берётся из сообщения внутри альбома, где есть текст.
- Антифлуд Telegram: есть задержка запросов, лимит истории и обработка FloodWait (настраивается через `.env`).
- Если FloodWait превышает интервал пайплайна, аккаунт ставится на паузу до конца FloodWait и owner-админ получает уведомление с датой окончания.

## Установка

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск

```
python main.py
```

или

```
python -m project_root.main
```

## Docker Compose

1) Создайте `.env` в корне проекта (как в разделе переменных окружения).

2) Локальный запуск:
```
touch tg_post_service.db
IMAGE_REPO=your_org/your_repo docker compose up -d
```

3) Логи:
```
docker compose logs -f
```

## CI/CD (GitHub Actions + GHCR)

Workflow делает:
- CI: сборка образа и публикация в GHCR.
- CD: деплой на сервер по SSH с `docker compose pull` + `up -d`.

Нужные secrets в GitHub:
- `SSH_HOST`, `SSH_USER`, `SSH_KEY`, `SSH_PORT`
- `DEPLOY_PATH` (путь на сервере с `docker-compose.yml` и `.env`)
- `GHCR_USERNAME`, `GHCR_TOKEN` (PAT с `read:packages`)
