# Telegram News Reposter

Сервис читает новости из указанных Telegram-каналов, перефразирует их через OpenAI и публикует в ваш канал текст или текст + изображение.

## Переменные окружения

Создайте файл `.env` в корне проекта:

```
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_SESSION_NAME=tg_post_service
DEST_CHANNEL=@your_destination_channel
SOURCE_CHANNELS=["@source_a","@source_b","@source_c"]
OPENAI_API_KEY=sk-...
POSTING_MODE=TEXT_IMAGE
POSTING_INTERVAL_SECONDS=300
MIN_TEXT_LENGTH=100
MAX_POSTS_PER_RUN=1
TELEGRAM_REQUEST_DELAY_SECONDS=1.0
TELEGRAM_HISTORY_LIMIT=30
FLOOD_WAIT_ANTIBLOCK=true
FLOOD_WAIT_MAX_SECONDS=300
BLACKBOX_EVERY_N_POSTS=5
```

`SOURCE_CHANNELS` можно задавать как JSON-список или строку через запятую.

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
