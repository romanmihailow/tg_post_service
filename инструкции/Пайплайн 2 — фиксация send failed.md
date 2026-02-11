# Пайплайн 2 — фиксация send failed

## Проблема

При отправке живого ответа Pipeline 2 вызывается `send_reply_text(...)`. Если Telegram возвращает ошибку (нет прав, бот не в чате, сеть и т.п.), выбрасывается исключение. В блоке `except Exception` выполнялись только:

- `logger.exception("user reply cancelled: send failed")`
- `_update_pipeline_status(..., state="cancelled", message="reply X: send failed")`

Вызов **`mark_discussion_reply_cancelled(...)` отсутствовал**, поэтому запись в таблице `discussion_replies` оставалась со статусом **pending**. При следующих циклах она снова попадала в `list_due_discussion_replies`, снова пытались отправить, снова исключение — бесконечный цикл, сообщения в чат не доходили.

## Что изменено в scheduler.py

**Файл:** `project_root/scheduler.py`  
**Функция:** `_send_due_user_replies`

В блоке `except` при ошибке вызова `send_reply_text` добавлен вызов:

```python
mark_discussion_reply_cancelled(
    session,
    reply,
    f"send failed: {exc.__class__.__name__}",
)
```

- Обработчик переименован в `except Exception as exc:`, чтобы подставить класс исключения в `cancelled_reason`.
- `logger.exception(...)` и `_update_pipeline_status(...)` оставлены без изменений.
- Импорт `mark_discussion_reply_cancelled` из `project_root.db` уже был в файле, дополнять не потребовалось.

## Поведение Pipeline 2 при ошибке Telegram после правки

1. При исключении из `send_reply_text` запись в `discussion_replies` переводится в **status='cancelled'**.
2. В **cancelled_reason** пишется строка вида `send failed: <ИмяКлассаИсключения>` (например `send failed: ChatWriteForbidden`).
3. Эта запись больше не попадает в выборку due (`status != 'pending'`), повторных попыток отправки по ней не будет.
4. В логах по-прежнему пишется полный traceback; в статусе пайплайна в боте — «cancelled» с сообщением «reply X: send failed».

## Как проверить результат

1. **По БД:** после ошибки отправки у соответствующей строки в `discussion_replies` должны быть `status='cancelled'` и `cancelled_reason` вида `send failed: ...`:
   ```sql
   SELECT id, status, cancelled_reason
   FROM discussion_replies
   WHERE kind = 'user_reply' AND status = 'cancelled'
   ORDER BY id DESC LIMIT 10;
   ```

2. **По боту:** при сбое отправки статус Pipeline 2 переходит в «cancelled» с текстом «reply &lt;id&gt;: send failed»; запланированные ответы с этим id больше не будут бесконечно переобрабатываться.

3. **По логам:** при ошибке по-прежнему пишется `logger.exception("user reply cancelled: send failed")` с полным traceback — по нему можно понять тип ошибки Telegram (права, чат, сеть и т.д.).
