# Пайплайн 2 — диагностика (после увеличения max age)

Диагностика проведена **без изменений** в коде, БД и конфигурации. Опора на предыдущий отчёт «Пайплайн 2 — диагностика (runtime).md» и на текущее состояние БД и кода после увеличения `user_reply_max_age_minutes` до 120 для discuss_news_blackbox.

---

## Краткое резюме

- **Проверено:** состояние `discussion_replies` (kind='user_reply'), `chat_state`, `discussion_settings`; код формирования статуса в боте; логика выбора due-ответов и проверок перед отправкой; обработка ошибок при вызове Telegram.
- **Записи user_reply:** после ваших сообщений в @news_backstage появились **новые** записи (id 80, 81, 82), привязанные к сообщениям 269 и 270 и боту t9174800805 (и 81 — t9174803110). Все три в статусе **pending**; **sent_at** и **cancelled_reason** пустые.
- **Отмена по «message too old»:** для этих записей не применялась (max age = 120, время сообщений 12:56 и 13:02, send_at 13:06–13:21 — в пределах лимита). Старые отменённые записи (id 41, 44–48, 54) по-прежнему с причиной «message too old».
- **Попытки отправки:** по коду due-ответы с send_at в прошлом должны выбираться и для них вызываться `send_reply_text`. То, что записи 80, 81, 82 остаются **pending** и без **sent_at**, при том что время отправки уже прошло, указывает на то, что либо отправка не вызывалась (например, сервис не работал в нужное время), либо при вызове произошла **ошибка**, но запись в БД **не обновляется**.
- **Критическая находка в коде:** при исключении в `send_reply_text` (строки 1868–1876 в `scheduler.py`) обновляется только статус пайплайна в памяти («cancelled», «reply X: send failed»), но **не вызывается** `mark_discussion_reply_cancelled(session, reply, "send failed")`. Поэтому строка в `discussion_replies` остаётся со статусом **pending**. При следующих циклах те же записи снова попадают в due, снова пытаемся отправить, снова исключение — в чате сообщений нет, в БД всё ещё pending. Это согласуется с наблюдаемой картиной: «запланировано» по боту, потом «ожидание» / «no candidates», ответов в чате нет.

---

## 1. DISCUSSION-пайплайн и текущие настройки

- **Пайплайн:** id=3, name=`discuss_news_blackbox`, type=DISCUSSION.
- **discussion_settings** (pipeline_id=3):  
  `target_chat=@news_backstage`, `user_reply_max_age_minutes=120`, `max_auto_replies_per_chat_per_day=30`, `inactivity_pause_minutes=0`.  
  То есть лимит возраста сообщения уже 120 минут; остальные параметры не менялись.

---

## 2. Разбор discussion_replies по последним событиям

Запрос к БД (последние записи kind='user_reply'):

```text
id  pipeline_id  kind       chat_id         account_name   status    send_at              sent_at  cancelled_reason  reply_to_message_id  source_message_at
82  3            user_reply @news_backstage t9174800805    pending   2026-02-11 13:21:59  NULL     NULL              270                   2026-02-11 13:02:59
81  3            user_reply @news_backstage t9174803110   pending   2026-02-11 13:12:59  NULL     NULL              270                   2026-02-11 13:02:59
80  3            user_reply @news_backstage t9174800805    pending   2026-02-11 13:06:46  NULL     NULL              269                   2026-02-11 12:56:46
54  3            user_reply @news_backstage t9876002641    cancelled ...                  NULL     message too old   222                   ...
...
```

**Сопоставление со статусом в боте:**

- Сообщение «**Статус: запланировано**, Детали: **message 269: bot t9174800805**» формируется в `scheduler.py` в `_plan_user_reply_for_candidate` сразу после `create_discussion_reply` и `_update_pipeline_status(..., state="scheduled", next_action_at=send_at, message=f"message {candidate.get('message_id')}: bot {bot_weight.account_name}")`. То есть **message 269** — это id сообщения в чате (reply_to_message_id), для которого **уже создана** запись в `discussion_replies`. Этому соответствует запись **id=80**: reply_to_message_id=269, account_name=t9174800805, send_at=13:06:46.
- Аналогично «**message 270: bot t9174800805**» соответствует записи **id=82** (reply_to_message_id=270, account_name=t9174800805, send_at=13:21:59); для сообщения 270 также создана запись **id=81** (bot t9174803110, send_at=13:12:59).

**Вывод по статусам:**

- Записи 80, 81, 82 — **новые**, после увеличения max age; созданы под ваши сообщения 269 и 270.
- Все три в статусе **pending**, **sent_at** и **cancelled_reason** пустые — ни одна не была ни отправлена, ни отменена в БД.
- Время отправки (send_at) уже в прошлом (13:06, 13:12, 13:21), значит они должны были попадать в выборку due и обрабатываться. То, что они до сих пор pending, означает либо что в момент их send_at сервис не обрабатывал Pipeline 2, либо при обработке произошла ошибка **до** записи в БД (и при этом, как показано ниже, при ошибке отправки статус в БД не меняется).

---

## 3. Состояние chat_state и влияние на ситуацию

Для pipeline_id=3, chat_id=@news_backstage:

- **last_seen_message_id** = 270 — сервис видел сообщения как минимум до 270 (в т.ч. ваши 269 и 270).
- **last_human_message_at** = 2026-02-11 13:02:59 — время последнего человеческого сообщения.
- **replies_today** = 3, **replies_today_date** = 2026-02-11 — за день запланировано 3 ответа (счётчик увеличивается при создании записи в `_plan_user_reply_for_candidate`), лимит 30 не исчерпан.
- **next_scan_at** = 2026-02-11 13:07:19 — следующий скан разрешён с этого момента.

Вывод: chat_state не мешает ни сканам, ни планированию. Сообщения 269 и 270 были обработаны, ответы запланированы. Проблема не в «нет кандидатов» и не в лимите дня, а в том, что запланированные ответы не переходят в «sent» и не помечаются «cancelled» в БД после момента send_at.

---

## 4. Анализ отправки due-ответов: почему записи не доходят до sent

**Как выбираются due-ответы:**  
В `_send_due_user_replies` вызывается `list_due_discussion_replies(session, pipeline.id, now, kind="user_reply")` (`db.py`): выбираются строки с `pipeline_id`, `status='pending'`, `send_at <= now`, `kind='user_reply'`, порядок по `send_at`. Записи 80, 81, 82 под это подходят (send_at в прошлом, status pending).

**Проверки перед отправкой (scheduler.py, _send_due_user_replies):**

1. **allow_send** — если мы вне окна активности, все due отменяются с «outside activity window»; у вас окна не заданы, так что не причина.
2. **inactivity** — при `inactivity_pause_minutes > 0` проверяется давность `last_human_message_at`; у вас 0, проверка не выполняется.
3. **reply_to_message_id** — если NULL, отмена с «missing reply_to»; у 80, 81, 82 он задан.
4. **Возраст сообщения** — если `(now - source_message_at)` в минутах > `user_reply_max_age_minutes` (120), отмена с «message too old». Для 80, 81, 82 source_message_at 12:56 и 13:02 — в пределах 120 минут от типичного времени запуска.
5. **account** — если `accounts.get(reply.account_name)` нет, отмена с «account_missing».
6. **_can_use_bot_for_reply** — лимит в день и cooldown по `discussion_bot_weights`; при неудаче отмена с «cooldown/limit».

При любой из этих отмен вызывается **mark_discussion_reply_cancelled** — в БД появился бы **cancelled_reason**. У записей 80, 81, 82 его нет, значит до отмены по этим пунктам дело не дошло (или они не выполнялись в момент обработки этих reply).

**Отправка и обработка ошибок:**

- Вызов: `sent = await send_reply_text(account.writer_client, reply.chat_id or settings.target_chat, reply.reply_text, reply_to_message_id=..., ...)` (строки 1858–1866).
- При **успехе**: `mark_discussion_reply_sent(session, reply, now)`, `_update_bot_usage(...)`, лог «user reply sent», обновление статуса пайплайна на «sent». В БД у reply был бы **status='sent'**, **sent_at** заполнен.
- При **исключении** (строки 1868–1876):

  ```python
  except Exception:
      logger.exception("user reply cancelled: send failed")
      _update_pipeline_status(
          pipeline,
          category="pipeline2",
          state="cancelled",
          message=f"reply {reply.id}: send failed",
      )
      continue
  ```

  Здесь **не вызывается** `mark_discussion_reply_cancelled(session, reply, "send failed")`. В БД запись остаётся **pending**, **sent_at** и **cancelled_reason** не меняются. В следующих циклах эта же запись снова попадает в due, снова вызывается send_reply_text, снова может выбросить исключение — цикл без перехода в sent и без фиксации отмены в БД.

Итог: по коду единственное объяснение того, что записи 80, 81, 82 остаются pending при уже прошедшем send_at — либо due-ответы в момент send_at не обрабатывались (сервис не работал/не доходил до Pipeline 2), либо при каждой попытке отправки возникает исключение и из-за отсутствия вызова `mark_discussion_reply_cancelled` статус в БД не обновляется. Второй вариант хорошо согласуется с тем, что в чате сообщений нет, а в БД — только pending.

---

## 5. Ошибки Telegram и отсутствие обновления статуса в БД

В **telegram_client.py** `send_reply_text` просто вызывает `client.send_message(...)` через `_send_with_flood_wait`. Любая ошибка Telegram (ChatWriteForbidden, UserBannedInChannel, бот не в чате, сеть и т.д.) всплывает как исключение. Обработка делается **только в scheduler.py** в блоке `except Exception` (1868–1876): логирование и обновление статуса пайплайна; **запись в discussion_replies не обновляется**.

Итог:

- Ошибки Telegram **не** попадают в поле **cancelled_reason** и **не** переводят статус в «cancelled» — только в логах и в статусе пайплайна («reply X: send failed»).
- Если отправка падает (например, юзербот не в чате @news_backstage или нет прав), запись навсегда остаётся pending, при следующих запусках попытки отправки повторяются и снова падают. Это **отдельная потенциальная причина** невидимых в чате ответов и «зависших» pending-записей.

---

## 6. Логи

Каталог `/home/tg_post_service/logs/` пуст (файлов нет). Подтвердить по логам «user reply scheduled/sent/cancelled» или «send failed» нельзя; выводы сделаны по БД и коду.

---

## 7. Вероятная причина, почему ответов в чате нет

1. **Ответы планируются корректно.** Сообщения 269 и 270 видны (last_seen_message_id=270), по ним созданы записи 80, 81, 82 в `discussion_replies` со статусом pending и с send_at в прошлом. Статус в боте «запланировано / message 269: bot t9174800805» и т.п. соответствует этим записям (reply_to_message_id, account_name).

2. **Отмена по «message too old» для этих записей не срабатывает.** В discussion_settings для pipeline_id=3 стоит user_reply_max_age_minutes=120; source_message_at 12:56 и 13:02 укладываются в окно, так что при обычном времени работы сервиса проверка возраста не должна их отменять.

3. **При попытке отправить due-ответ вызов Telegram, по всей видимости, падает с исключением** (например, юзербот не в чате @news_backstage, нет прав на запись, или сетевая/API ошибка). В коде при этом выполняется только `logger.exception` и `_update_pipeline_status(..., "cancelled", "reply X: send failed")`, **без** `mark_discussion_reply_cancelled`. Поэтому в БД записи 80, 81, 82 остаются **pending**, без sent_at и без cancelled_reason.

4. **Следствие:** при каждом следующем цикле те же три записи снова попадают в list_due_discussion_replies, снова вызывается send_reply_text, снова исключение — в чате ничего не появляется, в БД по-прежнему три pending. Это согласуется с наблюдаемым: «запланировано» по message 269/270 и боту, затем «ожидание» / «no candidates», ответов в чате нет.

**Привязка к коду и БД:**

- **БД:** discussion_replies, id in (80, 81, 82): status='pending', sent_at=NULL, cancelled_reason=NULL; send_at и source_message_at в прошлом.
- **Код:** `scheduler.py`, `_send_due_user_replies`, блок except после `send_reply_text` (примерно строки 1868–1876): отсутствует вызов `mark_discussion_reply_cancelled(session, reply, "send failed")`.

---

## 8. На что смотреть дальше (только рекомендации)

1. **Исправить обработку ошибки отправки:** в блоке `except` после `send_reply_text` вызывать `mark_discussion_reply_cancelled(session, reply, "send failed")` (и при необходимости `session.flush()`/commit в рамках существующей транзакции), чтобы при падении отправки запись переходила в cancelled и не подбиралась в due бесконечно. Так можно и подтвердить гипотезу: после фикса в БД у таких записей появится cancelled_reason.

2. **Проверить доступ юзерботов к чату:** убедиться, что аккаунты t9174800805 и t9174803110 добавлены в @news_backstage и могут писать в чат. Если бот не в чате или без прав, Telegram вернёт ошибку — как раз тот случай, который сейчас не фиксируется в discussion_replies.

3. **Логирование:** при исключении в send_reply_text логировать reply.id, account_name, chat_id и текст исключения (и по возможности тип ошибки Telegram). Это позволит по логам отличать «send failed» из-за прав/чата от сетевых сбоев.

4. **Проверить, что due обрабатываются в нужное время:** при следующем деплое убедиться, что сервис запущен в момент после send_at (например через 5–10 минут после вашего сообщения) и что в логах есть либо «user reply sent», либо «user reply cancelled: send failed» / exception. Если после добавления mark_discussion_reply_cancelled в except появятся записи с cancelled_reason «send failed», это будет прямым подтверждением ошибки отправки.

5. **Не менять логику планирования и проверок** (вероятности, лимиты, возраст, inactivity) — по текущим данным они работают: записи создаются, до отправки доходят; проблема в моменте отправки и в обновлении статуса при ошибке.

---

*Диагностика выполнена только чтением БД и кода; изменений в коде, конфигурации и БД не вносилось.*
