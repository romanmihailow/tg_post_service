"""Telegram bot service (admin control panel)."""

from __future__ import annotations

import json
import logging
import os
import re
import secrets
import string
from datetime import datetime, timedelta, timezone
from typing import Iterable

from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from project_root.config import Config
from project_root.config import (
    BehaviorProfileConfig,
    OpenAIAccountConfig,
    PipelineConfig,
    TelegramCredentials,
    TelegramAccountConfig,
)
from project_root.openai_client import OpenAIClient
from project_root.runtime import AccountRuntime
from project_root.telegram_client import create_client
from project_root.db import (
    add_pipeline_source,
    create_pipeline,
    create_invite,
    create_invite_code,
    delete_pipeline,
    get_all_pipelines,
    get_discussion_settings,
    get_invite,
    get_invite_code,
    get_pipeline_by_name,
    get_pipeline_sources,
    get_session,
    get_userbot_persona,
    mark_invite_code_used,
    mark_invite_used,
    update_pipeline_destination,
    update_pipeline_interval,
    update_pipeline_mode,
    remove_pipeline_source,
    toggle_pipeline_enabled,
    upsert_userbot_persona,
)

logger = logging.getLogger(__name__)
AUDIT_LOG_PATH = "logs/audit.log"



def _is_admin(user_id: int | None, admins: Iterable[int]) -> bool:
    return user_id is not None and user_id in set(admins)
ROLE_PERMISSIONS = {
    "owner": {
        "view",
        "create",
        "edit",
        "delete",
        "behavior",
        "prompt",
        "persona",
        "logs",
        "invite",
    },
    "admin": {"view", "create", "edit", "behavior", "prompt", "persona", "logs", "invite"},
    "editor": {"view", "create", "edit", "delete", "prompt", "persona"},
    "viewer": {"view"},
}


def _role_for_user(config: Config, user_id: int | None) -> str | None:
    return config.admin_role(user_id)


def _has_permission(config: Config, user_id: int | None, permission: str) -> bool:
    role = _role_for_user(config, user_id)
    if not role:
        return False
    return permission in ROLE_PERMISSIONS.get(role, set())


def _generate_invite_token() -> str:
    return secrets.token_urlsafe(12)


def _generate_invite_code(length: int = 6) -> str:
    return "".join(secrets.choice(string.digits) for _ in range(length))


def _strip_masked_value(value: str) -> str | None:
    cleaned = value.strip()
    if len(cleaned) < 7:
        return None
    return cleaned[3:-3]


def _derive_session_name(name: str, existing_sessions: set[str]) -> str:
    base = re.sub(r"[^a-z0-9_]+", "", name.strip().lower().replace(" ", "_"))
    if not base:
        base = "tg_account"
    candidate = base
    suffix = 2
    while candidate in existing_sessions:
        candidate = f"{base}_{suffix}"
        suffix += 1
    return candidate


def _read_prompt_file(path: str) -> str:
    if not os.path.exists(path):
        raise FileNotFoundError(f"System prompt file not found: {path}")
    with open(path, "r", encoding="utf-8") as file_handle:
        content = file_handle.read().strip()
    if not content:
        raise ValueError(f"System prompt file is empty: {path}")
    return content


def _account_has_session(account: TelegramAccountConfig) -> bool:
    session_path = f"{account.reader.session}.session"
    return os.path.exists(session_path)


def _can_access_account(config: Config, user_id: int | None, account_name: str) -> bool:
    allowed = config.admin_accounts(user_id)
    if not allowed:
        return False
    return "*" in allowed or account_name in allowed



MENU_MAIN = "main"
MENU_ACCOUNTS = "accounts"
MENU_ACCOUNT = "account"
MENU_PIPELINES = "pipelines"
MENU_PIPELINE = "pipeline"
MENU_BEHAVIOR = "behavior"
MENU_BEHAVIOR_LEVEL = "behavior_level"
MENU_PERSONA = "persona"
MENU_LOGS = "logs"

HEADER_TITLES = {
    MENU_MAIN: "🧭 Главное меню",
    MENU_ACCOUNTS: "👤 Аккаунты",
    MENU_ACCOUNT: "👤 Аккаунт",
    MENU_PIPELINES: "🧩 Пайплайны",
    MENU_PIPELINE: "⚙️ Пайплайн",
    MENU_BEHAVIOR: "🧠 Поведение",
    MENU_BEHAVIOR_LEVEL: "🧠 Поведение",
    MENU_PERSONA: "🎭 Личность",
    MENU_LOGS: "📁 Логи",
}


def _main_menu_keyboard(
    config: Config,
    *,
    user_id: int | None = None,
    has_account: bool = False,
    has_pipeline: bool = False,
) -> ReplyKeyboardMarkup:
    rows: list[list[str]] = []
    rows.append(["Аккаунты", "Пайплайны"])
    quick_row: list[str] = []
    if has_account:
        quick_row.append("Аккаунт")
    if has_pipeline:
        quick_row.append("Пайплайн")
    if quick_row:
        rows.append(quick_row)
    account_actions: list[str] = []
    if has_account and _has_permission(config, user_id, "behavior"):
        account_actions.append("Поведение")
    if has_account and _has_permission(config, user_id, "prompt"):
        account_actions.append("Промпты")
    if account_actions:
        rows.append(account_actions)
    status_row = ["Статус"]
    if _has_permission(config, user_id, "logs"):
        status_row.append("Логи")
    rows.append(status_row)
    if _is_admin(user_id, config.bot_admin_ids):
        rows.append(["Мои"])
    if _has_permission(config, user_id, "invite"):
        rows.append(["Пригласить"])
    rows.append(["Справка"])
    rows.append(["Меню"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _account_list_keyboard(accounts: list[str]) -> ReplyKeyboardMarkup:
    rows = []
    row: list[str] = []
    for name in accounts:
        row.append(name)
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append(["Назад", "Меню", "Статус"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _account_menu_keyboard(
    config: Config,
    *,
    user_id: int | None = None,
    has_account: bool = False,
) -> ReplyKeyboardMarkup:
    rows: list[list[str]] = [["Каналы", "Пайплайны"]]
    actions: list[str] = []
    if has_account and _has_permission(config, user_id, "behavior"):
        actions.append("Поведение")
    if has_account and _has_permission(config, user_id, "prompt"):
        actions.append("Промпт")
    if has_account and _has_permission(config, user_id, "persona"):
        actions.append("Личность")
    if actions:
        rows.append(actions)
    rows.append(["Назад", "Меню", "Статус"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _pipelines_list_keyboard(
    pipelines: list[str], *, can_create: bool = False
) -> ReplyKeyboardMarkup:
    rows = []
    row: list[str] = []
    for name in pipelines:
        row.append(name)
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    if can_create:
        rows.append(["Создать пайплайн"])
    rows.append(["Назад", "Меню", "Статус"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _pipeline_menu_keyboard(
    *, can_edit: bool = False, can_delete: bool = False, can_view: bool = True
) -> ReplyKeyboardMarkup:
    rows: list[list[str]] = []
    if can_view:
        rows.append(["Инфо"])
    if can_edit:
        rows.append(["Канал назначения"])
        rows.append(["Добавить источник", "Удалить источник"])
        rows.append(["Режим", "Интервал"])
        rows.append(["Вкл/выкл"])
    if can_delete:
        rows.append(["Удалить пайплайн"])
    rows.append(["Назад", "Меню", "Статус"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _behavior_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["Простой уровень"],
            ["Темп", "Нагрузка"],
            ["Осторожность", "Контент"],
            ["Назад", "Меню", "Статус"],
        ],
        resize_keyboard=True,
    )


def _logs_menu_keyboard(*, can_view: bool = False) -> ReplyKeyboardMarkup:
    rows: list[list[str]] = []
    if can_view:
        rows.append(["Ошибки", "Посты/расходы"])
    rows.append(["Назад", "Меню", "Статус"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _level_keyboard_with_labels(
    labels: list[str], current: int | None = None
) -> ReplyKeyboardMarkup:
    row = []
    for idx, label in enumerate(labels, start=1):
        if current == idx:
            row.append(f"{label} ✓")
        else:
            row.append(label)
    return ReplyKeyboardMarkup([row, ["Назад", "Меню", "Статус"]], resize_keyboard=True)


def _interval_keyboard(current_minutes: int | None = None) -> ReplyKeyboardMarkup:
    options = ["1 (60м)", "2 (120м)", "3 (180м)", "4 (240м)", "5 (вручную)"]
    row = []
    for option in options:
        if current_minutes and option.startswith("1") and current_minutes == 60:
            row.append(f"{option} ✓")
        elif current_minutes and option.startswith("2") and current_minutes == 120:
            row.append(f"{option} ✓")
        elif current_minutes and option.startswith("3") and current_minutes == 180:
            row.append(f"{option} ✓")
        elif current_minutes and option.startswith("4") and current_minutes == 240:
            row.append(f"{option} ✓")
        else:
            row.append(option)
    return ReplyKeyboardMarkup([row, ["Назад", "Меню", "Статус"]], resize_keyboard=True)


def _persona_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["Тон", "Краткость"], ["Стиль"], ["Сбросить"], ["Назад", "Меню", "Статус"]],
        resize_keyboard=True,
    )


def _persona_tone_keyboard(current: str | None) -> ReplyKeyboardMarkup:
    options = [
        ("neutral", "Нейтральный"),
        ("analytical", "Аналитичный"),
        ("emotional", "Эмоциональный"),
        ("ironic", "Ироничный"),
        ("skeptical", "Скептичный"),
    ]
    row = []
    for key, label in options:
        if current == key:
            row.append(f"{label} ✓")
        else:
            row.append(label)
    return ReplyKeyboardMarkup([row, ["Назад", "Меню", "Статус"]], resize_keyboard=True)


def _persona_verbosity_keyboard(current: str | None) -> ReplyKeyboardMarkup:
    options = [("short", "Коротко"), ("medium", "Средне")]
    row = []
    for key, label in options:
        if current == key:
            row.append(f"{label} ✓")
        else:
            row.append(label)
    return ReplyKeyboardMarkup([row, ["Назад", "Меню", "Статус"]], resize_keyboard=True)


def _parse_persona_tone(text: str) -> str | None:
    cleaned = text.replace(" ✓", "").strip().lower()
    mapping = {
        "нейтральный": "neutral",
        "аналитичный": "analytical",
        "эмоциональный": "emotional",
        "ироничный": "ironic",
        "скептичный": "skeptical",
    }
    return mapping.get(cleaned)


def _parse_persona_verbosity(text: str) -> str | None:
    cleaned = text.replace(" ✓", "").strip().lower()
    mapping = {"коротко": "short", "средне": "medium"}
    return mapping.get(cleaned)


def _parse_level_from_label(text: str) -> int | None:
    cleaned = text.strip()
    if not cleaned:
        return None
    digit = cleaned[0]
    if digit.isdigit():
        value = int(digit)
        if 1 <= value <= 5:
            return value
    return None


async def _set_menu(
    update: Update, context: ContextTypes.DEFAULT_TYPE, menu: str, text: str, keyboard
) -> None:
    context.user_data["menu"] = menu
    header = _header_text(menu, context)
    message = f"{header}\n{text}" if text else header
    await update.message.reply_text(message, reply_markup=keyboard)


def _header_text(menu: str, context: ContextTypes.DEFAULT_TYPE) -> str:
    title = HEADER_TITLES.get(menu, "🧭 Меню")
    suffix = _header_suffix(context, menu)
    return f"{title}{suffix}"


def _header_suffix(context: ContextTypes.DEFAULT_TYPE, menu: str) -> str:
    account = context.user_data.get("account")
    pipeline = context.user_data.get("pipeline")
    parts: list[str] = []
    if menu in {MENU_MAIN, MENU_ACCOUNTS, MENU_ACCOUNT, MENU_BEHAVIOR, MENU_BEHAVIOR_LEVEL}:
        if account:
            parts.append(str(account))
    if menu in {MENU_MAIN, MENU_PIPELINES, MENU_PIPELINE}:
        if pipeline:
            parts.append(str(pipeline))
    if not parts:
        return ""
    return " · " + " · ".join(parts)


async def _start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.bot_data["config"]
    if context.args and context.args[0].startswith("ref_"):
        await _handle_ref_start(update, context, context.args[0][4:])
        return
    if not _is_admin(
        update.effective_user.id if update.effective_user else None,
        config.bot_admin_ids,
    ):
        user_id = update.effective_user.id if update.effective_user else None
        await update.message.reply_text(
            f"Нет доступа. Ваш id: {user_id}. Админы: {config.bot_admin_ids}"
        )
        return
    await _set_menu(
        update,
        context,
        MENU_MAIN,
        "",
        _main_menu_keyboard(
            config,
            user_id=update.effective_user.id if update.effective_user else None,
            has_account=bool(context.user_data.get("account")),
            has_pipeline=bool(context.user_data.get("pipeline")),
        ),
    )


async def _menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _start(update, context)


async def _back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _go_back(update, context)


async def _handle_ref_start(
    update: Update, context: ContextTypes.DEFAULT_TYPE, token: str
) -> None:
    user_id = update.effective_user.id if update.effective_user else None
    if user_id is None:
        await update.message.reply_text("Не удалось определить пользователя.")
        return
    with get_session() as session:
        invite = get_invite(session, token)
        if not invite:
            await update.message.reply_text("Ссылка недействительна.")
            return
        if invite.used_at is not None:
            await update.message.reply_text("Ссылка уже использована.")
            return
        if invite.expires_at < datetime.utcnow():
            await update.message.reply_text("Ссылка просрочена.")
            return
        code = _generate_invite_code()
        expires_at = datetime.utcnow() + timedelta(minutes=10)
        create_invite_code(
            session=session,
            token=token,
            code=code,
            created_for=user_id,
            expires_at=expires_at,
        )
        session.commit()
    await context.bot.send_message(
        chat_id=invite.created_by,
        text=f"Код доступа для пользователя {user_id}: {code}",
    )
    context.user_data["awaiting"] = {"type": "invite_code", "token": token}
    context.user_data["invite_token"] = token
    _audit_log("invite.code", user_id, f"token={token}")
    await update.message.reply_text("Введите код, который пришёл администратору.")


async def _create_invite_link(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    user_id = update.effective_user.id if update.effective_user else None
    if user_id is None:
        await update.message.reply_text("Не удалось определить пользователя.")
        return
    token = _generate_invite_token()
    expires_at = datetime.utcnow() + timedelta(hours=24)
    with get_session() as session:
        create_invite(session, token=token, created_by=user_id, expires_at=expires_at)
        session.commit()
    bot = await context.bot.get_me()
    link = f"https://t.me/{bot.username}?start=ref_{token}"
    _audit_log("invite.create", user_id, f"token={token}")
    await update.message.reply_text(f"Ссылка для приглашения:\n{link}")


async def _account_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.bot_data["config"]
    user_id = update.effective_user.id if update.effective_user else None
    account = context.user_data.get("account")
    if account and _can_access_account(config, user_id, account):
        account_cfg = _get_account_config(context, account)
        await _set_menu(
            update,
            context,
            MENU_ACCOUNT,
            "",
            _account_menu_keyboard(
                config,
                user_id=user_id,
                has_account=bool(context.user_data.get("account")),
            ),
        )
        return
    await _set_menu(
        update,
        context,
        MENU_ACCOUNTS,
        "",
        _account_list_keyboard(
            [
                item.name
                for item in config.telegram_accounts()
                if _can_access_account(config, user_id, item.name)
            ]
        ),
    )


async def _pipeline_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.bot_data["config"]
    user_id = update.effective_user.id if update.effective_user else None
    pipeline = context.user_data.get("pipeline")
    if pipeline:
        pipeline_account = _pipeline_account_name(pipeline)
        if pipeline_account and not _can_access_account(
            config, user_id, pipeline_account
        ):
            await update.message.reply_text("Недостаточно прав.")
            return
        await _set_menu(
            update,
            context,
            MENU_PIPELINE,
            "",
            _pipeline_menu_keyboard(
                can_edit=_has_permission(config, user_id, "edit"),
                can_delete=_has_permission(config, user_id, "delete"),
                can_view=_has_permission(config, user_id, "view"),
            ),
        )
        return
    await _set_menu(
        update,
        context,
        MENU_PIPELINES,
        "",
        _pipelines_list_keyboard(
            _list_pipeline_names(user_id=user_id, config=config),
            can_create=_has_permission(config, user_id, "create"),
        ),
    )


async def _status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.bot_data["config"]
    user_id = update.effective_user.id if update.effective_user else None
    await update.message.reply_text(f"📊 Статус\n{_pipeline_summary(config, user_id)}")


async def _my_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.bot_data["config"]
    user_id = update.effective_user.id if update.effective_user else None
    if not _is_admin(user_id, config.bot_admin_ids):
        await update.message.reply_text("Нет доступа.")
        return
    role = config.admin_role(user_id) or "viewer"
    accounts = config.admin_accounts(user_id)
    if not accounts:
        accounts_text = "-"
    elif "*" in accounts:
        accounts_text = "все"
    else:
        accounts_text = ", ".join(accounts)
    await update.message.reply_text(
        f"👤 Мои права\nРоль: {role}\nАккаунты: {accounts_text}"
    )


def _format_behavior_levels(behavior: BehaviorProfileConfig | None) -> str:
    if not behavior:
        return "Текущий профиль: по умолчанию (уровень 3)"
    return (
        "Текущие уровни:\n"
        f"- общий: {behavior.simple_profile_level or 3}\n"
        f"- темп: {behavior.group_tempo_level or '-'}\n"
        f"- нагрузка: {behavior.group_load_level or '-'}\n"
        f"- осторожность: {behavior.group_safety_level or '-'}\n"
        f"- контент: {behavior.group_content_level or '-'}"
    )


def _format_persona_summary(account_name: str) -> str:
    with get_session() as session:
        persona = get_userbot_persona(session, account_name)
    tone_key = persona.persona_tone if persona and persona.persona_tone else "neutral"
    verbosity = (
        persona.persona_verbosity if persona and persona.persona_verbosity else "short"
    )
    tone_labels = {
        "neutral": "нейтральный",
        "analytical": "аналитичный",
        "emotional": "эмоциональный",
        "ironic": "ироничный",
        "skeptical": "скептичный",
    }
    verbosity_labels = {"short": "коротко", "medium": "средне"}
    style_hint = (
        persona.persona_style_hint if persona and persona.persona_style_hint else "-"
    )
    return (
        "Текущие настройки:\n"
        f"- тон: {tone_labels.get(tone_key, tone_key)}\n"
        f"- краткость: {verbosity_labels.get(verbosity, verbosity)}\n"
        f"- стиль: {style_hint}"
    )


def _format_behavior_settings(settings) -> str:
    return (
        "Поведение применено. "
        "Детальные параметры скрыты."
    )


def _pipeline_summary(config: Config, user_id: int | None = None) -> str:
    with get_session() as session:
        pipelines = get_all_pipelines(session)
        if user_id is not None:
            pipelines = [
                item
                for item in pipelines
                if _can_access_account(config, user_id, item.account_name)
            ]
        if not pipelines:
            return "Пайплайнов пока нет."
        lines = []
        for pipeline in pipelines:
            lines.append(
                f"{pipeline.name} | аккаунт: {pipeline.account_name} | "
                f"режим: {pipeline.posting_mode} | "
                f"интервал: {int(pipeline.interval_seconds / 60)} мин | "
                f"{'включен' if pipeline.is_enabled else 'выключен'}"
            )
        return "Пайплайны:\n" + "\n".join(lines)


def _account_pipelines_summary(config: Config, account_name: str) -> str:
    with get_session() as session:
        pipelines = [
            item for item in get_all_pipelines(session) if item.account_name == account_name
        ]
        if not pipelines:
            return "Для аккаунта пока нет пайплайнов."
        lines = []
        for pipeline in pipelines:
            sources = get_pipeline_sources(session, pipeline.id)
            lines.append(
                f"{pipeline.name} | режим: {pipeline.posting_mode} | "
                f"интервал: {int(pipeline.interval_seconds / 60)} мин | "
                f"источников: {len(sources)} | "
                f"{'включен' if pipeline.is_enabled else 'выключен'}"
            )
        return f"Пайплайны аккаунта {account_name}:\n" + "\n".join(lines)


def _account_channels_summary(config: Config, account_name: str) -> str:
    with get_session() as session:
        pipelines = [
            item for item in get_all_pipelines(session) if item.account_name == account_name
        ]
        if not pipelines:
            return "Для аккаунта пока нет пайплайнов."
        lines = []
        for pipeline in pipelines:
            sources = get_pipeline_sources(session, pipeline.id)
            source_list = ", ".join(item.source_channel for item in sources) or "нет"
            lines.append(
                f"{pipeline.name}\n  канал: {pipeline.destination_channel}\n  источники: {source_list}"
            )
        return f"Каналы аккаунта {account_name}:\n" + "\n".join(lines)


def _pipeline_detail_summary(pipeline_name: str) -> str:
    with get_session() as session:
        pipeline = get_pipeline_by_name(session, pipeline_name)
        if not pipeline:
            return "Пайплайн не найден."
        sources = get_pipeline_sources(session, pipeline.id)
        source_list = ", ".join(item.source_channel for item in sources) or "нет"
        summary = (
            f"Пайплайн: {pipeline.name}\n"
            f"Аккаунт: {pipeline.account_name}\n"
            f"Канал назначения: {pipeline.destination_channel}\n"
            f"Каналы-источники: {source_list}\n"
            f"Режим: {pipeline.posting_mode}\n"
            f"Тип: {pipeline.pipeline_type}\n"
            f"Интервал: {int(pipeline.interval_seconds / 60)} мин\n"
            f"BLACKBOX: {pipeline.blackbox_every_n}\n"
            f"Статус: {'включен' if pipeline.is_enabled else 'выключен'}"
        )
        if pipeline.pipeline_type == "DISCUSSION":
            settings = get_discussion_settings(session, pipeline.id)
            if settings:
                summary += (
                    f"\nЧат: {settings.target_chat}\n"
                    f"Источник: {settings.source_pipeline_name}\n"
                    f"K: {settings.k_min}-{settings.k_max}\n"
                    f"TZ: {settings.activity_timezone}\n"
                    f"Окна будни: {settings.activity_windows_weekdays_json or 'нет'}\n"
                    f"Окна выходные: {settings.activity_windows_weekends_json or 'нет'}\n"
                    f"Интервал: {settings.min_interval_minutes}-{settings.max_interval_minutes} мин\n"
                    f"Пауза при тишине: {settings.inactivity_pause_minutes} мин"
                )
        return summary


def _pipeline_current_state(pipeline_name: str):
    with get_session() as session:
        return get_pipeline_by_name(session, pipeline_name)


def _load_accounts_from_bot_data(context: ContextTypes.DEFAULT_TYPE) -> list[TelegramAccountConfig]:
    return context.application.bot_data.get("accounts_config", [])


def _get_account_config(context: ContextTypes.DEFAULT_TYPE, name: str) -> TelegramAccountConfig | None:
    return next(
        (item for item in _load_accounts_from_bot_data(context) if item.name == name),
        None,
    )


def _persist_accounts_json(accounts: list[TelegramAccountConfig]) -> str:
    data = [item.model_dump() for item in accounts]
    serialized = json.dumps(data, ensure_ascii=True, separators=(",", ":"))
    path = ".env"
    with open(path, "r", encoding="utf-8") as file_handle:
        lines = file_handle.read().splitlines()
    updated = False
    for idx, line in enumerate(lines):
        if line.startswith("TELEGRAM_ACCOUNTS_JSON="):
            lines[idx] = f"TELEGRAM_ACCOUNTS_JSON='{serialized}'"
            updated = True
            break
    if not updated:
        lines.append(f"TELEGRAM_ACCOUNTS_JSON='{serialized}'")
    with open(path, "w", encoding="utf-8") as file_handle:
        file_handle.write("\n".join(lines) + "\n")
    return serialized


def _persist_admins_json(admins: dict[int, dict[str, object]]) -> str:
    data = []
    for admin_id, info in admins.items():
        data.append(
            {
                "id": admin_id,
                "role": info.get("role", "owner"),
                "accounts": info.get("accounts", ["*"]),
            }
        )
    serialized = json.dumps(data, ensure_ascii=True, separators=(",", ":"))
    path = ".env"
    with open(path, "r", encoding="utf-8") as file_handle:
        lines = file_handle.read().splitlines()
    updated = False
    for idx, line in enumerate(lines):
        if line.startswith("TG_BOT_ADMINS_JSON="):
            lines[idx] = f"TG_BOT_ADMINS_JSON={serialized}"
            updated = True
            break
    if not updated:
        lines.append(f"TG_BOT_ADMINS_JSON={serialized}")
    with open(path, "w", encoding="utf-8") as file_handle:
        file_handle.write("\n".join(lines) + "\n")
    return serialized


async def _handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.bot_data["config"]
    accounts_runtime = context.application.bot_data.get("accounts_runtime", {})
    user_id = update.effective_user.id if update.effective_user else None
    context.user_data["user_id"] = user_id
    is_admin = _is_admin(
        update.effective_user.id if update.effective_user else None,
        config.bot_admin_ids,
    )
    awaiting = context.user_data.get("awaiting")
    if not is_admin:
        if not (
            context.user_data.get("registration")
            or (awaiting and awaiting.get("type") == "invite_code")
        ):
            await update.message.reply_text("Нет доступа.")
            return
    text = (update.message.text or "").strip()
    menu = context.user_data.get("menu", MENU_MAIN)
    wizard = context.user_data.get("wizard")

    if context.user_data.get("registration"):
        handled = await _handle_registration_step(update, context, text)
        if handled:
            return

    if text in {"Меню", "Главное меню"}:
        await _set_menu(
            update,
            context,
            MENU_MAIN,
            "",
            _main_menu_keyboard(
                config,
                user_id=user_id,
                has_account=bool(context.user_data.get("account")),
                has_pipeline=bool(context.user_data.get("pipeline")),
            ),
        )
        return
    if text == "Статус":
        await _status_cmd(update, context)
        return
    if text == "Назад":
        await _go_back(update, context)
        return

    if awaiting:
        result = await _handle_awaiting(update, context, awaiting, text)
        if result:
            return
    if wizard:
        response = _wizard_next_step(context, text)
        if response:
            await update.message.reply_text(response)
            return
        if wizard.get("step") == "done":
            await _create_pipeline(update, context, wizard)
            context.user_data.pop("wizard", None)
            await _set_menu(
                update,
                context,
                MENU_PIPELINES,
                "",
                _pipelines_list_keyboard(
                    _list_pipeline_names(user_id=user_id, config=config),
                    can_create=_has_permission(config, user_id, "create"),
                ),
            )
            return

    if menu == MENU_MAIN:
        if text == "Аккаунты":
            if not _has_permission(config, user_id, "view"):
                await update.message.reply_text("Недостаточно прав.")
                return
            accounts = [
                item.name
                for item in config.telegram_accounts()
                if _can_access_account(config, user_id, item.name)
            ]
            await _set_menu(
                update,
                context,
                MENU_ACCOUNTS,
                "",
                _account_list_keyboard(accounts),
            )
            return
        if text == "Аккаунт":
            if not context.user_data.get("account"):
                await update.message.reply_text("Сначала выберите аккаунт.")
                return
            await _account_cmd(update, context)
            return
        if text == "Пайплайны":
            if not _has_permission(config, user_id, "view"):
                await update.message.reply_text("Недостаточно прав.")
                return
            await _set_menu(
                update,
                context,
                MENU_PIPELINES,
                "",
                _pipelines_list_keyboard(
                    _list_pipeline_names(user_id=user_id, config=config),
                    can_create=_has_permission(config, user_id, "create"),
                ),
            )
            return
        if text == "Пайплайн":
            if not context.user_data.get("pipeline"):
                await update.message.reply_text("Сначала выберите пайплайн.")
                return
            await _pipeline_cmd(update, context)
            return
        if text == "Поведение":
            if not _has_permission(config, user_id, "behavior"):
                await update.message.reply_text("Недостаточно прав.")
                return
            account_name = context.user_data.get("account")
            if not account_name:
                await update.message.reply_text("Сначала выберите аккаунт.")
                return
            account = _get_account_config(context, account_name)
            if not account:
                await update.message.reply_text("Аккаунт не найден.")
                return
            await update.message.reply_text(
                f"🧠 Поведение\n{_format_behavior_levels(account.behavior)}"
            )
            await _set_menu(
                update,
                context,
                MENU_BEHAVIOR,
                "",
                _behavior_menu_keyboard(),
            )
            return
        if text == "Промпты":
            if not _has_permission(config, user_id, "prompt"):
                await update.message.reply_text("Недостаточно прав.")
                return
            account_name = context.user_data.get("account")
            if not account_name:
                await update.message.reply_text("Сначала выберите аккаунт.")
                return
            account = _get_account_config(context, account_name)
            if not account or not account.openai or not account.openai.system_prompt_path:
                await update.message.reply_text(
                    f"📝 Промпт\n{config.OPENAI_SYSTEM_PROMPT_PATH}"
                )
                return
            await update.message.reply_text(
                f"📝 Промпт\n{account.openai.system_prompt_path}"
            )
            return
        if text == "Мои":
            await _my_cmd(update, context)
            return
        if text == "Статус":
            await update.message.reply_text(
                f"📊 Статус\n{_pipeline_summary(config, user_id)}"
            )
            return
        if text == "Логи":
            if not _has_permission(config, user_id, "logs"):
                await update.message.reply_text("Недостаточно прав.")
                return
            await _set_menu(
                update,
                context,
                MENU_LOGS,
                "",
                _logs_menu_keyboard(can_view=_has_permission(config, user_id, "logs")),
            )
            return
        if text == "Справка":
            await update.message.reply_text("ℹ️ Справка")
            return
        if text == "Пригласить":
            if not _has_permission(config, user_id, "invite"):
                await update.message.reply_text("Недостаточно прав.")
                return
            await _create_invite_link(update, context)
            return

    if menu == MENU_ACCOUNTS:
        accounts = [
            item.name
            for item in config.telegram_accounts()
            if _can_access_account(config, user_id, item.name)
        ]
        if text in accounts:
            context.user_data["account"] = text
            account_cfg = _get_account_config(context, text)
            await _set_menu(
                update,
                context,
                MENU_ACCOUNT,
                "",
            _account_menu_keyboard(
                config,
                user_id=user_id,
                has_account=True,
            ),
            )
            return

    if menu == MENU_ACCOUNT:
        account_name = context.user_data.get("account")
        if not account_name:
            await _set_menu(
                update,
                context,
                MENU_ACCOUNTS,
                "",
                _account_list_keyboard(
                    [
                        item.name
                        for item in config.telegram_accounts()
                        if _can_access_account(config, user_id, item.name)
                    ]
                ),
            )
            return
        if not _can_access_account(config, user_id, account_name):
            await update.message.reply_text("Недостаточно прав.")
            return
        if text == "Каналы":
            if not _has_permission(config, user_id, "view"):
                await update.message.reply_text("Недостаточно прав.")
                return
            await update.message.reply_text(
                f"👤 Аккаунт {account_name}\n{_account_channels_summary(config, account_name)}"
            )
            return
        if text == "Пайплайны":
            if not _has_permission(config, user_id, "view"):
                await update.message.reply_text("Недостаточно прав.")
                return
            pipelines = _list_pipeline_names(account_name=account_name)
            await _set_menu(
                update,
                context,
                MENU_PIPELINES,
                "",
                _pipelines_list_keyboard(
                    pipelines, can_create=_has_permission(config, user_id, "create")
                ),
            )
            return
        if text == "Поведение":
            if not _has_permission(config, user_id, "behavior"):
                await update.message.reply_text("Недостаточно прав.")
                return
            account = _get_account_config(context, account_name)
            if not account:
                await update.message.reply_text("Аккаунт не найден.")
                return
            await update.message.reply_text(
                f"🧠 Поведение\n{_format_behavior_levels(account.behavior)}"
            )
            await _set_menu(
                update,
                context,
                MENU_BEHAVIOR,
                "",
                _behavior_menu_keyboard(),
            )
            return
        if text == "Промпт":
            if not _has_permission(config, user_id, "prompt"):
                await update.message.reply_text("Недостаточно прав.")
                return
            account = _get_account_config(context, account_name)
            if not account or not account.openai or not account.openai.system_prompt_path:
                await update.message.reply_text(
                    f"📝 Промпт\n{config.OPENAI_SYSTEM_PROMPT_PATH}"
                )
                return
            await update.message.reply_text(
                f"📝 Промпт\n{account.openai.system_prompt_path}"
            )
            context.user_data["awaiting"] = {"type": "prompt_update"}
            await update.message.reply_text("Введите новый текст промпта:")
            return
        if text == "Личность":
            if not _has_permission(config, user_id, "persona"):
                await update.message.reply_text("Недостаточно прав.")
                return
            await update.message.reply_text(
                f"🎭 Личность\n{_format_persona_summary(account_name)}"
            )
            await _set_menu(
                update,
                context,
                MENU_PERSONA,
                "",
                _persona_menu_keyboard(),
            )
            return

    if menu == MENU_PIPELINES:
        pipeline_names = _list_pipeline_names(
            account_name=context.user_data.get("account"),
            user_id=user_id,
            config=config,
        )
        if text in pipeline_names:
            context.user_data["pipeline"] = text
            await _set_menu(
                update,
                context,
                MENU_PIPELINE,
                "",
                _pipeline_menu_keyboard(
                    can_edit=_has_permission(config, user_id, "edit"),
                    can_delete=_has_permission(config, user_id, "delete"),
                    can_view=_has_permission(config, user_id, "view"),
                ),
            )
            return
        if text == "Создать пайплайн":
            if not _has_permission(config, user_id, "create"):
                await update.message.reply_text("Недостаточно прав.")
                return
            context.user_data["wizard"] = {"step": "account"}
            await update.message.reply_text("Создание пайплайна. Укажите аккаунт:")
            await update.message.reply_text(
                "Доступные аккаунты: "
                + ", ".join(
                    [
                        item.name
                        for item in _load_accounts_from_bot_data(context)
                        if _can_access_account(config, user_id, item.name)
                    ]
                )
            )
            return

    if menu == MENU_PIPELINE:
        pipeline_name = context.user_data.get("pipeline")
        if not pipeline_name:
            await _set_menu(
                update,
                context,
                MENU_PIPELINES,
                "",
                _pipelines_list_keyboard(
                    _list_pipeline_names(user_id=user_id, config=config),
                    can_create=_has_permission(config, user_id, "create"),
                ),
            )
            return
        pipeline_account = _pipeline_account_name(pipeline_name)
        if pipeline_account and not _can_access_account(
            config, user_id, pipeline_account
        ):
            await update.message.reply_text("Недостаточно прав.")
            return
        if text == "Инфо":
            if not _has_permission(config, user_id, "view"):
                await update.message.reply_text("Недостаточно прав.")
                return
            await update.message.reply_text(
                f"⚙️ Пайплайн\n{_pipeline_detail_summary(pipeline_name)}"
            )
            return
        if text == "Канал назначения":
            if not _has_permission(config, user_id, "edit"):
                await update.message.reply_text("Недостаточно прав.")
                return
            context.user_data["awaiting"] = {"type": "pipeline_destination"}
            pipeline = _pipeline_current_state(pipeline_name)
            current = pipeline.destination_channel if pipeline else "неизвестно"
            await update.message.reply_text(f"⚙️ Пайплайн\nТекущий канал: {current}")
            await update.message.reply_text("Введите новый канал назначения (например @channel):")
            return
        if text == "Добавить источник":
            if not _has_permission(config, user_id, "edit"):
                await update.message.reply_text("Недостаточно прав.")
                return
            context.user_data["awaiting"] = {"type": "pipeline_add_source"}
            await update.message.reply_text(
                f"⚙️ Пайплайн\n{_pipeline_detail_summary(pipeline_name)}"
            )
            await update.message.reply_text("Введите канал-источник (например @source):")
            return
        if text == "Удалить источник":
            if not _has_permission(config, user_id, "edit"):
                await update.message.reply_text("Недостаточно прав.")
                return
            await update.message.reply_text(
                f"⚙️ Пайплайн\n{_pipeline_detail_summary(pipeline_name)}"
            )
            context.user_data["awaiting"] = {"type": "pipeline_remove_source"}
            await update.message.reply_text("Введите канал-источник для удаления:")
            return
        if text == "Интервал":
            if not _has_permission(config, user_id, "edit"):
                await update.message.reply_text("Недостаточно прав.")
                return
            context.user_data["awaiting"] = {"type": "pipeline_intensity"}
            pipeline = _pipeline_current_state(pipeline_name)
            if pipeline:
                minutes = max(1, int(pipeline.interval_seconds / 60))
                await update.message.reply_text(
                    f"⚙️ Пайплайн\nТекущий интервал: {minutes} мин"
                )
            await update.message.reply_text(
                "Выберите интервал (минуты):",
                reply_markup=_interval_keyboard(minutes if pipeline else None),
            )
            return
        if text == "Режим":
            if not _has_permission(config, user_id, "edit"):
                await update.message.reply_text("Недостаточно прав.")
                return
            pipeline = _pipeline_current_state(pipeline_name)
            if pipeline:
                await update.message.reply_text(
                    f"⚙️ Пайплайн\nТекущий режим: {pipeline.posting_mode}"
                )
            context.user_data["awaiting"] = {"type": "pipeline_mode"}
            await update.message.reply_text(
                "Выберите режим: TEXT / TEXT_IMAGE / TEXT_MEDIA / PLAGIAT"
            )
            return
        if text == "Вкл/выкл":
            if not _has_permission(config, user_id, "edit"):
                await update.message.reply_text("Недостаточно прав.")
                return
            await _update_pipeline_toggle(update, context)
            return
        if text == "Удалить пайплайн":
            if not _has_permission(config, user_id, "delete"):
                await update.message.reply_text("Недостаточно прав.")
                return
            context.user_data["awaiting"] = {"type": "pipeline_delete"}
            await update.message.reply_text(
                "Подтвердите удаление: Удалить навсегда",
                reply_markup=ReplyKeyboardMarkup(
                    [["Удалить навсегда"], ["Отмена"]], resize_keyboard=True
                ),
            )
            return

    if menu == MENU_BEHAVIOR:
        if text == "Простой уровень":
            if not _has_permission(config, user_id, "behavior"):
                await update.message.reply_text("Недостаточно прав.")
                return
            account = _get_account_config(context, context.user_data.get("account"))
            current = account.behavior.simple_profile_level if account and account.behavior else 3
            await update.message.reply_text(
                f"Текущий уровень: {current}\n"
                "Общий профиль ритма и осторожности."
            )
            context.user_data["behavior_group"] = "simple"
            await _set_menu(
                update,
                context,
                MENU_BEHAVIOR_LEVEL,
                "Уровень:",
                _level_keyboard_with_labels(
                    ["1 — 0.5с", "2 — 1с", "3 — 2с", "4 — 4с", "5 — 8с"],
                    current,
                ),
            )
            return
        if text == "Темп":
            if not _has_permission(config, user_id, "behavior"):
                await update.message.reply_text("Недостаточно прав.")
                return
            account = _get_account_config(context, context.user_data.get("account"))
            current = account.behavior.group_tempo_level if account and account.behavior else "-"
            await update.message.reply_text(
                f"Текущий уровень: {current}\n"
                "Влияет на паузы и скорость работы."
            )
            context.user_data["behavior_group"] = "tempo"
            await _set_menu(
                update,
                context,
                MENU_BEHAVIOR_LEVEL,
                "Уровень темпа:",
                _level_keyboard_with_labels(
                    ["1 — 0.5с", "2 — 1с", "3 — 2с", "4 — 4с", "5 — 8с"],
                    current if isinstance(current, int) else None,
                ),
            )
            return
        if text == "Нагрузка":
            if not _has_permission(config, user_id, "behavior"):
                await update.message.reply_text("Недостаточно прав.")
                return
            account = _get_account_config(context, context.user_data.get("account"))
            current = account.behavior.group_load_level if account and account.behavior else "-"
            await update.message.reply_text(
                f"Текущий уровень: {current}\n"
                "Влияет на объём действий за цикл."
            )
            context.user_data["behavior_group"] = "load"
            await _set_menu(
                update,
                context,
                MENU_BEHAVIOR_LEVEL,
                "Уровень нагрузки:",
                _level_keyboard_with_labels(
                    ["1 — 30", "2 — 20", "3 — 10", "4 — 10", "5 — 5"],
                    current if isinstance(current, int) else None,
                ),
            )
            return
        if text == "Осторожность":
            if not _has_permission(config, user_id, "behavior"):
                await update.message.reply_text("Недостаточно прав.")
                return
            account = _get_account_config(context, context.user_data.get("account"))
            current = account.behavior.group_safety_level if account and account.behavior else "-"
            await update.message.reply_text(
                f"Текущий уровень: {current}\n"
                "Влияет на аккуратность и ожидания."
            )
            context.user_data["behavior_group"] = "safety"
            await _set_menu(
                update,
                context,
                MENU_BEHAVIOR_LEVEL,
                "Уровень осторожности:",
                _level_keyboard_with_labels(
                    ["1 — 300с", "2 — 600с", "3 — 900с", "4 — 1200с", "5 — 1800с"],
                    current if isinstance(current, int) else None,
                ),
            )
            return
        if text == "Контент":
            if not _has_permission(config, user_id, "behavior"):
                await update.message.reply_text("Недостаточно прав.")
                return
            account = _get_account_config(context, context.user_data.get("account"))
            current = account.behavior.group_content_level if account and account.behavior else "-"
            await update.message.reply_text(
                f"Текущий уровень: {current}\n"
                "Влияет на выбор источников и пропуски."
            )
            context.user_data["behavior_group"] = "content"
            await _set_menu(
                update,
                context,
                MENU_BEHAVIOR_LEVEL,
                "Уровень контента:",
                _level_keyboard_with_labels(
                    ["1 — 0%", "2 — 5%", "3 — 10%", "4 — 20%", "5 — 30%"],
                    current if isinstance(current, int) else None,
                ),
            )
            return

    if menu == MENU_BEHAVIOR_LEVEL:
        level = _parse_level_from_label(text)
        if level is not None:
            await _apply_behavior_level(update, context, level, accounts_runtime)
            return

    if menu == MENU_PERSONA:
        account_name = context.user_data.get("account")
        if not account_name:
            await update.message.reply_text("Сначала выберите аккаунт.")
            return
        if text == "Тон":
            await update.message.reply_text(
                f"🎭 Личность\n{_format_persona_summary(account_name)}"
            )
            with get_session() as session:
                persona = get_userbot_persona(session, account_name)
            current = persona.persona_tone if persona and persona.persona_tone else "neutral"
            await _set_menu(
                update,
                context,
                MENU_PERSONA,
                "Выберите тон:",
                _persona_tone_keyboard(current),
            )
            return
        if text == "Краткость":
            await update.message.reply_text(
                f"🎭 Личность\n{_format_persona_summary(account_name)}"
            )
            with get_session() as session:
                persona = get_userbot_persona(session, account_name)
            current = (
                persona.persona_verbosity if persona and persona.persona_verbosity else "short"
            )
            await _set_menu(
                update,
                context,
                MENU_PERSONA,
                "Выберите краткость:",
                _persona_verbosity_keyboard(current),
            )
            return
        if text == "Стиль":
            context.user_data["awaiting"] = {"type": "persona_style_hint"}
            await update.message.reply_text(
                "Введите подсказку стиля (или '-' чтобы очистить):"
            )
            return
        if text == "Сбросить":
            with get_session() as session:
                upsert_userbot_persona(
                    session,
                    account_name=account_name,
                    persona_tone=None,
                    persona_verbosity=None,
                    persona_style_hint=None,
                )
                session.commit()
            _audit_log("persona.reset", context.user_data.get("user_id"), account_name)
            await update.message.reply_text(
                "✅ Личность сброшена к значениям по умолчанию."
            )
            return
        tone = _parse_persona_tone(text)
        if tone is not None:
            with get_session() as session:
                persona = get_userbot_persona(session, account_name)
                upsert_userbot_persona(
                    session,
                    account_name=account_name,
                    persona_tone=tone,
                    persona_verbosity=(
                        persona.persona_verbosity if persona and persona.persona_verbosity else None
                    ),
                    persona_style_hint=(
                        persona.persona_style_hint if persona and persona.persona_style_hint else None
                    ),
                )
                session.commit()
            tone_labels = {
                "neutral": "нейтральный",
                "analytical": "аналитичный",
                "emotional": "эмоциональный",
                "ironic": "ироничный",
                "skeptical": "скептичный",
            }
            _audit_log(
                "persona.tone",
                context.user_data.get("user_id"),
                f"{account_name} -> {tone}",
            )
            await update.message.reply_text(f"✅ Тон: {tone_labels.get(tone, tone)}")
            return
        verbosity = _parse_persona_verbosity(text)
        if verbosity is not None:
            with get_session() as session:
                persona = get_userbot_persona(session, account_name)
                upsert_userbot_persona(
                    session,
                    account_name=account_name,
                    persona_tone=(
                        persona.persona_tone if persona and persona.persona_tone else None
                    ),
                    persona_verbosity=verbosity,
                    persona_style_hint=(
                        persona.persona_style_hint if persona and persona.persona_style_hint else None
                    ),
                )
                session.commit()
            verbosity_labels = {"short": "коротко", "medium": "средне"}
            _audit_log(
                "persona.verbosity",
                context.user_data.get("user_id"),
                f"{account_name} -> {verbosity}",
            )
            await update.message.reply_text(
                f"✅ Краткость: {verbosity_labels.get(verbosity, verbosity)}"
            )
            return

    if menu == MENU_LOGS:
        if text == "Ошибки":
            if not _has_permission(config, user_id, "logs"):
                await update.message.reply_text("Недостаточно прав.")
                return
            await update.message.reply_text(
                f"📁 Логи — ошибки\n{_read_log_excerpt('logs/service.log')}"
            )
            return
        if text == "Посты/расходы":
            if not _has_permission(config, user_id, "logs"):
                await update.message.reply_text("Недостаточно прав.")
                return
            await update.message.reply_text(
                f"📁 Логи — посты/расходы\n{_read_log_excerpt('logs/news_usage.log')}"
            )
            return

    await update.message.reply_text("Используйте кнопки меню.")


async def _go_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.bot_data["config"]
    menu = context.user_data.get("menu", MENU_MAIN)
    if menu == MENU_PIPELINE:
        await _set_menu(
            update,
            context,
            MENU_PIPELINES,
            "",
            _pipelines_list_keyboard(
                _list_pipeline_names(
                    user_id=context.user_data.get("user_id"), config=config
                ),
                can_create=_has_permission(config, context.user_data.get("user_id"), "create"),
            ),
        )
        return
    if menu == MENU_BEHAVIOR:
        await _set_menu(
            update,
            context,
            MENU_ACCOUNT,
            "",
            _account_menu_keyboard(
                config,
                user_id=context.user_data.get("user_id"),
                has_account=bool(context.user_data.get("account")),
            ),
        )
        return
    if menu == MENU_PERSONA:
        await _set_menu(
            update,
            context,
            MENU_ACCOUNT,
            "",
            _account_menu_keyboard(
                config,
                user_id=context.user_data.get("user_id"),
                has_account=bool(context.user_data.get("account")),
            ),
        )
        return
    if menu == MENU_LOGS:
        await _set_menu(
            update,
            context,
            MENU_MAIN,
            "",
            _main_menu_keyboard(
                config,
                user_id=context.user_data.get("user_id"),
                has_account=bool(context.user_data.get("account")),
                has_pipeline=bool(context.user_data.get("pipeline")),
            ),
        )
        return
    if menu == MENU_ACCOUNT:
        await _set_menu(
            update,
            context,
            MENU_ACCOUNTS,
            "",
            _account_list_keyboard(
                [
                    item.name
                    for item in config.telegram_accounts()
                    if _can_access_account(config, context.user_data.get("user_id"), item.name)
                ]
            ),
        )
        return
    if menu == MENU_BEHAVIOR_LEVEL:
        await _set_menu(
            update,
            context,
            MENU_BEHAVIOR,
            "",
            _behavior_menu_keyboard(),
        )
        return
    await _set_menu(
        update,
        context,
        MENU_MAIN,
        "",
        _main_menu_keyboard(
            config,
            user_id=context.user_data.get("user_id"),
            has_account=bool(context.user_data.get("account")),
            has_pipeline=bool(context.user_data.get("pipeline")),
        ),
    )


def _list_pipeline_names(
    account_name: str | None = None,
    *,
    user_id: int | None = None,
    config: Config | None = None,
) -> list[str]:
    with get_session() as session:
        pipelines = get_all_pipelines(session)
        if account_name:
            pipelines = [item for item in pipelines if item.account_name == account_name]
        if user_id is not None and config is not None:
            pipelines = [
                item
                for item in pipelines
                if _can_access_account(config, user_id, item.account_name)
            ]
        return [item.name for item in pipelines]


def _pipeline_account_name(pipeline_name: str) -> str | None:
    with get_session() as session:
        pipeline = get_pipeline_by_name(session, pipeline_name)
        if not pipeline:
            return None
        return pipeline.account_name


async def _handle_awaiting(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    awaiting: dict,
    text: str,
) -> bool:
    if awaiting.get("type") == "pipeline_destination":
        await _update_pipeline_destination(update, context, text)
        return True
    if awaiting.get("type") == "pipeline_add_source":
        await _update_pipeline_add_source(update, context, text)
        return True
    if awaiting.get("type") == "pipeline_remove_source":
        await _update_pipeline_remove_source(update, context, text)
        return True
    if awaiting.get("type") == "pipeline_interval":
        await _update_pipeline_interval(update, context, text)
        return True
    if awaiting.get("type") == "pipeline_intensity":
        await _update_pipeline_intensity(update, context, text)
        return True
    if awaiting.get("type") == "pipeline_mode":
        await _update_pipeline_mode(update, context, text)
        return True
    if awaiting.get("type") == "pipeline_delete":
        await _update_pipeline_delete(update, context, text)
        return True
    if awaiting.get("type") == "prompt_update":
        await _update_prompt(update, context, text)
        return True
    if awaiting.get("type") == "persona_style_hint":
        await _update_persona_style_hint(update, context, text)
        return True
    if awaiting.get("type") == "invite_code":
        await _handle_invite_code(update, context, text)
        return True
    return False


async def _handle_invite_code(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
) -> None:
    token = context.user_data.get("invite_token")
    user_id = context.user_data.get("user_id")
    if not token or user_id is None:
        await update.message.reply_text("Код не принят.")
        return
    with get_session() as session:
        invite = get_invite(session, token)
        if not invite or invite.expires_at < datetime.utcnow() or invite.used_at is not None:
            await update.message.reply_text("Ссылка недействительна.")
            return
        code_row = get_invite_code(session, token, text.strip(), user_id)
        if not code_row:
            await update.message.reply_text("Неверный код.")
            return
        if code_row.used_at is not None or code_row.expires_at < datetime.utcnow():
            await update.message.reply_text("Код просрочен.")
            return
        mark_invite_code_used(session, code_row)
        mark_invite_used(session, invite, user_id)
        session.commit()
    context.user_data.pop("awaiting", None)
    context.user_data["registration"] = {"step": "name"}
    _audit_log("invite.accept", user_id, f"token={token}")
    await update.message.reply_text("Введите название аккаунта:")


async def _handle_registration_step(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
) -> bool:
    registration = context.user_data.get("registration")
    if not registration:
        return False
    step = registration.get("step")
    if step == "name":
        name = text.strip()
        if not name:
            await update.message.reply_text("Введите название аккаунта.")
            return True
        if _get_account_config(context, name):
            await update.message.reply_text("Такое имя уже есть, введите другое.")
            return True
        registration["name"] = name
        existing_sessions = {
            item.reader.session for item in _load_accounts_from_bot_data(context)
        }
        registration["session"] = _derive_session_name(name, existing_sessions)
        registration["step"] = "api_id"
        await update.message.reply_text(
            "Введите код доступа (цифры). Формат: XXX<код>YYY"
        )
        return True
    if step == "api_id":
        stripped = _strip_masked_value(text)
        if not stripped or not stripped.isdigit():
            await update.message.reply_text("Неверный формат. Пример: ABC123456XYZ")
            return True
        registration["api_id"] = int(stripped)
        registration["step"] = "api_hash"
        await update.message.reply_text(
            "Введите код доступа (буквы и цифры). Формат: XXX<код>YYY"
        )
        return True
    if step == "api_hash":
        stripped = _strip_masked_value(text)
        if not stripped or not re.fullmatch(r"[a-fA-F0-9]{32,}", stripped):
            await update.message.reply_text("Неверный формат. Пример: ABC<код>XYZ")
            return True
        registration["api_hash"] = stripped
        await _finalize_registration(update, context, registration)
        return True
    return False


async def _finalize_registration(
    update: Update, context: ContextTypes.DEFAULT_TYPE, registration: dict
) -> None:
    config: Config = context.bot_data["config"]
    user_id = context.user_data.get("user_id")
    name = registration["name"]
    account = TelegramAccountConfig(
        name=name,
        reader=TelegramCredentials(
            api_id=registration["api_id"],
            api_hash=registration["api_hash"],
            session=registration["session"],
        ),
    )
    accounts = _load_accounts_from_bot_data(context)
    accounts.append(account)
    serialized = _persist_accounts_json(accounts)
    config.TELEGRAM_ACCOUNTS_JSON = serialized
    context.application.bot_data["accounts_config"] = accounts
    admins = config.bot_admins()
    if user_id is not None:
        entry = admins.get(user_id, {"role": "editor", "accounts": []})
        accounts_list = entry.get("accounts", [])
        if "*" not in accounts_list:
            if name not in accounts_list:
                accounts_list.append(name)
        entry["role"] = entry.get("role", "editor")
        entry["accounts"] = accounts_list
        admins[user_id] = entry
        config.TG_BOT_ADMINS_JSON = _persist_admins_json(admins)
    context.user_data.pop("registration", None)
    context.user_data["account"] = name
    _audit_log("account.create", user_id, name)
    await _try_attach_account_runtime(context, account)
    await update.message.reply_text("✅ Аккаунт создан.")
    await update.message.reply_text("Авторизация выполняется через консоль.")
    await _set_menu(
        update,
        context,
        MENU_ACCOUNT,
        "",
        _account_menu_keyboard(
            config,
            user_id=user_id,
            has_account=True,
        ),
    )


async def _try_attach_account_runtime(
    context: ContextTypes.DEFAULT_TYPE, account: TelegramAccountConfig
) -> bool:
    config: Config = context.bot_data["config"]
    runtime_map: dict[str, AccountRuntime] = context.application.bot_data.get(
        "accounts_runtime", {}
    )
    session_path = f"{account.reader.session}.session"
    if not os.path.exists(session_path):
        return False
    try:
        behavior = config.resolve_behavior_settings(account.behavior)
        openai_settings = config.resolve_openai_settings(account.openai)
        system_prompt = _read_prompt_file(openai_settings.system_prompt_path)
        openai_client = OpenAIClient(
            api_key=openai_settings.api_key,
            system_prompt=system_prompt,
            text_model=openai_settings.text_model,
            vision_model=openai_settings.vision_model,
            image_model=openai_settings.image_model,
        )
        reader_client = await create_client(
            api_id=account.reader.api_id,
            api_hash=account.reader.api_hash,
            session_name=account.reader.session,
            start=False,
        )
        if not await reader_client.is_user_authorized():
            logger.warning(
                "Account %s: reader session not authorized, skipping attach",
                account.name,
            )
            await reader_client.disconnect()
            return False
        if account.writer:
            writer_client = await create_client(
                api_id=account.writer.api_id,
                api_hash=account.writer.api_hash,
                session_name=account.writer.session,
                start=False,
            )
            if not await writer_client.is_user_authorized():
                logger.warning(
                    "Account %s: writer session not authorized, using reader",
                    account.name,
                )
                await writer_client.disconnect()
                writer_client = reader_client
        else:
            writer_client = reader_client
        runtime_map[account.name] = AccountRuntime(
            name=account.name,
            reader_client=reader_client,
            writer_client=writer_client,
            openai_client=openai_client,
            behavior=behavior,
            openai_settings=openai_settings,
        )
        context.application.bot_data["accounts_runtime"] = runtime_map
        return True
    except Exception:
        logger.exception("Failed to attach new account runtime")
        return False




def _get_selected_pipeline(context: ContextTypes.DEFAULT_TYPE):
    pipeline_name = context.user_data.get("pipeline")
    if not pipeline_name:
        return None
    with get_session() as session:
        pipeline = get_pipeline_by_name(session, pipeline_name)
        if pipeline is None:
            return None
        return pipeline_name


async def _update_pipeline_destination(
    update: Update, context: ContextTypes.DEFAULT_TYPE, destination: str
) -> None:
    context.user_data.pop("awaiting", None)
    pipeline_name = context.user_data.get("pipeline")
    if not pipeline_name:
        await update.message.reply_text("Пайплайн не выбран.")
        return
    with get_session() as session:
        pipeline = get_pipeline_by_name(session, pipeline_name)
        if not pipeline:
            await update.message.reply_text("Пайплайн не найден.")
            return
        update_pipeline_destination(session, pipeline, destination)
        session.commit()
    _audit_log(
        "pipeline.destination",
        context.user_data.get("user_id"),
        f"{pipeline_name} -> {destination}",
    )
    await update.message.reply_text(f"✅ Канал: {destination}")


async def _update_pipeline_add_source(
    update: Update, context: ContextTypes.DEFAULT_TYPE, source: str
) -> None:
    context.user_data.pop("awaiting", None)
    pipeline_name = context.user_data.get("pipeline")
    if not pipeline_name:
        await update.message.reply_text("Пайплайн не выбран.")
        return
    with get_session() as session:
        pipeline = get_pipeline_by_name(session, pipeline_name)
        if not pipeline:
            await update.message.reply_text("Пайплайн не найден.")
            return
        add_pipeline_source(session, pipeline, source)
        session.commit()
    _audit_log(
        "pipeline.source_add",
        context.user_data.get("user_id"),
        f"{pipeline_name} + {source}",
    )
    await update.message.reply_text(f"✅ Источник: {source}")


async def _update_pipeline_remove_source(
    update: Update, context: ContextTypes.DEFAULT_TYPE, source: str
) -> None:
    context.user_data.pop("awaiting", None)
    pipeline_name = context.user_data.get("pipeline")
    if not pipeline_name:
        await update.message.reply_text("Пайплайн не выбран.")
        return
    with get_session() as session:
        pipeline = get_pipeline_by_name(session, pipeline_name)
        if not pipeline:
            await update.message.reply_text("Пайплайн не найден.")
            return
        removed = remove_pipeline_source(session, pipeline, source)
        session.commit()
    if removed:
        _audit_log(
            "pipeline.source_remove",
            context.user_data.get("user_id"),
            f"{pipeline_name} - {source}",
        )
        await update.message.reply_text(f"✅ Источник удалён: {source}")
    else:
        await update.message.reply_text("Такого источника нет.")


async def _update_pipeline_interval(
    update: Update, context: ContextTypes.DEFAULT_TYPE, interval_text: str
) -> None:
    context.user_data.pop("awaiting", None)
    try:
        minutes = int(interval_text)
    except ValueError:
        await update.message.reply_text("Нужно целое число минут.")
        return
    if minutes <= 0:
        await update.message.reply_text("Минуты должны быть больше 0.")
        return
    pipeline_name = context.user_data.get("pipeline")
    if not pipeline_name:
        await update.message.reply_text("Пайплайн не выбран.")
        return
    interval = minutes * 60
    with get_session() as session:
        pipeline = get_pipeline_by_name(session, pipeline_name)
        if not pipeline:
            await update.message.reply_text("Пайплайн не найден.")
            return
        update_pipeline_interval(session, pipeline, interval)
        session.commit()
    _audit_log(
        "pipeline.interval",
        context.user_data.get("user_id"),
        f"{pipeline_name} -> {minutes}m",
    )
    await update.message.reply_text(f"✅ Интервал: {minutes} мин")


async def _update_pipeline_intensity(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
) -> None:
    context.user_data.pop("awaiting", None)
    cleaned = text.replace(" ✓", "")
    mapping = {
        "1 (60м)": 60 * 60,
        "2 (120м)": 120 * 60,
        "3 (180м)": 180 * 60,
        "4 (240м)": 240 * 60,
    }
    if cleaned == "5 (вручную)":
        context.user_data["awaiting"] = {"type": "pipeline_interval"}
        await update.message.reply_text("Введите интервал в минутах:")
        return
    interval = mapping.get(cleaned)
    if interval is None:
        await update.message.reply_text("Выберите значение с кнопок.")
        return
    pipeline_name = context.user_data.get("pipeline")
    if not pipeline_name:
        await update.message.reply_text("Пайплайн не выбран.")
        return
    with get_session() as session:
        pipeline = get_pipeline_by_name(session, pipeline_name)
        if not pipeline:
            await update.message.reply_text("Пайплайн не найден.")
            return
        update_pipeline_interval(session, pipeline, interval)
        session.commit()
    _audit_log(
        "pipeline.interval",
        context.user_data.get("user_id"),
        f"{pipeline_name} -> {int(interval / 60)}m",
    )
    await update.message.reply_text(f"✅ Интервал: {int(interval / 60)} мин")


async def _update_pipeline_mode(
    update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str
) -> None:
    context.user_data.pop("awaiting", None)
    normalized = mode.strip().upper()
    if normalized not in {"TEXT", "TEXT_IMAGE", "TEXT_MEDIA", "PLAGIAT"}:
        await update.message.reply_text("Неверный режим.")
        return
    pipeline_name = context.user_data.get("pipeline")
    if not pipeline_name:
        await update.message.reply_text("Пайплайн не выбран.")
        return
    with get_session() as session:
        pipeline = get_pipeline_by_name(session, pipeline_name)
        if not pipeline:
            await update.message.reply_text("Пайплайн не найден.")
            return
        update_pipeline_mode(session, pipeline, normalized)
        session.commit()
    _audit_log(
        "pipeline.mode",
        context.user_data.get("user_id"),
        f"{pipeline_name} -> {normalized}",
    )
    await update.message.reply_text(f"✅ Режим: {normalized}")


async def _update_pipeline_delete(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
) -> None:
    context.user_data.pop("awaiting", None)
    if text != "Удалить навсегда":
        await update.message.reply_text("Удаление отменено.")
        return
    pipeline_name = context.user_data.get("pipeline")
    if not pipeline_name:
        await update.message.reply_text("Пайплайн не выбран.")
        return
    with get_session() as session:
        pipeline = get_pipeline_by_name(session, pipeline_name)
        if not pipeline:
            await update.message.reply_text("Пайплайн не найден.")
            return
        delete_pipeline(session, pipeline)
        session.commit()
    _audit_log("pipeline.delete", context.user_data.get("user_id"), pipeline_name)
    await update.message.reply_text("✅ Пайплайн удалён")
    context.user_data.pop("pipeline", None)


async def _update_pipeline_toggle(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    pipeline_name = context.user_data.get("pipeline")
    if not pipeline_name:
        await update.message.reply_text("Пайплайн не выбран.")
        return
    with get_session() as session:
        pipeline = get_pipeline_by_name(session, pipeline_name)
        if not pipeline:
            await update.message.reply_text("Пайплайн не найден.")
            return
        enabled = toggle_pipeline_enabled(session, pipeline)
        session.commit()
    _audit_log(
        "pipeline.toggle",
        context.user_data.get("user_id"),
        f"{pipeline_name} -> {'on' if enabled else 'off'}",
    )
    await update.message.reply_text(
        f"✅ Статус: {'включен' if enabled else 'выключен'}"
    )


async def _apply_behavior_level(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    level: int,
    accounts_runtime: dict,
) -> None:
    account_name = context.user_data.get("account")
    if not account_name:
        await update.message.reply_text("Сначала выберите аккаунт.")
        return
    config: Config = context.bot_data["config"]
    user_id = context.user_data.get("user_id")
    if not _can_access_account(config, user_id, account_name):
        await update.message.reply_text("Недостаточно прав.")
        return
    account_config = _get_account_config(context, account_name)
    if not account_config:
        await update.message.reply_text("Аккаунт не найден.")
        return
    behavior = account_config.behavior or BehaviorProfileConfig()
    group = context.user_data.get("behavior_group")
    if group == "simple":
        behavior.simple_profile_level = level
    elif group == "tempo":
        behavior.group_tempo_level = level
    elif group == "load":
        behavior.group_load_level = level
    elif group == "safety":
        behavior.group_safety_level = level
    elif group == "content":
        behavior.group_content_level = level
    else:
        await update.message.reply_text("Группа поведения не выбрана.")
        return
    account_config.behavior = behavior
    config: Config = context.bot_data["config"]
    serialized = _persist_accounts_json(_load_accounts_from_bot_data(context))
    config.TELEGRAM_ACCOUNTS_JSON = serialized
    runtime = accounts_runtime.get(account_name)
    if runtime:
        runtime.behavior = context.bot_data["config"].resolve_behavior_settings(behavior)
    _audit_log(
        "behavior.level",
        context.user_data.get("user_id"),
        f"{account_name} {context.user_data.get('behavior_group')} -> {level}",
    )
    await update.message.reply_text(f"✅ Уровень: {level}")


async def _update_prompt(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
) -> None:
    context.user_data.pop("awaiting", None)
    account_name = context.user_data.get("account")
    if not account_name:
        await update.message.reply_text("Сначала выберите аккаунт.")
        return
    config: Config = context.bot_data["config"]
    user_id = context.user_data.get("user_id")
    if not _can_access_account(config, user_id, account_name):
        await update.message.reply_text("Недостаточно прав.")
        return
    account = _get_account_config(context, account_name)
    if not account:
        await update.message.reply_text("Аккаунт не найден.")
        return
    prompt_path = (
        account.openai.system_prompt_path
        if account.openai and account.openai.system_prompt_path
        else f"openai_prompt_{account_name}.txt"
    )
    with open(prompt_path, "w", encoding="utf-8") as file_handle:
        file_handle.write(text.strip() + "\n")
    if not account.openai:
        account.openai = OpenAIAccountConfig()
    account.openai.system_prompt_path = prompt_path
    config: Config = context.bot_data["config"]
    serialized = _persist_accounts_json(_load_accounts_from_bot_data(context))
    config.TELEGRAM_ACCOUNTS_JSON = serialized
    runtime = context.application.bot_data.get("accounts_runtime", {}).get(account_name)
    if runtime:
        runtime.openai_client.system_prompt = text.strip()
        runtime.openai_settings.system_prompt_path = prompt_path
    _audit_log(
        "prompt.update",
        context.user_data.get("user_id"),
        f"{account_name} -> {prompt_path}",
    )
    await update.message.reply_text(f"✅ Промпт: {prompt_path}")


async def _update_persona_style_hint(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
) -> None:
    context.user_data.pop("awaiting", None)
    account_name = context.user_data.get("account")
    if not account_name:
        await update.message.reply_text("Сначала выберите аккаунт.")
        return
    config: Config = context.bot_data["config"]
    user_id = context.user_data.get("user_id")
    if not _can_access_account(config, user_id, account_name):
        await update.message.reply_text("Недостаточно прав.")
        return
    hint = text.strip()
    if hint == "-" or not hint:
        hint_value = None
    else:
        hint_value = hint
    with get_session() as session:
        persona = get_userbot_persona(session, account_name)
        upsert_userbot_persona(
            session,
            account_name=account_name,
            persona_tone=persona.persona_tone if persona and persona.persona_tone else None,
            persona_verbosity=(
                persona.persona_verbosity if persona and persona.persona_verbosity else None
            ),
            persona_style_hint=hint_value,
        )
        session.commit()
    _audit_log(
        "persona.style_hint",
        context.user_data.get("user_id"),
        f"{account_name} -> {hint_value or '-'}",
    )
    await update.message.reply_text("✅ Стиль обновлён.")
    await _set_menu(
        update,
        context,
        MENU_PERSONA,
        "",
        _persona_menu_keyboard(),
    )


async def _create_pipeline(
    update: Update, context: ContextTypes.DEFAULT_TYPE, data: dict
) -> None:
    try:
        pipeline_config = PipelineConfig(
            name=data["name"],
            account=data["account"],
            destination=data["destination"],
            sources=data["sources"],
            mode=data["mode"],
            pipeline_type=data.get("pipeline_type", "STANDARD"),
            interval_seconds=int(data["interval"]),
            blackbox_every_n=int(data["blackbox"]),
            discussion_target_chat=data.get("discussion_target_chat"),
            discussion_source_pipeline=data.get("discussion_source_pipeline"),
        )
    except Exception:
        await update.message.reply_text("Проверьте введённые данные.")
        return
    config: Config = context.bot_data["config"]
    user_id = context.user_data.get("user_id")
    if not _can_access_account(config, user_id, pipeline_config.account):
        await update.message.reply_text("Недостаточно прав.")
        return
    with get_session() as session:
        if get_pipeline_by_name(session, pipeline_config.name):
            await update.message.reply_text("Пайплайн с таким именем уже есть.")
            return
        create_pipeline(session, pipeline_config)
        session.commit()
    _audit_log(
        "pipeline.create",
        context.user_data.get("user_id"),
        f"{pipeline_config.name} ({pipeline_config.account})",
    )
    await update.message.reply_text(f"✅ Пайплайн: {pipeline_config.name}")


def _wizard_next_step(context: ContextTypes.DEFAULT_TYPE, text: str) -> str:
    wizard = context.user_data.get("wizard")
    if not wizard:
        return ""
    step = wizard.get("step")
    if step == "account":
        config: Config = context.bot_data["config"]
        user_id = context.user_data.get("user_id")
        accounts = [
            item.name
            for item in _load_accounts_from_bot_data(context)
            if _can_access_account(config, user_id, item.name)
        ]
        if text not in accounts:
            return "Неверный аккаунт. Введите имя из списка."
        wizard["account"] = text
        wizard["step"] = "name"
        return "Введите имя пайплайна:"
    if step == "name":
        wizard["name"] = text
        wizard["step"] = "type"
        return "Введите тип (Обычный/Обсуждение):"
    if step == "type":
        normalized = text.strip().lower()
        if normalized in {"обсуждение", "discussion"}:
            wizard["pipeline_type"] = "DISCUSSION"
            wizard["step"] = "discussion_source"
            return "Введите имя пайплайна-источника для обсуждения:"
        wizard["pipeline_type"] = "STANDARD"
        wizard["step"] = "destination"
        return "Введите канал назначения:"
    if step == "destination":
        wizard["destination"] = text
        wizard["step"] = "sources"
        return "Введите источники через запятую:"
    if step == "sources":
        sources = [item.strip() for item in text.split(",") if item.strip()]
        wizard["sources"] = sources
        wizard["step"] = "mode"
        return "Введите режим (TEXT/TEXT_IMAGE/TEXT_MEDIA/PLAGIAT):"
    if step == "mode":
        normalized = text.strip().upper()
        if normalized not in {"TEXT", "TEXT_IMAGE", "TEXT_MEDIA", "PLAGIAT"}:
            return "Неверный режим. Введите: TEXT/TEXT_IMAGE/TEXT_MEDIA/PLAGIAT"
        wizard["mode"] = normalized
        wizard["step"] = "interval"
        return "Введите интервал в секундах:"
    if step == "discussion_source":
        source_name = text.strip()
        if not source_name:
            return "Введите имя пайплайна-источника."
        wizard["discussion_source_pipeline"] = source_name
        wizard["step"] = "discussion_target"
        return "Введите чат/группу для обсуждения (например @chat):"
    if step == "discussion_target":
        target_chat = text.strip()
        if not target_chat:
            return "Введите чат/группу для обсуждения."
        wizard["discussion_target_chat"] = target_chat
        wizard["step"] = "interval"
        return "Введите интервал в секундах:"
    if step == "interval":
        wizard["interval"] = text
        if wizard.get("pipeline_type") == "DISCUSSION":
            wizard["blackbox"] = "0"
            wizard["mode"] = "TEXT"
            wizard["sources"] = []
            wizard["destination"] = wizard.get("discussion_target_chat") or ""
            wizard["step"] = "done"
            return ""
        wizard["step"] = "blackbox"
        return "Введите BLACKBOX (N или 0):"
    if step == "blackbox":
        wizard["blackbox"] = text
        wizard["step"] = "done"
        return ""
    return ""


def _read_log_excerpt(path: str, limit: int = 20) -> str:
    try:
        with open(path, "r", encoding="utf-8") as file_handle:
            lines = file_handle.read().splitlines()
    except FileNotFoundError:
        return "Лог пока не создан."
    if not lines:
        return "Лог пуст."
    excerpt = lines[-limit:]
    if path.endswith("news_usage.log"):
        content = _format_usage_log_excerpt(excerpt)
    elif path.endswith("service.log"):
        content = _format_service_log_excerpt(excerpt)
    else:
        content = "\n".join(excerpt)
    if len(content) > 3900:
        content = content[-3900:]
    return content


def _format_usage_log_excerpt(lines: list[str]) -> str:
    formatted: list[str] = []
    daily_totals: dict[str, float] = {}
    for raw in lines:
        parts = raw.split("\t")
        if len(parts) < 6:
            formatted.append(raw)
            continue
        timestamp = parts[0]
        text_model = parts[1] if len(parts) > 1 else "-"
        text_in = parts[2] if len(parts) > 2 else "-"
        text_out = parts[3] if len(parts) > 3 else "-"
        text_total = parts[4] if len(parts) > 4 else "-"
        text_cost = parts[5] if len(parts) > 5 else "-"
        image_cost = parts[9] if len(parts) > 9 else "0"
        content = parts[10] if len(parts) > 10 else ""
        date_key = timestamp.split(" ")[0] if timestamp else "unknown"
        try:
            total_cost = float(text_cost) + float(image_cost)
        except ValueError:
            total_cost = 0.0
        daily_totals[date_key] = daily_totals.get(date_key, 0.0) + total_cost
        channel = "-"
        text_preview = content
        if content and content.rfind("@") != -1:
            maybe_channel = content[content.rfind("@") :].strip()
            if maybe_channel.startswith("@") and " " not in maybe_channel:
                channel = maybe_channel
                text_preview = content[: content.rfind("@")].strip()
        if not text_preview:
            text_preview = "(без текста)"
        if len(text_preview) > 120:
            text_preview = text_preview[:117] + "..."
        formatted.append(
            f"{timestamp} | {channel}\n"
            f"модель: {text_model}, токены: {text_in}/{text_out} ({text_total}), $: {text_cost}\n"
            f"{text_preview}"
        )
    if daily_totals:
        totals_lines = ["\nИтого $ за день:"]
        for date_key in sorted(daily_totals.keys()):
            totals_lines.append(f"{date_key}: {daily_totals[date_key]:.6f}")
        formatted.append("\n".join(totals_lines))
    return "\n\n".join(formatted)


def _format_service_log_excerpt(lines: list[str]) -> str:
    formatted: list[str] = []
    for raw in lines:
        parts = [item.strip() for item in raw.split("|", maxsplit=3)]
        if len(parts) == 4:
            timestamp, level, logger_name, message = parts
            short_time = timestamp.split(",")[0]
            formatted.append(f"{short_time} | {level} | {message}")
        else:
            formatted.append(raw)
    return "\n".join(formatted)


def _audit_log(event: str, user_id: int | None, details: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    user = str(user_id) if user_id is not None else "unknown"
    line = f"{timestamp}\tuser={user}\t{event}\t{details}\n"
    try:
        with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as file_handle:
            file_handle.write(line)
    except OSError:
        logger.exception("Failed to write audit log")


def build_bot_application(
    config: Config, accounts_runtime: dict[str, object]
) -> Application:
    application = ApplicationBuilder().token(config.TG_BOT_TOKEN).build()
    application.bot_data["config"] = config
    application.bot_data["accounts_config"] = config.telegram_accounts()
    application.bot_data["accounts_runtime"] = accounts_runtime
    application.add_handler(CommandHandler("start", _start))
    application.add_handler(CommandHandler("menu", _menu))
    application.add_handler(CommandHandler("back", _back))
    application.add_handler(CommandHandler("account", _account_cmd))
    application.add_handler(CommandHandler("pipeline", _pipeline_cmd))
    application.add_handler(CommandHandler("status", _status_cmd))
    application.add_handler(CommandHandler("my", _my_cmd))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_text))
    return application


async def start_bot(config: Config, accounts_runtime: dict[str, object]) -> Application:
    if not config.TG_BOT_TOKEN:
        raise ValueError("TG_BOT_TOKEN is not configured")
    logger.info("TG_BOT_ADMINS_JSON raw: %s", config.TG_BOT_ADMINS_JSON)
    logger.info("TG_BOT_ADMINS_JSON parsed: %s", config.bot_admins())
    application = build_bot_application(config, accounts_runtime)
    await application.initialize()
    await application.start()
    if application.updater is None:
        raise RuntimeError("Bot updater is not available")
    await application.updater.start_polling()
    logger.info("Bot started and polling")
    return application


async def stop_bot(application: Application | None) -> None:
    if application is None:
        return
    if application.updater:
        await application.updater.stop()
    await application.stop()
    await application.shutdown()
