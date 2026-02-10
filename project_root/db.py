"""Database initialization and session management."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator, Iterable
import json

from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from project_root.config import Config, PipelineConfig
from project_root.models import (
    Base,
    BotInvite,
    BotInviteCode,
    ChatState,
    DiscussionBotWeight,
    DiscussionReply,
    DiscussionSettings,
    DiscussionState,
    GlobalState,
    Pipeline,
    PipelineSource,
    PipelineState,
    PostHistory,
    SourceChannel,
    UserbotPersona,
)


def create_db_engine() -> Engine:
    """Create a SQLite engine."""
    return create_engine(
        "sqlite:///./tg_post_service.db",
        connect_args={"check_same_thread": False},
        future=True,
    )


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


ENGINE = create_db_engine()
SessionLocal = sessionmaker(bind=ENGINE, expire_on_commit=False, class_=Session)


def init_db(config: Config) -> None:
    """Create tables and seed initial data based on configuration."""
    Base.metadata.create_all(bind=ENGINE)
    _ensure_global_state_schema()
    _ensure_pipelines_schema()
    _ensure_discussion_settings_schema()
    _ensure_discussion_state_schema()
    _ensure_discussion_replies_schema()
    _ensure_userbot_persona_schema()
    with SessionLocal() as session:
        _ensure_pipelines(session, config.pipelines)
        _ensure_pipeline_states(session)
        _ensure_discussion_states(session)
        _ensure_global_state(session)
        _seed_userbot_persona(session, [item.name for item in config.telegram_accounts()])
        session.commit()


def _ensure_source_channels(session: Session, channels: Iterable[str]) -> None:
    for identifier in channels:
        exists = session.execute(
            select(SourceChannel).where(SourceChannel.channel_identifier == identifier)
        ).scalar_one_or_none()
        if exists is None:
            session.add(SourceChannel(channel_identifier=identifier))


def _ensure_global_state(session: Session) -> None:
    state = session.get(GlobalState, 1)
    if state is None:
        session.add(GlobalState(id=1, current_channel_index=0, post_counter=0))


def _ensure_global_state_schema() -> None:
    with ENGINE.connect() as connection:
        result = connection.execute(text("PRAGMA table_info(global_state)"))
        columns = {row[1] for row in result.fetchall()}
        if "post_counter" not in columns:
            connection.execute(
                text("ALTER TABLE global_state ADD COLUMN post_counter INTEGER DEFAULT 0")
            )
            connection.commit()


def _ensure_pipelines_schema() -> None:
    with ENGINE.connect() as connection:
        result = connection.execute(text("PRAGMA table_info(pipelines)"))
        columns = {row[1] for row in result.fetchall()}
        if "account_name" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE pipelines ADD COLUMN account_name TEXT DEFAULT 'default'"
                )
            )
            connection.execute(
                text(
                    "UPDATE pipelines SET account_name = 'default' "
                    "WHERE account_name IS NULL OR account_name = ''"
                )
            )
        if "is_enabled" not in columns:
            connection.execute(
                text("ALTER TABLE pipelines ADD COLUMN is_enabled INTEGER DEFAULT 1")
            )
            connection.execute(
                text(
                    "UPDATE pipelines SET is_enabled = 1 "
                    "WHERE is_enabled IS NULL"
                )
            )
        if "pipeline_type" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE pipelines ADD COLUMN pipeline_type TEXT DEFAULT 'STANDARD'"
                )
            )
            connection.execute(
                text(
                    "UPDATE pipelines SET pipeline_type = 'STANDARD' "
                    "WHERE pipeline_type IS NULL OR pipeline_type = ''"
                )
            )
        connection.commit()


def _ensure_discussion_settings_schema() -> None:
    with ENGINE.connect() as connection:
        result = connection.execute(text("PRAGMA table_info(discussion_settings)"))
        columns = {row[1] for row in result.fetchall()}
        if "activity_windows_weekdays_json" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE discussion_settings ADD COLUMN activity_windows_weekdays_json TEXT"
                )
            )
        if "activity_windows_weekends_json" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE discussion_settings ADD COLUMN activity_windows_weekends_json TEXT"
                )
            )
        if "activity_timezone" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE discussion_settings ADD COLUMN activity_timezone TEXT DEFAULT 'Europe/Moscow'"
                )
            )
        if "min_interval_minutes" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE discussion_settings ADD COLUMN min_interval_minutes INTEGER DEFAULT 90"
                )
            )
        if "max_interval_minutes" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE discussion_settings ADD COLUMN max_interval_minutes INTEGER DEFAULT 180"
                )
            )
        if "inactivity_pause_minutes" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE discussion_settings ADD COLUMN inactivity_pause_minutes INTEGER DEFAULT 60"
                )
            )
        if "max_auto_replies_per_chat_per_day" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE discussion_settings ADD COLUMN max_auto_replies_per_chat_per_day INTEGER DEFAULT 30"
                )
            )
        if "user_reply_max_age_minutes" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE discussion_settings ADD COLUMN user_reply_max_age_minutes INTEGER DEFAULT 30"
                )
            )
        connection.commit()


def _ensure_discussion_state_schema() -> None:
    with ENGINE.connect() as connection:
        result = connection.execute(text("PRAGMA table_info(discussion_state)"))
        columns = {row[1] for row in result.fetchall()}
        if "next_due_at" not in columns:
            connection.execute(
                text("ALTER TABLE discussion_state ADD COLUMN next_due_at DATETIME")
            )
        if "last_source_post_id" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE discussion_state ADD COLUMN last_source_post_id INTEGER"
                )
            )
        if "last_source_post_at" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE discussion_state ADD COLUMN last_source_post_at DATETIME"
                )
            )
        if "recent_topics_json" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE discussion_state ADD COLUMN recent_topics_json TEXT"
                )
            )
        connection.commit()


def _ensure_discussion_replies_schema() -> None:
    with ENGINE.connect() as connection:
        result = connection.execute(text("PRAGMA table_info(discussion_replies)"))
        columns = {row[1] for row in result.fetchall()}
        if "kind" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE discussion_replies ADD COLUMN kind TEXT DEFAULT 'discussion'"
                )
            )
            connection.execute(
                text(
                    "UPDATE discussion_replies SET kind = 'discussion' WHERE kind IS NULL"
                )
            )
        else:
            connection.execute(
                text(
                    "UPDATE discussion_replies SET kind = 'discussion' WHERE kind IS NULL"
                )
            )
        if "chat_id" not in columns:
            connection.execute(
                text("ALTER TABLE discussion_replies ADD COLUMN chat_id TEXT")
            )
        if "source_message_at" not in columns:
            connection.execute(
                text("ALTER TABLE discussion_replies ADD COLUMN source_message_at DATETIME")
            )
        connection.commit()


def _ensure_userbot_persona_schema() -> None:
    with ENGINE.connect() as connection:
        result = connection.execute(text("PRAGMA table_info(userbot_persona)"))
        columns = {row[1] for row in result.fetchall()}
        if not columns:
            return
        if "persona_tone" not in columns:
            connection.execute(
                text("ALTER TABLE userbot_persona ADD COLUMN persona_tone TEXT")
            )
        if "persona_verbosity" not in columns:
            connection.execute(
                text("ALTER TABLE userbot_persona ADD COLUMN persona_verbosity TEXT")
            )
        if "persona_style_hint" not in columns:
            connection.execute(
                text("ALTER TABLE userbot_persona ADD COLUMN persona_style_hint TEXT")
            )
        if "persona_topics" not in columns:
            connection.execute(
                text("ALTER TABLE userbot_persona ADD COLUMN persona_topics TEXT")
            )
        if "persona_offtopic_tolerance" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE userbot_persona "
                    "ADD COLUMN persona_offtopic_tolerance INTEGER DEFAULT 50"
                )
            )
        if "persona_topic_priority" not in columns:
            connection.execute(
                text(
                    "ALTER TABLE userbot_persona "
                    "ADD COLUMN persona_topic_priority INTEGER DEFAULT 50"
                )
            )
        if "persona_offtopic_tolerance" in columns:
            connection.execute(
                text(
                    "UPDATE userbot_persona SET persona_offtopic_tolerance = 50 "
                    "WHERE persona_offtopic_tolerance IS NULL"
                )
            )
        if "persona_topic_priority" in columns:
            connection.execute(
                text(
                    "UPDATE userbot_persona SET persona_topic_priority = 50 "
                    "WHERE persona_topic_priority IS NULL"
                )
            )
        connection.commit()


def _seed_userbot_persona(session: Session, account_names: list[str]) -> None:
    if not account_names:
        return
    personas = [
        {
            "persona_tone": "analytical",
            "persona_verbosity": "medium",
            "persona_style_hint": (
                "Спокойный ведущий дискуссии. Формулирует нейтральные вопросы, "
                "собирает ключевые факты и задаёт рамку обсуждения. Пишет без оценок, "
                "без сленга, без эмоций, подчёркивает разные стороны темы."
            ),
            "persona_topics": ["политика", "экономика", "технологии", "медиа", "общество"],
            "persona_offtopic_tolerance": 80,
            "persona_topic_priority": 70,
        },
        {
            "persona_tone": "analytical",
            "persona_verbosity": "medium",
            "persona_style_hint": (
                "Внимательный читатель, который следит за экономикой и технологиями. "
                "Объясняет контекст, связывает новости про бизнес, рынки и IT без "
                "кликбейта и жёстких оценок."
            ),
            "persona_topics": [
                "экономика",
                "бизнес",
                "финансы",
                "рынки",
                "технологии",
                "it",
            ],
            "persona_offtopic_tolerance": 70,
            "persona_topic_priority": 75,
        },
        {
            "persona_tone": "skeptical",
            "persona_verbosity": "short",
            "persona_style_hint": (
                "Сдержанный скептик. Пишет коротко, мягко ставит под сомнение выводы, "
                "указывает на недостающие факты и альтернативные версии. "
                "Без агрессии и сарказма."
            ),
            "persona_topics": ["политика", "общество", "безопасность"],
            "persona_offtopic_tolerance": 60,
            "persona_topic_priority": 80,
        },
        {
            "persona_tone": "ironic",
            "persona_verbosity": "short",
            "persona_style_hint": (
                "Короткие комментарии с лёгкой иронией. Подмечает странные детали в "
                "новостях про медиа, соцсети, спорт и поп-культуру. Юмор мягкий, "
                "без токсичности и нападок."
            ),
            "persona_topics": ["соцсети", "медиа", "культура", "спорт"],
            "persona_offtopic_tolerance": 50,
            "persona_topic_priority": 70,
        },
        {
            "persona_tone": "neutral",
            "persona_verbosity": "short",
            "persona_style_hint": (
                "Пишет простым человеческим языком. Спрашивает, что новость значит "
                "для обычных людей: семья, работа, образование, здоровье. "
                "Без пафоса и сложных терминов."
            ),
            "persona_topics": [
                "общество",
                "образование",
                "здоровье",
                "психология",
                "культура",
            ],
            "persona_offtopic_tolerance": 90,
            "persona_topic_priority": 60,
        },
        {
            "persona_tone": "emotional",
            "persona_verbosity": "medium",
            "persona_style_hint": (
                "Эмоциональный, но адекватный болельщик. Реагирует на спортивные "
                "новости, трансферы и турниры. Может немного порадоваться или "
                "расстроиться, но без токсичности."
            ),
            "persona_topics": ["спорт", "футбол", "киберспорт"],
            "persona_offtopic_tolerance": 50,
            "persona_topic_priority": 85,
        },
        {
            "persona_tone": "analytical",
            "persona_verbosity": "medium",
            "persona_style_hint": (
                "Смотрит на новости через призму города и цифровых сервисов. "
                "Обсуждает, как технологии, транспорт и соцсети влияют на "
                "повседневную жизнь. Пишет спокойно и практично."
            ),
            "persona_topics": [
                "город",
                "урбанистика",
                "технологии",
                "it",
                "интернет",
                "соцсети",
            ],
            "persona_offtopic_tolerance": 70,
            "persona_topic_priority": 75,
        },
        {
            "persona_tone": "analytical",
            "persona_verbosity": "medium",
            "persona_style_hint": (
                "Сдержанный аналитик по безопасности. Обсуждает новости про армию, "
                "конфликты и геополитику в сухом фактическом стиле, "
                "без истерики и пропаганды."
            ),
            "persona_topics": ["безопасность", "армия", "конфликты", "геополитика", "политика"],
            "persona_offtopic_tolerance": 55,
            "persona_topic_priority": 85,
        },
        {
            "persona_tone": "neutral",
            "persona_verbosity": "medium",
            "persona_style_hint": (
                "Любит кино, сериалы и культуру. Пишет небольшие наблюдения о том, "
                "как события отражаются в медиа и трендах. Без снобизма, но с "
                "интересом к деталям."
            ),
            "persona_topics": ["кино", "сериалы", "культура", "искусство", "медиа"],
            "persona_offtopic_tolerance": 65,
            "persona_topic_priority": 80,
        },
        {
            "persona_tone": "neutral",
            "persona_verbosity": "short",
            "persona_style_hint": (
                "Спокойный, немного консервативный читатель. Обращает внимание на "
                "деньги, курс, цены и социальные последствия. Пишет коротко, "
                "без споров."
            ),
            "persona_topics": ["финансы", "экономика", "общество"],
            "persona_offtopic_tolerance": 100,
            "persona_topic_priority": 50,
        },
    ]
    default_persona = personas[-1]
    for idx, account_name in enumerate(sorted(account_names)):
        exists = session.execute(
            select(UserbotPersona).where(UserbotPersona.account_name == account_name)
        ).scalar_one_or_none()
        persona = personas[idx] if idx < len(personas) else default_persona
        topics = persona["persona_topics"]
        topics_json = json.dumps(topics, ensure_ascii=False) if topics else None
        if exists is None:
            session.add(
                UserbotPersona(
                    account_name=account_name,
                    persona_tone=persona["persona_tone"],
                    persona_verbosity=persona["persona_verbosity"],
                    persona_style_hint=persona["persona_style_hint"],
                    persona_topics=topics_json,
                    persona_offtopic_tolerance=persona["persona_offtopic_tolerance"],
                    persona_topic_priority=persona["persona_topic_priority"],
                )
            )
            continue
        if not exists.persona_tone:
            exists.persona_tone = persona["persona_tone"]
        if not exists.persona_verbosity:
            exists.persona_verbosity = persona["persona_verbosity"]
        if not exists.persona_style_hint:
            exists.persona_style_hint = persona["persona_style_hint"]
        if exists.persona_topics is None and topics_json is not None:
            exists.persona_topics = topics_json
        if exists.persona_offtopic_tolerance is None:
            exists.persona_offtopic_tolerance = persona["persona_offtopic_tolerance"]
        if exists.persona_topic_priority is None:
            exists.persona_topic_priority = persona["persona_topic_priority"]


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _ensure_pipelines(session: Session, pipelines: Iterable[PipelineConfig]) -> None:
    for pipeline_config in pipelines:
        pipeline = session.execute(
            select(Pipeline).where(Pipeline.name == pipeline_config.name)
        ).scalar_one_or_none()
        if pipeline is None:
            pipeline = Pipeline(
                name=pipeline_config.name,
                account_name=pipeline_config.account,
                destination_channel=pipeline_config.destination,
                posting_mode=pipeline_config.mode,
                interval_seconds=pipeline_config.interval_seconds,
                blackbox_every_n=pipeline_config.blackbox_every_n,
                pipeline_type=pipeline_config.pipeline_type,
            )
            session.add(pipeline)
            session.flush()
        else:
            pipeline.destination_channel = pipeline_config.destination
            pipeline.posting_mode = pipeline_config.mode
            pipeline.interval_seconds = pipeline_config.interval_seconds
            pipeline.blackbox_every_n = pipeline_config.blackbox_every_n
            pipeline.account_name = pipeline_config.account
            pipeline.pipeline_type = pipeline_config.pipeline_type

        existing_sources = {
            source.source_channel for source in pipeline.sources
        }
        for channel in pipeline_config.sources:
            if channel not in existing_sources:
                session.add(
                    PipelineSource(
                        pipeline_id=pipeline.id, source_channel=channel
                    )
                )
        if pipeline_config.pipeline_type == "DISCUSSION":
            upsert_discussion_settings(
                session,
                pipeline_id=pipeline.id,
                target_chat=pipeline_config.discussion_target_chat
                or pipeline_config.destination,
                source_pipeline_name=pipeline_config.discussion_source_pipeline
                or pipeline_config.name,
                k_min=pipeline_config.discussion_k_min,
                k_max=pipeline_config.discussion_k_max,
                reply_to_reply_probability=pipeline_config.discussion_reply_to_reply_probability,
                activity_windows_weekdays_json=pipeline_config.discussion_activity_windows_weekdays_json,
                activity_windows_weekends_json=pipeline_config.discussion_activity_windows_weekends_json,
                activity_timezone=pipeline_config.discussion_activity_timezone,
                min_interval_minutes=pipeline_config.discussion_min_interval_minutes,
                max_interval_minutes=pipeline_config.discussion_max_interval_minutes,
                inactivity_pause_minutes=pipeline_config.discussion_inactivity_pause_minutes,
                max_auto_replies_per_chat_per_day=(
                    pipeline_config.discussion_max_auto_replies_per_chat_per_day
                ),
                user_reply_max_age_minutes=(
                    pipeline_config.discussion_user_reply_max_age_minutes
                ),
            )


def _ensure_pipeline_states(session: Session) -> None:
    pipelines = session.execute(select(Pipeline)).scalars().all()
    for pipeline in pipelines:
        state = session.get(PipelineState, pipeline.id)
        if state is None:
            session.add(
                PipelineState(
                    pipeline_id=pipeline.id,
                    current_source_index=0,
                    total_posts=0,
                    last_run_at=None,
                )
            )


def _ensure_discussion_states(session: Session) -> None:
    pipelines = (
        session.execute(select(Pipeline).where(Pipeline.pipeline_type == "DISCUSSION"))
        .scalars()
        .all()
    )
    for pipeline in pipelines:
        state = session.get(DiscussionState, pipeline.id)
        if state is None:
            session.add(
                DiscussionState(
                    pipeline_id=pipeline.id,
                    question_message_id=None,
                    question_created_at=None,
                    expires_at=None,
                    replies_planned=0,
                    replies_sent=0,
                    last_bot_reply_at=None,
                    last_reply_parent_id=None,
                    last_bot_reply_message_id=None,
                    last_source_post_id=None,
                    last_source_post_at=None,
                    recent_topics_json=None,
                    next_due_at=None,
                )
            )


def get_all_pipelines(session: Session) -> list[Pipeline]:
    return session.execute(select(Pipeline).order_by(Pipeline.id)).scalars().all()


def get_pipeline_sources(session: Session, pipeline_id: int) -> list[PipelineSource]:
    return (
        session.execute(
            select(PipelineSource)
            .where(PipelineSource.pipeline_id == pipeline_id)
            .order_by(PipelineSource.id)
        )
        .scalars()
        .all()
    )


def get_pipeline_state(session: Session, pipeline_id: int) -> PipelineState:
    state = session.get(PipelineState, pipeline_id)
    if state is None:
        state = PipelineState(
            pipeline_id=pipeline_id,
            current_source_index=0,
            total_posts=0,
            last_run_at=None,
        )
        session.add(state)
        session.flush()
    return state


def get_pipeline_by_name(session: Session, name: str) -> Pipeline | None:
    return session.execute(select(Pipeline).where(Pipeline.name == name)).scalar_one_or_none()


def create_pipeline(session: Session, config: PipelineConfig) -> Pipeline:
    pipeline = Pipeline(
        name=config.name,
        account_name=config.account,
        destination_channel=config.destination,
        posting_mode=config.mode,
        interval_seconds=config.interval_seconds,
        blackbox_every_n=config.blackbox_every_n,
        pipeline_type=config.pipeline_type,
        is_enabled=1,
    )
    session.add(pipeline)
    session.flush()
    for channel in config.sources:
        session.add(PipelineSource(pipeline_id=pipeline.id, source_channel=channel))
    return pipeline


def update_pipeline_destination(
    session: Session, pipeline: Pipeline, destination: str
) -> None:
    pipeline.destination_channel = destination


def update_pipeline_interval(
    session: Session, pipeline: Pipeline, interval_seconds: int
) -> None:
    pipeline.interval_seconds = interval_seconds


def update_pipeline_mode(session: Session, pipeline: Pipeline, mode: str) -> None:
    pipeline.posting_mode = mode


def toggle_pipeline_enabled(session: Session, pipeline: Pipeline) -> bool:
    pipeline.is_enabled = 0 if pipeline.is_enabled else 1
    return bool(pipeline.is_enabled)


def add_pipeline_source(
    session: Session, pipeline: Pipeline, source_channel: str
) -> None:
    exists = session.execute(
        select(PipelineSource).where(
            PipelineSource.pipeline_id == pipeline.id,
            PipelineSource.source_channel == source_channel,
        )
    ).scalar_one_or_none()
    if exists is None:
        session.add(
            PipelineSource(pipeline_id=pipeline.id, source_channel=source_channel)
        )


def remove_pipeline_source(
    session: Session, pipeline: Pipeline, source_channel: str
) -> bool:
    item = session.execute(
        select(PipelineSource).where(
            PipelineSource.pipeline_id == pipeline.id,
            PipelineSource.source_channel == source_channel,
        )
    ).scalar_one_or_none()
    if item is None:
        return False
    session.delete(item)
    return True


def delete_pipeline(session: Session, pipeline: Pipeline) -> None:
    session.delete(pipeline)


def create_invite(
    session: Session,
    token: str,
    created_by: int,
    expires_at: datetime,
) -> BotInvite:
    invite = BotInvite(
        token=token,
        created_by=created_by,
        created_at=datetime.utcnow(),
        expires_at=expires_at,
    )
    session.add(invite)
    session.flush()
    return invite


def get_invite(session: Session, token: str) -> BotInvite | None:
    return session.execute(select(BotInvite).where(BotInvite.token == token)).scalar_one_or_none()


def mark_invite_used(session: Session, invite: BotInvite, user_id: int) -> None:
    invite.used_by = user_id
    invite.used_at = datetime.utcnow()


def create_invite_code(
    session: Session,
    token: str,
    code: str,
    created_for: int,
    expires_at: datetime,
) -> BotInviteCode:
    item = BotInviteCode(
        token=token,
        code=code,
        created_for=created_for,
        created_at=datetime.utcnow(),
        expires_at=expires_at,
    )
    session.add(item)
    session.flush()
    return item


def get_invite_code(
    session: Session, token: str, code: str, user_id: int
) -> BotInviteCode | None:
    return session.execute(
        select(BotInviteCode).where(
            BotInviteCode.token == token,
            BotInviteCode.code == code,
            BotInviteCode.created_for == user_id,
        )
    ).scalar_one_or_none()


def mark_invite_code_used(session: Session, item: BotInviteCode) -> None:
    item.used_at = datetime.utcnow()


def get_discussion_settings(
    session: Session, pipeline_id: int
) -> DiscussionSettings | None:
    return session.get(DiscussionSettings, pipeline_id)


def upsert_discussion_settings(
    session: Session,
    *,
    pipeline_id: int,
    target_chat: str,
    source_pipeline_name: str,
    k_min: int,
    k_max: int,
    reply_to_reply_probability: int,
    activity_windows_weekdays_json: str | None = None,
    activity_windows_weekends_json: str | None = None,
    activity_timezone: str | None = None,
    min_interval_minutes: int | None = None,
    max_interval_minutes: int | None = None,
    inactivity_pause_minutes: int | None = None,
    max_auto_replies_per_chat_per_day: int | None = None,
    user_reply_max_age_minutes: int | None = None,
) -> DiscussionSettings:
    settings = session.get(DiscussionSettings, pipeline_id)
    if settings is None:
        settings = DiscussionSettings(
            pipeline_id=pipeline_id,
            target_chat=target_chat,
            source_pipeline_name=source_pipeline_name,
            k_min=k_min,
            k_max=k_max,
            reply_to_reply_probability=reply_to_reply_probability,
            activity_windows_weekdays_json=activity_windows_weekdays_json,
            activity_windows_weekends_json=activity_windows_weekends_json,
            activity_timezone=activity_timezone or "Europe/Moscow",
            min_interval_minutes=min_interval_minutes or 90,
            max_interval_minutes=max_interval_minutes or 180,
            inactivity_pause_minutes=inactivity_pause_minutes or 60,
            max_auto_replies_per_chat_per_day=max_auto_replies_per_chat_per_day or 30,
            user_reply_max_age_minutes=user_reply_max_age_minutes or 30,
        )
        session.add(settings)
        session.flush()
        return settings
    settings.target_chat = target_chat
    settings.source_pipeline_name = source_pipeline_name
    settings.k_min = k_min
    settings.k_max = k_max
    settings.reply_to_reply_probability = reply_to_reply_probability
    if activity_windows_weekdays_json is not None:
        settings.activity_windows_weekdays_json = activity_windows_weekdays_json
    if activity_windows_weekends_json is not None:
        settings.activity_windows_weekends_json = activity_windows_weekends_json
    if activity_timezone is not None:
        settings.activity_timezone = activity_timezone
    if min_interval_minutes is not None:
        settings.min_interval_minutes = min_interval_minutes
    if max_interval_minutes is not None:
        settings.max_interval_minutes = max_interval_minutes
    if inactivity_pause_minutes is not None:
        settings.inactivity_pause_minutes = inactivity_pause_minutes
    if max_auto_replies_per_chat_per_day is not None:
        settings.max_auto_replies_per_chat_per_day = max_auto_replies_per_chat_per_day
    if user_reply_max_age_minutes is not None:
        settings.user_reply_max_age_minutes = user_reply_max_age_minutes
    return settings


def get_discussion_state(session: Session, pipeline_id: int) -> DiscussionState:
    state = session.get(DiscussionState, pipeline_id)
    if state is None:
        state = DiscussionState(
            pipeline_id=pipeline_id,
            question_message_id=None,
            question_created_at=None,
            expires_at=None,
            replies_planned=0,
            replies_sent=0,
            last_bot_reply_at=None,
            last_reply_parent_id=None,
            last_bot_reply_message_id=None,
            last_source_post_id=None,
            last_source_post_at=None,
            recent_topics_json=None,
            next_due_at=None,
        )
        session.add(state)
        session.flush()
    return state


def get_recent_post_history(
    session: Session, pipeline_id: int, limit: int
) -> list[PostHistory]:
    return (
        session.execute(
            select(PostHistory)
            .where(PostHistory.pipeline_id == pipeline_id)
            .order_by(PostHistory.id.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )


def list_discussion_bot_weights(
    session: Session, pipeline_id: int
) -> list[DiscussionBotWeight]:
    return (
        session.execute(
            select(DiscussionBotWeight)
            .where(DiscussionBotWeight.pipeline_id == pipeline_id)
            .order_by(DiscussionBotWeight.id)
        )
        .scalars()
        .all()
    )


def upsert_discussion_bot_weight(
    session: Session,
    *,
    pipeline_id: int,
    account_name: str,
    weight: int,
    daily_limit: int,
    cooldown_minutes: int,
) -> DiscussionBotWeight:
    row = session.execute(
        select(DiscussionBotWeight).where(
            DiscussionBotWeight.pipeline_id == pipeline_id,
            DiscussionBotWeight.account_name == account_name,
        )
    ).scalar_one_or_none()
    if row is None:
        row = DiscussionBotWeight(
            pipeline_id=pipeline_id,
            account_name=account_name,
            weight=weight,
            daily_limit=daily_limit,
            cooldown_minutes=cooldown_minutes,
            used_today=0,
            used_today_date=None,
            last_used_at=None,
        )
        session.add(row)
        session.flush()
        return row
    row.weight = weight
    row.daily_limit = daily_limit
    row.cooldown_minutes = cooldown_minutes
    return row


def create_discussion_reply(
    session: Session,
    *,
    pipeline_id: int,
    kind: str = "discussion",
    chat_id: str | None = None,
    account_name: str,
    reply_text: str,
    send_at: datetime,
    reply_to_message_id: int | None,
    source_message_at: datetime | None = None,
) -> DiscussionReply:
    send_at = _naive_utc(send_at)
    if source_message_at is not None:
        source_message_at = _naive_utc(source_message_at)
    reply = DiscussionReply(
        pipeline_id=pipeline_id,
        kind=kind,
        chat_id=chat_id,
        account_name=account_name,
        reply_text=reply_text,
        send_at=send_at,
        status="pending",
        reply_to_message_id=reply_to_message_id,
        source_message_at=source_message_at,
        sent_at=None,
        cancelled_reason=None,
    )
    session.add(reply)
    session.flush()
    return reply


def list_due_discussion_replies(
    session: Session,
    pipeline_id: int,
    now: datetime,
    *,
    kind: str | None = None,
) -> list[DiscussionReply]:
    now = _naive_utc(now)
    return (
        session.execute(
            select(DiscussionReply)
            .where(
                DiscussionReply.pipeline_id == pipeline_id,
                DiscussionReply.status == "pending",
                DiscussionReply.send_at <= now,
                *( [DiscussionReply.kind == kind] if kind else []),
            )
            .order_by(DiscussionReply.send_at)
        )
        .scalars()
        .all()
    )


def mark_discussion_reply_sent(
    session: Session, reply: DiscussionReply, sent_at: datetime
) -> None:
    reply.status = "sent"
    reply.sent_at = _naive_utc(sent_at)


def mark_discussion_reply_cancelled(
    session: Session, reply: DiscussionReply, reason: str
) -> None:
    reply.status = "cancelled"
    reply.cancelled_reason = reason


def get_chat_state(session: Session, pipeline_id: int, chat_id: str) -> ChatState:
    state = session.execute(
        select(ChatState).where(
            ChatState.pipeline_id == pipeline_id,
            ChatState.chat_id == chat_id,
        )
    ).scalar_one_or_none()
    if state is None:
        state = ChatState(
            pipeline_id=pipeline_id,
            chat_id=chat_id,
            last_seen_message_id=None,
            last_human_message_at=None,
            replies_today=0,
            replies_today_date=None,
            next_scan_at=None,
        )
        session.add(state)
        session.flush()
    return state


def get_userbot_persona(session: Session, account_name: str) -> UserbotPersona | None:
    return session.execute(
        select(UserbotPersona).where(UserbotPersona.account_name == account_name)
    ).scalar_one_or_none()


def upsert_userbot_persona(
    session: Session,
    *,
    account_name: str,
    persona_tone: str | None,
    persona_verbosity: str | None,
    persona_style_hint: str | None,
) -> UserbotPersona:
    row = session.execute(
        select(UserbotPersona).where(UserbotPersona.account_name == account_name)
    ).scalar_one_or_none()
    if row is None:
        row = UserbotPersona(
            account_name=account_name,
            persona_tone=persona_tone,
            persona_verbosity=persona_verbosity,
            persona_style_hint=persona_style_hint,
        )
        session.add(row)
        session.flush()
        return row
    row.persona_tone = persona_tone
    row.persona_verbosity = persona_verbosity
    row.persona_style_hint = persona_style_hint
    return row
