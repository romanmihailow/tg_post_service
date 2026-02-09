"""In-memory status store for pipeline observability."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Optional


@dataclass
class PipelineStatusEntry:
    pipeline_id: int
    pipeline_name: str
    pipeline_type: str
    category: str
    state: str
    progress_current: Optional[int] = None
    progress_total: Optional[int] = None
    next_action_at: Optional[datetime] = None
    message: Optional[str] = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


_STATUS: dict[tuple[int, str], PipelineStatusEntry] = {}
_LOCK = Lock()


def set_status(
    *,
    pipeline_id: int,
    pipeline_name: str,
    pipeline_type: str,
    category: str,
    state: str,
    progress_current: Optional[int] = None,
    progress_total: Optional[int] = None,
    next_action_at: Optional[datetime] = None,
    message: Optional[str] = None,
) -> PipelineStatusEntry:
    key = (pipeline_id, category)
    now = datetime.now(timezone.utc)
    with _LOCK:
        entry = _STATUS.get(key)
        if entry is None:
            entry = PipelineStatusEntry(
                pipeline_id=pipeline_id,
                pipeline_name=pipeline_name,
                pipeline_type=pipeline_type,
                category=category,
                state=state,
                progress_current=progress_current,
                progress_total=progress_total,
                next_action_at=next_action_at,
                message=message,
                updated_at=now,
            )
            _STATUS[key] = entry
            return entry
        entry.pipeline_name = pipeline_name
        entry.pipeline_type = pipeline_type
        entry.state = state
        entry.progress_current = progress_current
        entry.progress_total = progress_total
        entry.next_action_at = next_action_at
        entry.message = message
        entry.updated_at = now
        return entry


def get_status(pipeline_id: int, category: str) -> Optional[PipelineStatusEntry]:
    with _LOCK:
        return _STATUS.get((pipeline_id, category))


def list_statuses(
    *, pipeline_ids: Optional[set[int]] = None, category: Optional[str] = None
) -> list[PipelineStatusEntry]:
    with _LOCK:
        entries = list(_STATUS.values())
    if pipeline_ids is not None:
        entries = [item for item in entries if item.pipeline_id in pipeline_ids]
    if category is not None:
        entries = [item for item in entries if item.category == category]
    entries.sort(key=lambda item: (item.pipeline_name, item.category))
    return entries
