"""监控调度相关的时间与状态辅助函数。"""

from __future__ import annotations

from datetime import datetime, timedelta

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
MIN_MONITOR_INTERVAL_MINUTES = 5
MAX_MONITOR_INTERVAL_MINUTES = 60 * 24 * 7
DEFAULT_MONITOR_INTERVAL_MINUTES = 60


def current_timestamp(now: datetime | None = None) -> str:
    return (now or datetime.now()).strftime(DATETIME_FORMAT)


def parse_timestamp(value: str | None) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        return datetime.strptime(normalized, DATETIME_FORMAT)
    except ValueError:
        return None


def normalize_schedule_interval_minutes(value: object) -> int:
    try:
        interval = int(value or DEFAULT_MONITOR_INTERVAL_MINUTES)
    except (TypeError, ValueError):
        interval = DEFAULT_MONITOR_INTERVAL_MINUTES
    return max(MIN_MONITOR_INTERVAL_MINUTES, min(interval, MAX_MONITOR_INTERVAL_MINUTES))


def compute_next_run_at(
    *,
    interval_minutes: int,
    base_time: datetime | str | None = None,
) -> str:
    normalized_interval = normalize_schedule_interval_minutes(interval_minutes)
    if isinstance(base_time, datetime):
        base_datetime = base_time
    elif isinstance(base_time, str):
        base_datetime = parse_timestamp(base_time) or datetime.now()
    else:
        base_datetime = datetime.now()
    return current_timestamp(base_datetime + timedelta(minutes=normalized_interval))


def schedule_status(*, enabled: bool, paused_at: str | None) -> str:
    if not enabled:
        return "disabled"
    if str(paused_at or "").strip():
        return "paused"
    return "active"


def is_due(*, next_run_at: str | None, now: datetime | None = None) -> bool:
    next_run = parse_timestamp(next_run_at)
    if next_run is None:
        return False
    return next_run <= (now or datetime.now())
