"""Notification event persistence helpers for SQLiteTaskStore."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable

from smart_extractor.web.monitor_schedule import current_timestamp
from smart_extractor.web.task_models import NotificationEventRecord

ConnectionFactory = Callable[[], sqlite3.Connection]


def _normalize_limit(value: int, *, default: int = 20) -> int:
    try:
        limit = int(value or default)
    except (TypeError, ValueError):
        limit = default
    return max(limit, 1)


def create_notification_event(
    *,
    lock: Any,
    connect: ConnectionFactory,
    monitor_id: str,
    task_id: str,
    channel_type: str,
    target: str,
    event_type: str,
    status: str,
    status_message: str,
    attempt_no: int = 1,
    max_attempts: int = 3,
    next_retry_at: str = "",
    response_code: int | None = None,
    error_type: str = "",
    error_message: str = "",
    payload_snapshot: dict[str, Any] | None = None,
    sent_at: str = "",
    retry_of_notification_id: str = "",
    triggered_by: str = "system",
) -> str:
    now = current_timestamp()
    normalized_sent_at = str(sent_at or "").strip()
    normalized_status = str(status or "").strip().lower()
    if not normalized_sent_at and normalized_status in {"sent", "failed", "retry_pending", "skipped"}:
        normalized_sent_at = now

    with lock:
        with connect() as conn:
            row_id = conn.execute(
                """
                INSERT INTO monitor_notification_events (
                    notification_id, monitor_id, task_id, channel_type, target, event_type,
                    status, status_message, attempt_no, max_attempts, next_retry_at,
                    response_code, error_type, error_message, payload_snapshot_json,
                    created_at, sent_at, retry_of_notification_id, triggered_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "",
                    str(monitor_id or "").strip(),
                    str(task_id or "").strip(),
                    str(channel_type or "webhook").strip() or "webhook",
                    str(target or "").strip(),
                    str(event_type or "monitor_alert").strip() or "monitor_alert",
                    normalized_status,
                    str(status_message or "").strip(),
                    max(int(attempt_no or 1), 1),
                    max(int(max_attempts or 1), 1),
                    str(next_retry_at or "").strip(),
                    response_code,
                    str(error_type or "").strip(),
                    str(error_message or "").strip(),
                    json.dumps(payload_snapshot or {}, ensure_ascii=False),
                    now,
                    normalized_sent_at,
                    str(retry_of_notification_id or "").strip(),
                    str(triggered_by or "system").strip() or "system",
                ),
            ).lastrowid
            notification_id = f"ntf-{int(row_id):06d}"
            conn.execute(
                """
                UPDATE monitor_notification_events
                SET notification_id=?
                WHERE id=?
                """,
                (notification_id, row_id),
            )
            conn.commit()
    return notification_id


def fetch_notification_event(
    *,
    connect: ConnectionFactory,
    notification_id: str,
) -> NotificationEventRecord | None:
    with connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM monitor_notification_events
            WHERE notification_id=?
            """,
            (str(notification_id or "").strip(),),
        ).fetchone()
    if row is None:
        return None
    return NotificationEventRecord.from_row(row)


def fetch_notification_events(
    *,
    connect: ConnectionFactory,
    limit: int = 20,
    monitor_id: str = "",
    status: str = "",
    task_id: str = "",
    event_type: str = "",
    created_after: str = "",
) -> list[NotificationEventRecord]:
    conditions: list[str] = []
    params: list[Any] = []

    normalized_monitor_id = str(monitor_id or "").strip()
    if normalized_monitor_id:
        conditions.append("monitor_id=?")
        params.append(normalized_monitor_id)

    normalized_status = str(status or "").strip().lower()
    if normalized_status:
        conditions.append("status=?")
        params.append(normalized_status)

    normalized_task_id = str(task_id or "").strip()
    if normalized_task_id:
        conditions.append("task_id=?")
        params.append(normalized_task_id)

    normalized_event_type = str(event_type or "").strip().lower()
    if normalized_event_type:
        conditions.append("event_type=?")
        params.append(normalized_event_type)

    normalized_created_after = str(created_after or "").strip()
    if normalized_created_after:
        conditions.append("created_at>=?")
        params.append(normalized_created_after)

    where_sql = ""
    if conditions:
        where_sql = "WHERE " + " AND ".join(conditions)

    query = f"""
        SELECT *
        FROM monitor_notification_events
        {where_sql}
        ORDER BY created_at DESC, id DESC
        LIMIT ?
    """
    params.append(_normalize_limit(limit))

    with connect() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [NotificationEventRecord.from_row(row) for row in rows]


def update_notification_event(
    *,
    lock: Any,
    connect: ConnectionFactory,
    notification_id: str,
    fields: dict[str, Any],
) -> None:
    allowed_fields = {
        "status",
        "status_message",
        "next_retry_at",
        "response_code",
        "error_type",
        "error_message",
        "sent_at",
        "triggered_by",
    }
    normalized_fields = {
        key: value for key, value in fields.items() if key in allowed_fields
    }
    if not normalized_fields:
        return

    assignments: list[str] = []
    params: list[Any] = []
    for key, value in normalized_fields.items():
        assignments.append(f"{key}=?")
        params.append(value)
    params.append(str(notification_id or "").strip())

    with lock:
        with connect() as conn:
            conn.execute(
                f"""
                UPDATE monitor_notification_events
                SET {", ".join(assignments)}
                WHERE notification_id=?
                """,
                tuple(params),
            )
            conn.commit()


def fetch_due_notification_retries(
    *,
    connect: ConnectionFactory,
    due_before: str,
    limit: int = 10,
) -> list[NotificationEventRecord]:
    normalized_due_before = str(due_before or "").strip()
    if not normalized_due_before:
        return []

    with connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM monitor_notification_events
            WHERE status='retry_pending'
              AND next_retry_at<>''
              AND next_retry_at<=?
            ORDER BY next_retry_at ASC, created_at ASC, id ASC
            LIMIT ?
            """,
            (
                normalized_due_before,
                _normalize_limit(limit, default=10),
            ),
        ).fetchall()
    return [NotificationEventRecord.from_row(row) for row in rows]
