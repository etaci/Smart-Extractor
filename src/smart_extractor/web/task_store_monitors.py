"""Monitor persistence helpers for the task store."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Callable

from smart_extractor.web.monitor_schedule import (
    compute_next_run_at,
    current_timestamp,
    normalize_schedule_interval_minutes,
)
from smart_extractor.web.task_models import MonitorRecord

ConnectionFactory = Callable[[], object]

ACTIVE_MONITOR_TASK_STATUSES = {"pending", "queued", "running"}


def _all_tenants(tenant_id: str) -> bool:
    return str(tenant_id or "").strip() == "*"


def _normalize_limit(value: int, *, default: int = 5) -> int:
    try:
        limit = int(value or default)
    except (TypeError, ValueError):
        limit = default
    return max(limit, 1)


def _normalize_lease_seconds(value: float, *, default: float = 120.0) -> float:
    try:
        lease_seconds = float(value or default)
    except (TypeError, ValueError):
        lease_seconds = default
    return max(lease_seconds, 5.0)


def _build_lease_until(*, now: datetime, lease_seconds: float) -> str:
    return current_timestamp(now + timedelta(seconds=_normalize_lease_seconds(lease_seconds)))


def _has_active_monitor_task(conn, last_task_id: str, *, tenant_id: str) -> bool:
    normalized_task_id = str(last_task_id or "").strip()
    if not normalized_task_id:
        return False
    task_row = conn.execute(
        "SELECT status FROM web_tasks WHERE tenant_id=? AND task_id=?",
        (tenant_id, normalized_task_id),
    ).fetchone()
    if task_row is None:
        return False
    return str(task_row["status"] or "").strip().lower() in ACTIVE_MONITOR_TASK_STATUSES


def _insert_monitor_row(conn, values: tuple[Any, ...]) -> int:
    if getattr(conn, "dialect", "sqlite") == "postgres":
        row = conn.execute(
            """
            INSERT INTO monitor_profiles (
                monitor_id, tenant_id, name, url, schema_name, storage_format, use_static,
                selected_fields_json, field_labels_json, profile_json, created_at, updated_at,
                schedule_enabled, schedule_interval_minutes, schedule_next_run_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            values,
        ).fetchone()
        return int(row["id"] or 0)
    cursor = conn.execute(
        """
        INSERT INTO monitor_profiles (
            monitor_id, tenant_id, name, url, schema_name, storage_format, use_static,
            selected_fields_json, field_labels_json, profile_json, created_at, updated_at,
            schedule_enabled, schedule_interval_minutes, schedule_next_run_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        values,
    )
    return int(cursor.lastrowid or 0)


def upsert_monitor(
    *,
    lock: Any,
    connect: ConnectionFactory,
    name: str,
    url: str,
    schema_name: str,
    storage_format: str,
    use_static: bool,
    selected_fields: list[str],
    field_labels: dict[str, str],
    profile: dict[str, Any] | None = None,
    monitor_id: str = "",
    schedule_enabled: bool = False,
    schedule_interval_minutes: int = 60,
    tenant_id: str = "default",
) -> str:
    now = current_timestamp()
    normalized_monitor_id = str(monitor_id or "").strip()
    normalized_profile = dict(profile or {})
    normalized_interval = normalize_schedule_interval_minutes(schedule_interval_minutes)

    with lock:
        with connect() as conn:
            if normalized_monitor_id:
                existing = conn.execute(
                    """
                    SELECT id, schedule_enabled, schedule_interval_minutes,
                           schedule_next_run_at, schedule_paused_at
                    FROM monitor_profiles
                    WHERE tenant_id=? AND monitor_id=?
                    """,
                    (tenant_id, normalized_monitor_id),
                ).fetchone()
                if existing is not None:
                    existing_enabled = bool(existing["schedule_enabled"] or 0)
                    interval_changed = (
                        int(existing["schedule_interval_minutes"] or 60)
                        != normalized_interval
                    )
                    existing_paused_at = str(existing["schedule_paused_at"] or "").strip()
                    if schedule_enabled:
                        if existing_enabled and existing_paused_at:
                            schedule_paused_at = existing_paused_at
                            schedule_next_run_at = ""
                        elif existing_enabled and not interval_changed:
                            schedule_paused_at = ""
                            schedule_next_run_at = (
                                str(existing["schedule_next_run_at"] or "").strip()
                                or compute_next_run_at(
                                    interval_minutes=normalized_interval,
                                    base_time=now,
                                )
                            )
                        else:
                            schedule_paused_at = ""
                            schedule_next_run_at = compute_next_run_at(
                                interval_minutes=normalized_interval,
                                base_time=now,
                            )
                    else:
                        schedule_paused_at = ""
                        schedule_next_run_at = ""
                    conn.execute(
                        """
                        UPDATE monitor_profiles
                        SET name=?, url=?, schema_name=?, storage_format=?, use_static=?,
                            selected_fields_json=?, field_labels_json=?, profile_json=?,
                            schedule_enabled=?, schedule_interval_minutes=?,
                            schedule_next_run_at=?, schedule_paused_at=?,
                            schedule_claimed_by='', schedule_claimed_at='',
                            schedule_lease_until='', updated_at=?
                        WHERE tenant_id=? AND monitor_id=?
                        """,
                        (
                            name,
                            url,
                            schema_name,
                            storage_format,
                            1 if use_static else 0,
                            json.dumps(selected_fields, ensure_ascii=False),
                            json.dumps(field_labels, ensure_ascii=False),
                            json.dumps(normalized_profile, ensure_ascii=False),
                            1 if schedule_enabled else 0,
                            normalized_interval,
                            schedule_next_run_at,
                            schedule_paused_at,
                            now,
                            tenant_id,
                            normalized_monitor_id,
                        ),
                    )
                else:
                    schedule_next_run_at = (
                        compute_next_run_at(
                            interval_minutes=normalized_interval,
                            base_time=now,
                        )
                        if schedule_enabled
                        else ""
                    )
                    conn.execute(
                        """
                        INSERT INTO monitor_profiles (
                            monitor_id, tenant_id, name, url, schema_name, storage_format, use_static,
                            selected_fields_json, field_labels_json, profile_json,
                            created_at, updated_at, schedule_enabled,
                            schedule_interval_minutes, schedule_next_run_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            normalized_monitor_id,
                            tenant_id,
                            name,
                            url,
                            schema_name,
                            storage_format,
                            1 if use_static else 0,
                            json.dumps(selected_fields, ensure_ascii=False),
                            json.dumps(field_labels, ensure_ascii=False),
                            json.dumps(normalized_profile, ensure_ascii=False),
                            now,
                            now,
                            1 if schedule_enabled else 0,
                            normalized_interval,
                            schedule_next_run_at,
                        ),
                    )
            else:
                schedule_next_run_at = (
                    compute_next_run_at(
                        interval_minutes=normalized_interval,
                        base_time=now,
                    )
                    if schedule_enabled
                    else ""
                )
                row_id = _insert_monitor_row(
                    conn,
                    (
                        "",
                        tenant_id,
                        name,
                        url,
                        schema_name,
                        storage_format,
                        1 if use_static else 0,
                        json.dumps(selected_fields, ensure_ascii=False),
                        json.dumps(field_labels, ensure_ascii=False),
                        json.dumps(normalized_profile, ensure_ascii=False),
                        now,
                        now,
                        1 if schedule_enabled else 0,
                        normalized_interval,
                        schedule_next_run_at,
                    ),
                )
                normalized_monitor_id = f"mon-{int(row_id):06d}"
                conn.execute(
                    "UPDATE monitor_profiles SET monitor_id=? WHERE id=?",
                    (normalized_monitor_id, row_id),
                )
            conn.commit()

    return normalized_monitor_id


def fetch_monitor(
    *,
    connect: ConnectionFactory,
    monitor_id: str,
    tenant_id: str = "default",
) -> MonitorRecord | None:
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM monitor_profiles WHERE tenant_id=? AND monitor_id=?",
            (tenant_id, monitor_id),
        ).fetchone()
    if row is None:
        return None
    return MonitorRecord.from_row(row)


def fetch_monitors(
    *,
    connect: ConnectionFactory,
    limit: int = 20,
    tenant_id: str = "default",
) -> list[MonitorRecord]:
    with connect() as conn:
        if _all_tenants(tenant_id):
            rows = conn.execute(
                "SELECT * FROM monitor_profiles ORDER BY updated_at DESC, id DESC LIMIT ?",
                (_normalize_limit(limit, default=20),),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM monitor_profiles WHERE tenant_id=? ORDER BY updated_at DESC, id DESC LIMIT ?",
                (tenant_id, _normalize_limit(limit, default=20)),
            ).fetchall()
    return [MonitorRecord.from_row(row) for row in rows]


def fetch_due_monitors(
    *,
    connect: ConnectionFactory,
    due_before: str,
    limit: int = 5,
    tenant_id: str = "default",
) -> list[MonitorRecord]:
    normalized_due_before = str(due_before or "").strip()
    if not normalized_due_before:
        return []

    due_monitors: list[MonitorRecord] = []
    with connect() as conn:
        if _all_tenants(tenant_id):
            rows = conn.execute(
                """
                SELECT *
                FROM monitor_profiles
                WHERE schedule_enabled=1
                  AND schedule_paused_at=''
                  AND schedule_next_run_at<>''
                  AND schedule_next_run_at<=?
                  AND (
                        schedule_claimed_by=''
                     OR schedule_lease_until=''
                     OR schedule_lease_until<=?
                  )
                ORDER BY schedule_next_run_at ASC, updated_at ASC, id ASC
                LIMIT ?
                """,
                (
                    normalized_due_before,
                    normalized_due_before,
                    _normalize_limit(limit),
                ),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT *
                FROM monitor_profiles
                WHERE tenant_id=?
                  AND schedule_enabled=1
                  AND schedule_paused_at=''
                  AND schedule_next_run_at<>''
                  AND schedule_next_run_at<=?
                  AND (
                        schedule_claimed_by=''
                     OR schedule_lease_until=''
                     OR schedule_lease_until<=?
                  )
                ORDER BY schedule_next_run_at ASC, updated_at ASC, id ASC
                LIMIT ?
                """,
                (
                    tenant_id,
                    normalized_due_before,
                    normalized_due_before,
                    _normalize_limit(limit),
                ),
            ).fetchall()
        for row in rows:
            row_tenant_id = str(row["tenant_id"] or "").strip() or "default"
            if _has_active_monitor_task(
                conn,
                row["last_task_id"],
                tenant_id=row_tenant_id,
            ):
                continue
            due_monitors.append(MonitorRecord.from_row(row))
    return due_monitors


def claim_due_monitors(
    *,
    lock: Any,
    connect: ConnectionFactory,
    due_before: str,
    claimer_id: str,
    lease_seconds: float = 120.0,
    limit: int = 5,
    tenant_id: str = "default",
) -> list[MonitorRecord]:
    return claim_due_monitors_batch(
        lock=lock,
        connect=connect,
        due_before=due_before,
        claimer_id=claimer_id,
        lease_seconds=lease_seconds,
        limit=limit,
        tenant_id=tenant_id,
    )["monitors"]


def claim_due_monitors_batch(
    *,
    lock: Any,
    connect: ConnectionFactory,
    due_before: str,
    claimer_id: str,
    lease_seconds: float = 120.0,
    limit: int = 5,
    tenant_id: str = "default",
) -> dict[str, Any]:
    normalized_due_before = str(due_before or "").strip()
    normalized_claimer_id = str(claimer_id or "").strip()
    if not normalized_due_before or not normalized_claimer_id:
        return {
            "monitors": [],
            "claimed_count": 0,
            "reclaimed_count": 0,
            "skipped_active_task_count": 0,
        }

    claim_limit = _normalize_limit(limit)
    now_dt = datetime.now()
    now = current_timestamp(now_dt)
    lease_until = _build_lease_until(now=now_dt, lease_seconds=lease_seconds)
    claimed_monitors: list[MonitorRecord] = []
    reclaimed_count = 0
    skipped_active_task_count = 0

    with lock:
        with connect() as conn:
            conn.begin_immediate()
            if _all_tenants(tenant_id):
                rows = conn.execute(
                    """
                    SELECT *
                    FROM monitor_profiles
                    WHERE schedule_enabled=1
                      AND schedule_paused_at=''
                      AND schedule_next_run_at<>''
                      AND schedule_next_run_at<=?
                      AND (
                            schedule_claimed_by=''
                         OR schedule_lease_until=''
                         OR schedule_lease_until<=?
                      )
                    ORDER BY schedule_next_run_at ASC, updated_at ASC, id ASC
                    LIMIT ?
                    """,
                    (
                        normalized_due_before,
                        normalized_due_before,
                        claim_limit * 4,
                    ),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM monitor_profiles
                    WHERE tenant_id=?
                      AND schedule_enabled=1
                      AND schedule_paused_at=''
                      AND schedule_next_run_at<>''
                      AND schedule_next_run_at<=?
                      AND (
                            schedule_claimed_by=''
                         OR schedule_lease_until=''
                         OR schedule_lease_until<=?
                      )
                    ORDER BY schedule_next_run_at ASC, updated_at ASC, id ASC
                    LIMIT ?
                    """,
                    (
                        tenant_id,
                        normalized_due_before,
                        normalized_due_before,
                        claim_limit * 4,
                    ),
                ).fetchall()

            for row in rows:
                if len(claimed_monitors) >= claim_limit:
                    break
                row_tenant_id = str(row["tenant_id"] or "").strip() or "default"
                if _has_active_monitor_task(
                    conn,
                    row["last_task_id"],
                    tenant_id=row_tenant_id,
                ):
                    skipped_active_task_count += 1
                    continue

                was_previously_claimed = bool(str(row["schedule_claimed_by"] or "").strip())
                updated = conn.execute(
                    """
                    UPDATE monitor_profiles
                    SET schedule_claimed_by=?,
                        schedule_claimed_at=?,
                        schedule_lease_until=?,
                        schedule_claim_count=schedule_claim_count+1,
                        updated_at=?
                    WHERE tenant_id=?
                      AND monitor_id=?
                      AND schedule_enabled=1
                      AND schedule_paused_at=''
                      AND schedule_next_run_at<>''
                      AND schedule_next_run_at<=?
                      AND (
                            schedule_claimed_by=''
                         OR schedule_lease_until=''
                         OR schedule_lease_until<=?
                      )
                    """,
                    (
                        normalized_claimer_id,
                        now,
                        lease_until,
                        now,
                        row_tenant_id,
                        row["monitor_id"],
                        normalized_due_before,
                        normalized_due_before,
                    ),
                ).rowcount
                if updated <= 0:
                    continue
                if was_previously_claimed:
                    reclaimed_count += 1
                claimed_row = conn.execute(
                    "SELECT * FROM monitor_profiles WHERE tenant_id=? AND monitor_id=?",
                    (row_tenant_id, row["monitor_id"]),
                ).fetchone()
                if claimed_row is None:
                    continue
                claimed_monitors.append(MonitorRecord.from_row(claimed_row))
            conn.commit()

    return {
        "monitors": claimed_monitors,
        "claimed_count": len(claimed_monitors),
        "reclaimed_count": reclaimed_count,
        "skipped_active_task_count": skipped_active_task_count,
    }


def persist_monitor_result(
    *,
    lock: Any,
    connect: ConnectionFactory,
    monitor_id: str,
    task_id: str,
    task_status: str,
    alert_level: str,
    alert_message: str,
    changed_fields: list[dict[str, Any]],
    extraction_strategy: str,
    learned_profile_id: str,
    tenant_id: str = "default",
) -> None:
    now = current_timestamp()
    with lock:
        with connect() as conn:
            conn.execute(
                """
                UPDATE monitor_profiles
                SET last_task_id=?, last_checked_at=?, last_status=?, last_alert_level=?,
                    last_alert_message=?, last_changed_fields_json=?, updated_at=?,
                    last_extraction_strategy=?, last_learned_profile_id=?
                WHERE tenant_id=? AND monitor_id=?
                """,
                (
                    task_id,
                    now,
                    task_status,
                    alert_level,
                    alert_message,
                    json.dumps(changed_fields, ensure_ascii=False),
                    now,
                    extraction_strategy,
                    learned_profile_id,
                    tenant_id,
                    monitor_id,
                ),
            )
            conn.commit()


def persist_monitor_notification(
    *,
    lock: Any,
    connect: ConnectionFactory,
    monitor_id: str,
    status: str,
    message: str,
    tenant_id: str = "default",
) -> None:
    now = current_timestamp()
    with lock:
        with connect() as conn:
            conn.execute(
                """
                UPDATE monitor_profiles
                SET last_notification_status=?, last_notification_message=?, last_notification_at=?, updated_at=?
                WHERE tenant_id=? AND monitor_id=?
                """,
                (
                    str(status or "").strip(),
                    str(message or "").strip(),
                    now,
                    now,
                    tenant_id,
                    monitor_id,
                ),
            )
            conn.commit()


def mark_monitor_run_scheduled(
    *,
    lock: Any,
    connect: ConnectionFactory,
    monitor_id: str,
    task_id: str,
    trigger_source: str,
    claimed_by: str = "",
    tenant_id: str = "default",
) -> None:
    now = current_timestamp()
    normalized_trigger_source = str(trigger_source or "").strip().lower() or "manual"
    normalized_claimed_by = str(claimed_by or "").strip()
    with lock:
        with connect() as conn:
            row = conn.execute(
                """
                SELECT schedule_enabled, schedule_interval_minutes, schedule_paused_at
                FROM monitor_profiles
                WHERE tenant_id=? AND monitor_id=?
                """,
                (tenant_id, monitor_id),
            ).fetchone()
            if row is None:
                return

            schedule_enabled = bool(row["schedule_enabled"] or 0)
            schedule_paused_at = str(row["schedule_paused_at"] or "").strip()
            schedule_next_run_at = ""
            if schedule_enabled and not schedule_paused_at:
                schedule_next_run_at = compute_next_run_at(
                    interval_minutes=int(row["schedule_interval_minutes"] or 60),
                    base_time=now,
                )

            if normalized_claimed_by:
                conn.execute(
                    """
                    UPDATE monitor_profiles
                    SET last_task_id=?, last_trigger_source=?, schedule_last_run_at=?,
                        schedule_next_run_at=?, schedule_claimed_by='',
                        schedule_claimed_at='', schedule_lease_until='',
                        schedule_last_error='', updated_at=?
                    WHERE tenant_id=? AND monitor_id=?
                      AND (schedule_claimed_by='' OR schedule_claimed_by=?)
                    """,
                    (
                        str(task_id or "").strip(),
                        normalized_trigger_source,
                        now,
                        schedule_next_run_at,
                        now,
                        tenant_id,
                        monitor_id,
                        normalized_claimed_by,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE monitor_profiles
                    SET last_task_id=?, last_trigger_source=?, schedule_last_run_at=?,
                        schedule_next_run_at=?, schedule_claimed_by='',
                        schedule_claimed_at='', schedule_lease_until='',
                        schedule_last_error='', updated_at=?
                    WHERE tenant_id=? AND monitor_id=?
                    """,
                    (
                        str(task_id or "").strip(),
                        normalized_trigger_source,
                        now,
                        schedule_next_run_at,
                        now,
                        tenant_id,
                        monitor_id,
                    ),
                )
            conn.commit()


def fail_monitor_claim(
    *,
    lock: Any,
    connect: ConnectionFactory,
    monitor_id: str,
    error: str,
    claimed_by: str = "",
    tenant_id: str = "default",
) -> None:
    now = current_timestamp()
    normalized_claimed_by = str(claimed_by or "").strip()
    normalized_error = str(error or "").strip()
    with lock:
        with connect() as conn:
            if normalized_claimed_by:
                conn.execute(
                    """
                    UPDATE monitor_profiles
                    SET schedule_last_error=?, updated_at=?
                    WHERE tenant_id=? AND monitor_id=? AND schedule_claimed_by=?
                    """,
                    (
                        normalized_error,
                        now,
                        tenant_id,
                        monitor_id,
                        normalized_claimed_by,
                    ),
                )
            else:
                conn.execute(
                    """
                    UPDATE monitor_profiles
                    SET schedule_last_error=?, updated_at=?
                    WHERE tenant_id=? AND monitor_id=?
                    """,
                    (
                        normalized_error,
                        now,
                        tenant_id,
                        monitor_id,
                    ),
                )
            conn.commit()


def pause_monitor_schedule(
    *,
    lock: Any,
    connect: ConnectionFactory,
    monitor_id: str,
    tenant_id: str = "default",
) -> None:
    now = current_timestamp()
    with lock:
        with connect() as conn:
            conn.execute(
                """
                UPDATE monitor_profiles
                SET schedule_paused_at=?,
                    schedule_next_run_at='',
                    schedule_claimed_by='',
                    schedule_claimed_at='',
                    schedule_lease_until='',
                    updated_at=?
                WHERE tenant_id=? AND monitor_id=? AND schedule_enabled=1
                """,
                (
                    now,
                    now,
                    tenant_id,
                    monitor_id,
                ),
            )
            conn.commit()


def resume_monitor_schedule(
    *,
    lock: Any,
    connect: ConnectionFactory,
    monitor_id: str,
    tenant_id: str = "default",
) -> None:
    now = current_timestamp()
    with lock:
        with connect() as conn:
            row = conn.execute(
                """
                SELECT schedule_enabled, schedule_interval_minutes
                FROM monitor_profiles
                WHERE tenant_id=? AND monitor_id=?
                """,
                (tenant_id, monitor_id),
            ).fetchone()
            if row is None or not bool(row["schedule_enabled"] or 0):
                return

            next_run_at = compute_next_run_at(
                interval_minutes=int(row["schedule_interval_minutes"] or 60),
                base_time=now,
            )
            conn.execute(
                """
                UPDATE monitor_profiles
                SET schedule_paused_at='',
                    schedule_next_run_at=?,
                    schedule_claimed_by='',
                    schedule_claimed_at='',
                    schedule_lease_until='',
                    updated_at=?
                WHERE tenant_id=? AND monitor_id=?
                """,
                (
                    next_run_at,
                    now,
                    tenant_id,
                    monitor_id,
                ),
            )
            conn.commit()
