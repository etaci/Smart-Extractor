"""Task CRUD helpers for the task store."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from smart_extractor.web.task_models import TaskRecord

ConnectionFactory = Callable[[], object]


def _insert_task_row(conn, values: tuple[Any, ...]) -> int:
    if getattr(conn, "dialect", "sqlite") == "postgres":
        row = conn.execute(
            """
            INSERT INTO web_tasks (
                task_id, tenant_id, request_id, url, schema_name, storage_format, batch_group_id,
                task_kind, parent_task_id, total_items, completed_items, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            values,
        ).fetchone()
        return int(row["id"] or 0)
    cursor = conn.execute(
        """
        INSERT INTO web_tasks (
            task_id, tenant_id, request_id, url, schema_name, storage_format, batch_group_id,
            task_kind, parent_task_id, total_items, completed_items, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        values,
    )
    return int(cursor.lastrowid or 0)


def create_task(
    *,
    lock: Any,
    connect: ConnectionFactory,
    url: str,
    schema_name: str,
    storage_format: str,
    request_id: str = "-",
    batch_group_id: str = "",
    task_kind: str = "single",
    parent_task_id: str = "",
    total_items: int = 0,
    tenant_id: str = "default",
) -> str:
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with lock:
        with connect() as conn:
            row_id = _insert_task_row(
                conn,
                (
                    "",
                    tenant_id,
                    request_id,
                    url,
                    schema_name,
                    storage_format,
                    batch_group_id,
                    task_kind,
                    parent_task_id,
                    max(0, int(total_items)),
                    0,
                    "pending",
                    created_at,
                ),
            )
            task_id = f"task-{row_id:06d}"
            conn.execute(
                "UPDATE web_tasks SET task_id=? WHERE id=?",
                (task_id, row_id),
            )
            conn.commit()
    return task_id


def fetch_task(
    *,
    connect: ConnectionFactory,
    task_id: str,
    tenant_id: str = "default",
) -> TaskRecord | None:
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM web_tasks WHERE tenant_id=? AND task_id=?",
            (tenant_id, task_id),
        ).fetchone()
    if row is None:
        return None
    return TaskRecord.from_row(row)


def fetch_root_tasks(
    *,
    connect: ConnectionFactory,
    limit: int = 50,
    batch_group_id: str = "",
    tenant_id: str = "default",
) -> list[TaskRecord]:
    with connect() as conn:
        if batch_group_id:
            rows = conn.execute(
                "SELECT * FROM web_tasks WHERE tenant_id=? AND batch_group_id=? AND parent_task_id='' ORDER BY id DESC LIMIT ?",
                (tenant_id, batch_group_id, int(limit)),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM web_tasks WHERE tenant_id=? AND parent_task_id='' ORDER BY id DESC LIMIT ?",
                (tenant_id, int(limit)),
            ).fetchall()
    return [TaskRecord.from_row(row) for row in rows]


def fetch_child_tasks(
    *,
    connect: ConnectionFactory,
    parent_task_id: str,
    tenant_id: str = "default",
) -> list[TaskRecord]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM web_tasks WHERE tenant_id=? AND parent_task_id=? ORDER BY id ASC",
            (tenant_id, parent_task_id),
        ).fetchall()
    return [TaskRecord.from_row(row) for row in rows]


def fetch_task_stats(
    *,
    connect: ConnectionFactory,
    tenant_id: str = "default",
) -> dict[str, Any]:
    with connect() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS success,
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running,
                SUM(CASE WHEN status IN ('pending', 'queued') THEN 1 ELSE 0 END) AS pending
            FROM web_tasks
            WHERE tenant_id=? AND parent_task_id=''
            """,
            (tenant_id,),
        ).fetchone()

    total = int(row["total"] or 0)
    success = int(row["success"] or 0)
    failed = int(row["failed"] or 0)
    running = int(row["running"] or 0)
    pending = int(row["pending"] or 0)
    return {
        "total": total,
        "success": success,
        "failed": failed,
        "running": running,
        "pending": pending,
        "success_rate": f"{success / max(total, 1):.1%}",
    }


def update_task_fields(
    *,
    lock: Any,
    connect: ConnectionFactory,
    task_id: str,
    allowed_fields: set[str],
    fields: dict[str, Any],
    tenant_id: str = "default",
) -> None:
    if not fields:
        return

    invalid_fields = [key for key in fields if key not in allowed_fields]
    if invalid_fields:
        raise ValueError(f"invalid task fields: {invalid_fields}")

    set_clause = ", ".join(f"{key}=?" for key in fields.keys())
    values = list(fields.values()) + [tenant_id, task_id]
    with lock:
        with connect() as conn:
            conn.execute(
                f"UPDATE web_tasks SET {set_clause} WHERE tenant_id=? AND task_id=?",  # nosec B608
                values,
            )
            conn.commit()
