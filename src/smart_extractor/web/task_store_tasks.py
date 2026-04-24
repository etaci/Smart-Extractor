"""SQLiteTaskStore 的任务 CRUD 与状态更新辅助函数。"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Callable

from smart_extractor.web.task_models import TaskRecord

ConnectionFactory = Callable[[], sqlite3.Connection]


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
) -> str:
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with lock:
        with connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO web_tasks (
                    task_id, request_id, url, schema_name, storage_format, batch_group_id,
                    task_kind, parent_task_id, total_items, completed_items, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "",
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
            row_id = cursor.lastrowid
            task_id = f"task-{row_id:06d}"
            conn.execute("UPDATE web_tasks SET task_id=? WHERE id=?", (task_id, row_id))
            conn.commit()
    return task_id


def fetch_task(*, connect: ConnectionFactory, task_id: str) -> TaskRecord | None:
    with connect() as conn:
        row = conn.execute("SELECT * FROM web_tasks WHERE task_id=?", (task_id,)).fetchone()
    if row is None:
        return None
    return TaskRecord.from_row(row)


def fetch_root_tasks(
    *,
    connect: ConnectionFactory,
    limit: int = 50,
    batch_group_id: str = "",
) -> list[TaskRecord]:
    with connect() as conn:
        if batch_group_id:
            rows = conn.execute(
                "SELECT * FROM web_tasks WHERE batch_group_id=? AND parent_task_id='' ORDER BY id DESC LIMIT ?",
                (batch_group_id, int(limit)),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM web_tasks WHERE parent_task_id='' ORDER BY id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
    return [TaskRecord.from_row(row) for row in rows]


def fetch_child_tasks(*, connect: ConnectionFactory, parent_task_id: str) -> list[TaskRecord]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM web_tasks WHERE parent_task_id=? ORDER BY id ASC",
            (parent_task_id,),
        ).fetchall()
    return [TaskRecord.from_row(row) for row in rows]


def fetch_task_stats(*, connect: ConnectionFactory) -> dict[str, Any]:
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
            WHERE parent_task_id=''
            """
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
) -> None:
    if not fields:
        return

    invalid_fields = [key for key in fields if key not in allowed_fields]
    if invalid_fields:
        raise ValueError(f"invalid task fields: {invalid_fields}")

    set_clause = ", ".join(f"{key}=?" for key in fields.keys())
    values = list(fields.values()) + [task_id]
    with lock:
        with connect() as conn:
            conn.execute(
                f"UPDATE web_tasks SET {set_clause} WHERE task_id=?",  # nosec B608
                values,
            )
            conn.commit()
