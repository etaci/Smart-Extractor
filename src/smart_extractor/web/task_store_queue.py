"""队列化调度的 SQLite 持久化辅助函数。"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Callable

ConnectionFactory = Callable[[], sqlite3.Connection]


def enqueue_task_payload(
    *,
    lock: Any,
    connect: ConnectionFactory,
    task_id: str,
    payload: dict[str, Any],
) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload_json = json.dumps(payload, ensure_ascii=False)
    with lock:
        with connect() as conn:
            conn.execute(
                """
                INSERT INTO web_task_dispatch_queue (
                    task_id, payload_json, status, created_at, updated_at, claimed_at, worker_id, last_error
                ) VALUES (?, ?, 'queued', ?, ?, '', '', '')
                ON CONFLICT(task_id) DO UPDATE SET
                    payload_json=excluded.payload_json,
                    status='queued',
                    updated_at=excluded.updated_at,
                    claimed_at='',
                    worker_id='',
                    last_error=''
                """,
                (task_id, payload_json, now, now),
            )
            conn.commit()


def claim_queued_task_payload(
    *,
    lock: Any,
    connect: ConnectionFactory,
    worker_id: str,
    stale_after_seconds: float = 0.0,
) -> dict[str, Any] | None:
    now = datetime.now()
    now_text = now.strftime("%Y-%m-%d %H:%M:%S")
    stale_before = (
        (now - timedelta(seconds=max(float(stale_after_seconds), 0.0))).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        if stale_after_seconds > 0
        else ""
    )

    with lock:
        with connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if stale_before:
                row = conn.execute(
                    """
                    SELECT * FROM web_task_dispatch_queue
                    WHERE status='queued' OR (status='running' AND claimed_at<>'' AND claimed_at<=?)
                    ORDER BY
                        CASE WHEN status='queued' THEN 0 ELSE 1 END,
                        id ASC
                    LIMIT 1
                    """,
                    (stale_before,),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT * FROM web_task_dispatch_queue
                    WHERE status='queued'
                    ORDER BY id ASC
                    LIMIT 1
                    """
                ).fetchone()

            if row is None:
                conn.commit()
                return None

            if stale_before:
                conn.execute(
                    """
                    UPDATE web_task_dispatch_queue
                    SET status='running', claimed_at=?, updated_at=?, worker_id=?, last_error=''
                    WHERE task_id=? AND (status='queued' OR (status='running' AND claimed_at<>'' AND claimed_at<=?))
                    """,
                    (now_text, now_text, worker_id, row["task_id"], stale_before),
                )
            else:
                conn.execute(
                    """
                    UPDATE web_task_dispatch_queue
                    SET status='running', claimed_at=?, updated_at=?, worker_id=?, last_error=''
                    WHERE task_id=? AND status='queued'
                    """,
                    (now_text, now_text, worker_id, row["task_id"]),
                )

            if conn.total_changes <= 0:
                conn.commit()
                return None

            claimed_row = conn.execute(
                "SELECT * FROM web_task_dispatch_queue WHERE task_id=?",
                (row["task_id"],),
            ).fetchone()
            conn.commit()

    if claimed_row is None:
        return None
    payload = json.loads(claimed_row["payload_json"] or "{}")
    return {
        "task_id": claimed_row["task_id"],
        "payload": payload if isinstance(payload, dict) else {},
        "status": claimed_row["status"] or "",
        "worker_id": claimed_row["worker_id"] or "",
    }


def complete_task_payload(
    *,
    lock: Any,
    connect: ConnectionFactory,
    task_id: str,
) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with lock:
        with connect() as conn:
            conn.execute(
                """
                UPDATE web_task_dispatch_queue
                SET status='done', updated_at=?, last_error=''
                WHERE task_id=?
                """,
                (now, task_id),
            )
            conn.commit()


def fail_task_payload(
    *,
    lock: Any,
    connect: ConnectionFactory,
    task_id: str,
    error: str,
) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with lock:
        with connect() as conn:
            conn.execute(
                """
                UPDATE web_task_dispatch_queue
                SET status='failed', updated_at=?, last_error=?
                WHERE task_id=?
                """,
                (now, str(error or "").strip(), task_id),
            )
            conn.commit()
