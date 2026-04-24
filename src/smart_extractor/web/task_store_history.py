"""历史查询与任务详情组装辅助函数。"""

from __future__ import annotations

import sqlite3
from typing import Any, Callable

from smart_extractor.web.management_helpers import (
    serialize_task_batch_child_item,
    serialize_task_history_item,
)
from smart_extractor.web.task_insights import (
    build_comparison_payload,
    build_history_summary_payload,
    build_task_detail_payload as compose_task_detail_payload,
)
from smart_extractor.web.task_models import TaskRecord

ConnectionFactory = Callable[[], sqlite3.Connection]


def fetch_tasks_by_url(
    *,
    connect: ConnectionFactory,
    url: str,
    limit: int = 10,
) -> list[TaskRecord]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM web_tasks WHERE url=? ORDER BY id DESC LIMIT ?",
            (url, int(limit)),
        ).fetchall()
    return [TaskRecord.from_row(row) for row in rows]


def fetch_tasks_by_learned_profile(
    *,
    connect: ConnectionFactory,
    profile_id: str,
    limit: int = 12,
) -> list[TaskRecord]:
    normalized_profile_id = str(profile_id or "").strip()
    if not normalized_profile_id:
        return []

    matched: list[TaskRecord] = []
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM web_tasks WHERE parent_task_id='' ORDER BY id DESC"
        ).fetchall()
    for row in rows:
        task = TaskRecord.from_row(row)
        task_data = task.data if isinstance(task.data, dict) else {}
        if str(task_data.get("learned_profile_id") or "").strip() != normalized_profile_id:
            continue
        matched.append(task)
        if len(matched) >= int(limit):
            break
    return matched


def fetch_previous_success(
    *,
    connect: ConnectionFactory,
    task: TaskRecord,
) -> TaskRecord | None:
    with connect() as conn:
        row = conn.execute(
            """
            SELECT * FROM web_tasks
            WHERE url=? AND status='success' AND id < ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (task.url, int(task.db_id)),
        ).fetchone()
    if row is None:
        return None
    return TaskRecord.from_row(row)


def fetch_history_summary(
    *,
    connect: ConnectionFactory,
    task: TaskRecord,
) -> dict[str, Any]:
    with connect() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_runs,
                SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS success_runs,
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_runs,
                MIN(created_at) AS first_seen_at,
                MAX(CASE WHEN status='success' THEN completed_at ELSE '' END) AS last_success_at
            FROM web_tasks
            WHERE url=?
            """,
            (task.url,),
        ).fetchone()

    previous_success = fetch_previous_success(connect=connect, task=task)
    total_runs = int(row["total_runs"] or 0)
    success_runs = int(row["success_runs"] or 0)
    failed_runs = int(row["failed_runs"] or 0)
    return build_history_summary_payload(
        total_runs=total_runs,
        success_runs=success_runs,
        failed_runs=failed_runs,
        first_seen_at=row["first_seen_at"] or "",
        last_success_at=row["last_success_at"] or "",
        previous_success_task_id=previous_success.task_id if previous_success else "",
    )


def build_comparison_with_previous(
    *,
    connect: ConnectionFactory,
    task: TaskRecord,
) -> dict[str, Any]:
    previous = fetch_previous_success(connect=connect, task=task)
    return build_comparison_payload(task=task, previous=previous)


def build_task_detail_payload(
    *,
    task_id: str,
    history_limit: int,
    get_task: Callable[[str], TaskRecord | None],
    list_children: Callable[[str], list[TaskRecord]],
    list_by_url: Callable[[str, int], list[TaskRecord]],
    get_history_summary: Callable[[TaskRecord], dict[str, Any]],
    compare_with_previous: Callable[[TaskRecord], dict[str, Any]],
) -> dict[str, Any] | None:
    task = get_task(task_id)
    if task is None:
        return None

    return compose_task_detail_payload(
        task=task,
        history_summary=get_history_summary(task),
        comparison=compare_with_previous(task),
        batch_children=[
            serialize_task_batch_child_item(item)
            for item in list_children(task.task_id)
        ]
        if task.task_kind == "batch"
        else [],
        recent_history=[
            serialize_task_history_item(item)
            for item in list_by_url(task.url, limit=history_limit)
        ],
    )
