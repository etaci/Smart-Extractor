"""SQLiteTaskStore 运行态字段更新辅助函数。"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from smart_extractor.web.task_activity import summarize_batch_children
from smart_extractor.web.task_models import TaskRecord


def build_queued_fields() -> dict[str, Any]:
    return {
        "status": "queued",
        "error": "",
        "completed_at": "",
        "progress_percent": 2.0,
        "progress_stage": "任务已进入队列，等待 worker 处理",
    }


def build_running_fields() -> dict[str, Any]:
    return {
        "status": "running",
        "error": "",
        "completed_at": "",
        "progress_percent": 6.0,
        "progress_stage": "任务已开始，正在准备抓取页面",
    }


def build_progress_fields(
    *,
    progress_percent: float,
    progress_stage: str,
) -> dict[str, Any]:
    return {
        "status": "running",
        "progress_percent": max(0.0, min(100.0, float(progress_percent))),
        "progress_stage": str(progress_stage or "").strip(),
    }


def build_success_fields(
    *,
    elapsed_ms: float,
    quality_score: float,
    data: Optional[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "status": "success",
        "elapsed_ms": float(elapsed_ms),
        "quality_score": float(quality_score),
        "progress_percent": 100.0,
        "progress_stage": "网页提取完成",
        "data_json": json.dumps(data, ensure_ascii=False) if data is not None else "",
        "error": "",
        "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def build_failed_fields(*, elapsed_ms: float, error: str) -> dict[str, Any]:
    return {
        "status": "failed",
        "elapsed_ms": float(elapsed_ms),
        "progress_percent": 100.0,
        "progress_stage": "网页提取失败",
        "error": str(error or "").strip(),
        "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def build_parent_refresh_fields(
    *,
    task: TaskRecord | None,
    children: list[TaskRecord],
) -> tuple[str, dict[str, Any]] | None:
    if task is None or not task.parent_task_id or not children:
        return None

    summary = summarize_batch_children(children)
    return (
        task.parent_task_id,
        {
            "status": summary["status"],
            "completed_at": summary["completed_at"],
            "elapsed_ms": summary["elapsed_ms"],
            "quality_score": summary["quality_score"],
            "progress_percent": summary["progress_percent"],
            "progress_stage": summary["progress_stage"],
            "error": summary["error"],
            "data_json": json.dumps(summary["data_json_payload"], ensure_ascii=False),
            "completed_items": summary["completed_items"],
            "total_items": summary["total_items"],
        },
    )
