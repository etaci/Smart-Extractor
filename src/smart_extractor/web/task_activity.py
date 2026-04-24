"""Batch/task activity helpers for SQLiteTaskStore."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Callable

from smart_extractor.web.task_insights import extract_domain
from smart_extractor.web.task_models import MonitorRecord, TaskRecord


def build_initial_batch_payload(urls: list[str]) -> dict[str, Any]:
    normalized_urls = [str(url or "").strip() for url in urls if str(url or "").strip()]
    return {
        "task_type": "batch",
        "urls": normalized_urls,
        "summary": {
            "total": len(normalized_urls),
            "completed": 0,
            "success": 0,
            "failed": 0,
            "running": 0,
            "pending": len(normalized_urls),
        },
    }


def summarize_batch_children(children: list[TaskRecord]) -> dict[str, Any]:
    total = len(children)
    success_count = sum(1 for item in children if item.status == "success")
    failed_count = sum(1 for item in children if item.status == "failed")
    running_count = sum(1 for item in children if item.status == "running")
    queued_count = sum(1 for item in children if item.status == "queued")
    completed_count = success_count + failed_count
    pending_count = max(total - completed_count - running_count - queued_count, 0)
    percent = round((completed_count / max(total, 1)) * 100, 1)
    positive_quality_items = [item.quality_score for item in children if item.quality_score > 0]
    avg_quality = (
        sum(positive_quality_items) / max(len(positive_quality_items), 1)
        if positive_quality_items
        else 0.0
    )
    total_elapsed = sum(float(item.elapsed_ms or 0.0) for item in children)
    urls = [item.url for item in children]

    if completed_count >= total:
        status = "failed" if failed_count > 0 else "success"
        stage = (
            f"批量任务完成（成功 {success_count} / 失败 {failed_count}）"
            if failed_count > 0
            else f"批量任务完成（共 {total} 个 URL）"
        )
    elif running_count > 0 or completed_count > 0:
        status = "running"
        stage = f"批量任务进行中（已完成 {completed_count}/{total}）"
        percent = max(percent, 6.0)
    elif queued_count > 0:
        status = "queued"
        stage = f"批量任务已入队，等待 worker 处理（共 {total} 个 URL）"
        percent = max(percent, 2.0)
    else:
        status = "pending"
        stage = f"批量任务已创建，等待执行（共 {total} 个 URL）"
        percent = max(percent, 2.0)

    error_message = ""
    if failed_count > 0:
        failed_examples = [
            item.error for item in children if item.status == "failed" and item.error
        ]
        if failed_examples:
            error_message = (
                f"批量任务中有 {failed_count} 个 URL 失败；最近错误：{failed_examples[-1]}"
            )

    return {
        "status": status,
        "completed_at": (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if completed_count >= total
            else ""
        ),
        "elapsed_ms": total_elapsed,
        "quality_score": avg_quality,
        "progress_percent": percent,
        "progress_stage": stage,
        "error": error_message,
        "data_json_payload": {
            "task_type": "batch",
            "urls": urls,
            "summary": {
                "total": total,
                "completed": completed_count,
                "success": success_count,
                "failed": failed_count,
                "running": running_count,
                "queued": queued_count,
                "pending": pending_count,
            },
        },
        "completed_items": completed_count,
        "total_items": total,
    }


def build_learned_profile_activity_payload(
    *,
    tasks: list[TaskRecord],
    monitors: list[MonitorRecord],
    compare_with_previous: Callable[[TaskRecord], dict[str, Any]],
) -> dict[str, Any]:
    strategy_breakdown: dict[str, int] = defaultdict(int)
    recent_hits: list[dict[str, Any]] = []
    for task in tasks:
        task_data = task.data if isinstance(task.data, dict) else {}
        strategy = str(task_data.get("extraction_strategy") or "").strip() or "unknown"
        strategy_breakdown[strategy] += 1
        recent_hits.append(
            {
                "task_id": task.task_id,
                "url": task.url,
                "domain": extract_domain(task.url),
                "status": task.status,
                "created_at": task.created_at,
                "completed_at": task.completed_at,
                "quality_score": task.quality_score,
                "elapsed_ms": task.elapsed_ms,
                "extraction_strategy": strategy,
                "changed": compare_with_previous(task).get("changed", False)
                if task.status == "success"
                else False,
            }
        )

    related_monitors = [
        {
            "monitor_id": item.monitor_id,
            "name": item.name,
            "url": item.url,
            "domain": extract_domain(item.url),
            "profile": item.profile if isinstance(item.profile, dict) else {},
            "last_extraction_strategy": item.last_extraction_strategy,
            "last_notification_status": item.last_notification_status,
        }
        for item in monitors
    ]
    return {
        "recent_hits": recent_hits,
        "related_monitors": related_monitors,
        "summary": {
            "task_hits": len(tasks),
            "monitor_links": len(monitors),
            "rule_hits": strategy_breakdown.get("rule", 0),
            "llm_hits": strategy_breakdown.get("llm", 0),
            "fallback_hits": strategy_breakdown.get("fallback", 0),
            "changed_hits": sum(1 for item in recent_hits if item.get("changed")),
        },
    }
