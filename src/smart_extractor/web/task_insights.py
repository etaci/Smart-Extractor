"""
Web 任务详情、差异分析与仪表盘辅助函数。
"""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from smart_extractor.web.management_helpers import (
    enrich_monitor_payload,
    notification_channels_from_profile,
)
from smart_extractor.web.task_models import MonitorRecord, TaskRecord


def extract_domain(url: str) -> str:
    if str(url or "").startswith("批量任务（"):
        return "batch"
    domain = urlparse(url).netloc.strip().lower()
    return domain or "unknown"


def normalize_task_data(data: Optional[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}

    dynamic_payload = data.get("data")
    if isinstance(dynamic_payload, dict):
        return dynamic_payload

    ignored_keys = {
        "formatted_text",
        "candidate_fields",
        "selected_fields",
        "field_labels",
    }
    return {
        key: value
        for key, value in data.items()
        if key not in ignored_keys and value not in ("", None, [], {})
    }


def build_progress_payload(task: TaskRecord) -> dict[str, Any]:
    stage = (task.progress_stage or "").strip()
    percent = max(0.0, min(100.0, float(task.progress_percent or 0.0)))

    if task.status in {"pending", "queued"}:
        percent = max(percent, 2.0)
        stage = stage or (
            "任务已进入队列，等待 worker 处理"
            if task.status == "queued"
            else "任务已创建，等待执行"
        )
    elif task.status == "running":
        percent = min(max(percent, 8.0), 96.0)
        stage = stage or "正在抓取并提取网页内容"
    elif task.status == "success":
        percent = 100.0
        stage = stage or "网页提取完成"
    elif task.status == "failed":
        percent = 100.0
        stage = stage or "网页提取失败"

    return {
        "percent": round(percent, 1),
        "stage": stage,
        "status": task.status,
    }


def display_value(value: Any) -> str:
    if value in ("", None, [], {}):
        return "空"
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    else:
        text = str(value)
    return text if len(text) <= 72 else f"{text[:69]}..."


def diff_payloads(
    previous_payload: dict[str, Any],
    current_payload: dict[str, Any],
    current_data: Optional[dict[str, Any]],
    previous_data: Optional[dict[str, Any]],
) -> list[dict[str, Any]]:
    label_map: dict[str, str] = {}
    if isinstance(previous_data, dict) and isinstance(previous_data.get("field_labels"), dict):
        label_map.update(previous_data["field_labels"])
    if isinstance(current_data, dict) and isinstance(current_data.get("field_labels"), dict):
        label_map.update(current_data["field_labels"])

    changes: list[dict[str, Any]] = []
    for field_name in sorted(set(previous_payload) | set(current_payload)):
        before = previous_payload.get(field_name)
        after = current_payload.get(field_name)
        if display_value(before) == display_value(after):
            continue

        label = label_map.get(field_name, field_name)
        if field_name not in previous_payload:
            change_type = "added"
            summary = f"{label} 新增为 {display_value(after)}"
        elif field_name not in current_payload:
            change_type = "removed"
            summary = f"{label} 已移除，之前为 {display_value(before)}"
        else:
            change_type = "updated"
            summary = f"{label} 从 {display_value(before)} 变为 {display_value(after)}"

        changes.append(
            {
                "field": field_name,
                "label": label,
                "change_type": change_type,
                "before": before,
                "after": after,
                "before_text": display_value(before),
                "after_text": display_value(after),
                "summary": summary,
            }
        )
    return changes


def build_change_impact_summary(changed_fields: list[dict[str, Any]]) -> str:
    if not changed_fields:
        return "当前与上一条成功记录相比没有字段变化。"

    updated_fields = [
        str(item.get("label") or item.get("field") or "字段")
        for item in changed_fields
        if item.get("change_type") == "updated"
    ]
    added_fields = [
        str(item.get("label") or item.get("field") or "字段")
        for item in changed_fields
        if item.get("change_type") == "added"
    ]
    removed_fields = [
        str(item.get("label") or item.get("field") or "字段")
        for item in changed_fields
        if item.get("change_type") == "removed"
    ]

    segments: list[str] = []
    if updated_fields:
        segments.append(f"主要变动集中在 {'、'.join(updated_fields[:3])}")
    if added_fields:
        segments.append(f"新增字段 {'、'.join(added_fields[:2])}")
    if removed_fields:
        segments.append(f"消失字段 {'、'.join(removed_fields[:2])}")
    return "；".join(segments) or "检测到字段变化，建议进一步核对。"


def build_change_actions(changed_fields: list[dict[str, Any]]) -> list[str]:
    if not changed_fields:
        return [
            "当前页面表现稳定，可继续保留在监控名单中。",
            "如果这是关键业务页面，建议补上通知通道，避免后续变化被忽略。",
        ]

    updated_fields = [
        str(item.get("label") or item.get("field") or "字段")
        for item in changed_fields
        if item.get("change_type") == "updated"
    ]
    actions = [
        "先人工复核变化字段，确认这是业务真实变化而不是页面噪声。",
        "如需长期追踪，建议为该页面配置通知通道，把变化直接推送给业务方。",
    ]
    if updated_fields:
        actions.insert(
            0,
            f"优先核对 {'、'.join(updated_fields[:3])} 这些核心字段是否影响当前判断。",
        )
    if any(item.get("change_type") == "removed" for item in changed_fields):
        actions.append("有字段消失，建议检查目标站点是否改版或字段是否被折叠。")
    return actions[:3]


def build_history_summary_payload(
    *,
    total_runs: int,
    success_runs: int,
    failed_runs: int,
    first_seen_at: str,
    last_success_at: str,
    previous_success_task_id: str,
) -> dict[str, Any]:
    return {
        "total_runs": total_runs,
        "success_runs": success_runs,
        "failed_runs": failed_runs,
        "first_seen_at": first_seen_at,
        "last_success_at": last_success_at,
        "has_history": total_runs > 1,
        "repeat_url": total_runs > 1,
        "previous_success_task_id": previous_success_task_id,
    }


def build_comparison_payload(
    *,
    task: TaskRecord,
    previous: TaskRecord | None,
) -> dict[str, Any]:
    if previous is None:
        return {
            "has_previous": False,
            "changed": False,
            "changed_fields_count": 0,
            "changed_fields": [],
            "summary_lines": [],
            "change_breakdown": {"added": 0, "removed": 0, "updated": 0},
            "impact_summary": "",
            "suggested_actions": [],
            "previous_task_id": "",
            "previous_completed_at": "",
        }

    current_payload = normalize_task_data(task.data)
    previous_payload = normalize_task_data(previous.data)
    changed_fields = diff_payloads(previous_payload, current_payload, task.data, previous.data)
    change_breakdown = {
        "added": sum(1 for item in changed_fields if item.get("change_type") == "added"),
        "removed": sum(1 for item in changed_fields if item.get("change_type") == "removed"),
        "updated": sum(1 for item in changed_fields if item.get("change_type") == "updated"),
    }
    return {
        "has_previous": True,
        "changed": bool(changed_fields),
        "changed_fields_count": len(changed_fields),
        "changed_fields": changed_fields,
        "summary_lines": [item["summary"] for item in changed_fields[:5]],
        "change_breakdown": change_breakdown,
        "impact_summary": build_change_impact_summary(changed_fields),
        "suggested_actions": build_change_actions(changed_fields),
        "previous_task_id": previous.task_id,
        "previous_completed_at": previous.completed_at,
    }


def build_task_detail_payload(
    *,
    task: TaskRecord,
    history_summary: dict[str, Any],
    comparison: dict[str, Any],
    batch_children: list[dict[str, Any]],
    recent_history: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "task_id": task.task_id,
        "url": task.url,
        "storage_format": task.storage_format,
        "status": task.status,
        "created_at": task.created_at,
        "elapsed_ms": task.elapsed_ms,
        "quality_score": task.quality_score,
        "task_kind": task.task_kind,
        "total_items": task.total_items,
        "completed_items": task.completed_items,
        "data": task.data,
        "error": task.error,
        "domain": extract_domain(task.url),
        "progress": build_progress_payload(task),
        "history_summary": history_summary,
        "comparison": comparison,
        "batch_children": batch_children,
        "recent_history": recent_history,
    }


def build_dashboard_insights_payload(
    *,
    recent_tasks: list[TaskRecord],
    monitors: list[MonitorRecord],
    compare_with_previous: Callable[[TaskRecord], dict[str, Any]],
) -> dict[str, Any]:
    grouped_by_url: dict[str, list[TaskRecord]] = defaultdict(list)
    grouped_by_domain: dict[str, list[TaskRecord]] = defaultdict(list)
    recent_changes: list[dict[str, Any]] = []
    rule_based_tasks = 0
    fallback_tasks = 0
    learned_profile_hits = 0
    llm_total_calls = 0
    llm_prompt_tokens = 0
    llm_completion_tokens = 0
    llm_total_tokens = 0
    llm_estimated_cost_usd = 0.0
    llm_cost_samples = 0
    llm_api_usage_calls = 0
    llm_estimated_usage_calls = 0

    for task in recent_tasks:
        grouped_by_url[task.url].append(task)
        grouped_by_domain[extract_domain(task.url)].append(task)
        if isinstance(task.data, dict):
            strategy = str(task.data.get("extraction_strategy") or "").strip()
            if strategy == "rule":
                rule_based_tasks += 1
            elif strategy == "fallback":
                fallback_tasks += 1
            if str(task.data.get("learned_profile_id") or "").strip():
                learned_profile_hits += 1
            extractor_stats = (
                task.data.get("_extractor_stats")
                if isinstance(task.data.get("_extractor_stats"), dict)
                else {}
            )
            llm_total_calls += int(extractor_stats.get("total_calls", 0) or 0)
            llm_prompt_tokens += int(extractor_stats.get("prompt_tokens", 0) or 0)
            llm_completion_tokens += int(
                extractor_stats.get("completion_tokens", 0) or 0
            )
            llm_total_tokens += int(extractor_stats.get("total_tokens", 0) or 0)
            llm_api_usage_calls += int(
                extractor_stats.get("api_usage_calls", 0) or 0
            )
            llm_estimated_usage_calls += int(
                extractor_stats.get("estimated_usage_calls", 0) or 0
            )
            task_cost = float(extractor_stats.get("estimated_cost_usd", 0.0) or 0.0)
            llm_estimated_cost_usd += task_cost
            if task_cost > 0:
                llm_cost_samples += 1

        if task.status != "success":
            continue
        comparison = compare_with_previous(task)
        if comparison["changed"]:
            changed_count = int(comparison["changed_fields_count"] or 0)
            if changed_count >= 4:
                severity = "high"
            elif changed_count >= 2:
                severity = "medium"
            else:
                severity = "low"
            recent_changes.append(
                {
                    "task_id": task.task_id,
                    "url": task.url,
                    "domain": extract_domain(task.url),
                    "changed_fields_count": comparison["changed_fields_count"],
                    "summary": "；".join(comparison["summary_lines"][:3]),
                    "previous_task_id": comparison["previous_task_id"],
                    "severity": severity,
                    "recommended_action": (
                        comparison.get("suggested_actions", ["继续观察后续变化"])[0]
                        if comparison.get("suggested_actions")
                        else "继续观察后续变化"
                    ),
                }
            )

    domain_leaderboard: list[dict[str, Any]] = []
    for domain, tasks in grouped_by_domain.items():
        success_count = sum(1 for item in tasks if item.status == "success")
        latest_task = tasks[0]
        domain_leaderboard.append(
            {
                "domain": domain,
                "total": len(tasks),
                "success": success_count,
                "success_rate": f"{success_count / max(len(tasks), 1):.0%}",
                "latest_task_id": latest_task.task_id,
            }
        )
    domain_leaderboard.sort(key=lambda item: (-item["total"], item["domain"]))

    watchlist: list[dict[str, Any]] = []
    for url, tasks in grouped_by_url.items():
        if len(tasks) < 2:
            continue
        latest_task = tasks[0]
        success_count = sum(1 for item in tasks if item.status == "success")
        watchlist.append(
            {
                "url": url,
                "domain": extract_domain(url),
                "total_runs": len(tasks),
                "success_runs": success_count,
                "latest_task_id": latest_task.task_id,
                "latest_status": latest_task.status,
                "latest_quality": latest_task.quality_score,
            }
        )
    watchlist.sort(key=lambda item: (-item["total_runs"], item["url"]))

    quality_values = [task.quality_score for task in recent_tasks if task.quality_score > 0]
    notification_ready_count = sum(
        1
        for item in monitors
        if notification_channels_from_profile(
            item.profile if isinstance(item.profile, dict) else {}
        )
    )
    notification_success_count = sum(
        1 for item in monitors if item.last_notification_status == "sent"
    )
    scenario_summary: dict[str, int] = defaultdict(int)
    for item in monitors:
        scenario_label = str(item.profile.get("scenario_label") or "").strip()
        if scenario_label:
            scenario_summary[scenario_label] += 1

    enriched_monitors = [enrich_monitor_payload(item.to_dict()) for item in monitors]
    monitor_alerts = [
        item
        for item in enriched_monitors
        if str(item.get("last_alert_level") or "").strip().lower() in {"changed", "error"}
    ]

    summary = {
        "unique_domains": len([domain for domain in grouped_by_domain if domain != "unknown"]),
        "repeat_urls": len(watchlist),
        "changed_tasks": len(recent_changes),
        "active_monitors": len(monitors),
        "notification_ready_monitors": notification_ready_count,
        "notification_success_monitors": notification_success_count,
        "rule_based_tasks": rule_based_tasks,
        "fallback_tasks": fallback_tasks,
        "learned_profile_hits": learned_profile_hits,
        "site_memory_saved_runs": rule_based_tasks,
        "memory_ready_pages": learned_profile_hits,
        "llm_total_calls": llm_total_calls,
        "llm_prompt_tokens": llm_prompt_tokens,
        "llm_completion_tokens": llm_completion_tokens,
        "llm_total_tokens": llm_total_tokens,
        "llm_estimated_cost_usd": round(llm_estimated_cost_usd, 6),
        "llm_api_usage_calls": llm_api_usage_calls,
        "llm_estimated_usage_calls": llm_estimated_usage_calls,
        "llm_api_usage_ratio": round(
            llm_api_usage_calls / max(llm_api_usage_calls + llm_estimated_usage_calls, 1),
            4,
        ),
        "site_memory_estimated_saved_cost_usd": round(
            (
                (llm_estimated_cost_usd / max(llm_cost_samples, 1)) * rule_based_tasks
                if llm_cost_samples > 0
                else 0.0
            ),
            6,
        ),
        "high_priority_alerts": sum(
            1
            for item in monitor_alerts
            if str(item.get("severity") or "").strip().lower() in {"high", "critical"}
        ),
        "avg_quality": (
            f"{(sum(quality_values) / max(len(quality_values), 1)):.0%}"
            if quality_values
            else "-"
        ),
    }
    return {
        "summary": summary,
        "domain_leaderboard": domain_leaderboard[:5],
        "watchlist": [
            {
                **item,
                "monitor_readiness": (
                    "适合加入监控"
                    if int(item.get("total_runs") or 0) >= 3 and int(item.get("success_runs") or 0) >= 2
                    else "继续积累历史"
                ),
            }
            for item in watchlist[:5]
        ],
        "recent_changes": recent_changes[:5],
        "scenario_summary": [
            {"label": label, "count": count}
            for label, count in sorted(
                scenario_summary.items(), key=lambda item: (-item[1], item[0])
            )
        ][:5],
        "monitors": [
            {**item, "domain": extract_domain(str(item.get("url") or ""))}
            for item in enriched_monitors
        ],
        "monitor_alerts": monitor_alerts[:5],
    }
