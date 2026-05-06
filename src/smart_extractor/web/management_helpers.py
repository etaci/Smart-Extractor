"""管理类路由的辅助函数。"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Callable

from smart_extractor.web.monitor_schedule import (
    current_timestamp,
    normalize_schedule_interval_minutes,
    parse_timestamp,
    schedule_status,
)


def masked_secret(prefix: str = "") -> str:
    return f"{prefix}{'*' * 3}"


def _normalize_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "y"}


def normalize_digest_hour(value: object, default: int = 9) -> int:
    try:
        hour = int(value if value not in {None, ""} else default)
    except (TypeError, ValueError):
        hour = default
    return max(0, min(hour, 23))


def normalize_hour(value: object, default: int = 0) -> int:
    try:
        hour = int(value if value not in {None, ""} else default)
    except (TypeError, ValueError):
        hour = default
    return max(0, min(hour, 23))


def normalize_minutes(value: object, default: int = 0, *, minimum: int = 0) -> int:
    try:
        minutes = int(value if value not in {None, ""} else default)
    except (TypeError, ValueError):
        minutes = default
    return max(minutes, minimum)


def normalize_ratio(value: object, default: float = 0.0) -> float:
    try:
        ratio = float(value if value not in {None, ""} else default)
    except (TypeError, ValueError):
        ratio = default
    return max(0.0, min(ratio, 1.0))


def normalize_notification_channels(
    channels: object,
    *,
    webhook_url: str = "",
    webhook_secret: str = "",
) -> list[dict[str, object]]:
    normalized_channels: list[dict[str, object]] = []

    def append_channel(payload: dict[str, object]) -> None:
        channel_type = str(
            payload.get("channel_type") or payload.get("type") or "webhook"
        ).strip().lower()
        if channel_type not in {"webhook", "slack", "teams", "discord"}:
            channel_type = "webhook"
        target = str(
            payload.get("target")
            or payload.get("url")
            or payload.get("webhook_url")
            or ""
        ).strip()
        if not target:
            return
        secret = str(
            payload.get("secret")
            or payload.get("webhook_secret")
            or ""
        ).strip()
        enabled = _normalize_bool(payload.get("enabled", True))
        name = str(payload.get("name") or "").strip()
        key = (channel_type, target)
        for existing in normalized_channels:
            if (
                str(existing.get("channel_type") or "").strip().lower(),
                str(existing.get("target") or "").strip(),
            ) != key:
                continue
            if secret and not str(existing.get("secret") or "").strip():
                existing["secret"] = secret
            if name and not str(existing.get("name") or "").strip():
                existing["name"] = name
            if enabled:
                existing["enabled"] = True
            return
        normalized_channels.append(
            {
                "channel_type": channel_type,
                "name": name,
                "target": target,
                "secret": secret,
                "enabled": enabled,
            }
        )

    if isinstance(channels, list):
        for item in channels:
            if isinstance(item, dict):
                append_channel(item)

    legacy_target = str(webhook_url or "").strip()
    if legacy_target:
        append_channel(
            {
                "channel_type": "webhook",
                "name": "默认 Webhook",
                "target": legacy_target,
                "secret": str(webhook_secret or "").strip(),
                "enabled": True,
            }
        )

    return normalized_channels


def notification_channels_from_profile(profile: object) -> list[dict[str, object]]:
    if not isinstance(profile, dict):
        return []
    return [
        item
        for item in normalize_notification_channels(
        profile.get("notification_channels"),
        webhook_url=str(profile.get("webhook_url") or "").strip(),
        webhook_secret=str(profile.get("webhook_secret") or "").strip(),
        )
        if bool(item.get("enabled", True))
    ]


def primary_notification_channel(profile: object) -> dict[str, object]:
    channels = notification_channels_from_profile(profile)
    for item in channels:
        if bool(item.get("enabled", True)):
            return item
    return channels[0] if channels else {}


def find_notification_channel(
    profile: object,
    *,
    target: str = "",
    channel_type: str = "",
) -> dict[str, object]:
    normalized_target = str(target or "").strip()
    normalized_type = str(channel_type or "").strip().lower()
    channels = notification_channels_from_profile(profile)
    if not normalized_target and not normalized_type:
        return primary_notification_channel(profile)

    for item in channels:
        item_target = str(item.get("target") or "").strip()
        item_type = str(item.get("channel_type") or "webhook").strip().lower()
        if normalized_target and item_target != normalized_target:
            continue
        if normalized_type and item_type != normalized_type:
            continue
        return item

    if normalized_target:
        for item in channels:
            if str(item.get("target") or "").strip() == normalized_target:
                return item
    return {}


def normalize_profile_payload(payload: dict[str, object]) -> dict[str, object]:
    if not isinstance(payload, dict):
        return {}

    normalized_notify_on = [
        str(item).strip().lower()
        for item in payload.get("notify_on", [])
        if str(item).strip()
    ]
    if not normalized_notify_on:
        normalized_notify_on = ["changed", "error"]

    normalized_channels = normalize_notification_channels(
        payload.get("notification_channels"),
        webhook_url=str(payload.get("webhook_url", "")).strip(),
        webhook_secret=str(payload.get("webhook_secret", "")).strip(),
    )
    primary_channel = primary_notification_channel(
        {"notification_channels": normalized_channels}
    )
    primary_target = str(primary_channel.get("target") or "").strip()
    primary_secret = str(primary_channel.get("secret") or "").strip()

    return {
        "scenario_label": str(payload.get("scenario_label", "")).strip(),
        "business_goal": str(payload.get("business_goal", "")).strip(),
        "alert_focus": str(payload.get("alert_focus", "")).strip(),
        "notify_on": normalized_notify_on,
        "webhook_url": str(payload.get("webhook_url", "")).strip() or primary_target,
        "webhook_secret": str(payload.get("webhook_secret", "")).strip() or primary_secret,
        "notification_channels": normalized_channels,
        "summary_style": str(payload.get("summary_style", "")).strip(),
        "digest_enabled": _normalize_bool(payload.get("digest_enabled", False)),
        "digest_hour": normalize_digest_hour(payload.get("digest_hour", 9)),
        "digest_only": _normalize_bool(payload.get("digest_only", False)),
        "always_notify_error": _normalize_bool(payload.get("always_notify_error", True)),
        "quiet_hours_enabled": _normalize_bool(payload.get("quiet_hours_enabled", False)),
        "quiet_hours_start": normalize_hour(payload.get("quiet_hours_start", 22)),
        "quiet_hours_end": normalize_hour(payload.get("quiet_hours_end", 8)),
        "notification_cooldown_minutes": normalize_minutes(
            payload.get("notification_cooldown_minutes", 0),
            0,
        ),
        "min_change_count": normalize_minutes(payload.get("min_change_count", 0), 0),
        "min_change_ratio": normalize_ratio(payload.get("min_change_ratio", 0.0), 0.0),
        "playbook": [
            str(item).strip()
            for item in payload.get("playbook", [])
            if str(item).strip()
        ],
    }


def apply_monitor_notification_defaults(
    profile: dict[str, object] | None,
    *,
    suggested_channel_types: list[str] | None = None,
) -> dict[str, object]:
    raw_profile = dict(profile or {})
    normalized_profile = normalize_profile_payload(raw_profile)
    for key, value in raw_profile.items():
        normalized_profile.setdefault(str(key), value)
    normalized_channel_types = [
        str(item).strip().lower()
        for item in (suggested_channel_types or ["webhook"])
        if str(item).strip()
    ]
    if not normalized_channel_types:
        normalized_channel_types = ["webhook"]

    notification_defaults = {
        "notify_on": list(normalized_profile.get("notify_on", ["changed", "error"])),
        "always_notify_error": bool(normalized_profile.get("always_notify_error", True)),
        "digest_enabled": bool(normalized_profile.get("digest_enabled", False)),
        "digest_only": bool(normalized_profile.get("digest_only", False)),
        "notification_cooldown_minutes": normalize_minutes(
            normalized_profile.get("notification_cooldown_minutes", 0),
            0,
        ),
        "min_change_count": normalize_minutes(
            normalized_profile.get("min_change_count", 0),
            0,
        ),
        "min_change_ratio": normalize_ratio(
            normalized_profile.get("min_change_ratio", 0.0),
            0.0,
        ),
        "channel_required": True,
        "suggested_channel_types": normalized_channel_types,
    }
    normalized_profile["notification_strategy_version"] = "v1"
    normalized_profile["notification_setup_status"] = (
        "ready" if notification_channels_from_profile(normalized_profile) else "pending_channel"
    )
    normalized_profile["notification_defaults"] = notification_defaults
    normalized_profile["notification_activation_checklist"] = [
        "配置 webhook、Slack 或 Teams 通知目标",
        "确认 changed / error 的通知级别",
        "确认静默时段、冷却时间与 digest 设置",
    ]
    return normalized_profile


def normalize_selected_fields(values: list[object]) -> list[str]:
    return [str(item).strip() for item in values if str(item).strip()]


def normalize_field_labels(values: dict[object, object]) -> dict[str, str]:
    return {
        str(key).strip(): str(value).strip()
        for key, value in values.items()
        if str(key).strip()
    }


def learned_profile_risk_level(profile: Any) -> str:
    rule_failures = int(getattr(profile, "rule_failure_count", 0) or 0)
    rule_success = int(getattr(profile, "rule_success_count", 0) or 0)
    completeness = float(getattr(profile, "last_completeness", 0.0) or 0.0)
    if not bool(getattr(profile, "is_active", True)):
        return "paused"
    if rule_failures >= max(2, rule_success + 1) or completeness < 0.35:
        return "high"
    if rule_failures > 0 or completeness < 0.6:
        return "medium"
    return "low"


def learned_profile_risk_label(level: str) -> str:
    normalized = str(level or "").strip().lower()
    if normalized == "high":
        return "高风险"
    if normalized == "medium":
        return "需观察"
    if normalized == "paused":
        return "已暂停"
    return "稳定"


def learned_profile_recommended_actions(profile: Any) -> list[str]:
    risk_level = learned_profile_risk_level(profile)
    actions: list[str] = []
    if risk_level == "paused":
        actions.append("暂不参与规则复用；确认页面结构稳定后再恢复")
        if str(getattr(profile, "disabled_reason", "") or "").strip():
            actions.append("先复核停用原因，再决定是否重新启用")
        return actions

    rule_failures = int(getattr(profile, "rule_failure_count", 0) or 0)
    rule_success = int(getattr(profile, "rule_success_count", 0) or 0)
    completeness = float(getattr(profile, "last_completeness", 0.0) or 0.0)
    selected_fields = list(getattr(profile, "selected_fields", []) or [])
    if risk_level == "high":
        actions.append("建议先停用这条学习档案，避免低质量规则继续命中")
        actions.append("对最近命中的页面重新执行一次 LLM 学习，刷新字段方案")
        if len(selected_fields) >= 4:
            actions.append("优先缩小字段范围，只保留稳定且业务关键的字段")
        return actions

    if rule_failures > 0:
        actions.append("建议观察最近命中页面，确认是否出现局部结构漂移")
    if completeness < 0.6:
        actions.append("建议补充或精简字段集合，提升规则命中完整度")
    if rule_success == 0:
        actions.append("建议先用相同站点多跑几次，确认这条档案是否值得继续沉淀")
    if not actions:
        actions.append("保持当前规则复用策略，继续观察命中质量即可")
    return actions


def learned_profile_stability_rate(profile: Any) -> float:
    rule_success = int(getattr(profile, "rule_success_count", 0) or 0)
    rule_failure = int(getattr(profile, "rule_failure_count", 0) or 0)
    total = rule_success + rule_failure
    if total <= 0:
        return 0.0
    return rule_success / total


def learned_profile_memory_strength(profile: Any) -> str:
    if not bool(getattr(profile, "is_active", True)):
        return "paused"

    rule_success = int(getattr(profile, "rule_success_count", 0) or 0)
    stability_rate = learned_profile_stability_rate(profile)
    if rule_success >= 8 and stability_rate >= 0.8:
        return "strong"
    if rule_success >= 3 and stability_rate >= 0.6:
        return "warming"
    return "learning"


def learned_profile_memory_strength_label(level: str) -> str:
    normalized = str(level or "").strip().lower()
    if normalized == "strong":
        return "记忆稳定"
    if normalized == "warming":
        return "正在成型"
    if normalized == "paused":
        return "已暂停"
    return "刚开始学习"


def estimated_saved_llm_calls(profile: Any) -> int:
    return int(getattr(profile, "rule_success_count", 0) or 0)


def build_site_memory_snapshot(profile: Any) -> dict[str, object]:
    strength = learned_profile_memory_strength(profile)
    return {
        "profile_id": str(getattr(profile, "profile_id", "") or ""),
        "memory_strength": strength,
        "memory_strength_label": learned_profile_memory_strength_label(strength),
        "stability_rate": round(learned_profile_stability_rate(profile), 4),
    }


def serialize_learned_profile(profile: Any) -> dict[str, object]:
    payload = profile.to_dict()
    payload["status"] = "active" if profile.is_active else "disabled"
    payload["status_label"] = "可复用" if profile.is_active else "已停用"
    payload["risk_level"] = learned_profile_risk_level(profile)
    payload["risk_label"] = learned_profile_risk_label(payload["risk_level"])
    payload["recommended_actions"] = learned_profile_recommended_actions(profile)
    payload["stability_rate"] = round(learned_profile_stability_rate(profile), 4)
    payload["estimated_saved_llm_calls"] = estimated_saved_llm_calls(profile)
    payload["memory_strength"] = learned_profile_memory_strength(profile)
    payload["memory_strength_label"] = learned_profile_memory_strength_label(
        payload["memory_strength"]
    )
    payload["manual_annotation_count"] = int(
        getattr(profile, "manual_annotation_count", 0) or 0
    )
    payload["auto_repair_count"] = int(getattr(profile, "auto_repair_count", 0) or 0)
    payload["last_annotation_at"] = str(
        getattr(profile, "last_annotation_at", "") or ""
    )
    payload["last_repair_at"] = str(getattr(profile, "last_repair_at", "") or "")
    payload["repair_recommendation"] = (
        "建议继续保留自动修复闭环"
        if payload["auto_repair_count"] > 0
        else (
            "建议补一次人工标注，提升模板长期稳定性"
            if payload["risk_level"] in {"medium", "high"}
            else "当前模板稳定，可继续观察"
        )
    )
    return payload


def _split_keywords(text: str) -> list[str]:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return []
    return [
        item
        for item in re.split(r"[\s,，、/|；;]+", normalized)
        if item and len(item) >= 1
    ]


def monitor_alert_label(level: str) -> str:
    normalized = str(level or "").strip().lower()
    if normalized == "changed":
        return "检测到变化"
    if normalized == "stable":
        return "页面稳定"
    if normalized == "error":
        return "执行失败"
    return "等待检查"


def monitor_severity_label(level: str) -> str:
    normalized = str(level or "").strip().lower()
    if normalized == "critical":
        return "需立即处理"
    if normalized == "high":
        return "高优先级"
    if normalized == "medium":
        return "建议复核"
    return "常规关注"


def notification_status_label(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized == "sent":
        return "已发送"
    if normalized == "retry_pending":
        return "等待重试"
    if normalized == "retrying":
        return "重试中"
    if normalized == "retried":
        return "已转入重试"
    if normalized == "failed":
        return "发送失败"
    if normalized == "skipped":
        return "已跳过"
    return "未发送"


def notification_trigger_label(triggered_by: str) -> str:
    normalized = str(triggered_by or "").strip().lower()
    if normalized == "manual":
        return "手动补发"
    if normalized == "retry":
        return "自动重试"
    return "系统发送"


def serialize_notification_event(event: Any) -> dict[str, object]:
    payload = event.to_dict()
    return {
        "notification_id": payload.get("notification_id", ""),
        "monitor_id": payload.get("monitor_id", ""),
        "task_id": payload.get("task_id", ""),
        "channel_type": payload.get("channel_type", "webhook"),
        "target": payload.get("target", ""),
        "event_type": payload.get("event_type", "monitor_alert"),
        "status": payload.get("status", ""),
        "status_label": notification_status_label(payload.get("status", "")),
        "status_message": payload.get("status_message", ""),
        "response_code": payload.get("response_code"),
        "error_message": payload.get("error_message", ""),
        "next_retry_at": payload.get("next_retry_at", ""),
        "created_at": payload.get("created_at", ""),
        "retry_of_notification_id": payload.get("retry_of_notification_id", ""),
        "triggered_by": payload.get("triggered_by", ""),
        "triggered_by_label": notification_trigger_label(
            payload.get("triggered_by", "")
        ),
        "can_resend": bool(
            str(payload.get("target") or "").strip() or payload.get("payload_snapshot")
        ),
    }


def monitor_schedule_status_label(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized == "active":
        return "自动巡检中"
    if normalized == "paused":
        return "已暂停"
    return "自动巡检已关闭"


def monitor_claim_status(payload: dict[str, Any]) -> str:
    claimed_by = str(payload.get("schedule_claimed_by") or "").strip()
    if not claimed_by:
        return "idle"
    lease_until = parse_timestamp(str(payload.get("schedule_lease_until") or "").strip())
    now = parse_timestamp(current_timestamp())
    if lease_until is None or now is None:
        return "claimed"
    if lease_until <= now:
        return "expired"
    return "claimed"


def monitor_claim_status_label(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized == "claimed":
        return "调度抢占中"
    if normalized == "expired":
        return "等待回收"
    return "空闲"


def monitor_trigger_source_label(source: str) -> str:
    normalized = str(source or "").strip().lower()
    if normalized == "auto":
        return "自动巡检"
    if normalized == "manual":
        return "手动触发"
    return "暂未执行"


def monitor_schedule_interval_label(interval_minutes: object) -> str:
    interval = normalize_schedule_interval_minutes(interval_minutes)
    if interval % (60 * 24) == 0:
        return f"每 {interval // (60 * 24)} 天"
    if interval % 60 == 0:
        return f"每 {interval // 60} 小时"
    return f"每 {interval} 分钟"


def _monitor_focus_terms(payload: dict[str, Any]) -> list[str]:
    profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
    field_labels = (
        payload.get("field_labels") if isinstance(payload.get("field_labels"), dict) else {}
    )
    selected_fields = payload.get("selected_fields") or []
    terms: list[str] = []
    for item in _split_keywords(str(profile.get("alert_focus") or "")):
        if item not in terms:
            terms.append(item)
    for field_name in selected_fields:
        normalized_name = str(field_name or "").strip().lower()
        label = str(field_labels.get(field_name, "") or "").strip().lower()
        for item in (normalized_name, label):
            if item and item not in terms:
                terms.append(item)
    return terms


def _match_focus_changes(payload: dict[str, Any]) -> list[str]:
    changed_fields = payload.get("last_changed_fields") or []
    focus_terms = _monitor_focus_terms(payload)
    matches: list[str] = []
    for item in changed_fields:
        label = str(item.get("label") or item.get("field") or "字段").strip()
        normalized_label = label.lower()
        normalized_field = str(item.get("field") or "").strip().lower()
        if not focus_terms:
            continue
        if any(
            term in normalized_label
            or term in normalized_field
            or normalized_label in term
            or normalized_field in term
            for term in focus_terms
        ):
            if label not in matches:
                matches.append(label)
    return matches


def monitor_alert_severity(payload: dict[str, Any]) -> str:
    alert_level = str(payload.get("last_alert_level") or "").strip().lower()
    changed_fields = payload.get("last_changed_fields") or []
    focus_matches = _match_focus_changes(payload)
    removed_count = sum(
        1 for item in changed_fields if str(item.get("change_type") or "") == "removed"
    )

    if alert_level == "error":
        return "critical"
    if alert_level != "changed":
        return "low"
    if removed_count > 0 or len(focus_matches) >= 2 or len(changed_fields) >= 4:
        return "high"
    if focus_matches or len(changed_fields) >= 2:
        return "medium"
    return "low"


def _monitor_business_summary(payload: dict[str, Any]) -> str:
    profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
    scenario_label = str(profile.get("scenario_label") or "页面变化监控").strip()
    alert_focus = str(profile.get("alert_focus") or "").strip()
    alert_level = str(payload.get("last_alert_level") or "").strip().lower()
    changed_fields = payload.get("last_changed_fields") or []
    focus_matches = _match_focus_changes(payload)

    if alert_level == "error":
        return f"{scenario_label} 本次巡检失败，暂时无法产出更新结论。"
    if alert_level == "stable":
        focus_text = alert_focus or "关键字段"
        return f"{scenario_label} 本次未发现 {focus_text} 的明显变化，可继续自动巡检。"

    if focus_matches:
        return (
            f"{scenario_label} 检测到 {'、'.join(focus_matches[:3])} 等关键字段发生变化，"
            "适合立即输出变化简报。"
        )
    if changed_fields:
        return f"{scenario_label} 本次发现 {len(changed_fields)} 项字段变化，建议尽快复核。"
    return f"{scenario_label} 已完成巡检，本次暂无可执行变化结论。"


def _append_unique_action(actions: list[str], action: str) -> None:
    normalized = str(action or "").strip()
    if not normalized or normalized in actions:
        return
    actions.append(normalized)


def _monitor_recommended_actions(payload: dict[str, Any]) -> list[str]:
    profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
    notification_channels = notification_channels_from_profile(profile)
    business_goal = str(profile.get("business_goal") or "").strip()
    alert_level = str(payload.get("last_alert_level") or "").strip().lower()
    severity = monitor_alert_severity(payload)
    focus_matches = _match_focus_changes(payload)
    actions: list[str] = []

    if alert_level == "error":
        _append_unique_action(actions, "先手动重跑一次，确认页面是否卡在加载态或触发风控。")
        _append_unique_action(actions, "如果连续失败，建议切换抓取模式或重新学习该站点记忆。")
        if not payload.get("learned_profile") and not payload.get("site_memory"):
            _append_unique_action(actions, "建议先分析页面并沉淀站点记忆，降低后续恢复成本。")
        return actions[:3]

    if alert_level == "changed":
        if focus_matches:
            _append_unique_action(
                actions,
                f"优先核对 {'、'.join(focus_matches[:3])} 是否影响当前业务判断。",
            )
        else:
            _append_unique_action(actions, "优先复核本次变化字段，确认不是页面噪声。")
        if business_goal:
            _append_unique_action(actions, f"建议围绕“{business_goal}”同步本次变化结论。")
        if severity in {"high", "critical"}:
            _append_unique_action(actions, "建议立即导出变化摘要，并同步给负责人或相关团队。")
        else:
            _append_unique_action(actions, "建议补一次人工复核，再决定是否升级通知。")
        if not notification_channels:
            _append_unique_action(actions, "建议补上通知通道，避免后续关键变化遗漏。")
        return actions[:3]

    _append_unique_action(actions, "当前页面表现稳定，可继续保留自动巡检。")
    if not notification_channels:
        _append_unique_action(actions, "如果这是重点页面，建议补上通知通道。")
    _append_unique_action(actions, "如需对团队同步，可直接导出摘要而不是手动整理原始字段。")
    return actions[:3]


def enrich_monitor_payload(payload: dict[str, Any]) -> dict[str, Any]:
    profile = payload.get("profile") if isinstance(payload.get("profile"), dict) else {}
    normalized_profile = dict(profile)
    normalized_channels = notification_channels_from_profile(normalized_profile)
    primary_channel = primary_notification_channel(normalized_profile)
    normalized_profile["digest_enabled"] = _normalize_bool(
        normalized_profile.get("digest_enabled", False)
    )
    normalized_profile["digest_hour"] = normalize_digest_hour(
        normalized_profile.get("digest_hour", 9)
    )
    normalized_profile["digest_only"] = _normalize_bool(
        normalized_profile.get("digest_only", False)
    )
    normalized_profile["always_notify_error"] = _normalize_bool(
        normalized_profile.get("always_notify_error", True)
    )
    normalized_profile["quiet_hours_enabled"] = _normalize_bool(
        normalized_profile.get("quiet_hours_enabled", False)
    )
    normalized_profile["quiet_hours_start"] = normalize_hour(
        normalized_profile.get("quiet_hours_start", 22)
    )
    normalized_profile["quiet_hours_end"] = normalize_hour(
        normalized_profile.get("quiet_hours_end", 8)
    )
    normalized_profile["notification_cooldown_minutes"] = normalize_minutes(
        normalized_profile.get("notification_cooldown_minutes", 0),
        0,
    )
    normalized_profile["min_change_count"] = normalize_minutes(
        normalized_profile.get("min_change_count", 0),
        0,
    )
    normalized_profile["min_change_ratio"] = normalize_ratio(
        normalized_profile.get("min_change_ratio", 0.0),
        0.0,
    )
    normalized_profile["notification_channels"] = normalized_channels
    normalized_profile["webhook_url"] = str(normalized_profile.get("webhook_url") or "").strip() or str(
        primary_channel.get("target") or ""
    ).strip()
    normalized_profile["webhook_secret"] = str(
        normalized_profile.get("webhook_secret") or ""
    ).strip() or str(primary_channel.get("secret") or "").strip()
    normalized_payload = dict(payload)
    normalized_payload["profile"] = normalized_profile
    normalized_payload["schedule_status"] = schedule_status(
        enabled=bool(normalized_payload.get("schedule_enabled")),
        paused_at=str(normalized_payload.get("schedule_paused_at") or "").strip(),
    )
    normalized_payload["schedule_status_label"] = monitor_schedule_status_label(
        normalized_payload["schedule_status"]
    )
    normalized_payload["schedule_interval_label"] = monitor_schedule_interval_label(
        normalized_payload.get("schedule_interval_minutes", 60)
    )
    normalized_payload["schedule_claim_status"] = monitor_claim_status(
        normalized_payload
    )
    normalized_payload["schedule_claim_status_label"] = monitor_claim_status_label(
        normalized_payload["schedule_claim_status"]
    )
    normalized_payload["last_trigger_source_label"] = monitor_trigger_source_label(
        normalized_payload.get("last_trigger_source", "")
    )
    normalized_payload["alert_label"] = monitor_alert_label(
        normalized_payload.get("last_alert_level", "")
    )
    normalized_payload["severity"] = monitor_alert_severity(normalized_payload)
    normalized_payload["severity_label"] = monitor_severity_label(
        normalized_payload["severity"]
    )
    normalized_payload["notification_channel_count"] = len(normalized_channels)
    normalized_payload["notification_status_label"] = notification_status_label(
        normalized_payload.get("last_notification_status", "")
    )
    changed_fields = normalized_payload.get("last_changed_fields") or []
    selected_fields = normalized_payload.get("selected_fields") or []
    changed_fields_count = len(changed_fields)
    changed_ratio = (
        changed_fields_count / max(len(selected_fields), 1)
        if selected_fields
        else float(changed_fields_count > 0)
    )
    normalized_payload["business_summary"] = _monitor_business_summary(normalized_payload)
    normalized_payload["recommended_actions"] = _monitor_recommended_actions(
        normalized_payload
    )
    return normalized_payload


def serialize_monitor(monitor: Any, learned_profile_store: Any) -> dict[str, object]:
    payload = monitor.to_dict()
    learned_profile_id = str(payload.get("last_learned_profile_id") or "").strip()
    if learned_profile_id:
        learned_profile = learned_profile_store.get_profile(learned_profile_id)
        if learned_profile is not None:
            payload["learned_profile"] = {
                "profile_id": learned_profile.profile_id,
                "status_label": "可复用" if learned_profile.is_active else "已停用",
            }
            payload["site_memory"] = build_site_memory_snapshot(learned_profile)
    enriched = enrich_monitor_payload(payload)
    return {
        "monitor_id": enriched.get("monitor_id", ""),
        "name": enriched.get("name", ""),
        "url": enriched.get("url", ""),
        "selected_fields": enriched.get("selected_fields", []),
        "field_labels": enriched.get("field_labels", {}),
        "profile": enriched.get("profile", {}),
        "last_checked_at": enriched.get("last_checked_at", ""),
        "last_alert_level": enriched.get("last_alert_level", ""),
        "last_alert_message": enriched.get("last_alert_message", ""),
        "last_notification_status": enriched.get("last_notification_status", ""),
        "last_notification_at": enriched.get("last_notification_at", ""),
        "last_extraction_strategy": enriched.get("last_extraction_strategy", ""),
        "last_learned_profile_id": enriched.get("last_learned_profile_id", ""),
        "schedule_enabled": enriched.get("schedule_enabled", False),
        "schedule_interval_minutes": enriched.get("schedule_interval_minutes", 60),
        "schedule_next_run_at": enriched.get("schedule_next_run_at", ""),
        "schedule_last_run_at": enriched.get("schedule_last_run_at", ""),
        "schedule_paused_at": enriched.get("schedule_paused_at", ""),
        "schedule_claimed_by": enriched.get("schedule_claimed_by", ""),
        "schedule_lease_until": enriched.get("schedule_lease_until", ""),
        "schedule_last_error": enriched.get("schedule_last_error", ""),
        "last_trigger_source": enriched.get("last_trigger_source", ""),
        "schedule_status": enriched.get("schedule_status", "disabled"),
        "schedule_status_label": enriched.get("schedule_status_label", ""),
        "schedule_interval_label": enriched.get("schedule_interval_label", ""),
        "schedule_claim_status": enriched.get("schedule_claim_status", "idle"),
        "schedule_claim_status_label": enriched.get("schedule_claim_status_label", ""),
        "last_trigger_source_label": enriched.get("last_trigger_source_label", ""),
        "alert_label": enriched.get("alert_label", ""),
        "severity": enriched.get("severity", "low"),
        "severity_label": enriched.get("severity_label", ""),
        "notification_channel_count": enriched.get("notification_channel_count", 0),
        "notification_status_label": enriched.get("notification_status_label", ""),
        "business_summary": enriched.get("business_summary", ""),
        "recommended_actions": enriched.get("recommended_actions", []),
        "learned_profile": enriched.get("learned_profile"),
        "site_memory": enriched.get("site_memory"),
    }


def serialize_template(template: Any) -> dict[str, object]:
    payload = template.to_dict()
    return {
        "template_id": payload.get("template_id", ""),
        "name": payload.get("name", ""),
        "url": payload.get("url", ""),
        "page_type": payload.get("page_type", "unknown"),
        "schema_name": payload.get("schema_name", "auto"),
        "storage_format": payload.get("storage_format", "json"),
        "use_static": payload.get("use_static", False),
        "selected_fields": payload.get("selected_fields", []),
        "field_labels": payload.get("field_labels", {}),
        "profile": payload.get("profile", {}),
    }


def serialize_task_list_item(task: Any) -> dict[str, object]:
    payload = task.to_dict()
    return {
        "task_id": payload.get("task_id", ""),
        "url": payload.get("url", ""),
        "schema_name": payload.get("schema_name", "auto"),
        "storage_format": payload.get("storage_format", "json"),
        "status": payload.get("status", "pending"),
        "created_at": payload.get("created_at", ""),
        "elapsed_ms": payload.get("elapsed_ms", 0.0),
        "quality_score": payload.get("quality_score", 0.0),
        "progress_percent": payload.get("progress_percent", 0.0),
        "progress_stage": payload.get("progress_stage", ""),
        "batch_group_id": payload.get("batch_group_id", ""),
        "task_kind": payload.get("task_kind", "single"),
        "total_items": payload.get("total_items", 0),
        "completed_items": payload.get("completed_items", 0),
    }


def serialize_task_history_item(task: Any) -> dict[str, object]:
    payload = serialize_task_list_item(task)
    return {
        "task_id": payload["task_id"],
        "created_at": payload["created_at"],
        "status": payload["status"],
        "quality_score": payload["quality_score"],
    }


def serialize_task_batch_child_item(task: Any) -> dict[str, object]:
    payload = serialize_task_list_item(task)
    return {
        "task_id": payload["task_id"],
        "url": payload["url"],
        "status": payload["status"],
        "quality_score": payload["quality_score"],
    }


def serialize_actor_install(record: Any) -> dict[str, object]:
    payload = record.to_dict()
    return {
        "actor_instance_id": payload.get("actor_instance_id", ""),
        "actor_id": payload.get("actor_id", ""),
        "name": payload.get("name", ""),
        "version": payload.get("version", ""),
        "category": payload.get("category", ""),
        "capabilities": payload.get("capabilities", []),
        "config": payload.get("config", {}),
        "linked_template_id": payload.get("linked_template_id", ""),
        "linked_monitor_id": payload.get("linked_monitor_id", ""),
        "status": payload.get("status", "active"),
        "created_at": payload.get("created_at", ""),
        "updated_at": payload.get("updated_at", ""),
        "last_run_at": payload.get("last_run_at", ""),
    }


def serialize_worker_node(record: Any) -> dict[str, object]:
    payload = record.to_dict()
    return {
        "worker_id": payload.get("worker_id", ""),
        "display_name": payload.get("display_name", ""),
        "node_type": payload.get("node_type", "worker"),
        "status": payload.get("status", "idle"),
        "queue_scope": payload.get("queue_scope", "*"),
        "current_load": payload.get("current_load", 0),
        "capabilities": payload.get("capabilities", []),
        "metadata": payload.get("metadata", {}),
        "last_seen_at": payload.get("last_seen_at", ""),
        "created_at": payload.get("created_at", ""),
        "updated_at": payload.get("updated_at", ""),
        "last_error": payload.get("last_error", ""),
    }


def serialize_proxy_endpoint(record: Any) -> dict[str, object]:
    payload = record.to_dict()
    return {
        "proxy_id": payload.get("proxy_id", ""),
        "name": payload.get("name", ""),
        "proxy_url": payload.get("proxy_url", ""),
        "provider": payload.get("provider", ""),
        "status": payload.get("status", "idle"),
        "enabled": payload.get("enabled", True),
        "tags": payload.get("tags", []),
        "metadata": payload.get("metadata", {}),
        "success_count": payload.get("success_count", 0),
        "failure_count": payload.get("failure_count", 0),
        "last_used_at": payload.get("last_used_at", ""),
        "created_at": payload.get("created_at", ""),
        "updated_at": payload.get("updated_at", ""),
        "last_error": payload.get("last_error", ""),
    }


def serialize_site_policy(record: Any) -> dict[str, object]:
    payload = record.to_dict()
    return {
        "policy_id": payload.get("policy_id", ""),
        "domain": payload.get("domain", ""),
        "name": payload.get("name", ""),
        "min_interval_seconds": payload.get("min_interval_seconds", 0.0),
        "max_concurrency": payload.get("max_concurrency", 1),
        "use_proxy_pool": payload.get("use_proxy_pool", False),
        "preferred_proxy_tags": payload.get("preferred_proxy_tags", []),
        "assigned_worker_group": payload.get("assigned_worker_group", ""),
        "notes": payload.get("notes", ""),
        "created_at": payload.get("created_at", ""),
        "updated_at": payload.get("updated_at", ""),
    }


def serialize_task_annotation(record: Any) -> dict[str, object]:
    payload = record.to_dict()
    return {
        "annotation_id": payload.get("annotation_id", ""),
        "task_id": payload.get("task_id", ""),
        "profile_id": payload.get("profile_id", ""),
        "template_id": payload.get("template_id", ""),
        "corrected_data": payload.get("corrected_data", {}),
        "field_feedback": payload.get("field_feedback", {}),
        "notes": payload.get("notes", ""),
        "created_by": payload.get("created_by", ""),
        "created_at": payload.get("created_at", ""),
        "updated_at": payload.get("updated_at", ""),
    }


def serialize_repair_suggestion(record: Any) -> dict[str, object]:
    payload = record.to_dict()
    return {
        "repair_id": payload.get("repair_id", ""),
        "annotation_id": payload.get("annotation_id", ""),
        "task_id": payload.get("task_id", ""),
        "profile_id": payload.get("profile_id", ""),
        "template_id": payload.get("template_id", ""),
        "status": payload.get("status", "suggested"),
        "repair_strategy": payload.get("repair_strategy", "manual_feedback"),
        "suggested_fields": payload.get("suggested_fields", []),
        "suggested_field_labels": payload.get("suggested_field_labels", {}),
        "suggested_profile": payload.get("suggested_profile", {}),
        "reason": payload.get("reason", ""),
        "created_at": payload.get("created_at", ""),
        "updated_at": payload.get("updated_at", ""),
        "applied_at": payload.get("applied_at", ""),
    }


def serialize_funnel_event(record: Any) -> dict[str, object]:
    payload = record.to_dict()
    return {
        "funnel_event_id": payload.get("funnel_event_id", ""),
        "stage": payload.get("stage", ""),
        "channel": payload.get("channel", ""),
        "package_type": payload.get("package_type", ""),
        "package_id": payload.get("package_id", ""),
        "package_name": payload.get("package_name", ""),
        "task_id": payload.get("task_id", ""),
        "template_id": payload.get("template_id", ""),
        "monitor_id": payload.get("monitor_id", ""),
        "actor_instance_id": payload.get("actor_instance_id", ""),
        "metadata": payload.get("metadata", {}),
        "created_at": payload.get("created_at", ""),
    }


def list_risky_active_profiles(learned_profile_store: Any) -> list[Any]:
    risky_profiles = []
    for item in learned_profile_store.list_profiles():
        if not item.is_active:
            continue
        if learned_profile_risk_level(item) != "high":
            continue
        risky_profiles.append(item)
    return risky_profiles


def llm_basic_payload_from_sources(
    *,
    default_config_path: Path,
    load_config: Callable[..., Any],
    load_raw_yaml_config: Callable[..., dict[str, Any]],
    resolve_local_config_path: Callable[..., Path],
) -> dict[str, object]:
    local_config_path = resolve_local_config_path(default_config_path)
    raw_config = load_raw_yaml_config(local_config_path)
    llm_raw = raw_config.get("llm", {}) if isinstance(raw_config, dict) else {}
    effective = load_config()
    env_overrides = {
        "api_key": bool(os.environ.get("SMART_EXTRACTOR_API_KEY", "")),
        "base_url": bool(os.environ.get("SMART_EXTRACTOR_BASE_URL", "")),
        "model": bool(os.environ.get("SMART_EXTRACTOR_MODEL", "")),
    }
    return {
        "api_key": str(llm_raw.get("api_key", "") or effective.llm.api_key),
        "base_url": str(llm_raw.get("base_url", effective.llm.base_url)),
        "model": str(llm_raw.get("model", effective.llm.model)),
        "temperature": float(llm_raw.get("temperature", effective.llm.temperature)),
        "effective": {
            "api_key_masked": masked_secret(effective.llm.api_key[:8])
            if effective.llm.api_key
            else "",
            "base_url": effective.llm.base_url,
            "model": effective.llm.model,
            "temperature": effective.llm.temperature,
        },
        "env_overrides": env_overrides,
        "config_path": str(local_config_path),
        "has_local_override": bool(raw_config),
    }
