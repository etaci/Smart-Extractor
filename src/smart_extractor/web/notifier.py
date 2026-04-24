"""Monitor notification delivery helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import httpx
from loguru import logger

from smart_extractor.web.management_helpers import (
    enrich_monitor_payload,
    notification_channels_from_profile,
)
from smart_extractor.web.monitor_schedule import current_timestamp

DEFAULT_NOTIFICATION_MAX_ATTEMPTS = 3
_RETRY_DELAYS_MINUTES = (1, 5, 30)


@dataclass
class NotificationDeliveryResult:
    status: str
    message: str
    channel_type: str = "webhook"
    target: str = ""
    payload_snapshot: dict[str, Any] | None = None
    response_code: int | None = None
    error_type: str = ""
    error_message: str = ""
    should_retry: bool = False
    next_retry_at: str = ""
    sent_at: str = ""
    attempt_no: int = 1
    max_attempts: int = DEFAULT_NOTIFICATION_MAX_ATTEMPTS


def should_notify(monitor: dict[str, Any], alert_level: str) -> bool:
    should_send, _ = evaluate_notification_policy(monitor, alert_level)
    return should_send


def _is_now_in_quiet_hours(start_hour: int, end_hour: int) -> bool:
    now_hour = datetime.now().hour
    if start_hour == end_hour:
        return True
    if start_hour < end_hour:
        return start_hour <= now_hour < end_hour
    return now_hour >= start_hour or now_hour < end_hour


def evaluate_notification_policy(
    monitor: dict[str, Any],
    alert_level: str,
) -> tuple[bool, str]:
    profile = monitor.get("profile") if isinstance(monitor, dict) else {}
    if not isinstance(profile, dict):
        profile = {}
    if not notification_channels_from_profile(profile):
        return False, "未配置通知通道，已跳过发送"

    notify_on = profile.get("notify_on") or ["changed", "error"]
    normalized = {
        str(item or "").strip().lower()
        for item in notify_on
        if str(item or "").strip()
    }
    if not normalized:
        normalized = {"changed", "error"}
    normalized_alert_level = str(alert_level or "").strip().lower()
    always_notify_error = bool(profile.get("always_notify_error", True))
    if normalized_alert_level == "error" and always_notify_error:
        return True, ""
    if normalized_alert_level not in normalized:
        return False, "当前告警未命中 notify_on 规则"
    if bool(profile.get("digest_only", False)) and normalized_alert_level != "error":
        return False, "当前监控已启用仅 Digest 模式，实时通知已跳过"
    if bool(profile.get("quiet_hours_enabled", False)):
        start_hour = int(profile.get("quiet_hours_start", 22) or 22)
        end_hour = int(profile.get("quiet_hours_end", 8) or 8)
        if _is_now_in_quiet_hours(start_hour, end_hour):
            return False, "当前处于静默时段，实时通知已跳过"

    cooldown_minutes = int(profile.get("notification_cooldown_minutes", 0) or 0)
    last_notification_at = str(monitor.get("last_notification_at") or "").strip()
    if cooldown_minutes > 0 and last_notification_at and normalized_alert_level != "error":
        try:
            last_dt = datetime.strptime(last_notification_at, "%Y-%m-%d %H:%M:%S")
            if datetime.now() - last_dt < timedelta(minutes=cooldown_minutes):
                return False, "当前处于通知冷却时间内，实时通知已跳过"
        except ValueError:
            pass

    if normalized_alert_level == "changed":
        changed_fields = monitor.get("last_changed_fields") or []
        selected_fields = monitor.get("selected_fields") or []
        changed_count = len(changed_fields)
        min_change_count = int(profile.get("min_change_count", 0) or 0)
        if min_change_count > 0 and changed_count < min_change_count:
            return (
                False,
                f"变化字段数 {changed_count} 未达到阈值 {min_change_count}，已跳过实时通知",
            )
        min_change_ratio = float(profile.get("min_change_ratio", 0.0) or 0.0)
        changed_ratio = (
            changed_count / max(len(selected_fields), 1)
            if selected_fields
            else float(changed_count > 0)
        )
        if min_change_ratio > 0 and changed_ratio < min_change_ratio:
            return (
                False,
                f"变化占比 {changed_ratio:.0%} 未达到阈值 {min_change_ratio:.0%}，已跳过实时通知",
            )

    return True, ""


def build_monitor_notification_payload(
    monitor: dict[str, Any],
    task: dict[str, Any],
) -> dict[str, Any]:
    enriched_monitor = enrich_monitor_payload(monitor)
    changed_fields = enriched_monitor.get("last_changed_fields") or []
    profile = (
        enriched_monitor.get("profile")
        if isinstance(enriched_monitor.get("profile"), dict)
        else {}
    )
    return {
        "event": "smart_extractor.monitor_alert",
        "monitor": {
            "monitor_id": enriched_monitor.get("monitor_id", ""),
            "name": enriched_monitor.get("name", ""),
            "url": enriched_monitor.get("url", ""),
            "scenario_label": profile.get("scenario_label", ""),
            "business_goal": profile.get("business_goal", ""),
            "alert_focus": profile.get("alert_focus", ""),
            "notify_on": profile.get("notify_on", []),
            "recommended_outputs": enriched_monitor.get("recommended_outputs", []),
        },
        "alert": {
            "level": enriched_monitor.get("last_alert_level", ""),
            "level_label": enriched_monitor.get("alert_label", ""),
            "severity": enriched_monitor.get("severity", ""),
            "severity_label": enriched_monitor.get("severity_label", ""),
            "message": enriched_monitor.get("last_alert_message", ""),
            "business_summary": enriched_monitor.get("business_summary", ""),
            "recommended_actions": enriched_monitor.get("recommended_actions", []),
            "changed_fields_count": len(changed_fields),
            "changed_fields": changed_fields,
        },
        "task": {
            "task_id": task.get("task_id", ""),
            "status": task.get("status", ""),
            "quality_score": task.get("quality_score", 0),
            "elapsed_ms": task.get("elapsed_ms", 0),
            "completed_at": task.get("completed_at", ""),
            "error": task.get("error", ""),
        },
        "site_memory": enriched_monitor.get("site_memory") or {},
    }


def _compute_next_retry_at(*, attempt_no: int, max_attempts: int) -> str:
    normalized_attempt = max(int(attempt_no or 1), 1)
    normalized_max_attempts = max(int(max_attempts or 1), 1)
    if normalized_attempt >= normalized_max_attempts:
        return ""
    delay_index = min(normalized_attempt - 1, len(_RETRY_DELAYS_MINUTES) - 1)
    retry_at = datetime.now() + timedelta(minutes=_RETRY_DELAYS_MINUTES[delay_index])
    return current_timestamp(retry_at)


def _build_delivery_result(
    *,
    status: str,
    message: str,
    channel_type: str,
    target: str,
    payload_snapshot: dict[str, Any],
    response_code: int | None = None,
    error_type: str = "",
    error_message: str = "",
    should_retry: bool = False,
    attempt_no: int = 1,
    max_attempts: int = DEFAULT_NOTIFICATION_MAX_ATTEMPTS,
) -> NotificationDeliveryResult:
    next_retry_at = ""
    normalized_status = str(status or "").strip().lower()
    if should_retry:
        next_retry_at = _compute_next_retry_at(
            attempt_no=attempt_no,
            max_attempts=max_attempts,
        )
        if next_retry_at:
            normalized_status = "retry_pending"
            message = f"{message}，计划在 {next_retry_at} 重试"
        else:
            should_retry = False
    return NotificationDeliveryResult(
        status=normalized_status,
        message=str(message or "").strip(),
        channel_type=str(channel_type or "webhook").strip().lower() or "webhook",
        target=target,
        payload_snapshot=payload_snapshot,
        response_code=response_code,
        error_type=str(error_type or "").strip(),
        error_message=str(error_message or "").strip(),
        should_retry=bool(should_retry),
        next_retry_at=next_retry_at,
        sent_at=current_timestamp(),
        attempt_no=max(int(attempt_no or 1), 1),
        max_attempts=max(int(max_attempts or 1), 1),
    )


def _normalize_http_error_message(status_code: int) -> str:
    if status_code == 429:
        return "通知目标限流"
    if 500 <= status_code < 600:
        return f"通知目标服务异常（{status_code}）"
    if 400 <= status_code < 500:
        return f"通知请求参数无效（{status_code}）"
    return f"通知发送失败（{status_code}）"


def normalize_delivery_result(
    result: NotificationDeliveryResult | tuple[str, str],
    *,
    payload_snapshot: dict[str, Any],
    target: str,
    channel_type: str = "webhook",
    attempt_no: int = 1,
    max_attempts: int = DEFAULT_NOTIFICATION_MAX_ATTEMPTS,
) -> NotificationDeliveryResult:
    normalized_channel_type = (
        str(channel_type or "webhook").strip().lower() or "webhook"
    )
    if isinstance(result, NotificationDeliveryResult):
        normalized = result
        normalized.payload_snapshot = normalized.payload_snapshot or payload_snapshot
        normalized.target = normalized.target or target
        normalized.channel_type = (
            str(normalized.channel_type or normalized_channel_type).strip().lower()
            or normalized_channel_type
        )
        normalized.attempt_no = max(int(normalized.attempt_no or attempt_no), 1)
        normalized.max_attempts = max(int(normalized.max_attempts or max_attempts), 1)
        return normalized

    if isinstance(result, tuple) and len(result) >= 2:
        status, message = result[0], result[1]
        return NotificationDeliveryResult(
            status=str(status or "").strip().lower(),
            message=str(message or "").strip(),
            channel_type=normalized_channel_type,
            target=target,
            payload_snapshot=payload_snapshot,
            sent_at=current_timestamp(),
            attempt_no=max(int(attempt_no or 1), 1),
            max_attempts=max(int(max_attempts or 1), 1),
        )

    return NotificationDeliveryResult(
        status="failed",
        message="通知发送结果无效",
        channel_type=normalized_channel_type,
        target=target,
        payload_snapshot=payload_snapshot,
        error_type="invalid_result",
        error_message=f"unexpected result type: {type(result).__name__}",
        sent_at=current_timestamp(),
        attempt_no=max(int(attempt_no or 1), 1),
        max_attempts=max(int(max_attempts or 1), 1),
    )


def send_monitor_notification(
    monitor: dict[str, Any],
    task: dict[str, Any],
    timeout_seconds: float = 10.0,
    *,
    payload_override: dict[str, Any] | None = None,
    target_override: str = "",
    secret_override: str = "",
    channel_type_override: str = "",
    attempt_no: int = 1,
    max_attempts: int = DEFAULT_NOTIFICATION_MAX_ATTEMPTS,
) -> NotificationDeliveryResult:
    profile = monitor.get("profile") if isinstance(monitor.get("profile"), dict) else {}
    channel_type = (
        str(channel_type_override or "webhook").strip().lower() or "webhook"
    )
    webhook_url = str(target_override or profile.get("webhook_url") or "").strip()
    payload = payload_override or build_monitor_notification_payload(monitor, task)
    if not webhook_url:
        return NotificationDeliveryResult(
            status="skipped",
            message="未配置通知地址，已跳过发送",
            channel_type=channel_type,
            target=webhook_url,
            payload_snapshot=payload,
            error_type="missing_target",
            sent_at=current_timestamp(),
            attempt_no=max(int(attempt_no or 1), 1),
            max_attempts=max(int(max_attempts or 1), 1),
        )

    headers = {"Content-Type": "application/json", "User-Agent": "SmartExtractor/0.1"}
    secret = str(secret_override or profile.get("webhook_secret") or "").strip()
    if secret:
        headers["X-Smart-Extractor-Secret"] = secret

    try:
        with httpx.Client(timeout=timeout_seconds, follow_redirects=True) as client:
            response = client.post(webhook_url, json=payload, headers=headers)
        if 200 <= response.status_code < 300:
            logger.info(
                "Monitor notification sent: monitor_id={} channel_type={} status_code={}",
                monitor.get("monitor_id", "-"),
                channel_type,
                response.status_code,
            )
            return _build_delivery_result(
                status="sent",
                message=f"通知已发送到 {webhook_url}",
                channel_type=channel_type,
                target=webhook_url,
                payload_snapshot=payload,
                response_code=response.status_code,
                attempt_no=attempt_no,
                max_attempts=max_attempts,
            )

        should_retry_on_http = response.status_code == 429 or 500 <= response.status_code < 600
        logger.warning(
            "Monitor notification failed: monitor_id={} channel_type={} status_code={}",
            monitor.get("monitor_id", "-"),
            channel_type,
            response.status_code,
        )
        return _build_delivery_result(
            status="failed",
            message=_normalize_http_error_message(response.status_code),
            channel_type=channel_type,
            target=webhook_url,
            payload_snapshot=payload,
            response_code=response.status_code,
            error_type="http_error",
            error_message=_normalize_http_error_message(response.status_code),
            should_retry=should_retry_on_http,
            attempt_no=attempt_no,
            max_attempts=max_attempts,
        )
    except httpx.TimeoutException as exc:
        logger.warning(
            "Monitor notification timeout: monitor_id={} channel_type={} error={}",
            monitor.get("monitor_id", "-"),
            channel_type,
            exc,
        )
        return _build_delivery_result(
            status="failed",
            message="通知发送超时",
            channel_type=channel_type,
            target=webhook_url,
            payload_snapshot=payload,
            error_type="timeout",
            error_message=str(exc),
            should_retry=True,
            attempt_no=attempt_no,
            max_attempts=max_attempts,
        )
    except httpx.NetworkError as exc:
        logger.warning(
            "Monitor notification network error: monitor_id={} channel_type={} error={}",
            monitor.get("monitor_id", "-"),
            channel_type,
            exc,
        )
        return _build_delivery_result(
            status="failed",
            message="通知发送网络异常",
            channel_type=channel_type,
            target=webhook_url,
            payload_snapshot=payload,
            error_type="network_error",
            error_message=str(exc),
            should_retry=True,
            attempt_no=attempt_no,
            max_attempts=max_attempts,
        )
    except httpx.HTTPError as exc:
        logger.warning(
            "Monitor notification http error: monitor_id={} channel_type={} error={}",
            monitor.get("monitor_id", "-"),
            channel_type,
            exc,
        )
        return _build_delivery_result(
            status="failed",
            message=f"通知发送异常：{type(exc).__name__}",
            channel_type=channel_type,
            target=webhook_url,
            payload_snapshot=payload,
            error_type="http_client_error",
            error_message=str(exc),
            should_retry=True,
            attempt_no=attempt_no,
            max_attempts=max_attempts,
        )
    except Exception as exc:  # pragma: no cover
        logger.warning(
            "Monitor notification unexpected error: monitor_id={} channel_type={} error={}",
            monitor.get("monitor_id", "-"),
            channel_type,
            exc,
        )
        return _build_delivery_result(
            status="failed",
            message=f"通知发送异常：{type(exc).__name__}",
            channel_type=channel_type,
            target=webhook_url,
            payload_snapshot=payload,
            error_type="unexpected_error",
            error_message=str(exc),
            attempt_no=attempt_no,
            max_attempts=max_attempts,
        )
