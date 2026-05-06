"""Notification retry and digest helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from smart_extractor.web.management_helpers import (
    find_notification_channel,
    normalize_digest_hour,
    notification_channels_from_profile,
)
from smart_extractor.web.monitor_schedule import current_timestamp, parse_timestamp
from smart_extractor.web.notifier import normalize_delivery_result


def _window_start(*, now: datetime, window_hours: int) -> str:
    normalized_hours = max(int(window_hours or 24), 1)
    return current_timestamp(now - timedelta(hours=normalized_hours))


def _collect_digest_events(
    *,
    task_store,
    window_hours: int = 24,
    now: datetime | None = None,
    tenant_id: str = "",
) -> list[Any]:
    reference_time = now or datetime.now()
    return task_store.list_notification_events(
        limit=500,
        created_after=_window_start(now=reference_time, window_hours=window_hours),
        tenant_id=tenant_id,
    )


def _normalize_monitor_ids(monitor_ids: list[str] | None = None) -> set[str]:
    return {
        str(item or "").strip()
        for item in (monitor_ids or [])
        if str(item or "").strip()
    }


def _normalize_targets(targets: list[str] | None = None) -> set[str]:
    return {
        str(item or "").strip()
        for item in (targets or [])
        if str(item or "").strip()
    }


def collect_digest_target_configs(
    *,
    monitors: list[Any],
    digest_enabled_only: bool = False,
    due_hour: int | None = None,
) -> list[dict[str, Any]]:
    target_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    for monitor in monitors:
        tenant_id = str(getattr(monitor, "tenant_id", "") or "").strip() or "default"
        profile = monitor.profile if isinstance(monitor.profile, dict) else {}
        digest_enabled = bool(profile.get("digest_enabled"))
        if digest_enabled_only and not digest_enabled:
            continue

        digest_hour = normalize_digest_hour(profile.get("digest_hour", 9))
        if due_hour is not None and digest_hour != int(due_hour):
            continue

        for channel in notification_channels_from_profile(profile):
            if not bool(channel.get("enabled", True)):
                continue
            target = str(channel.get("target") or "").strip()
            channel_type = (
                str(channel.get("channel_type") or "webhook").strip().lower()
                or "webhook"
            )
            if not target:
                continue

            config = target_map.setdefault(
                (tenant_id, channel_type, target),
                {
                    "tenant_id": tenant_id,
                    "channel_type": channel_type,
                    "target": target,
                    "secret": "",
                    "name": str(channel.get("name") or "").strip(),
                    "monitor_ids": [],
                    "digest_enabled": False,
                    "digest_hour": digest_hour,
                },
            )
            secret = str(channel.get("secret") or "").strip()
            if secret and not config["secret"]:
                config["secret"] = secret
            if monitor.monitor_id not in config["monitor_ids"]:
                config["monitor_ids"].append(monitor.monitor_id)
            if digest_enabled:
                config["digest_enabled"] = True
                config["digest_hour"] = min(
                    int(config.get("digest_hour", digest_hour)),
                    digest_hour,
                )
    return list(target_map.values())


def _normalize_target_configs(
    target_configs: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    normalized_configs: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in target_configs or []:
        if not isinstance(item, dict):
            continue
        tenant_id = str(item.get("tenant_id") or "").strip() or "default"
        target = str(item.get("target") or "").strip()
        channel_type = str(item.get("channel_type") or "webhook").strip().lower() or "webhook"
        if not target:
            continue
        key = (tenant_id, channel_type, target)
        config = normalized_configs.setdefault(
            key,
            {
                "tenant_id": tenant_id,
                "channel_type": channel_type,
                "target": target,
                "secret": "",
                "name": str(item.get("name") or "").strip(),
                "monitor_ids": [],
            },
        )
        secret = str(item.get("secret") or "").strip()
        if secret and not config["secret"]:
            config["secret"] = secret
        if item.get("name") and not config["name"]:
            config["name"] = str(item.get("name") or "").strip()
        for monitor_id in item.get("monitor_ids") or []:
            normalized_monitor_id = str(monitor_id or "").strip()
            if (
                normalized_monitor_id
                and normalized_monitor_id not in config["monitor_ids"]
            ):
                config["monitor_ids"].append(normalized_monitor_id)
    return list(normalized_configs.values())


def dispatch_notification_attempt(
    *,
    source_event,
    task_store,
    send_monitor_notification_fn,
    triggered_by: str,
    reason: str = "",
) -> Any:
    tenant_id = str(getattr(source_event, "tenant_id", "") or "").strip()
    monitor = (
        task_store.get_monitor(source_event.monitor_id, tenant_id=tenant_id)
        if str(source_event.monitor_id or "").strip()
        else None
    )
    task = (
        task_store.get(source_event.task_id, tenant_id=tenant_id)
        if str(source_event.task_id or "").strip()
        else None
    )
    monitor_payload = monitor.to_dict() if monitor is not None else {}
    task_payload = task.to_dict() if task is not None else {}
    payload_snapshot = dict(source_event.payload_snapshot or {})
    profile = (
        monitor_payload.get("profile")
        if isinstance(monitor_payload.get("profile"), dict)
        else {}
    )
    matched_channel = find_notification_channel(
        profile,
        target=str(source_event.target or "").strip(),
        channel_type=str(source_event.channel_type or "").strip(),
    )
    target = str(
        source_event.target
        or matched_channel.get("target")
        or profile.get("webhook_url")
        or ""
    ).strip()
    channel_type = str(
        source_event.channel_type
        or matched_channel.get("channel_type")
        or "webhook"
    ).strip().lower() or "webhook"
    fallback_secret = ""
    if target and target == str(profile.get("webhook_url") or "").strip():
        fallback_secret = str(profile.get("webhook_secret") or "").strip()
    secret = str(matched_channel.get("secret") or fallback_secret or "").strip()

    next_attempt_no = max(int(source_event.attempt_no or 1), 1) + 1
    max_attempts = max(int(source_event.max_attempts or 1), 1)
    result = normalize_delivery_result(
        send_monitor_notification_fn(
            monitor_payload,
            task_payload,
            payload_override=payload_snapshot,
            target_override=target,
            secret_override=secret,
            channel_type_override=channel_type,
            attempt_no=next_attempt_no,
            max_attempts=max_attempts,
        ),
        payload_snapshot=payload_snapshot,
        target=target,
        channel_type=channel_type,
        attempt_no=next_attempt_no,
        max_attempts=max_attempts,
    )
    retry_event = task_store.create_notification_event(
        monitor_id=source_event.monitor_id,
        task_id=source_event.task_id,
        channel_type=result.channel_type,
        target=result.target,
        event_type=source_event.event_type,
        status=result.status,
        status_message=result.message,
        attempt_no=result.attempt_no,
        max_attempts=result.max_attempts,
        next_retry_at=result.next_retry_at,
        response_code=result.response_code,
        error_type=result.error_type,
        error_message=result.error_message,
        payload_snapshot=result.payload_snapshot,
        sent_at=result.sent_at,
        retry_of_notification_id=source_event.notification_id,
        triggered_by=triggered_by,
        tenant_id=tenant_id,
    )
    source_message = (
        f"已转入第 {retry_event.attempt_no} 次尝试"
        + (f"，原因：{reason}" if str(reason or "").strip() else "")
    )
    if source_event.status == "retry_pending":
        task_store.update_notification_event(
            source_event.notification_id,
            status="retried",
            status_message=source_message,
            next_retry_at="",
            tenant_id=tenant_id,
        )
    if str(source_event.monitor_id or "").strip():
        task_store.update_monitor_notification(
            source_event.monitor_id,
            status=retry_event.status,
            message=retry_event.status_message,
            tenant_id=tenant_id,
        )
    return retry_event


def build_notification_digest(
    *,
    task_store,
    window_hours: int = 24,
    now: datetime | None = None,
    monitor_ids: list[str] | None = None,
    targets: list[str] | None = None,
    tenant_id: str = "",
) -> dict[str, Any]:
    reference_time = now or datetime.now()
    selected_monitor_ids = _normalize_monitor_ids(monitor_ids)
    selected_targets = _normalize_targets(targets)
    events = _collect_digest_events(
        task_store=task_store,
        window_hours=window_hours,
        now=reference_time,
        tenant_id=tenant_id,
    )
    monitors = {
        item.monitor_id: item
        for item in task_store.list_monitors(limit=200, tenant_id=tenant_id)
    }
    if selected_monitor_ids:
        monitors = {
            monitor_id: monitor
            for monitor_id, monitor in monitors.items()
            if monitor_id in selected_monitor_ids
        }

    alert_events = [
        item
        for item in events
        if str(item.event_type or "").strip() == "monitor_alert"
        and (not selected_monitor_ids or item.monitor_id in selected_monitor_ids)
        and (not selected_targets or str(item.target or "").strip() in selected_targets)
    ]
    digest_events = [
        item
        for item in events
        if str(item.event_type or "").strip() == "daily_digest"
        and (not selected_targets or str(item.target or "").strip() in selected_targets)
    ]
    filtered_events = [
        item
        for item in events
        if item in alert_events
        or item in digest_events
        or (
            not selected_monitor_ids
            and not selected_targets
            and str(item.event_type or "").strip() not in {"monitor_alert", "daily_digest"}
        )
    ]

    monitor_rows: list[dict[str, Any]] = []
    for monitor_id, monitor in monitors.items():
        related_events = [item for item in alert_events if item.monitor_id == monitor_id]
        if not related_events:
            continue
        latest_event = sorted(
            related_events,
            key=lambda item: item.created_at,
            reverse=True,
        )[0]
        monitor_rows.append(
            {
                "monitor_id": monitor_id,
                "name": monitor.name,
                "url": monitor.url,
                "alert_level": monitor.last_alert_level,
                "alert_message": monitor.last_alert_message,
                "notification_count": len(related_events),
                "failed_count": sum(
                    1
                    for item in related_events
                    if str(item.status or "").strip() == "failed"
                ),
                "retry_pending_count": sum(
                    1
                    for item in related_events
                    if str(item.status or "").strip() == "retry_pending"
                ),
                "changed_fields_count": len(monitor.last_changed_fields or []),
                "latest_notification_status": latest_event.status,
                "latest_notification_at": latest_event.created_at,
            }
        )
    monitor_rows.sort(
        key=lambda item: (
            -int(item["failed_count"]),
            -int(item["retry_pending_count"]),
            -int(item["notification_count"]),
            str(item["name"] or ""),
        )
    )

    summary = {
        "window_hours": max(int(window_hours or 24), 1),
        "window_start": _window_start(now=reference_time, window_hours=window_hours),
        "window_end": current_timestamp(reference_time),
        "total_notifications": len(filtered_events),
        "monitor_alert_notifications": len(alert_events),
        "digest_notifications": len(digest_events),
        "unique_monitors": len({item.monitor_id for item in alert_events if item.monitor_id}),
        "sent_count": sum(1 for item in filtered_events if item.status == "sent"),
        "failed_count": sum(1 for item in filtered_events if item.status == "failed"),
        "retry_pending_count": sum(
            1 for item in filtered_events if item.status == "retry_pending"
        ),
        "manual_resend_count": sum(
            1 for item in filtered_events if str(item.triggered_by or "").strip() == "manual"
        ),
        "auto_retry_count": sum(
            1 for item in filtered_events if str(item.triggered_by or "").strip() == "retry"
        ),
        "changed_monitor_count": sum(
            1 for item in monitor_rows if str(item["alert_level"] or "").strip() == "changed"
        ),
        "error_monitor_count": sum(
            1 for item in monitor_rows if str(item["alert_level"] or "").strip() == "error"
        ),
    }

    actions: list[str] = []
    if summary["retry_pending_count"] > 0:
        actions.append(f"还有 {summary['retry_pending_count']} 条通知等待系统重试")
    if summary["failed_count"] > 0:
        actions.append(f"有 {summary['failed_count']} 条通知最终失败，建议人工复核目标通道配置")
    if summary["changed_monitor_count"] > 0:
        actions.append(f"有 {summary['changed_monitor_count']} 个监控在窗口内发生页面变化")
    if not actions:
        actions.append("通知链路整体稳定，当前窗口内没有需要升级处理的异常")

    return {
        "summary": summary,
        "recommended_actions": actions,
        "top_monitors": monitor_rows[:8],
        "recent_failures": [
            {
                "notification_id": item.notification_id,
                "monitor_id": item.monitor_id,
                "status": item.status,
                "status_message": item.status_message,
                "target": item.target,
                "created_at": item.created_at,
            }
            for item in filtered_events
            if str(item.status or "").strip() in {"failed", "retry_pending"}
        ][:8],
    }


def build_notification_digest_payload(
    *,
    digest: dict[str, Any],
) -> dict[str, Any]:
    return {
        "event": "smart_extractor.daily_digest",
        "window": {
            "hours": digest["summary"]["window_hours"],
            "start": digest["summary"]["window_start"],
            "end": digest["summary"]["window_end"],
        },
        "summary": digest["summary"],
        "recommended_actions": digest["recommended_actions"],
        "top_monitors": digest["top_monitors"],
        "recent_failures": digest["recent_failures"],
    }


def dispatch_digest_notifications(
    *,
    task_store,
    send_monitor_notification_fn,
    window_hours: int = 24,
    target_configs: list[dict[str, Any]] | None = None,
    now: datetime | None = None,
    tenant_id: str = "",
) -> list[Any]:
    monitors = task_store.list_monitors(limit=200, tenant_id=tenant_id or "*")
    normalized_target_configs = _normalize_target_configs(target_configs)
    if not normalized_target_configs:
        normalized_target_configs = collect_digest_target_configs(monitors=monitors)

    events = []
    for target_config in normalized_target_configs:
        config_tenant_id = str(target_config.get("tenant_id") or tenant_id or "").strip()
        if not config_tenant_id:
            config_tenant_id = "default"
        target = str(target_config.get("target") or "").strip()
        channel_type = (
            str(target_config.get("channel_type") or "webhook").strip().lower()
            or "webhook"
        )
        if not target:
            continue
        digest = build_notification_digest(
            task_store=task_store,
            window_hours=window_hours,
            now=now,
            monitor_ids=list(target_config.get("monitor_ids") or []),
            targets=[target],
            tenant_id=config_tenant_id,
        )
        digest_payload = build_notification_digest_payload(digest=digest)
        result = normalize_delivery_result(
            send_monitor_notification_fn(
                {},
                {},
                payload_override=digest_payload,
                target_override=target,
                secret_override=str(target_config.get("secret") or "").strip(),
                channel_type_override=channel_type,
                attempt_no=1,
                max_attempts=3,
            ),
            payload_snapshot=digest_payload,
            target=target,
            channel_type=channel_type,
            attempt_no=1,
            max_attempts=3,
        )
        events.append(
            task_store.create_notification_event(
                monitor_id="",
                task_id="",
                channel_type=result.channel_type,
                target=result.target,
                event_type="daily_digest",
                status=result.status,
                status_message=result.message,
                attempt_no=result.attempt_no,
                max_attempts=result.max_attempts,
                next_retry_at=result.next_retry_at,
                response_code=result.response_code,
                error_type=result.error_type,
                error_message=result.error_message,
                payload_snapshot=result.payload_snapshot,
                sent_at=result.sent_at,
                triggered_by="system",
                tenant_id=config_tenant_id,
            )
        )
    return events


def is_notification_due(event, *, now: datetime | None = None) -> bool:
    retry_at = parse_timestamp(getattr(event, "next_retry_at", ""))
    if retry_at is None:
        return False
    return retry_at <= (now or datetime.now())
