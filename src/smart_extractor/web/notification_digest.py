"""Automatic daily notification digest service."""

from __future__ import annotations

import threading
from datetime import datetime
from typing import Any, Callable
from uuid import uuid4

from loguru import logger

from smart_extractor.web.monitor_schedule import current_timestamp
from smart_extractor.web.notification_center import (
    collect_digest_target_configs,
    dispatch_digest_notifications,
)


def _start_of_day(now: datetime) -> str:
    return current_timestamp(
        now.replace(hour=0, minute=0, second=0, microsecond=0)
    )


class NotificationDigestService:
    """Periodically sends daily digests for eligible monitor targets."""

    def __init__(
        self,
        *,
        task_store,
        send_monitor_notification_fn: Callable[..., object],
        service_id: str = "",
        batch_size: int = 10,
    ) -> None:
        self._task_store = task_store
        self._send_monitor_notification_fn = send_monitor_notification_fn
        self.service_id = str(service_id or f"notification-digest-{uuid4().hex[:8]}")
        self.batch_size = max(int(batch_size or 1), 1)
        self._runtime_lock = threading.Lock()
        self._runtime_stats: dict[str, Any] = {
            "active": False,
            "last_run_started_at": "",
            "last_run_completed_at": "",
            "last_claimed_count": 0,
            "last_sent_count": 0,
            "last_retry_pending_count": 0,
            "last_failed_count": 0,
            "last_skipped_sent_today_count": 0,
            "last_error": "",
            "total_runs": 0,
            "total_claimed_count": 0,
            "total_sent_count": 0,
            "total_retry_pending_count": 0,
            "total_failed_count": 0,
        }

    def _update_runtime_stats(self, **fields: Any) -> None:
        with self._runtime_lock:
            self._runtime_stats.update(fields)

    def runtime_snapshot(self) -> dict[str, Any]:
        with self._runtime_lock:
            snapshot = dict(self._runtime_stats)
        snapshot["service_id"] = self.service_id
        snapshot["batch_size"] = self.batch_size
        return snapshot

    def _list_due_target_configs(self, now: datetime) -> tuple[list[dict[str, Any]], int]:
        monitors = self._task_store.list_monitors(limit=max(self.batch_size * 20, 200))
        candidate_configs = collect_digest_target_configs(
            monitors=monitors,
            digest_enabled_only=True,
            due_hour=now.hour,
        )
        if not candidate_configs:
            return [], 0

        sent_today = {
            (
                str(item.channel_type or "webhook").strip().lower() or "webhook",
                str(item.target or "").strip(),
            )
            for item in self._task_store.list_notification_events(
                limit=500,
                event_type="daily_digest",
                created_after=_start_of_day(now),
            )
            if str(item.target or "").strip()
        }
        due_configs: list[dict[str, Any]] = []
        skipped_sent_today_count = 0
        for item in candidate_configs:
            target_key = (
                str(item.get("channel_type") or "webhook").strip().lower() or "webhook",
                str(item.get("target") or "").strip(),
            )
            if target_key in sent_today:
                skipped_sent_today_count += 1
                continue
            due_configs.append(item)
        return due_configs[: self.batch_size], skipped_sent_today_count

    def run_once(self) -> int:
        reference_time = datetime.now()
        started_at = current_timestamp(reference_time)
        self._update_runtime_stats(
            active=True,
            last_run_started_at=started_at,
            last_error="",
        )
        due_configs, skipped_sent_today_count = self._list_due_target_configs(reference_time)
        sent_count = 0
        retry_pending_count = 0
        failed_count = 0
        last_error = ""

        for target_config in due_configs:
            target = str(target_config.get("target") or "").strip()
            try:
                events = dispatch_digest_notifications(
                    task_store=self._task_store,
                    send_monitor_notification_fn=self._send_monitor_notification_fn,
                    window_hours=24,
                    target_configs=[target_config],
                    now=reference_time,
                )
                for event in events:
                    if event.status == "sent":
                        sent_count += 1
                    elif event.status == "retry_pending":
                        retry_pending_count += 1
                    elif event.status == "failed":
                        failed_count += 1
                logger.info("Digest 发送完成: target={}", target)
            except Exception as exc:  # pragma: no cover
                failed_count += 1
                last_error = str(exc) or "Digest 发送失败"
                logger.exception("Digest 发送失败: target={}", target)

        completed_at = current_timestamp()
        snapshot = self.runtime_snapshot()
        self._update_runtime_stats(
            active=False,
            last_run_completed_at=completed_at,
            last_claimed_count=len(due_configs),
            last_sent_count=sent_count,
            last_retry_pending_count=retry_pending_count,
            last_failed_count=failed_count,
            last_skipped_sent_today_count=skipped_sent_today_count,
            last_error=last_error,
            total_runs=int(snapshot.get("total_runs") or 0) + 1,
            total_claimed_count=int(snapshot.get("total_claimed_count") or 0)
            + len(due_configs),
            total_sent_count=int(snapshot.get("total_sent_count") or 0) + sent_count,
            total_retry_pending_count=int(snapshot.get("total_retry_pending_count") or 0)
            + retry_pending_count,
            total_failed_count=int(snapshot.get("total_failed_count") or 0)
            + failed_count,
        )
        return sent_count + retry_pending_count

    def run_forever(
        self,
        *,
        poll_interval_seconds: float = 60.0,
        stop_event: threading.Event | None = None,
    ) -> None:
        interval = max(float(poll_interval_seconds or 0.0), 0.5)
        external_stop_event = stop_event or threading.Event()
        logger.info("Digest 服务启动: {}", self.service_id)
        try:
            while not external_stop_event.is_set():
                processed = self.run_once()
                if processed > 0:
                    continue
                external_stop_event.wait(interval)
        finally:
            self._update_runtime_stats(active=False)
            logger.info("Digest 服务停止: {}", self.service_id)


class ManagedNotificationDigestThread:
    """Managed thread wrapper for notification digest service."""

    def __init__(
        self,
        *,
        service: NotificationDigestService,
        poll_interval_seconds: float = 60.0,
    ) -> None:
        self._service = service
        self._poll_interval_seconds = max(float(poll_interval_seconds or 0.0), 0.5)
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._service.run_forever,
            kwargs={
                "poll_interval_seconds": self._poll_interval_seconds,
                "stop_event": self._stop_event,
            },
            name=f"smart-extractor-{self._service.service_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout_seconds: float = 5.0) -> None:
        self._stop_event.set()
        self._thread.join(timeout=max(float(timeout_seconds or 0.0), 0.1))

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def runtime_snapshot(self) -> dict[str, Any]:
        snapshot = self._service.runtime_snapshot()
        snapshot["is_alive"] = self.is_alive
        snapshot["poll_interval_seconds"] = self._poll_interval_seconds
        return snapshot
