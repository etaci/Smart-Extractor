"""Automatic notification retry service."""

from __future__ import annotations

import threading
from typing import Any, Callable
from uuid import uuid4

from loguru import logger

from smart_extractor.web.monitor_schedule import current_timestamp
from smart_extractor.web.notification_center import dispatch_notification_attempt


class NotificationRetryService:
    """Periodically retries notification events in retry_pending state."""

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
        self.service_id = str(service_id or f"notification-retry-{uuid4().hex[:8]}")
        self.batch_size = max(int(batch_size or 1), 1)
        self._runtime_lock = threading.Lock()
        self._runtime_stats: dict[str, Any] = {
            "active": False,
            "last_run_started_at": "",
            "last_run_completed_at": "",
            "last_claimed_count": 0,
            "last_retried_count": 0,
            "last_failed_count": 0,
            "last_error": "",
            "total_runs": 0,
            "total_claimed_count": 0,
            "total_retried_count": 0,
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

    def run_once(self) -> int:
        started_at = current_timestamp()
        self._update_runtime_stats(
            active=True,
            last_run_started_at=started_at,
            last_error="",
        )
        due_events = self._task_store.list_due_notification_retries(
            due_before=started_at,
            limit=self.batch_size,
        )
        retried_count = 0
        failed_count = 0
        last_error = ""

        for event in due_events:
            try:
                self._task_store.update_notification_event(
                    event.notification_id,
                    status="retrying",
                    status_message="系统正在发起自动重试",
                    next_retry_at="",
                )
                retry_event = dispatch_notification_attempt(
                    source_event=event,
                    task_store=self._task_store,
                    send_monitor_notification_fn=self._send_monitor_notification_fn,
                    triggered_by="retry",
                    reason="自动重试服务执行",
                )
                if retry_event is not None:
                    retried_count += 1
            except Exception as exc:  # pragma: no cover
                failed_count += 1
                last_error = str(exc) or "通知自动重试失败"
                logger.exception("通知自动重试失败: {}", event.notification_id)

        completed_at = current_timestamp()
        snapshot = self.runtime_snapshot()
        self._update_runtime_stats(
            active=False,
            last_run_completed_at=completed_at,
            last_claimed_count=len(due_events),
            last_retried_count=retried_count,
            last_failed_count=failed_count,
            last_error=last_error,
            total_runs=int(snapshot.get("total_runs") or 0) + 1,
            total_claimed_count=int(snapshot.get("total_claimed_count") or 0)
            + len(due_events),
            total_retried_count=int(snapshot.get("total_retried_count") or 0)
            + retried_count,
            total_failed_count=int(snapshot.get("total_failed_count") or 0)
            + failed_count,
        )
        return retried_count

    def run_forever(
        self,
        *,
        poll_interval_seconds: float = 20.0,
        stop_event: threading.Event | None = None,
    ) -> None:
        interval = max(float(poll_interval_seconds or 0.0), 0.5)
        external_stop_event = stop_event or threading.Event()
        logger.info("通知自动重试服务启动: {}", self.service_id)
        try:
            while not external_stop_event.is_set():
                processed = self.run_once()
                if processed > 0:
                    continue
                external_stop_event.wait(interval)
        finally:
            self._update_runtime_stats(active=False)
            logger.info("通知自动重试服务停止: {}", self.service_id)


class ManagedNotificationRetryThread:
    """Managed thread wrapper for notification retry service."""

    def __init__(
        self,
        *,
        service: NotificationRetryService,
        poll_interval_seconds: float = 20.0,
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
