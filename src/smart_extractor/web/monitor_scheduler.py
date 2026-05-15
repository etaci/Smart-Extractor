"""监控自动巡检调度服务。"""

from __future__ import annotations

import threading
from typing import Any, Callable
from uuid import uuid4

from loguru import logger

from smart_extractor.web.monitor_schedule import current_timestamp


def _trigger_with_optional_tenant(
    trigger_monitor_run: Callable[..., str | None],
    monitor,
    *,
    scheduler_id: str,
) -> str | None:
    try:
        return trigger_monitor_run(
            monitor.monitor_id,
            "auto",
            claimed_by=scheduler_id,
            tenant_id=monitor.tenant_id,
        )
    except TypeError as exc:
        if "tenant_id" not in str(exc):
            raise
        return trigger_monitor_run(
            monitor.monitor_id,
            "auto",
            claimed_by=scheduler_id,
        )


class MonitorScheduler:
    """按调度周期触发监控巡检。"""

    def __init__(
        self,
        *,
        task_store,
        trigger_monitor_run: Callable[..., str | None],
        scheduler_id: str = "",
        batch_size: int = 5,
        lease_seconds: float = 120.0,
    ) -> None:
        self._task_store = task_store
        self._trigger_monitor_run = trigger_monitor_run
        self.scheduler_id = str(scheduler_id or f"scheduler-{uuid4().hex[:8]}")
        self.batch_size = max(int(batch_size or 1), 1)
        self.lease_seconds = max(float(lease_seconds or 0.0), 5.0)
        self._runtime_lock = threading.Lock()
        self._runtime_stats: dict[str, Any] = {
            "active": False,
            "last_run_started_at": "",
            "last_run_completed_at": "",
            "last_claimed_count": 0,
            "last_triggered_count": 0,
            "last_failed_count": 0,
            "last_reclaimed_count": 0,
            "last_skipped_active_task_count": 0,
            "last_error": "",
            "total_runs": 0,
            "total_claimed_count": 0,
            "total_triggered_count": 0,
            "total_failed_count": 0,
            "total_reclaimed_count": 0,
        }

    def _update_runtime_stats(self, **fields: Any) -> None:
        with self._runtime_lock:
            self._runtime_stats.update(fields)

    def runtime_snapshot(self) -> dict[str, Any]:
        with self._runtime_lock:
            snapshot = dict(self._runtime_stats)
        snapshot["scheduler_id"] = self.scheduler_id
        snapshot["batch_size"] = self.batch_size
        snapshot["lease_seconds"] = self.lease_seconds
        return snapshot

    def run_once(self) -> int:
        started_at = current_timestamp()
        self._update_runtime_stats(
            active=True,
            last_run_started_at=started_at,
            last_error="",
        )

        claim_summary = self._task_store.claim_due_monitors_with_summary(
            due_before=started_at,
            claimer_id=self.scheduler_id,
            lease_seconds=self.lease_seconds,
            limit=self.batch_size,
            tenant_id="*",
        )
        due_monitors = list(claim_summary.get("monitors") or [])
        failed_count = 0
        triggered_count = 0
        last_error = ""

        for monitor in due_monitors:
            try:
                task_id = _trigger_with_optional_tenant(
                    self._trigger_monitor_run,
                    monitor,
                    scheduler_id=self.scheduler_id,
                )
                if task_id:
                    triggered_count += 1
                    logger.info(
                        "监控自动巡检已触发: monitor_id={} task_id={}",
                        monitor.monitor_id,
                        task_id,
                    )
            except Exception as exc:
                failed_count += 1
                last_error = str(exc) or "调度触发失败"
                self._task_store.fail_monitor_claim(
                    monitor.monitor_id,
                    error=last_error,
                    claimed_by=self.scheduler_id,
                    tenant_id=monitor.tenant_id,
                )
                logger.exception("监控自动巡检触发失败: {}", monitor.monitor_id)

        completed_at = current_timestamp()
        snapshot = self.runtime_snapshot()
        self._update_runtime_stats(
            active=False,
            last_run_completed_at=completed_at,
            last_claimed_count=int(claim_summary.get("claimed_count") or 0),
            last_triggered_count=triggered_count,
            last_failed_count=failed_count,
            last_reclaimed_count=int(claim_summary.get("reclaimed_count") or 0),
            last_skipped_active_task_count=int(
                claim_summary.get("skipped_active_task_count") or 0
            ),
            last_error=last_error,
            total_runs=int(snapshot.get("total_runs") or 0) + 1,
            total_claimed_count=int(snapshot.get("total_claimed_count") or 0)
            + int(claim_summary.get("claimed_count") or 0),
            total_triggered_count=int(snapshot.get("total_triggered_count") or 0)
            + triggered_count,
            total_failed_count=int(snapshot.get("total_failed_count") or 0)
            + failed_count,
            total_reclaimed_count=int(snapshot.get("total_reclaimed_count") or 0)
            + int(claim_summary.get("reclaimed_count") or 0),
        )
        return triggered_count

    def run_forever(
        self,
        *,
        poll_interval_seconds: float = 15.0,
        stop_event: threading.Event | None = None,
    ) -> None:
        interval = max(float(poll_interval_seconds or 0.0), 0.5)
        external_stop_event = stop_event or threading.Event()
        logger.info("监控调度服务启动: {}", self.scheduler_id)
        try:
            while not external_stop_event.is_set():
                processed = self.run_once()
                if processed > 0:
                    continue
                external_stop_event.wait(interval)
        finally:
            self._update_runtime_stats(active=False)
            logger.info("监控调度服务停止: {}", self.scheduler_id)


class ManagedMonitorSchedulerThread:
    """应用内托管的监控调度线程。"""

    def __init__(
        self,
        *,
        scheduler: MonitorScheduler,
        poll_interval_seconds: float = 15.0,
    ) -> None:
        self._scheduler = scheduler
        self._poll_interval_seconds = max(float(poll_interval_seconds or 0.0), 0.5)
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._scheduler.run_forever,
            kwargs={
                "poll_interval_seconds": self._poll_interval_seconds,
                "stop_event": self._stop_event,
            },
            name=f"smart-extractor-{self._scheduler.scheduler_id}",
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

    @property
    def scheduler_id(self) -> str:
        return self._scheduler.scheduler_id

    @property
    def poll_interval_seconds(self) -> float:
        return self._poll_interval_seconds

    def runtime_snapshot(self) -> dict[str, Any]:
        snapshot = self._scheduler.runtime_snapshot()
        snapshot["is_alive"] = self.is_alive
        snapshot["poll_interval_seconds"] = self._poll_interval_seconds
        return snapshot
