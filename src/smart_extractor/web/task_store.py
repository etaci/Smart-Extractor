"""
Web task persistence and insight helpers backed by SQLite.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from loguru import logger
from smart_extractor.web.task_activity import (
    build_initial_batch_payload,
    build_learned_profile_activity_payload,
)
from smart_extractor.web.task_insights import build_dashboard_insights_payload
from smart_extractor.web.task_models import (
    MonitorRecord,
    NotificationEventRecord,
    TaskRecord,
    TemplateRecord,
)
from smart_extractor.web.task_store_runtime import (
    build_failed_fields,
    build_parent_refresh_fields,
    build_progress_fields,
    build_queued_fields,
    build_running_fields,
    build_success_fields,
)
from smart_extractor.web.task_store_schema import initialize_task_store_schema
from smart_extractor.web.task_store_history import (
    build_comparison_with_previous,
    build_task_detail_payload as build_history_task_detail_payload,
    fetch_history_summary,
    fetch_previous_success,
    fetch_tasks_by_learned_profile,
    fetch_tasks_by_url,
)
from smart_extractor.web.task_store_monitors import (
    claim_due_monitors,
    claim_due_monitors_batch,
    fail_monitor_claim,
    fetch_due_monitors,
    fetch_monitor,
    fetch_monitors,
    mark_monitor_run_scheduled,
    pause_monitor_schedule,
    persist_monitor_notification,
    persist_monitor_result,
    resume_monitor_schedule,
    upsert_monitor,
)
from smart_extractor.web.task_store_notifications import (
    create_notification_event,
    fetch_due_notification_retries,
    fetch_notification_event,
    fetch_notification_events,
    update_notification_event,
)
from smart_extractor.web.task_store_queue import (
    claim_queued_task_payload,
    complete_task_payload,
    enqueue_task_payload,
    fail_task_payload,
)
from smart_extractor.web.task_store_templates import (
    fetch_template,
    fetch_templates,
    touch_template,
    upsert_template,
)
from smart_extractor.web.task_store_tasks import (
    create_task,
    fetch_child_tasks,
    fetch_root_tasks,
    fetch_task,
    fetch_task_stats,
    update_task_fields,
)


class SQLiteTaskStore:
    """Thread-safe SQLite task store."""

    _ALLOWED_UPDATE_FIELDS = {
        "request_id",
        "status",
        "completed_at",
        "elapsed_ms",
        "quality_score",
        "progress_percent",
        "progress_stage",
        "data_json",
        "error",
        "batch_group_id",
        "task_kind",
        "parent_task_id",
        "total_items",
        "completed_items",
    }

    def __init__(
        self,
        db_path: str | Path,
        *,
        sqlite_busy_timeout_ms: int = 5000,
        sqlite_enable_wal: bool = True,
        sqlite_synchronous: str = "NORMAL",
    ):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._sqlite_busy_timeout_ms = max(int(sqlite_busy_timeout_ms or 0), 0)
        self._sqlite_enable_wal = bool(sqlite_enable_wal)
        normalized_synchronous = (
            str(sqlite_synchronous or "NORMAL").strip().upper() or "NORMAL"
        )
        self._sqlite_synchronous = (
            normalized_synchronous
            if normalized_synchronous in {"OFF", "NORMAL", "FULL", "EXTRA"}
            else "NORMAL"
        )
        self._init_db()
        logger.info("Web task store initialized: {}", self._db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            timeout=max(self._sqlite_busy_timeout_ms / 1000, 1.0),
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"PRAGMA busy_timeout = {self._sqlite_busy_timeout_ms}")
        if self._sqlite_enable_wal:
            conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(f"PRAGMA synchronous = {self._sqlite_synchronous}")
        conn.execute("PRAGMA temp_store = MEMORY")
        return conn

    def _init_db(self) -> None:
        initialize_task_store_schema(connect=self._connect)

    def create(
        self,
        url: str,
        schema_name: str,
        storage_format: str,
        request_id: str = "-",
        batch_group_id: str = "",
        task_kind: str = "single",
        parent_task_id: str = "",
        total_items: int = 0,
    ) -> TaskRecord:
        task_id = create_task(
            lock=self._lock,
            connect=self._connect,
            url=url,
            schema_name=schema_name,
            storage_format=storage_format,
            request_id=request_id,
            batch_group_id=batch_group_id,
            task_kind=task_kind,
            parent_task_id=parent_task_id,
            total_items=total_items,
        )
        task = self.get(task_id)
        if task is None:
            raise RuntimeError(f"failed to load created task: {task_id}")
        return task

    def create_batch_root(
        self,
        urls: list[str],
        schema_name: str,
        storage_format: str,
        request_id: str = "-",
        batch_group_id: str = "",
    ) -> TaskRecord:
        normalized_urls = [
            str(url or "").strip() for url in urls if str(url or "").strip()
        ]
        summary_url = f"批量任务（{len(normalized_urls)} 个 URL）"
        task = self.create(
            url=summary_url,
            schema_name=schema_name,
            storage_format=storage_format,
            request_id=request_id,
            batch_group_id=batch_group_id,
            task_kind="batch",
            total_items=len(normalized_urls),
        )
        self._update_fields(
            task.task_id,
            data_json=json.dumps(
                build_initial_batch_payload(normalized_urls),
                ensure_ascii=False,
            ),
        )
        refreshed = self.get(task.task_id)
        if refreshed is None:
            raise RuntimeError(f"failed to load batch root task: {task.task_id}")
        return refreshed

    def get(self, task_id: str) -> Optional[TaskRecord]:
        return fetch_task(connect=self._connect, task_id=task_id)

    def create_or_update_template(
        self,
        *,
        name: str,
        url: str,
        page_type: str,
        schema_name: str,
        storage_format: str,
        use_static: bool,
        selected_fields: list[str],
        field_labels: dict[str, str],
        profile: Optional[dict[str, Any]] = None,
        template_id: str = "",
    ) -> TemplateRecord:
        normalized_template_id = upsert_template(
            lock=self._lock,
            connect=self._connect,
            name=name,
            url=url,
            page_type=page_type,
            schema_name=schema_name,
            storage_format=storage_format,
            use_static=use_static,
            selected_fields=selected_fields,
            field_labels=field_labels,
            profile=profile,
            template_id=template_id,
        )
        template = self.get_template(normalized_template_id)
        if template is None:
            raise RuntimeError(f"failed to load template: {normalized_template_id}")
        return template

    def list_templates(self, limit: int = 20) -> list[TemplateRecord]:
        return fetch_templates(connect=self._connect, limit=limit)

    def get_template(self, template_id: str) -> Optional[TemplateRecord]:
        return fetch_template(connect=self._connect, template_id=template_id)

    def mark_template_used(self, template_id: str) -> None:
        touch_template(lock=self._lock, connect=self._connect, template_id=template_id)

    def create_or_update_monitor(
        self,
        *,
        name: str,
        url: str,
        schema_name: str,
        storage_format: str,
        use_static: bool,
        selected_fields: list[str],
        field_labels: dict[str, str],
        profile: Optional[dict[str, Any]] = None,
        monitor_id: str = "",
        schedule_enabled: bool = False,
        schedule_interval_minutes: int = 60,
    ) -> MonitorRecord:
        normalized_monitor_id = upsert_monitor(
            lock=self._lock,
            connect=self._connect,
            name=name,
            url=url,
            schema_name=schema_name,
            storage_format=storage_format,
            use_static=use_static,
            selected_fields=selected_fields,
            field_labels=field_labels,
            profile=profile,
            monitor_id=monitor_id,
            schedule_enabled=schedule_enabled,
            schedule_interval_minutes=schedule_interval_minutes,
        )
        monitor = self.get_monitor(normalized_monitor_id)
        if monitor is None:
            raise RuntimeError(f"failed to load monitor: {normalized_monitor_id}")
        return monitor

    def get_monitor(self, monitor_id: str) -> Optional[MonitorRecord]:
        return fetch_monitor(connect=self._connect, monitor_id=monitor_id)

    def list_monitors(self, limit: int = 20) -> list[MonitorRecord]:
        return fetch_monitors(connect=self._connect, limit=limit)

    def list_due_monitors(
        self,
        *,
        due_before: str,
        limit: int = 5,
    ) -> list[MonitorRecord]:
        return fetch_due_monitors(
            connect=self._connect,
            due_before=due_before,
            limit=limit,
        )

    def claim_due_monitors(
        self,
        *,
        due_before: str,
        claimer_id: str,
        lease_seconds: float = 120.0,
        limit: int = 5,
    ) -> list[MonitorRecord]:
        return claim_due_monitors(
            lock=self._lock,
            connect=self._connect,
            due_before=due_before,
            claimer_id=claimer_id,
            lease_seconds=lease_seconds,
            limit=limit,
        )

    def claim_due_monitors_with_summary(
        self,
        *,
        due_before: str,
        claimer_id: str,
        lease_seconds: float = 120.0,
        limit: int = 5,
    ) -> dict[str, Any]:
        return claim_due_monitors_batch(
            lock=self._lock,
            connect=self._connect,
            due_before=due_before,
            claimer_id=claimer_id,
            lease_seconds=lease_seconds,
            limit=limit,
        )

    def update_monitor_result(
        self,
        monitor_id: str,
        task: TaskRecord,
    ) -> MonitorRecord | None:
        comparison = self.compare_with_previous(task)
        task_data = task.data if isinstance(task.data, dict) else {}
        extraction_strategy = str(task_data.get("extraction_strategy") or "").strip()
        learned_profile_id = str(task_data.get("learned_profile_id") or "").strip()
        if task.status == "success":
            if comparison.get("changed"):
                alert_level = "changed"
                changed_count = int(comparison.get("changed_fields_count", 0) or 0)
                impact_summary = str(comparison.get("impact_summary") or "").strip()
                alert_message = f"检测到 {changed_count} 个字段变化"
                if impact_summary:
                    alert_message = f"{alert_message}：{impact_summary}"
            else:
                alert_level = "stable"
                alert_message = "本次检查未发现字段变化"
        else:
            alert_level = "error"
            alert_message = task.error or "监控任务失败"

        persist_monitor_result(
            lock=self._lock,
            connect=self._connect,
            monitor_id=monitor_id,
            task_id=task.task_id,
            task_status=task.status,
            alert_level=alert_level,
            alert_message=alert_message,
            changed_fields=comparison.get("changed_fields", []),
            extraction_strategy=extraction_strategy,
            learned_profile_id=learned_profile_id,
        )
        return self.get_monitor(monitor_id)

    def update_monitor_notification(
        self,
        monitor_id: str,
        *,
        status: str,
        message: str,
    ) -> MonitorRecord | None:
        persist_monitor_notification(
            lock=self._lock,
            connect=self._connect,
            monitor_id=monitor_id,
            status=status,
            message=message,
        )
        return self.get_monitor(monitor_id)

    def create_notification_event(
        self,
        *,
        monitor_id: str,
        task_id: str,
        channel_type: str,
        target: str,
        event_type: str,
        status: str,
        status_message: str,
        attempt_no: int = 1,
        max_attempts: int = 3,
        next_retry_at: str = "",
        response_code: int | None = None,
        error_type: str = "",
        error_message: str = "",
        payload_snapshot: Optional[dict[str, Any]] = None,
        sent_at: str = "",
        retry_of_notification_id: str = "",
        triggered_by: str = "system",
    ) -> NotificationEventRecord:
        notification_id = create_notification_event(
            lock=self._lock,
            connect=self._connect,
            monitor_id=monitor_id,
            task_id=task_id,
            channel_type=channel_type,
            target=target,
            event_type=event_type,
            status=status,
            status_message=status_message,
            attempt_no=attempt_no,
            max_attempts=max_attempts,
            next_retry_at=next_retry_at,
            response_code=response_code,
            error_type=error_type,
            error_message=error_message,
            payload_snapshot=payload_snapshot,
            sent_at=sent_at,
            retry_of_notification_id=retry_of_notification_id,
            triggered_by=triggered_by,
        )
        notification = self.get_notification_event(notification_id)
        if notification is None:
            raise RuntimeError(f"failed to load notification event: {notification_id}")
        return notification

    def get_notification_event(
        self, notification_id: str
    ) -> Optional[NotificationEventRecord]:
        return fetch_notification_event(
            connect=self._connect,
            notification_id=notification_id,
        )

    def list_notification_events(
        self,
        *,
        limit: int = 20,
        monitor_id: str = "",
        status: str = "",
        task_id: str = "",
        event_type: str = "",
        created_after: str = "",
    ) -> list[NotificationEventRecord]:
        return fetch_notification_events(
            connect=self._connect,
            limit=limit,
            monitor_id=monitor_id,
            status=status,
            task_id=task_id,
            event_type=event_type,
            created_after=created_after,
        )

    def update_notification_event(self, notification_id: str, **fields: Any) -> None:
        update_notification_event(
            lock=self._lock,
            connect=self._connect,
            notification_id=notification_id,
            fields=fields,
        )

    def list_due_notification_retries(
        self,
        *,
        due_before: str,
        limit: int = 10,
    ) -> list[NotificationEventRecord]:
        return fetch_due_notification_retries(
            connect=self._connect,
            due_before=due_before,
            limit=limit,
        )

    def mark_monitor_run_scheduled(
        self,
        monitor_id: str,
        *,
        task_id: str,
        trigger_source: str,
        claimed_by: str = "",
    ) -> MonitorRecord | None:
        mark_monitor_run_scheduled(
            lock=self._lock,
            connect=self._connect,
            monitor_id=monitor_id,
            task_id=task_id,
            trigger_source=trigger_source,
            claimed_by=claimed_by,
        )
        return self.get_monitor(monitor_id)

    def fail_monitor_claim(
        self,
        monitor_id: str,
        *,
        error: str,
        claimed_by: str = "",
    ) -> MonitorRecord | None:
        fail_monitor_claim(
            lock=self._lock,
            connect=self._connect,
            monitor_id=monitor_id,
            error=error,
            claimed_by=claimed_by,
        )
        return self.get_monitor(monitor_id)

    def pause_monitor_schedule(self, monitor_id: str) -> MonitorRecord | None:
        pause_monitor_schedule(
            lock=self._lock,
            connect=self._connect,
            monitor_id=monitor_id,
        )
        return self.get_monitor(monitor_id)

    def resume_monitor_schedule(self, monitor_id: str) -> MonitorRecord | None:
        resume_monitor_schedule(
            lock=self._lock,
            connect=self._connect,
            monitor_id=monitor_id,
        )
        return self.get_monitor(monitor_id)

    def list_all(self, limit: int = 50, batch_group_id: str = "") -> list[TaskRecord]:
        return fetch_root_tasks(
            connect=self._connect,
            limit=limit,
            batch_group_id=batch_group_id,
        )

    def list_children(self, parent_task_id: str) -> list[TaskRecord]:
        return fetch_child_tasks(connect=self._connect, parent_task_id=parent_task_id)

    @staticmethod
    def new_batch_group_id() -> str:
        return f"batch-{uuid4().hex[:8]}"

    def list_by_url(self, url: str, limit: int = 10) -> list[TaskRecord]:
        return fetch_tasks_by_url(connect=self._connect, url=url, limit=limit)

    def list_by_learned_profile(
        self, profile_id: str, limit: int = 12
    ) -> list[TaskRecord]:
        return fetch_tasks_by_learned_profile(
            connect=self._connect,
            profile_id=profile_id,
            limit=limit,
        )

    def get_learned_profile_activity(
        self, profile_id: str, *, task_limit: int = 10
    ) -> dict[str, Any]:
        tasks = self.list_by_learned_profile(profile_id, limit=task_limit)
        monitors = [
            item for item in self.list_monitors(limit=100)
            if item.last_learned_profile_id == str(profile_id or "").strip()
        ]
        return build_learned_profile_activity_payload(
            tasks=tasks,
            monitors=monitors,
            compare_with_previous=self.compare_with_previous,
        )

    def stats(self) -> dict[str, Any]:
        return fetch_task_stats(connect=self._connect)

    def mark_queued(self, task_id: str) -> None:
        self._update_fields(task_id, **build_queued_fields())
        self._refresh_parent_task(task_id)

    def enqueue_task_spec(self, spec) -> None:
        enqueue_task_payload(
            lock=self._lock,
            connect=self._connect,
            task_id=spec.task_id,
            payload=spec.to_queue_payload(),
        )
        self.mark_queued(spec.task_id)

    def claim_next_queued_task(
        self,
        *,
        worker_id: str,
        stale_after_seconds: float = 0.0,
    ) -> tuple[TaskRecord, dict[str, Any]] | None:
        claimed = claim_queued_task_payload(
            lock=self._lock,
            connect=self._connect,
            worker_id=worker_id,
            stale_after_seconds=stale_after_seconds,
        )
        if not claimed:
            return None

        task_id = str(claimed["task_id"])
        task = self.get(task_id)
        if task is None:
            fail_task_payload(
                lock=self._lock,
                connect=self._connect,
                task_id=task_id,
                error="任务不存在，无法执行",
            )
            return None
        return task, dict(claimed.get("payload") or {})

    def mark_queue_done(self, task_id: str) -> None:
        complete_task_payload(
            lock=self._lock,
            connect=self._connect,
            task_id=task_id,
        )

    def mark_queue_failed(self, task_id: str, error: str) -> None:
        fail_task_payload(
            lock=self._lock,
            connect=self._connect,
            task_id=task_id,
            error=error,
        )

    def mark_running(self, task_id: str) -> None:
        self._update_fields(task_id, **build_running_fields())
        self._refresh_parent_task(task_id)

    def update_progress(
        self, task_id: str, progress_percent: float, progress_stage: str
    ) -> None:
        self._update_fields(
            task_id,
            **build_progress_fields(
                progress_percent=progress_percent,
                progress_stage=progress_stage,
            ),
        )
        self._refresh_parent_task(task_id)

    def mark_success(
        self,
        task_id: str,
        elapsed_ms: float,
        quality_score: float,
        data: Optional[dict[str, Any]],
    ) -> None:
        self._update_fields(
            task_id,
            **build_success_fields(
                elapsed_ms=elapsed_ms,
                quality_score=quality_score,
                data=data,
            ),
        )
        self._refresh_parent_task(task_id)

    def mark_failed(self, task_id: str, elapsed_ms: float, error: str) -> None:
        self._update_fields(
            task_id,
            **build_failed_fields(elapsed_ms=elapsed_ms, error=error),
        )
        self._refresh_parent_task(task_id)

    def _refresh_parent_task(self, task_id: str) -> None:
        task = self.get(task_id)
        if task is None:
            return

        children = self.list_children(task.parent_task_id) if task.parent_task_id else []
        refresh_payload = build_parent_refresh_fields(task=task, children=children)
        if refresh_payload is None:
            return
        parent_task_id, fields = refresh_payload
        self._update_fields(parent_task_id, **fields)

    def get_previous_success(self, task: TaskRecord) -> Optional[TaskRecord]:
        return fetch_previous_success(connect=self._connect, task=task)

    def get_history_summary(self, task: TaskRecord) -> dict[str, Any]:
        return fetch_history_summary(
            connect=self._connect,
            task=task,
        )

    def compare_with_previous(self, task: TaskRecord) -> dict[str, Any]:
        return build_comparison_with_previous(connect=self._connect, task=task)

    def get_task_detail_payload(
        self, task_id: str, history_limit: int = 6
    ) -> Optional[dict[str, Any]]:
        return build_history_task_detail_payload(
            task_id=task_id,
            history_limit=history_limit,
            get_task=self.get,
            list_children=self.list_children,
            list_by_url=self.list_by_url,
            get_history_summary=self.get_history_summary,
            compare_with_previous=self.compare_with_previous,
        )

    def build_dashboard_insights(self, recent_limit: int = 120) -> dict[str, Any]:
        recent_tasks = self.list_all(limit=recent_limit)
        monitors = self.list_monitors(limit=50)
        return build_dashboard_insights_payload(
            recent_tasks=recent_tasks,
            monitors=monitors,
            compare_with_previous=self.compare_with_previous,
        )

    def _update_fields(self, task_id: str, **fields) -> None:
        update_task_fields(
            lock=self._lock,
            connect=self._connect,
            task_id=task_id,
            allowed_fields=self._ALLOWED_UPDATE_FIELDS,
            fields=fields,
        )
