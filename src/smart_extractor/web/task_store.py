"""
Web task persistence and insight helpers backed by SQLite.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse
from uuid import uuid4

from loguru import logger
from smart_extractor.web.database import (
    build_connection_factory,
    resolve_sqlite_database_url,
)
from smart_extractor.web.task_activity import (
    build_initial_batch_payload,
    build_learned_profile_activity_payload,
)
from smart_extractor.web.task_insights import build_dashboard_insights_payload
from smart_extractor.web.task_models import (
    ActorInstallRecord,
    FunnelEventRecord,
    MonitorRecord,
    NotificationEventRecord,
    ProxyEndpointRecord,
    RepairSuggestionRecord,
    SitePolicyRecord,
    TaskRecord,
    TaskAnnotationRecord,
    TemplateRecord,
    WorkerNodeRecord,
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
    """Thread-safe task store backed by SQLite or PostgreSQL."""

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
        database_url: str = "",
        default_tenant_id: str = "default",
        sqlite_busy_timeout_ms: int = 5000,
        sqlite_enable_wal: bool = True,
        sqlite_synchronous: str = "NORMAL",
    ):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._database_url = str(database_url or "").strip() or resolve_sqlite_database_url(
            self._db_path
        )
        self._default_tenant_id = str(default_tenant_id or "default").strip() or "default"
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
        self._connect = build_connection_factory(
            database_url=self._database_url,
            sqlite_busy_timeout_ms=self._sqlite_busy_timeout_ms,
            sqlite_enable_wal=self._sqlite_enable_wal,
            sqlite_synchronous=self._sqlite_synchronous,
        )
        self._init_db()
        logger.info(
            "Web task store initialized: dialect={} database={}",
            getattr(self._connect, "database_dialect", "sqlite"),
            self._database_url,
        )

    @property
    def database_dialect(self) -> str:
        return str(getattr(self._connect, "database_dialect", "sqlite"))

    @property
    def default_tenant_id(self) -> str:
        return self._default_tenant_id

    def _normalize_tenant_id(self, tenant_id: str = "") -> str:
        if str(tenant_id or "").strip() == "*":
            return "*"
        return str(tenant_id or self._default_tenant_id).strip() or self._default_tenant_id

    def _init_db(self) -> None:
        initialize_task_store_schema(connect=self._connect)

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
        tenant_id: str = "",
    ) -> TaskRecord:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
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
            tenant_id=normalized_tenant_id,
        )
        task = self.get(task_id, tenant_id=normalized_tenant_id)
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
        tenant_id: str = "",
    ) -> TaskRecord:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
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
            tenant_id=normalized_tenant_id,
        )
        self._update_fields(
            task.task_id,
            data_json=json.dumps(
                build_initial_batch_payload(normalized_urls),
                ensure_ascii=False,
            ),
            tenant_id=normalized_tenant_id,
        )
        refreshed = self.get(task.task_id, tenant_id=normalized_tenant_id)
        if refreshed is None:
            raise RuntimeError(f"failed to load batch root task: {task.task_id}")
        return refreshed

    def get(self, task_id: str, tenant_id: str = "") -> Optional[TaskRecord]:
        return fetch_task(
            connect=self._connect,
            task_id=task_id,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

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
        tenant_id: str = "",
    ) -> TemplateRecord:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
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
            tenant_id=normalized_tenant_id,
        )
        template = self.get_template(normalized_template_id, tenant_id=normalized_tenant_id)
        if template is None:
            raise RuntimeError(f"failed to load template: {normalized_template_id}")
        return template

    def list_templates(self, limit: int = 20, tenant_id: str = "") -> list[TemplateRecord]:
        return fetch_templates(
            connect=self._connect,
            limit=limit,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def get_template(self, template_id: str, tenant_id: str = "") -> Optional[TemplateRecord]:
        return fetch_template(
            connect=self._connect,
            template_id=template_id,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def mark_template_used(self, template_id: str, tenant_id: str = "") -> None:
        touch_template(
            lock=self._lock,
            connect=self._connect,
            template_id=template_id,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

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
        tenant_id: str = "",
    ) -> MonitorRecord:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
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
            tenant_id=normalized_tenant_id,
        )
        monitor = self.get_monitor(normalized_monitor_id, tenant_id=normalized_tenant_id)
        if monitor is None:
            raise RuntimeError(f"failed to load monitor: {normalized_monitor_id}")
        return monitor

    def get_monitor(self, monitor_id: str, tenant_id: str = "") -> Optional[MonitorRecord]:
        return fetch_monitor(
            connect=self._connect,
            monitor_id=monitor_id,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def list_monitors(self, limit: int = 20, tenant_id: str = "") -> list[MonitorRecord]:
        return fetch_monitors(
            connect=self._connect,
            limit=limit,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def list_due_monitors(
        self,
        *,
        due_before: str,
        limit: int = 5,
        tenant_id: str = "",
    ) -> list[MonitorRecord]:
        return fetch_due_monitors(
            connect=self._connect,
            due_before=due_before,
            limit=limit,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def claim_due_monitors(
        self,
        *,
        due_before: str,
        claimer_id: str,
        lease_seconds: float = 120.0,
        limit: int = 5,
        tenant_id: str = "",
    ) -> list[MonitorRecord]:
        return claim_due_monitors(
            lock=self._lock,
            connect=self._connect,
            due_before=due_before,
            claimer_id=claimer_id,
            lease_seconds=lease_seconds,
            limit=limit,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def claim_due_monitors_with_summary(
        self,
        *,
        due_before: str,
        claimer_id: str,
        lease_seconds: float = 120.0,
        limit: int = 5,
        tenant_id: str = "",
    ) -> dict[str, Any]:
        return claim_due_monitors_batch(
            lock=self._lock,
            connect=self._connect,
            due_before=due_before,
            claimer_id=claimer_id,
            lease_seconds=lease_seconds,
            limit=limit,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def update_monitor_result(
        self,
        monitor_id: str,
        task: TaskRecord,
        tenant_id: str = "",
    ) -> MonitorRecord | None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id or task.tenant_id)
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
            tenant_id=normalized_tenant_id,
        )
        return self.get_monitor(monitor_id, tenant_id=normalized_tenant_id)

    def update_monitor_notification(
        self,
        monitor_id: str,
        *,
        status: str,
        message: str,
        tenant_id: str = "",
    ) -> MonitorRecord | None:
        persist_monitor_notification(
            lock=self._lock,
            connect=self._connect,
            monitor_id=monitor_id,
            status=status,
            message=message,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )
        return self.get_monitor(monitor_id, tenant_id=tenant_id)

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
        tenant_id: str = "",
    ) -> NotificationEventRecord:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
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
            tenant_id=normalized_tenant_id,
        )
        notification = self.get_notification_event(
            notification_id,
            tenant_id=normalized_tenant_id,
        )
        if notification is None:
            raise RuntimeError(f"failed to load notification event: {notification_id}")
        return notification

    def get_notification_event(
        self, notification_id: str, tenant_id: str = ""
    ) -> Optional[NotificationEventRecord]:
        return fetch_notification_event(
            connect=self._connect,
            notification_id=notification_id,
            tenant_id=self._normalize_tenant_id(tenant_id),
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
        tenant_id: str = "",
    ) -> list[NotificationEventRecord]:
        return fetch_notification_events(
            connect=self._connect,
            limit=limit,
            monitor_id=monitor_id,
            status=status,
            task_id=task_id,
            event_type=event_type,
            created_after=created_after,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def update_notification_event(
        self,
        notification_id: str,
        tenant_id: str = "",
        **fields: Any,
    ) -> None:
        update_notification_event(
            lock=self._lock,
            connect=self._connect,
            notification_id=notification_id,
            fields=fields,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def list_due_notification_retries(
        self,
        *,
        due_before: str,
        limit: int = 10,
        tenant_id: str = "",
    ) -> list[NotificationEventRecord]:
        return fetch_due_notification_retries(
            connect=self._connect,
            due_before=due_before,
            limit=limit,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def mark_monitor_run_scheduled(
        self,
        monitor_id: str,
        *,
        task_id: str,
        trigger_source: str,
        claimed_by: str = "",
        tenant_id: str = "",
    ) -> MonitorRecord | None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        mark_monitor_run_scheduled(
            lock=self._lock,
            connect=self._connect,
            monitor_id=monitor_id,
            task_id=task_id,
            trigger_source=trigger_source,
            claimed_by=claimed_by,
            tenant_id=normalized_tenant_id,
        )
        return self.get_monitor(monitor_id, tenant_id=normalized_tenant_id)

    def fail_monitor_claim(
        self,
        monitor_id: str,
        *,
        error: str,
        claimed_by: str = "",
        tenant_id: str = "",
    ) -> MonitorRecord | None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        fail_monitor_claim(
            lock=self._lock,
            connect=self._connect,
            monitor_id=monitor_id,
            error=error,
            claimed_by=claimed_by,
            tenant_id=normalized_tenant_id,
        )
        return self.get_monitor(monitor_id, tenant_id=normalized_tenant_id)

    def pause_monitor_schedule(self, monitor_id: str, tenant_id: str = "") -> MonitorRecord | None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        pause_monitor_schedule(
            lock=self._lock,
            connect=self._connect,
            monitor_id=monitor_id,
            tenant_id=normalized_tenant_id,
        )
        return self.get_monitor(monitor_id, tenant_id=normalized_tenant_id)

    def resume_monitor_schedule(self, monitor_id: str, tenant_id: str = "") -> MonitorRecord | None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        resume_monitor_schedule(
            lock=self._lock,
            connect=self._connect,
            monitor_id=monitor_id,
            tenant_id=normalized_tenant_id,
        )
        return self.get_monitor(monitor_id, tenant_id=normalized_tenant_id)

    def list_all(
        self,
        limit: int = 50,
        batch_group_id: str = "",
        tenant_id: str = "",
    ) -> list[TaskRecord]:
        return fetch_root_tasks(
            connect=self._connect,
            limit=limit,
            batch_group_id=batch_group_id,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def list_children(self, parent_task_id: str, tenant_id: str = "") -> list[TaskRecord]:
        return fetch_child_tasks(
            connect=self._connect,
            parent_task_id=parent_task_id,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    @staticmethod
    def new_batch_group_id() -> str:
        return f"batch-{uuid4().hex[:8]}"

    def list_by_url(self, url: str, limit: int = 10, tenant_id: str = "") -> list[TaskRecord]:
        return fetch_tasks_by_url(
            connect=self._connect,
            url=url,
            limit=limit,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def list_by_learned_profile(
        self,
        profile_id: str,
        limit: int = 12,
        tenant_id: str = "",
    ) -> list[TaskRecord]:
        return fetch_tasks_by_learned_profile(
            connect=self._connect,
            profile_id=profile_id,
            limit=limit,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def get_learned_profile_activity(
        self, profile_id: str, *, task_limit: int = 10, tenant_id: str = ""
    ) -> dict[str, Any]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        tasks = self.list_by_learned_profile(
            profile_id,
            limit=task_limit,
            tenant_id=normalized_tenant_id,
        )
        monitors = [
            item for item in self.list_monitors(limit=100, tenant_id=normalized_tenant_id)
            if item.last_learned_profile_id == str(profile_id or "").strip()
        ]
        return build_learned_profile_activity_payload(
            tasks=tasks,
            monitors=monitors,
            compare_with_previous=self.compare_with_previous,
        )

    def stats(self, tenant_id: str = "") -> dict[str, Any]:
        return fetch_task_stats(
            connect=self._connect,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def mark_queued(self, task_id: str, tenant_id: str = "") -> None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        self._update_fields(task_id, tenant_id=normalized_tenant_id, **build_queued_fields())
        self._refresh_parent_task(task_id, tenant_id=normalized_tenant_id)

    def enqueue_task_spec(self, spec, tenant_id: str = "") -> None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        enqueue_task_payload(
            lock=self._lock,
            connect=self._connect,
            task_id=spec.task_id,
            payload=spec.to_queue_payload(),
            tenant_id=normalized_tenant_id,
        )
        self.mark_queued(spec.task_id, tenant_id=normalized_tenant_id)

    def claim_next_queued_task(
        self,
        *,
        worker_id: str,
        stale_after_seconds: float = 0.0,
        tenant_id: str = "",
    ) -> tuple[TaskRecord, dict[str, Any]] | None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        claimed = claim_queued_task_payload(
            lock=self._lock,
            connect=self._connect,
            worker_id=worker_id,
            stale_after_seconds=stale_after_seconds,
            tenant_id=normalized_tenant_id,
        )
        if not claimed:
            return None

        task_id = str(claimed["task_id"])
        claimed_tenant_id = str(claimed.get("tenant_id") or normalized_tenant_id)
        task = self.get(task_id, tenant_id=claimed_tenant_id)
        if task is None:
            fail_task_payload(
                lock=self._lock,
                connect=self._connect,
                task_id=task_id,
                error="任务不存在，无法执行",
                tenant_id=claimed_tenant_id,
            )
            return None
        return task, dict(claimed.get("payload") or {})

    def mark_queue_done(self, task_id: str, tenant_id: str = "") -> None:
        complete_task_payload(
            lock=self._lock,
            connect=self._connect,
            task_id=task_id,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def mark_queue_failed(self, task_id: str, error: str, tenant_id: str = "") -> None:
        fail_task_payload(
            lock=self._lock,
            connect=self._connect,
            task_id=task_id,
            error=error,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )

    def mark_running(self, task_id: str, tenant_id: str = "") -> None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        self._update_fields(task_id, tenant_id=normalized_tenant_id, **build_running_fields())
        self._refresh_parent_task(task_id, tenant_id=normalized_tenant_id)

    def update_progress(
        self,
        task_id: str,
        progress_percent: float,
        progress_stage: str,
        tenant_id: str = "",
    ) -> None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        self._update_fields(
            task_id,
            tenant_id=normalized_tenant_id,
            **build_progress_fields(
                progress_percent=progress_percent,
                progress_stage=progress_stage,
            ),
        )
        self._refresh_parent_task(task_id, tenant_id=normalized_tenant_id)

    def mark_success(
        self,
        task_id: str,
        elapsed_ms: float,
        quality_score: float,
        data: Optional[dict[str, Any]],
        tenant_id: str = "",
    ) -> None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        self._update_fields(
            task_id,
            tenant_id=normalized_tenant_id,
            **build_success_fields(
                elapsed_ms=elapsed_ms,
                quality_score=quality_score,
                data=data,
            ),
        )
        self._refresh_parent_task(task_id, tenant_id=normalized_tenant_id)

    def mark_failed(self, task_id: str, elapsed_ms: float, error: str, tenant_id: str = "") -> None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        self._update_fields(
            task_id,
            tenant_id=normalized_tenant_id,
            **build_failed_fields(elapsed_ms=elapsed_ms, error=error),
        )
        self._refresh_parent_task(task_id, tenant_id=normalized_tenant_id)

    def _refresh_parent_task(self, task_id: str, tenant_id: str = "") -> None:
        task = self.get(task_id, tenant_id=tenant_id)
        if task is None:
            return

        children = (
            self.list_children(task.parent_task_id, tenant_id=task.tenant_id)
            if task.parent_task_id
            else []
        )
        refresh_payload = build_parent_refresh_fields(task=task, children=children)
        if refresh_payload is None:
            return
        parent_task_id, fields = refresh_payload
        self._update_fields(parent_task_id, tenant_id=task.tenant_id, **fields)

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
        self,
        task_id: str,
        history_limit: int = 6,
        tenant_id: str = "",
    ) -> Optional[dict[str, Any]]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        return build_history_task_detail_payload(
            task_id=task_id,
            history_limit=history_limit,
            get_task=lambda value: self.get(value, tenant_id=normalized_tenant_id),
            list_children=lambda value: self.list_children(
                value,
                tenant_id=normalized_tenant_id,
            ),
            list_by_url=lambda value, limit: self.list_by_url(
                value,
                limit=limit,
                tenant_id=normalized_tenant_id,
            ),
            get_history_summary=self.get_history_summary,
            compare_with_previous=self.compare_with_previous,
        )

    def build_dashboard_insights(
        self,
        recent_limit: int = 120,
        tenant_id: str = "",
    ) -> dict[str, Any]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        recent_tasks = self.list_all(limit=recent_limit, tenant_id=normalized_tenant_id)
        monitors = self.list_monitors(limit=50, tenant_id=normalized_tenant_id)
        return build_dashboard_insights_payload(
            recent_tasks=recent_tasks,
            monitors=monitors,
            compare_with_previous=self.compare_with_previous,
        )

    def create_or_update_actor_install(
        self,
        *,
        actor_id: str,
        name: str,
        version: str,
        category: str,
        capabilities: list[str],
        config: Optional[dict[str, Any]] = None,
        linked_template_id: str = "",
        linked_monitor_id: str = "",
        status: str = "active",
        actor_instance_id: str = "",
        tenant_id: str = "",
    ) -> ActorInstallRecord:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        normalized_actor_instance_id = str(actor_instance_id or "").strip() or f"actor-{uuid4().hex[:10]}"
        now = self._now_text()
        with self._lock:
            with self._connect() as conn:
                if getattr(conn, "dialect", "sqlite") == "postgres":
                    conn.execute(
                        """
                        INSERT INTO installed_actor_packages (
                            actor_instance_id, tenant_id, actor_id, name, version, category,
                            capabilities_json, config_json, linked_template_id, linked_monitor_id,
                            status, created_at, updated_at, last_run_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '')
                        ON CONFLICT(actor_instance_id) DO UPDATE SET
                            actor_id=EXCLUDED.actor_id,
                            name=EXCLUDED.name,
                            version=EXCLUDED.version,
                            category=EXCLUDED.category,
                            capabilities_json=EXCLUDED.capabilities_json,
                            config_json=EXCLUDED.config_json,
                            linked_template_id=EXCLUDED.linked_template_id,
                            linked_monitor_id=EXCLUDED.linked_monitor_id,
                            status=EXCLUDED.status,
                            updated_at=EXCLUDED.updated_at
                        """,
                        (
                            normalized_actor_instance_id,
                            normalized_tenant_id,
                            actor_id,
                            name,
                            version,
                            category,
                            json.dumps(capabilities or [], ensure_ascii=False),
                            json.dumps(config or {}, ensure_ascii=False),
                            linked_template_id,
                            linked_monitor_id,
                            status,
                            now,
                            now,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO installed_actor_packages (
                            actor_instance_id, tenant_id, actor_id, name, version, category,
                            capabilities_json, config_json, linked_template_id, linked_monitor_id,
                            status, created_at, updated_at, last_run_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '')
                        ON CONFLICT(actor_instance_id) DO UPDATE SET
                            actor_id=excluded.actor_id,
                            name=excluded.name,
                            version=excluded.version,
                            category=excluded.category,
                            capabilities_json=excluded.capabilities_json,
                            config_json=excluded.config_json,
                            linked_template_id=excluded.linked_template_id,
                            linked_monitor_id=excluded.linked_monitor_id,
                            status=excluded.status,
                            updated_at=excluded.updated_at
                        """,
                        (
                            normalized_actor_instance_id,
                            normalized_tenant_id,
                            actor_id,
                            name,
                            version,
                            category,
                            json.dumps(capabilities or [], ensure_ascii=False),
                            json.dumps(config or {}, ensure_ascii=False),
                            linked_template_id,
                            linked_monitor_id,
                            status,
                            now,
                            now,
                        ),
                    )
                row = conn.execute(
                    "SELECT * FROM installed_actor_packages WHERE tenant_id=? AND actor_instance_id=?",
                    (normalized_tenant_id, normalized_actor_instance_id),
                ).fetchone()
                conn.commit()
        if row is None:
            raise RuntimeError("failed to persist actor install")
        return ActorInstallRecord.from_row(row)

    def list_actor_installs(self, limit: int = 50, tenant_id: str = "") -> list[ActorInstallRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM installed_actor_packages WHERE tenant_id=? ORDER BY id DESC LIMIT ?",
                (self._normalize_tenant_id(tenant_id), int(limit)),
            ).fetchall()
        return [ActorInstallRecord.from_row(row) for row in rows]

    def heartbeat_worker_node(
        self,
        *,
        worker_id: str,
        display_name: str = "",
        node_type: str = "worker",
        status: str = "idle",
        queue_scope: str = "*",
        current_load: int = 0,
        capabilities: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
        last_error: str = "",
        tenant_id: str = "",
    ) -> WorkerNodeRecord:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        normalized_worker_id = str(worker_id or "").strip()
        if not normalized_worker_id:
            raise ValueError("worker_id cannot be empty")
        now = self._now_text()
        with self._lock:
            with self._connect() as conn:
                if getattr(conn, "dialect", "sqlite") == "postgres":
                    conn.execute(
                        """
                        INSERT INTO worker_nodes (
                            worker_id, tenant_id, display_name, node_type, status, queue_scope,
                            current_load, capabilities_json, metadata_json, last_seen_at,
                            created_at, updated_at, last_error
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(worker_id) DO UPDATE SET
                            display_name=EXCLUDED.display_name,
                            node_type=EXCLUDED.node_type,
                            status=EXCLUDED.status,
                            queue_scope=EXCLUDED.queue_scope,
                            current_load=EXCLUDED.current_load,
                            capabilities_json=EXCLUDED.capabilities_json,
                            metadata_json=EXCLUDED.metadata_json,
                            last_seen_at=EXCLUDED.last_seen_at,
                            updated_at=EXCLUDED.updated_at,
                            last_error=EXCLUDED.last_error
                        """,
                        (
                            normalized_worker_id,
                            normalized_tenant_id,
                            display_name or normalized_worker_id,
                            node_type,
                            status,
                            queue_scope or "*",
                            max(int(current_load or 0), 0),
                            json.dumps(capabilities or [], ensure_ascii=False),
                            json.dumps(metadata or {}, ensure_ascii=False),
                            now,
                            now,
                            now,
                            last_error,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO worker_nodes (
                            worker_id, tenant_id, display_name, node_type, status, queue_scope,
                            current_load, capabilities_json, metadata_json, last_seen_at,
                            created_at, updated_at, last_error
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(worker_id) DO UPDATE SET
                            display_name=excluded.display_name,
                            node_type=excluded.node_type,
                            status=excluded.status,
                            queue_scope=excluded.queue_scope,
                            current_load=excluded.current_load,
                            capabilities_json=excluded.capabilities_json,
                            metadata_json=excluded.metadata_json,
                            last_seen_at=excluded.last_seen_at,
                            updated_at=excluded.updated_at,
                            last_error=excluded.last_error
                        """,
                        (
                            normalized_worker_id,
                            normalized_tenant_id,
                            display_name or normalized_worker_id,
                            node_type,
                            status,
                            queue_scope or "*",
                            max(int(current_load or 0), 0),
                            json.dumps(capabilities or [], ensure_ascii=False),
                            json.dumps(metadata or {}, ensure_ascii=False),
                            now,
                            now,
                            now,
                            last_error,
                        ),
                    )
                row = conn.execute(
                    "SELECT * FROM worker_nodes WHERE worker_id=?",
                    (normalized_worker_id,),
                ).fetchone()
                conn.commit()
        if row is None:
            raise RuntimeError("failed to persist worker heartbeat")
        return WorkerNodeRecord.from_row(row)

    def list_worker_nodes(self, limit: int = 100, tenant_id: str = "") -> list[WorkerNodeRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM worker_nodes WHERE tenant_id=? ORDER BY updated_at DESC, id DESC LIMIT ?",
                (self._normalize_tenant_id(tenant_id), int(limit)),
            ).fetchall()
        return [WorkerNodeRecord.from_row(row) for row in rows]

    def create_or_update_proxy_endpoint(
        self,
        *,
        name: str,
        proxy_url: str,
        provider: str = "",
        status: str = "idle",
        enabled: bool = True,
        tags: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
        proxy_id: str = "",
        tenant_id: str = "",
    ) -> ProxyEndpointRecord:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        normalized_proxy_id = str(proxy_id or "").strip() or f"proxy-{uuid4().hex[:10]}"
        now = self._now_text()
        with self._lock:
            with self._connect() as conn:
                if getattr(conn, "dialect", "sqlite") == "postgres":
                    conn.execute(
                        """
                        INSERT INTO proxy_pool_endpoints (
                            proxy_id, tenant_id, name, proxy_url, provider, status, enabled,
                            tags_json, metadata_json, success_count, failure_count,
                            last_used_at, created_at, updated_at, last_error
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '', ?, ?, '')
                        ON CONFLICT(proxy_id) DO UPDATE SET
                            name=EXCLUDED.name,
                            proxy_url=EXCLUDED.proxy_url,
                            provider=EXCLUDED.provider,
                            status=EXCLUDED.status,
                            enabled=EXCLUDED.enabled,
                            tags_json=EXCLUDED.tags_json,
                            metadata_json=EXCLUDED.metadata_json,
                            updated_at=EXCLUDED.updated_at
                        """,
                        (
                            normalized_proxy_id,
                            normalized_tenant_id,
                            name,
                            proxy_url,
                            provider,
                            status,
                            1 if enabled else 0,
                            json.dumps(tags or [], ensure_ascii=False),
                            json.dumps(metadata or {}, ensure_ascii=False),
                            now,
                            now,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO proxy_pool_endpoints (
                            proxy_id, tenant_id, name, proxy_url, provider, status, enabled,
                            tags_json, metadata_json, success_count, failure_count,
                            last_used_at, created_at, updated_at, last_error
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '', ?, ?, '')
                        ON CONFLICT(proxy_id) DO UPDATE SET
                            name=excluded.name,
                            proxy_url=excluded.proxy_url,
                            provider=excluded.provider,
                            status=excluded.status,
                            enabled=excluded.enabled,
                            tags_json=excluded.tags_json,
                            metadata_json=excluded.metadata_json,
                            updated_at=excluded.updated_at
                        """,
                        (
                            normalized_proxy_id,
                            normalized_tenant_id,
                            name,
                            proxy_url,
                            provider,
                            status,
                            1 if enabled else 0,
                            json.dumps(tags or [], ensure_ascii=False),
                            json.dumps(metadata or {}, ensure_ascii=False),
                            now,
                            now,
                        ),
                    )
                row = conn.execute(
                    "SELECT * FROM proxy_pool_endpoints WHERE tenant_id=? AND proxy_id=?",
                    (normalized_tenant_id, normalized_proxy_id),
                ).fetchone()
                conn.commit()
        if row is None:
            raise RuntimeError("failed to persist proxy endpoint")
        return ProxyEndpointRecord.from_row(row)

    def list_proxy_endpoints(self, limit: int = 100, tenant_id: str = "") -> list[ProxyEndpointRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM proxy_pool_endpoints WHERE tenant_id=? ORDER BY id DESC LIMIT ?",
                (self._normalize_tenant_id(tenant_id), int(limit)),
            ).fetchall()
        return [ProxyEndpointRecord.from_row(row) for row in rows]

    def pick_proxy_endpoint(
        self,
        *,
        preferred_tags: Optional[list[str]] = None,
        tenant_id: str = "",
    ) -> ProxyEndpointRecord | None:
        normalized_tags = {
            str(item).strip()
            for item in (preferred_tags or [])
            if str(item).strip()
        }
        candidates = [
            item
            for item in self.list_proxy_endpoints(limit=200, tenant_id=tenant_id)
            if item.enabled
        ]
        if normalized_tags:
            tagged = [
                item
                for item in candidates
                if normalized_tags & {str(tag).strip() for tag in item.tags}
            ]
            if tagged:
                candidates = tagged
        if not candidates:
            return None
        candidates.sort(
            key=lambda item: (
                int(item.failure_count or 0),
                int(item.success_count or 0),
                str(item.last_used_at or ""),
            )
        )
        return candidates[0]

    def mark_proxy_endpoint_result(
        self,
        proxy_id: str,
        *,
        success: bool,
        error: str = "",
        tenant_id: str = "",
    ) -> None:
        now = self._now_text()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT success_count, failure_count FROM proxy_pool_endpoints WHERE tenant_id=? AND proxy_id=?",
                    (self._normalize_tenant_id(tenant_id), proxy_id),
                ).fetchone()
                if row is None:
                    conn.commit()
                    return
                success_count = int(row["success_count"] or 0)
                failure_count = int(row["failure_count"] or 0)
                if success:
                    success_count += 1
                else:
                    failure_count += 1
                conn.execute(
                    """
                    UPDATE proxy_pool_endpoints
                    SET status=?, success_count=?, failure_count=?, last_used_at=?, updated_at=?, last_error=?
                    WHERE tenant_id=? AND proxy_id=?
                    """,
                    (
                        "ready" if success else "degraded",
                        success_count,
                        failure_count,
                        now,
                        now,
                        str(error or "").strip(),
                        self._normalize_tenant_id(tenant_id),
                        proxy_id,
                    ),
                )
                conn.commit()

    def create_or_update_site_policy(
        self,
        *,
        domain: str,
        name: str,
        min_interval_seconds: float,
        max_concurrency: int,
        use_proxy_pool: bool = False,
        preferred_proxy_tags: Optional[list[str]] = None,
        assigned_worker_group: str = "",
        notes: str = "",
        policy_id: str = "",
        tenant_id: str = "",
    ) -> SitePolicyRecord:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        normalized_domain = str(domain or "").strip().lower()
        if not normalized_domain:
            raise ValueError("domain cannot be empty")
        now = self._now_text()
        with self._lock:
            with self._connect() as conn:
                existing = conn.execute(
                    "SELECT policy_id FROM site_execution_policies WHERE tenant_id=? AND domain=?",
                    (normalized_tenant_id, normalized_domain),
                ).fetchone()
                normalized_policy_id = (
                    str(policy_id or "").strip()
                    or (str(existing["policy_id"] or "").strip() if existing is not None else "")
                    or f"site-{uuid4().hex[:10]}"
                )
                if getattr(conn, "dialect", "sqlite") == "postgres":
                    conn.execute(
                        """
                        INSERT INTO site_execution_policies (
                            policy_id, tenant_id, domain, name, min_interval_seconds, max_concurrency,
                            use_proxy_pool, preferred_proxy_tags_json, assigned_worker_group, notes,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(policy_id) DO UPDATE SET
                            domain=EXCLUDED.domain,
                            name=EXCLUDED.name,
                            min_interval_seconds=EXCLUDED.min_interval_seconds,
                            max_concurrency=EXCLUDED.max_concurrency,
                            use_proxy_pool=EXCLUDED.use_proxy_pool,
                            preferred_proxy_tags_json=EXCLUDED.preferred_proxy_tags_json,
                            assigned_worker_group=EXCLUDED.assigned_worker_group,
                            notes=EXCLUDED.notes,
                            updated_at=EXCLUDED.updated_at
                        """,
                        (
                            normalized_policy_id,
                            normalized_tenant_id,
                            normalized_domain,
                            name,
                            float(min_interval_seconds or 0.0),
                            max(1, int(max_concurrency or 1)),
                            1 if use_proxy_pool else 0,
                            json.dumps(preferred_proxy_tags or [], ensure_ascii=False),
                            assigned_worker_group,
                            notes,
                            now,
                            now,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO site_execution_policies (
                            policy_id, tenant_id, domain, name, min_interval_seconds, max_concurrency,
                            use_proxy_pool, preferred_proxy_tags_json, assigned_worker_group, notes,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(policy_id) DO UPDATE SET
                            domain=excluded.domain,
                            name=excluded.name,
                            min_interval_seconds=excluded.min_interval_seconds,
                            max_concurrency=excluded.max_concurrency,
                            use_proxy_pool=excluded.use_proxy_pool,
                            preferred_proxy_tags_json=excluded.preferred_proxy_tags_json,
                            assigned_worker_group=excluded.assigned_worker_group,
                            notes=excluded.notes,
                            updated_at=excluded.updated_at
                        """,
                        (
                            normalized_policy_id,
                            normalized_tenant_id,
                            normalized_domain,
                            name,
                            float(min_interval_seconds or 0.0),
                            max(1, int(max_concurrency or 1)),
                            1 if use_proxy_pool else 0,
                            json.dumps(preferred_proxy_tags or [], ensure_ascii=False),
                            assigned_worker_group,
                            notes,
                            now,
                            now,
                        ),
                    )
                row = conn.execute(
                    "SELECT * FROM site_execution_policies WHERE tenant_id=? AND policy_id=?",
                    (normalized_tenant_id, normalized_policy_id),
                ).fetchone()
                conn.commit()
        if row is None:
            raise RuntimeError("failed to persist site policy")
        return SitePolicyRecord.from_row(row)

    def list_site_policies(self, limit: int = 100, tenant_id: str = "") -> list[SitePolicyRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM site_execution_policies WHERE tenant_id=? ORDER BY updated_at DESC, id DESC LIMIT ?",
                (self._normalize_tenant_id(tenant_id), int(limit)),
            ).fetchall()
        return [SitePolicyRecord.from_row(row) for row in rows]

    def get_site_policy_for_url(self, url: str, tenant_id: str = "") -> SitePolicyRecord | None:
        domain = urlparse(str(url or "").strip()).netloc.strip().lower()
        if not domain:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM site_execution_policies WHERE tenant_id=? AND domain=?",
                (self._normalize_tenant_id(tenant_id), domain),
            ).fetchone()
        return SitePolicyRecord.from_row(row) if row is not None else None

    def acquire_site_execution_slot(
        self,
        *,
        domain: str,
        tenant_id: str = "",
        min_interval_seconds: float = 0.0,
        max_concurrency: int = 1,
    ) -> dict[str, Any]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        normalized_domain = str(domain or "").strip().lower()
        if not normalized_domain:
            return {"acquired": True, "wait_seconds": 0.0}
        now = datetime.now()
        now_text = now.strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            with self._connect() as conn:
                conn.begin_immediate()
                row = conn.execute(
                    "SELECT * FROM site_execution_runtime WHERE tenant_id=? AND domain=?",
                    (normalized_tenant_id, normalized_domain),
                ).fetchone()
                if row is None:
                    next_allowed_at = datetime.fromtimestamp(
                        now.timestamp() + max(float(min_interval_seconds or 0.0), 0.0)
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    conn.execute(
                        """
                        INSERT INTO site_execution_runtime (
                            tenant_id, domain, active_count, next_allowed_at,
                            last_started_at, last_finished_at, updated_at
                        ) VALUES (?, ?, 1, ?, ?, '', ?)
                        """,
                        (normalized_tenant_id, normalized_domain, next_allowed_at, now_text, now_text),
                    )
                    conn.commit()
                    return {"acquired": True, "wait_seconds": 0.0, "domain": normalized_domain}

                active_count = int(row["active_count"] or 0)
                next_allowed_at_text = str(row["next_allowed_at"] or "").strip()
                wait_seconds = 0.0
                if active_count >= max(1, int(max_concurrency or 1)):
                    wait_seconds = max(wait_seconds, 0.5)
                if next_allowed_at_text:
                    try:
                        next_allowed_at = datetime.strptime(next_allowed_at_text, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        next_allowed_at = now
                    wait_seconds = max(wait_seconds, (next_allowed_at - now).total_seconds())
                if wait_seconds > 0.01:
                    conn.commit()
                    return {
                        "acquired": False,
                        "wait_seconds": round(max(wait_seconds, 0.1), 3),
                        "domain": normalized_domain,
                    }

                next_allowed_at = datetime.fromtimestamp(
                    now.timestamp() + max(float(min_interval_seconds or 0.0), 0.0)
                ).strftime("%Y-%m-%d %H:%M:%S")
                conn.execute(
                    """
                    UPDATE site_execution_runtime
                    SET active_count=?, next_allowed_at=?, last_started_at=?, updated_at=?
                    WHERE tenant_id=? AND domain=?
                    """,
                    (
                        active_count + 1,
                        next_allowed_at,
                        now_text,
                        now_text,
                        normalized_tenant_id,
                        normalized_domain,
                    ),
                )
                conn.commit()
        return {"acquired": True, "wait_seconds": 0.0, "domain": normalized_domain}

    def release_site_execution_slot(self, *, domain: str, tenant_id: str = "") -> None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        normalized_domain = str(domain or "").strip().lower()
        if not normalized_domain:
            return
        now = self._now_text()
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT active_count FROM site_execution_runtime WHERE tenant_id=? AND domain=?",
                    (normalized_tenant_id, normalized_domain),
                ).fetchone()
                if row is None:
                    conn.commit()
                    return
                active_count = max(int(row["active_count"] or 0) - 1, 0)
                conn.execute(
                    """
                    UPDATE site_execution_runtime
                    SET active_count=?, last_finished_at=?, updated_at=?
                    WHERE tenant_id=? AND domain=?
                    """,
                    (active_count, now, now, normalized_tenant_id, normalized_domain),
                )
                conn.commit()

    def create_task_annotation(
        self,
        *,
        task_id: str,
        profile_id: str = "",
        template_id: str = "",
        corrected_data: Optional[dict[str, Any]] = None,
        field_feedback: Optional[dict[str, Any]] = None,
        notes: str = "",
        created_by: str = "",
        tenant_id: str = "",
    ) -> TaskAnnotationRecord:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        annotation_id = f"ann-{uuid4().hex[:10]}"
        now = self._now_text()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO task_annotations (
                        annotation_id, tenant_id, task_id, profile_id, template_id,
                        corrected_data_json, field_feedback_json, notes, created_by, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        annotation_id,
                        normalized_tenant_id,
                        task_id,
                        profile_id,
                        template_id,
                        json.dumps(corrected_data or {}, ensure_ascii=False),
                        json.dumps(field_feedback or {}, ensure_ascii=False),
                        notes,
                        created_by,
                        now,
                        now,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM task_annotations WHERE tenant_id=? AND annotation_id=?",
                    (normalized_tenant_id, annotation_id),
                ).fetchone()
                conn.commit()
        if row is None:
            raise RuntimeError("failed to persist task annotation")
        return TaskAnnotationRecord.from_row(row)

    def list_task_annotations(
        self,
        *,
        limit: int = 50,
        task_id: str = "",
        tenant_id: str = "",
    ) -> list[TaskAnnotationRecord]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        with self._connect() as conn:
            if str(task_id or "").strip():
                rows = conn.execute(
                    "SELECT * FROM task_annotations WHERE tenant_id=? AND task_id=? ORDER BY id DESC LIMIT ?",
                    (normalized_tenant_id, task_id, int(limit)),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM task_annotations WHERE tenant_id=? ORDER BY id DESC LIMIT ?",
                    (normalized_tenant_id, int(limit)),
                ).fetchall()
        return [TaskAnnotationRecord.from_row(row) for row in rows]

    def create_repair_suggestion(
        self,
        *,
        annotation_id: str,
        task_id: str,
        profile_id: str = "",
        template_id: str = "",
        status: str = "suggested",
        repair_strategy: str = "manual_feedback",
        suggested_fields: Optional[list[str]] = None,
        suggested_field_labels: Optional[dict[str, str]] = None,
        suggested_profile: Optional[dict[str, Any]] = None,
        reason: str = "",
        tenant_id: str = "",
    ) -> RepairSuggestionRecord:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        repair_id = f"repair-{uuid4().hex[:10]}"
        now = self._now_text()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO template_repair_suggestions (
                        repair_id, tenant_id, annotation_id, task_id, profile_id, template_id,
                        status, repair_strategy, suggested_fields_json, suggested_field_labels_json,
                        suggested_profile_json, reason, created_at, updated_at, applied_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        repair_id,
                        normalized_tenant_id,
                        annotation_id,
                        task_id,
                        profile_id,
                        template_id,
                        status,
                        repair_strategy,
                        json.dumps(suggested_fields or [], ensure_ascii=False),
                        json.dumps(suggested_field_labels or {}, ensure_ascii=False),
                        json.dumps(suggested_profile or {}, ensure_ascii=False),
                        reason,
                        now,
                        now,
                        now if status == "applied" else "",
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM template_repair_suggestions WHERE tenant_id=? AND repair_id=?",
                    (normalized_tenant_id, repair_id),
                ).fetchone()
                conn.commit()
        if row is None:
            raise RuntimeError("failed to persist repair suggestion")
        return RepairSuggestionRecord.from_row(row)

    def list_repair_suggestions(
        self,
        *,
        limit: int = 50,
        task_id: str = "",
        tenant_id: str = "",
    ) -> list[RepairSuggestionRecord]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        with self._connect() as conn:
            if str(task_id or "").strip():
                rows = conn.execute(
                    "SELECT * FROM template_repair_suggestions WHERE tenant_id=? AND task_id=? ORDER BY id DESC LIMIT ?",
                    (normalized_tenant_id, task_id, int(limit)),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM template_repair_suggestions WHERE tenant_id=? ORDER BY id DESC LIMIT ?",
                    (normalized_tenant_id, int(limit)),
                ).fetchall()
        return [RepairSuggestionRecord.from_row(row) for row in rows]

    def create_funnel_event(
        self,
        *,
        stage: str,
        channel: str,
        package_type: str,
        package_id: str = "",
        package_name: str = "",
        task_id: str = "",
        template_id: str = "",
        monitor_id: str = "",
        actor_instance_id: str = "",
        metadata: Optional[dict[str, Any]] = None,
        tenant_id: str = "",
    ) -> FunnelEventRecord:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        funnel_event_id = f"funnel-{uuid4().hex[:10]}"
        now = self._now_text()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO growth_funnel_events (
                        funnel_event_id, tenant_id, stage, channel, package_type, package_id,
                        package_name, task_id, template_id, monitor_id, actor_instance_id,
                        metadata_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        funnel_event_id,
                        normalized_tenant_id,
                        stage,
                        channel,
                        package_type,
                        package_id,
                        package_name,
                        task_id,
                        template_id,
                        monitor_id,
                        actor_instance_id,
                        json.dumps(metadata or {}, ensure_ascii=False),
                        now,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM growth_funnel_events WHERE tenant_id=? AND funnel_event_id=?",
                    (normalized_tenant_id, funnel_event_id),
                ).fetchone()
                conn.commit()
        if row is None:
            raise RuntimeError("failed to persist funnel event")
        return FunnelEventRecord.from_row(row)

    def list_funnel_events(
        self,
        *,
        limit: int = 100,
        stage: str = "",
        package_id: str = "",
        tenant_id: str = "",
    ) -> list[FunnelEventRecord]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        normalized_stage = str(stage or "").strip()
        normalized_package_id = str(package_id or "").strip()
        sql = "SELECT * FROM growth_funnel_events WHERE tenant_id=?"
        params: list[Any] = [normalized_tenant_id]
        if normalized_stage:
            sql += " AND stage=?"
            params.append(normalized_stage)
        if normalized_package_id:
            sql += " AND package_id=?"
            params.append(normalized_package_id)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(int(limit))
        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [FunnelEventRecord.from_row(row) for row in rows]

    def build_funnel_summary(
        self,
        *,
        tenant_id: str = "",
    ) -> dict[str, Any]:
        events = self.list_funnel_events(limit=500, tenant_id=tenant_id)
        by_stage: dict[str, int] = {}
        by_package: dict[str, dict[str, Any]] = {}
        for item in events:
            by_stage[item.stage] = by_stage.get(item.stage, 0) + 1
            package_key = item.package_id or f"{item.package_type}:unknown"
            package_summary = by_package.setdefault(
                package_key,
                {
                    "package_id": item.package_id,
                    "package_name": item.package_name,
                    "package_type": item.package_type,
                    "stages": {},
                },
            )
            package_stages = package_summary["stages"]
            package_stages[item.stage] = package_stages.get(item.stage, 0) + 1
        return {
            "total_events": len(events),
            "by_stage": by_stage,
            "top_packages": sorted(
                by_package.values(),
                key=lambda payload: (
                    -sum(int(value) for value in payload["stages"].values()),
                    str(payload["package_id"] or ""),
                ),
            )[:10],
        }

    def _update_fields(self, task_id: str, tenant_id: str = "", **fields) -> None:
        update_task_fields(
            lock=self._lock,
            connect=self._connect,
            task_id=task_id,
            allowed_fields=self._ALLOWED_UPDATE_FIELDS,
            fields=fields,
            tenant_id=self._normalize_tenant_id(tenant_id),
        )
