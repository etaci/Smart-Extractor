"""
Web task persistence and insight helpers backed by SQLite.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta
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


_FETCH_FAILURE_MARKERS = (
    "404",
    "401",
    "403",
    "429",
    "anti_bot",
    "anti bot",
    "blocked",
    "captcha",
    "challenge",
    "timeout",
    "timed out",
    "network",
    "connection",
    "shell",
    "fetch",
)


def _classify_task_result_bucket(*, status: str, error: str, data_json: str) -> str:
    normalized_status = str(status or "").strip().lower()
    payload: dict[str, Any] = {}
    if data_json:
        try:
            parsed = json.loads(data_json)
            if isinstance(parsed, dict):
                payload = parsed
        except Exception:
            payload = {}

    validation = payload.get("_validation") if isinstance(payload.get("_validation"), dict) else {}
    validation_status = str(validation.get("status") or "").strip().lower()
    if normalized_status == "success":
        if validation_status == "partial_success":
            return "partial_success"
        if validation_status in {"failed", "validation_failed"}:
            return "validation_failed"
        return "full_success"

    diagnostic_text = " ".join(
        str(part or "")
        for part in (
            error,
            payload.get("error"),
            payload.get("failure_category"),
            validation_status,
            " ".join(str(item) for item in validation.get("errors", []) or []),
        )
    ).lower()
    if any(marker in diagnostic_text for marker in _FETCH_FAILURE_MARKERS):
        return "fetch_failed"
    if validation_status in {"failed", "validation_failed"} or "validation" in diagnostic_text:
        return "validation_failed"
    return "failed"


def _extract_template_runtime_diagnostics(data_json: str) -> dict[str, int | str]:
    try:
        parsed = json.loads(data_json) if data_json else {}
    except Exception:
        return {"template_id": ""}
    if not isinstance(parsed, dict):
        return {"template_id": ""}
    context = (
        parsed.get("_execution_context")
        if isinstance(parsed.get("_execution_context"), dict)
        else {}
    )
    template_id = str(context.get("template_id") or "").strip()
    if not template_id:
        return {"template_id": ""}
    strategy_details = (
        parsed.get("strategy_details")
        if isinstance(parsed.get("strategy_details"), dict)
        else {}
    )
    runtime = (
        parsed.get("_runtime_metrics")
        if isinstance(parsed.get("_runtime_metrics"), dict)
        else {}
    )
    source_fields = (
        strategy_details.get("source_fields")
        if isinstance(strategy_details.get("source_fields"), dict)
        else {}
    )
    return {
        "template_id": template_id,
        "structured_hit_count": 1 if source_fields else 0,
        "normalized_count": 1 if strategy_details.get("normalization_version") else 0,
        "specialized_rule_count": (
            1 if str(parsed.get("extraction_strategy") or "") == "specialized_rule" else 0
        ),
        "json_response_count": int(runtime.get("fetch_json_response_count", 0) or 0),
        "mobile_fallback_count": 1 if bool(runtime.get("mobile_ua_fallback")) else 0,
        "html_compare_count": (
            1
            if int(runtime.get("static_html_length", 0) or 0) > 0
            and int(runtime.get("dynamic_html_length", 0) or 0) > 0
            else 0
        ),
    }
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
        if task_kind != "batch":
            self.enforce_quota(
                tenant_id=normalized_tenant_id,
                event_type="task_create",
                amount=1,
                urls=1,
            )
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
        self.record_usage_event(
            tenant_id=normalized_tenant_id,
            tasks_created=1,
            urls_submitted=0 if task_kind == "batch" else 1,
        )
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
        self.enforce_quota(
            tenant_id=normalized_tenant_id,
            event_type="batch_create",
            amount=len(normalized_urls) + 1,
            urls=len(normalized_urls),
        )
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
        is_create = not str(monitor_id or "").strip()
        if is_create:
            self.enforce_quota(
                tenant_id=normalized_tenant_id,
                event_type="monitor_create",
                amount=1,
            )
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
        if is_create:
            self.record_usage_event(
                tenant_id=normalized_tenant_id,
                monitors_created=1,
            )
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
        dispatch_backend = getattr(spec, "dispatch_backend", "sqlite")
        queue_scope = getattr(spec, "queue_scope", "*")
        enqueue_task_payload(
            lock=self._lock,
            connect=self._connect,
            task_id=spec.task_id,
            payload=spec.to_queue_payload(),
            backend=str(dispatch_backend or "sqlite").strip().lower() or "sqlite",
            queue_scope=str(queue_scope or "").strip() or "*",
            tenant_id=normalized_tenant_id,
        )
        self.mark_queued(spec.task_id, tenant_id=normalized_tenant_id)

    def claim_next_queued_task(
        self,
        *,
        worker_id: str,
        stale_after_seconds: float = 0.0,
        queue_scope: str = "*",
        tenant_id: str = "",
    ) -> tuple[TaskRecord, dict[str, Any]] | None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        claimed = claim_queued_task_payload(
            lock=self._lock,
            connect=self._connect,
            worker_id=worker_id,
            stale_after_seconds=stale_after_seconds,
            queue_scope=queue_scope,
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
        normalized_data = self._ensure_observability_payload(
            data or {},
            status="success",
            elapsed_ms=elapsed_ms,
        )
        self._update_fields(
            task_id,
            tenant_id=normalized_tenant_id,
            **build_success_fields(
                elapsed_ms=elapsed_ms,
                quality_score=quality_score,
                data=normalized_data,
            ),
        )
        self._refresh_parent_task(task_id, tenant_id=normalized_tenant_id)
        self.record_task_operational_snapshot(task_id, tenant_id=normalized_tenant_id)

    def mark_failed(
        self,
        task_id: str,
        elapsed_ms: float,
        error: str,
        tenant_id: str = "",
        data: Optional[dict[str, Any]] = None,
    ) -> None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        fields = build_failed_fields(elapsed_ms=elapsed_ms, error=error)
        fields["data_json"] = json.dumps(
            self._ensure_observability_payload(
                data or {},
                status="failed",
                elapsed_ms=elapsed_ms,
                error=error,
            ),
            ensure_ascii=False,
        )
        self._update_fields(
            task_id,
            tenant_id=normalized_tenant_id,
            **fields,
        )
        self._refresh_parent_task(task_id, tenant_id=normalized_tenant_id)
        self.record_task_operational_snapshot(task_id, tenant_id=normalized_tenant_id)

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
        candidates = self.pick_proxy_endpoints(
            preferred_tags=preferred_tags,
            limit=1,
            tenant_id=tenant_id,
        )
        return candidates[0] if candidates else None

    def pick_proxy_endpoints(
        self,
        *,
        preferred_tags: Optional[list[str]] = None,
        limit: int = 5,
        tenant_id: str = "",
    ) -> list[ProxyEndpointRecord]:
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
            return []
        candidates.sort(
            key=lambda item: (
                int(item.failure_count or 0),
                -int(item.success_count or 0),
                str(item.last_used_at or ""),
            )
        )
        return candidates[: max(int(limit or 0), 1)]

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

    @staticmethod
    def _ensure_observability_payload(
        data: dict[str, Any] | None,
        *,
        status: str,
        elapsed_ms: float,
        error: str = "",
    ) -> dict[str, Any]:
        payload = dict(data or {})
        extractor_stats = (
            payload.get("_extractor_stats")
            if isinstance(payload.get("_extractor_stats"), dict)
            else {}
        )
        llm_usage = (
            payload.get("_llm_usage")
            if isinstance(payload.get("_llm_usage"), dict)
            else {}
        )
        payload["_llm_usage"] = {
            "total_calls": int(
                llm_usage.get("total_calls", extractor_stats.get("total_calls", 0)) or 0
            ),
            "prompt_tokens": int(
                llm_usage.get("prompt_tokens", extractor_stats.get("prompt_tokens", 0)) or 0
            ),
            "completion_tokens": int(
                llm_usage.get(
                    "completion_tokens",
                    extractor_stats.get("completion_tokens", 0),
                )
                or 0
            ),
            "total_tokens": int(
                llm_usage.get("total_tokens", extractor_stats.get("total_tokens", 0)) or 0
            ),
            "estimated_cost_usd": float(
                llm_usage.get(
                    "estimated_cost_usd",
                    extractor_stats.get("estimated_cost_usd", 0.0),
                )
                or 0.0
            ),
            "api_usage_calls": int(llm_usage.get("api_usage_calls", 0) or 0),
            "estimated_usage_calls": int(llm_usage.get("estimated_usage_calls", 0) or 0),
            "api_usage_ratio": float(llm_usage.get("api_usage_ratio", 0.0) or 0.0),
        }

        runtime_metrics = (
            payload.get("_runtime_metrics")
            if isinstance(payload.get("_runtime_metrics"), dict)
            else {}
        )
        fetch_elapsed_ms = float(runtime_metrics.get("fetch_elapsed_ms", 0.0) or 0.0)
        fetcher_type = str(runtime_metrics.get("fetcher_type") or "").strip()
        payload["_runtime_metrics"] = {
            "fetcher_type": fetcher_type or "unknown",
            "fetch_elapsed_ms": fetch_elapsed_ms,
            "playwright_elapsed_ms": float(
                runtime_metrics.get("playwright_elapsed_ms", 0.0) or 0.0
            ),
            "retry_count": int(runtime_metrics.get("retry_count", 0) or 0),
            "retry_cost_usd": float(runtime_metrics.get("retry_cost_usd", 0.0) or 0.0),
            "total_elapsed_ms": float(
                runtime_metrics.get("total_elapsed_ms", elapsed_ms) or 0.0
            ),
            "status": str(status or "").strip(),
            "error_type": str(runtime_metrics.get("error_type") or "").strip(),
        }
        if error and not payload["_runtime_metrics"]["error_type"]:
            payload["_runtime_metrics"]["error_type"] = error.split(":", 1)[0].strip()
        return payload

    @staticmethod
    def _usage_date() -> str:
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def _month_window() -> tuple[str, str]:
        today = datetime.now().date()
        month_start = today.replace(day=1)
        if month_start.month == 12:
            next_month = month_start.replace(year=month_start.year + 1, month=1)
        else:
            next_month = month_start.replace(month=month_start.month + 1)
        return month_start.strftime("%Y-%m-%d"), next_month.strftime("%Y-%m-%d")

    @staticmethod
    def _default_quota_plan(tenant_id: str) -> dict[str, Any]:
        return {
            "tenant_id": tenant_id,
            "plan_name": "trial",
            "monthly_task_limit": 1000,
            "monthly_url_limit": 3000,
            "monitor_limit": 50,
            "monthly_token_limit": 1000000,
            "export_limit": 200,
            "max_concurrency": 3,
            "overage_policy": "reject",
            "notes": "default trial quota",
            "created_at": "",
            "updated_at": "",
        }

    def get_quota_plan(self, tenant_id: str = "") -> dict[str, Any]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM tenant_quota_plans WHERE tenant_id=?",
                (normalized_tenant_id,),
            ).fetchone()
        if row is None:
            return self._default_quota_plan(normalized_tenant_id)
        return {
            "tenant_id": row["tenant_id"] or normalized_tenant_id,
            "plan_name": row["plan_name"] or "trial",
            "monthly_task_limit": int(row["monthly_task_limit"] or 0),
            "monthly_url_limit": int(row["monthly_url_limit"] or 0),
            "monitor_limit": int(row["monitor_limit"] or 0),
            "monthly_token_limit": int(row["monthly_token_limit"] or 0),
            "export_limit": int(row["export_limit"] or 0),
            "max_concurrency": int(row["max_concurrency"] or 0),
            "overage_policy": row["overage_policy"] or "reject",
            "notes": row["notes"] or "",
            "created_at": row["created_at"] or "",
            "updated_at": row["updated_at"] or "",
        }

    def upsert_quota_plan(
        self,
        *,
        tenant_id: str = "",
        plan_name: str = "trial",
        monthly_task_limit: int = 1000,
        monthly_url_limit: int = 3000,
        monitor_limit: int = 50,
        monthly_token_limit: int = 1000000,
        export_limit: int = 200,
        max_concurrency: int = 3,
        overage_policy: str = "reject",
        notes: str = "",
    ) -> dict[str, Any]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        now = self._now_text()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO tenant_quota_plans (
                        tenant_id, plan_name, monthly_task_limit, monthly_url_limit,
                        monitor_limit, monthly_token_limit, export_limit, max_concurrency,
                        overage_policy, notes, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(tenant_id) DO UPDATE SET
                        plan_name=excluded.plan_name,
                        monthly_task_limit=excluded.monthly_task_limit,
                        monthly_url_limit=excluded.monthly_url_limit,
                        monitor_limit=excluded.monitor_limit,
                        monthly_token_limit=excluded.monthly_token_limit,
                        export_limit=excluded.export_limit,
                        max_concurrency=excluded.max_concurrency,
                        overage_policy=excluded.overage_policy,
                        notes=excluded.notes,
                        updated_at=excluded.updated_at
                    """,
                    (
                        normalized_tenant_id,
                        str(plan_name or "trial").strip() or "trial",
                        max(int(monthly_task_limit or 0), 0),
                        max(int(monthly_url_limit or 0), 0),
                        max(int(monitor_limit or 0), 0),
                        max(int(monthly_token_limit or 0), 0),
                        max(int(export_limit or 0), 0),
                        max(int(max_concurrency or 0), 0),
                        str(overage_policy or "reject").strip().lower() or "reject",
                        str(notes or "").strip(),
                        now,
                        now,
                    ),
                )
                conn.commit()
        return self.get_quota_plan(normalized_tenant_id)

    def record_usage_event(
        self,
        *,
        tenant_id: str = "",
        tasks_created: int = 0,
        urls_submitted: int = 0,
        monitors_created: int = 0,
        exports_count: int = 0,
        total_tokens: int = 0,
        model_cost_usd: float = 0.0,
        playwright_elapsed_ms: float = 0.0,
        retry_count: int = 0,
    ) -> None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        usage_date = self._usage_date()
        now = self._now_text()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO tenant_usage_daily (
                        tenant_id, usage_date, tasks_created, urls_submitted,
                        monitors_created, exports_count, total_tokens, model_cost_usd,
                        playwright_elapsed_ms, retry_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(tenant_id, usage_date) DO UPDATE SET
                        tasks_created=tasks_created + excluded.tasks_created,
                        urls_submitted=urls_submitted + excluded.urls_submitted,
                        monitors_created=monitors_created + excluded.monitors_created,
                        exports_count=exports_count + excluded.exports_count,
                        total_tokens=total_tokens + excluded.total_tokens,
                        model_cost_usd=model_cost_usd + excluded.model_cost_usd,
                        playwright_elapsed_ms=playwright_elapsed_ms + excluded.playwright_elapsed_ms,
                        retry_count=retry_count + excluded.retry_count,
                        updated_at=excluded.updated_at
                    """,
                    (
                        normalized_tenant_id,
                        usage_date,
                        max(int(tasks_created or 0), 0),
                        max(int(urls_submitted or 0), 0),
                        max(int(monitors_created or 0), 0),
                        max(int(exports_count or 0), 0),
                        max(int(total_tokens or 0), 0),
                        max(float(model_cost_usd or 0.0), 0.0),
                        max(float(playwright_elapsed_ms or 0.0), 0.0),
                        max(int(retry_count or 0), 0),
                        now,
                        now,
                    ),
                )
                conn.commit()

    def build_usage_summary(self, tenant_id: str = "") -> dict[str, Any]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        month_start, next_month = self._month_window()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM tenant_usage_daily
                WHERE tenant_id=? AND usage_date>=? AND usage_date<?
                ORDER BY usage_date ASC
                """,
                (normalized_tenant_id, month_start, next_month),
            ).fetchall()
        totals: dict[str, Any] = {
            "tasks_created": 0,
            "urls_submitted": 0,
            "monitors_created": 0,
            "exports_count": 0,
            "total_tokens": 0,
            "model_cost_usd": 0.0,
            "playwright_elapsed_ms": 0.0,
            "retry_count": 0,
        }
        daily = []
        for row in rows:
            payload = {
                "usage_date": row["usage_date"],
                "tasks_created": int(row["tasks_created"] or 0),
                "urls_submitted": int(row["urls_submitted"] or 0),
                "monitors_created": int(row["monitors_created"] or 0),
                "exports_count": int(row["exports_count"] or 0),
                "total_tokens": int(row["total_tokens"] or 0),
                "model_cost_usd": float(row["model_cost_usd"] or 0.0),
                "playwright_elapsed_ms": float(row["playwright_elapsed_ms"] or 0.0),
                "retry_count": int(row["retry_count"] or 0),
            }
            daily.append(payload)
            for key in totals:
                totals[key] += payload[key]
        totals["model_cost_usd"] = round(float(totals["model_cost_usd"]), 6)
        totals["playwright_elapsed_ms"] = round(float(totals["playwright_elapsed_ms"]), 2)
        plan = self.get_quota_plan(normalized_tenant_id)
        monitor_count = len(self.list_monitors(limit=10000, tenant_id=normalized_tenant_id))
        limits = {
            "monthly_task_limit": int(plan["monthly_task_limit"]),
            "monthly_url_limit": int(plan["monthly_url_limit"]),
            "monitor_limit": int(plan["monitor_limit"]),
            "monthly_token_limit": int(plan["monthly_token_limit"]),
            "export_limit": int(plan["export_limit"]),
        }
        usage_ratio = {
            "tasks": round(totals["tasks_created"] / max(limits["monthly_task_limit"], 1), 4),
            "urls": round(totals["urls_submitted"] / max(limits["monthly_url_limit"], 1), 4),
            "monitors": round(monitor_count / max(limits["monitor_limit"], 1), 4),
            "tokens": round(totals["total_tokens"] / max(limits["monthly_token_limit"], 1), 4),
            "exports": round(totals["exports_count"] / max(limits["export_limit"], 1), 4),
        }
        return {
            "tenant_id": normalized_tenant_id,
            "period": {"start": month_start, "end": next_month},
            "plan": plan,
            "totals": {**totals, "monitor_count": monitor_count},
            "limits": limits,
            "usage_ratio": usage_ratio,
            "daily": daily,
        }

    def build_operational_overview(self, tenant_id: str = "") -> dict[str, Any]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        today = self._usage_date()
        usage = self.build_usage_summary(normalized_tenant_id)
        today_usage = next(
            (item for item in usage["daily"] if item.get("usage_date") == today),
            {
                "tasks_created": 0,
                "exports_count": 0,
                "total_tokens": 0,
                "model_cost_usd": 0.0,
            },
        )
        with self._connect() as conn:
            status_rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM web_tasks
                WHERE tenant_id=? AND substr(created_at, 1, 10)=?
                GROUP BY status
                """,
                (normalized_tenant_id, today),
            ).fetchall()
            task_rows = conn.execute(
                """
                SELECT task_id, status, error, data_json
                FROM web_tasks
                WHERE tenant_id=? AND substr(created_at, 1, 10)=?
                """,
                (normalized_tenant_id, today),
            ).fetchall()
            failure_rows = conn.execute(
                """
                SELECT failure_category, COUNT(*) AS count
                FROM task_operational_metrics
                WHERE tenant_id=? AND status='failed'
                    AND substr(updated_at, 1, 10)=?
                GROUP BY failure_category
                ORDER BY count DESC, failure_category ASC
                LIMIT 8
                """,
                (normalized_tenant_id, today),
            ).fetchall()
        by_status = {
            str(row["status"] or "unknown"): int(row["count"] or 0)
            for row in status_rows
        }
        today_total = sum(by_status.values())
        today_success = int(by_status.get("success", 0))
        success_rate = round(today_success / max(today_total, 1), 4)
        failure_breakdown = [
            {
                "category": str(row["failure_category"] or "unknown"),
                "count": int(row["count"] or 0),
            }
            for row in failure_rows
        ]
        result_counts = {
            "full_success": 0,
            "partial_success": 0,
            "fetch_failed": 0,
            "validation_failed": 0,
            "failed": 0,
        }
        for row in task_rows:
            bucket = _classify_task_result_bucket(
                status=str(row["status"] or ""),
                error=str(row["error"] or ""),
                data_json=str(row["data_json"] or ""),
            )
            result_counts[bucket] = result_counts.get(bucket, 0) + 1
        result_breakdown = [
            {"category": category, "count": count}
            for category, count in result_counts.items()
            if count > 0
        ]
        return {
            "tenant_id": normalized_tenant_id,
            "date": today,
            "today_tasks": int(today_usage.get("tasks_created") or today_total or 0),
            "today_success_rate": success_rate,
            "today_success_rate_label": f"{success_rate * 100:.1f}%",
            "failure_breakdown": failure_breakdown,
            "result_breakdown": result_breakdown,
            "token_cost": {
                "total_tokens": int(usage["totals"].get("total_tokens") or 0),
                "today_tokens": int(today_usage.get("total_tokens") or 0),
                "model_cost_usd": float(usage["totals"].get("model_cost_usd") or 0.0),
                "today_model_cost_usd": float(today_usage.get("model_cost_usd") or 0.0),
            },
            "quota_usage_ratio": usage["usage_ratio"],
            "active_monitors": int(usage["totals"].get("monitor_count") or 0),
            "exports_count": int(usage["totals"].get("exports_count") or 0),
            "today_exports_count": int(today_usage.get("exports_count") or 0),
            "usage": usage,
        }

    def build_template_scores(self, tenant_id: str = "", limit: int = 100) -> dict[str, Any]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        with self._connect() as conn:
            template_rows = conn.execute(
                """
                SELECT *
                FROM extraction_templates
                WHERE tenant_id=?
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                (normalized_tenant_id, max(1, min(int(limit or 100), 500))),
            ).fetchall()
            metric_rows = conn.execute(
                """
                SELECT template_id,
                    COUNT(*) AS total_count,
                    SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_count,
                    SUM(field_count) AS field_count,
                    SUM(filled_field_count) AS filled_field_count,
                    SUM(empty_field_count) AS empty_field_count
                FROM task_operational_metrics
                WHERE tenant_id=? AND template_id!=''
                GROUP BY template_id
                """,
                (normalized_tenant_id,),
            ).fetchall()
            feedback_rows = conn.execute(
                """
                SELECT template_id, feedback_status, COUNT(*) AS count
                FROM field_quality_feedback
                WHERE tenant_id=? AND template_id!=''
                GROUP BY template_id, feedback_status
                """,
                (normalized_tenant_id,),
            ).fetchall()
            failure_rows = conn.execute(
                """
                SELECT template_id, failure_category, updated_at
                FROM task_operational_metrics
                WHERE tenant_id=? AND template_id!='' AND status='failed'
                ORDER BY updated_at DESC, id DESC
                """,
                (normalized_tenant_id,),
            ).fetchall()
            diagnostic_rows = conn.execute(
                """
                SELECT data_json
                FROM web_tasks
                WHERE tenant_id=? AND data_json!=''
                ORDER BY created_at DESC, id DESC
                LIMIT 500
                """,
                (normalized_tenant_id,),
            ).fetchall()

        metrics_by_template = {
            str(row["template_id"] or ""): {
                "total_count": int(row["total_count"] or 0),
                "success_count": int(row["success_count"] or 0),
                "failed_count": int(row["failed_count"] or 0),
                "field_count": int(row["field_count"] or 0),
                "filled_field_count": int(row["filled_field_count"] or 0),
                "empty_field_count": int(row["empty_field_count"] or 0),
            }
            for row in metric_rows
        }
        feedback_by_template: dict[str, dict[str, int]] = {}
        for row in feedback_rows:
            template_id = str(row["template_id"] or "")
            status = str(row["feedback_status"] or "unknown")
            summary = feedback_by_template.setdefault(
                template_id,
                {"correct": 0, "incorrect": 0, "missing": 0, "unknown": 0},
            )
            summary[status if status in summary else "unknown"] += int(row["count"] or 0)

        recent_failure_by_template: dict[str, dict[str, Any]] = {}
        failure_trend_by_template: dict[str, dict[str, int]] = {}
        for row in failure_rows:
            template_id = str(row["template_id"] or "")
            category = str(row["failure_category"] or "unknown")
            trend = failure_trend_by_template.setdefault(template_id, {})
            trend[category] = int(trend.get(category, 0)) + 1
            if template_id in recent_failure_by_template:
                continue
            recent_failure_by_template[template_id] = {
                "category": category,
                "at": str(row["updated_at"] or ""),
            }

        diagnostics_by_template: dict[str, dict[str, int]] = {}
        for row in diagnostic_rows:
            diagnostics = _extract_template_runtime_diagnostics(str(row["data_json"] or ""))
            template_id = diagnostics.pop("template_id", "")
            if not template_id:
                continue
            summary = diagnostics_by_template.setdefault(
                template_id,
                {
                    "structured_hit_count": 0,
                    "normalized_count": 0,
                    "specialized_rule_count": 0,
                    "json_response_count": 0,
                    "mobile_fallback_count": 0,
                    "html_compare_count": 0,
                },
            )
            for key, value in diagnostics.items():
                summary[key] = int(summary.get(key, 0)) + int(value or 0)

        template_ids = {
            str(row["template_id"] or "").strip()
            for row in template_rows
            if str(row["template_id"] or "").strip()
        }
        template_ids.update(metrics_by_template)
        template_ids.update(feedback_by_template)

        template_names = {
            str(row["template_id"] or ""): str(row["name"] or row["template_id"] or "")
            for row in template_rows
        }
        scores = []
        for template_id in sorted(template_ids):
            metric = metrics_by_template.get(
                template_id,
                {
                    "total_count": 0,
                    "success_count": 0,
                    "failed_count": 0,
                    "field_count": 0,
                    "filled_field_count": 0,
                    "empty_field_count": 0,
                },
            )
            feedback = feedback_by_template.get(
                template_id,
                {"correct": 0, "incorrect": 0, "missing": 0, "unknown": 0},
            )
            feedback_total = sum(int(value or 0) for value in feedback.values())
            success_rate = metric["success_count"] / max(metric["total_count"], 1)
            field_hit_rate = metric["filled_field_count"] / max(metric["field_count"], 1)
            field_correct_rate = feedback["correct"] / max(feedback_total, 1)
            field_missing_rate = feedback["missing"] / max(feedback_total, 1)
            quality_score = round(
                (success_rate * 0.6)
                + (field_correct_rate * 0.3)
                + ((1.0 - field_missing_rate) * 0.1),
                4,
            )
            scores.append(
                {
                    "template_id": template_id,
                    "name": template_names.get(template_id, template_id),
                    "success_rate": round(success_rate, 4),
                    "success_count": metric["success_count"],
                    "failed_count": metric["failed_count"],
                    "total_count": metric["total_count"],
                    "field_count": metric["field_count"],
                    "filled_field_count": metric["filled_field_count"],
                    "empty_field_count": metric["empty_field_count"],
                    "field_hit_rate": round(field_hit_rate, 4),
                    "field_correct_rate": round(field_correct_rate, 4),
                    "field_missing_rate": round(field_missing_rate, 4),
                    "field_feedback_count": feedback_total,
                    "field_feedback": feedback,
                    "failure_trend": failure_trend_by_template.get(template_id, {}),
                    "runtime_diagnostics": diagnostics_by_template.get(
                        template_id,
                        {
                            "structured_hit_count": 0,
                            "normalized_count": 0,
                            "specialized_rule_count": 0,
                            "json_response_count": 0,
                            "mobile_fallback_count": 0,
                            "html_compare_count": 0,
                        },
                    ),
                    "recent_failure": recent_failure_by_template.get(
                        template_id,
                        {"category": "", "at": ""},
                    ),
                    "quality_score": quality_score,
                }
            )

        sorted_scores = sorted(
            scores,
            key=lambda item: (
                -float(item["quality_score"]),
                -int(item["total_count"]),
                str(item["template_id"]),
            ),
        )
        return {
            "tenant_id": normalized_tenant_id,
            "templates": sorted_scores[: max(1, min(int(limit or 100), 500))],
        }

    def build_customer_success_dashboard(self, tenant_id: str = "") -> dict[str, Any]:
        viewer_tenant_id = self._normalize_tenant_id(tenant_id)
        now_date = datetime.now().date()
        recent_start = (now_date - timedelta(days=7)).strftime("%Y-%m-%d")
        previous_start = (now_date - timedelta(days=14)).strftime("%Y-%m-%d")
        with self._connect() as conn:
            tenant_rows = conn.execute(
                """
                SELECT tenant_id FROM tenant_quota_plans
                UNION
                SELECT tenant_id FROM tenant_usage_daily
                UNION
                SELECT tenant_id FROM web_tasks
                """
            ).fetchall()
            task_rows = conn.execute(
                """
                SELECT tenant_id,
                    COUNT(*) AS total_count,
                    SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_count,
                    MAX(created_at) AS last_task_at
                FROM web_tasks
                WHERE parent_task_id=''
                GROUP BY tenant_id
                """
            ).fetchall()
            template_rows = conn.execute(
                """
                SELECT m.template_id, COUNT(*) AS success_count,
                    COALESCE(t.name, m.template_id) AS template_name,
                    COALESCE(t.page_type, '') AS template_page_type
                FROM task_operational_metrics m
                LEFT JOIN extraction_templates t
                    ON t.tenant_id=m.tenant_id AND t.template_id=m.template_id
                WHERE m.status='success' AND m.template_id!=''
                GROUP BY m.template_id, template_name, template_page_type
                ORDER BY success_count DESC, m.template_id ASC
                LIMIT 10
                """
            ).fetchall()
            template_trend_rows = conn.execute(
                """
                SELECT tenant_id, template_id,
                    SUM(CASE WHEN status='success' AND substr(updated_at, 1, 10)>=? THEN 1 ELSE 0 END) AS recent_success,
                    SUM(CASE WHEN substr(updated_at, 1, 10)>=? THEN 1 ELSE 0 END) AS recent_total,
                    SUM(CASE WHEN status='success' AND substr(updated_at, 1, 10)>=? AND substr(updated_at, 1, 10)<? THEN 1 ELSE 0 END) AS previous_success,
                    SUM(CASE WHEN substr(updated_at, 1, 10)>=? AND substr(updated_at, 1, 10)<? THEN 1 ELSE 0 END) AS previous_total
                FROM task_operational_metrics
                WHERE template_id!=''
                GROUP BY tenant_id, template_id
                """
                ,
                (
                    recent_start,
                    recent_start,
                    previous_start,
                    recent_start,
                    previous_start,
                    recent_start,
                ),
            ).fetchall()
        tenant_ids = sorted(
            {
                str(row["tenant_id"] or "").strip()
                for row in tenant_rows
                if str(row["tenant_id"] or "").strip()
            }
        )
        if not tenant_ids:
            tenant_ids = [viewer_tenant_id]

        task_stats_by_tenant = {
            str(row["tenant_id"] or ""): {
                "total_count": int(row["total_count"] or 0),
                "success_count": int(row["success_count"] or 0),
                "failed_count": int(row["failed_count"] or 0),
                "last_task_at": str(row["last_task_at"] or ""),
            }
            for row in task_rows
        }
        quota_watch = []
        failure_watch = []
        automation_alerts = []
        for current_tenant_id in tenant_ids:
            usage = self.build_usage_summary(current_tenant_id)
            ratios = usage.get("usage_ratio", {})
            max_ratio = max(
                float(ratios.get("tasks", 0.0) or 0.0),
                float(ratios.get("urls", 0.0) or 0.0),
                float(ratios.get("monitors", 0.0) or 0.0),
                float(ratios.get("tokens", 0.0) or 0.0),
                float(ratios.get("exports", 0.0) or 0.0),
            )
            quota_item = {
                "tenant_id": current_tenant_id,
                "plan_name": usage["plan"].get("plan_name", "trial"),
                "max_usage_ratio": round(max_ratio, 4),
                "usage_ratio": ratios,
                "totals": usage.get("totals", {}),
                "limits": usage.get("limits", {}),
                "near_quota": max_ratio >= 0.8,
            }
            quota_watch.append(quota_item)
            if quota_item["near_quota"]:
                automation_alerts.append(
                    {
                        "type": "near_quota",
                        "severity": "warning" if max_ratio < 0.95 else "danger",
                        "tenant_id": current_tenant_id,
                        "title": "租户接近额度",
                        "message": f"当前最高额度使用率 {max_ratio:.1%}",
                        "recommended_action": "联系客户确认是否需要扩容、升级套餐或清理无效监控。",
                    }
                )

            stats = task_stats_by_tenant.get(
                current_tenant_id,
                {"total_count": 0, "success_count": 0, "failed_count": 0, "last_task_at": ""},
            )
            total_count = int(stats["total_count"] or 0)
            failed_count = int(stats["failed_count"] or 0)
            failure_rate = failed_count / max(total_count, 1)
            failure_item = {
                    "tenant_id": current_tenant_id,
                    "total_count": total_count,
                    "success_count": int(stats["success_count"] or 0),
                    "failed_count": failed_count,
                    "failure_rate": round(failure_rate, 4),
                    "high_failure": total_count >= 3 and failure_rate >= 0.3,
            }
            failure_watch.append(failure_item)
            if failure_item["high_failure"]:
                automation_alerts.append(
                    {
                        "type": "high_failure_rate",
                        "severity": "danger",
                        "tenant_id": current_tenant_id,
                        "title": "租户失败率偏高",
                        "message": f"失败率 {failure_rate:.1%}，失败 {failed_count}/{total_count}",
                        "recommended_action": "优先查看失败自诊断，检查代理池、站点限速、模型配置和模板字段质量。",
                    }
                )

            last_task_at = str(stats.get("last_task_at") or "")
            if last_task_at:
                try:
                    last_date = datetime.strptime(last_task_at[:10], "%Y-%m-%d").date()
                except ValueError:
                    last_date = now_date
                inactive_days = (now_date - last_date).days
                if inactive_days >= 14:
                    automation_alerts.append(
                        {
                            "type": "inactive_tenant",
                            "severity": "warning",
                            "tenant_id": current_tenant_id,
                            "title": "租户长时间未使用",
                            "message": f"最近 {inactive_days} 天没有新任务",
                            "recommended_action": "触达客户确认卡点，推荐从成功模板或监控场景重新激活。",
                        }
                    )

        templates = []
        for row in template_rows:
            template_id = str(row["template_id"] or "").strip()
            templates.append(
                {
                    "template_id": template_id,
                    "name": str(row["template_name"] or template_id),
                    "success_count": int(row["success_count"] or 0),
                    "page_type": str(row["template_page_type"] or ""),
                }
            )

        for row in template_trend_rows:
            row_tenant_id = str(row["tenant_id"] or "").strip()
            template_id = str(row["template_id"] or "").strip()
            recent_total = int(row["recent_total"] or 0)
            previous_total = int(row["previous_total"] or 0)
            if recent_total < 3 or previous_total < 3:
                continue
            recent_rate = int(row["recent_success"] or 0) / max(recent_total, 1)
            previous_rate = int(row["previous_success"] or 0) / max(previous_total, 1)
            if previous_rate - recent_rate >= 0.2:
                automation_alerts.append(
                    {
                        "type": "template_success_rate_drop",
                        "severity": "warning",
                        "tenant_id": row_tenant_id,
                        "template_id": template_id,
                        "title": "模板成功率下降",
                        "message": f"近 7 天成功率 {recent_rate:.1%}，前 7 天 {previous_rate:.1%}",
                        "recommended_action": "检查最近失败原因和字段级反馈，必要时重新标注并修复模板。",
                    }
                )

        return {
            "tenant_id": viewer_tenant_id,
            "quota_watch": sorted(
                quota_watch,
                key=lambda item: (-float(item["max_usage_ratio"]), item["tenant_id"]),
            )[:10],
            "failure_watch": sorted(
                failure_watch,
                key=lambda item: (-float(item["failure_rate"]), item["tenant_id"]),
            )[:10],
            "top_success_templates": templates,
            "automation_alerts": automation_alerts[:30],
        }

    def enforce_quota(
        self,
        *,
        tenant_id: str = "",
        event_type: str,
        amount: int = 1,
        urls: int = 0,
        tokens: int = 0,
    ) -> None:
        summary = self.build_usage_summary(tenant_id)
        plan = summary["plan"]
        if str(plan.get("overage_policy") or "reject").lower() != "reject":
            return
        totals = summary["totals"]
        limits = summary["limits"]
        checks = []
        if event_type in {"task_create", "batch_create"}:
            checks.append(("monthly_task_limit", totals["tasks_created"] + amount))
            checks.append(("monthly_url_limit", totals["urls_submitted"] + max(urls, amount)))
        if event_type == "monitor_create":
            checks.append(("monitor_limit", totals["monitor_count"] + amount))
        if event_type == "export":
            checks.append(("export_limit", totals["exports_count"] + amount))
        if tokens > 0:
            checks.append(("monthly_token_limit", totals["total_tokens"] + tokens))
        exceeded = [
            {"limit": key, "used_after": used_after, "limit_value": int(limits[key])}
            for key, used_after in checks
            if int(limits.get(key, 0) or 0) > 0 and used_after > int(limits[key])
        ]
        if exceeded:
            from fastapi import HTTPException

            raise HTTPException(
                status_code=402,
                detail={
                    "message": "当前租户用量已超过套餐额度",
                    "event_type": event_type,
                    "exceeded": exceeded,
                },
            )

    @staticmethod
    def _classify_failure_for_metrics(error: str) -> str:
        normalized = str(error or "").strip().lower()
        if not normalized:
            return ""
        if "404" in normalized or "not found" in normalized:
            return "not_found"
        if "timeout" in normalized:
            return "timeout"
        if "429" in normalized or "rate limit" in normalized:
            return "rate_limit"
        if "401" in normalized or "403" in normalized:
            return "blocked"
        if any(
            marker in normalized
            for marker in (
                "verification",
                "shell",
                "empty page",
                "loading page",
                "anti bot",
                "anti-bot",
            )
        ):
            return "anti_bot_or_shell"
        if "challenge" in normalized or "captcha" in normalized or "验证" in normalized:
            return "anti_bot_or_shell"
        if "schema" in normalized or "quality" in normalized or "字段" in normalized:
            return "quality"
        if "network" in normalized or "connect" in normalized or "dns" in normalized:
            return "network"
        return "other"

    @staticmethod
    def _count_fields(data: dict[str, Any]) -> tuple[int, int, int]:
        payload = data.get("data") if isinstance(data.get("data"), dict) else data
        ignored = {
            "_extractor_stats",
            "_llm_usage",
            "_runtime_metrics",
            "_execution_context",
            "selected_fields",
            "field_labels",
        }
        fields = {
            key: value
            for key, value in (payload or {}).items()
            if isinstance(key, str) and key not in ignored and not key.startswith("_")
        }
        field_count = len(fields)
        empty_count = sum(1 for value in fields.values() if value in (None, "", [], {}))
        return field_count, field_count - empty_count, empty_count

    def record_task_operational_snapshot(self, task_id: str, tenant_id: str = "") -> None:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        task = self.get(task_id, tenant_id=normalized_tenant_id)
        if task is None:
            return
        data = task.data if isinstance(task.data, dict) else {}
        usage = data.get("_llm_usage") if isinstance(data.get("_llm_usage"), dict) else {}
        runtime = data.get("_runtime_metrics") if isinstance(data.get("_runtime_metrics"), dict) else {}
        context = data.get("_execution_context") if isinstance(data.get("_execution_context"), dict) else {}
        field_count, filled_count, empty_count = self._count_fields(data)
        now = self._now_text()
        payload = {
            "tenant_id": normalized_tenant_id,
            "task_id": task.task_id,
            "status": task.status,
            "schema_name": task.schema_name,
            "page_type": str(data.get("page_type") or ""),
            "monitor_id": str(context.get("monitor_id") or ""),
            "template_id": str(context.get("template_id") or ""),
            "failure_category": self._classify_failure_for_metrics(task.error),
            "quality_score": float(task.quality_score or 0.0),
            "field_count": field_count,
            "filled_field_count": filled_count,
            "empty_field_count": empty_count,
            "total_tokens": int(usage.get("total_tokens", 0) or 0),
            "prompt_tokens": int(usage.get("prompt_tokens", 0) or 0),
            "completion_tokens": int(usage.get("completion_tokens", 0) or 0),
            "estimated_cost_usd": float(usage.get("estimated_cost_usd", 0.0) or 0.0),
            "fetcher_type": str(runtime.get("fetcher_type") or ""),
            "fetch_elapsed_ms": float(runtime.get("fetch_elapsed_ms", 0.0) or 0.0),
            "playwright_elapsed_ms": float(runtime.get("playwright_elapsed_ms", 0.0) or 0.0),
            "retry_count": int(runtime.get("retry_count", 0) or 0),
            "proxy_pool_size": int(context.get("proxy_pool_size", 0) or 0),
            "elapsed_ms": float(task.elapsed_ms or 0.0),
        }
        with self._connect() as conn:
            previous_metric = conn.execute(
                """
                SELECT total_tokens, estimated_cost_usd, playwright_elapsed_ms, retry_count
                FROM task_operational_metrics
                WHERE tenant_id=? AND task_id=?
                """,
                (normalized_tenant_id, task.task_id),
            ).fetchone()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO task_operational_metrics (
                        tenant_id, task_id, status, schema_name, page_type, monitor_id,
                        template_id, failure_category, quality_score, field_count,
                        filled_field_count, empty_field_count, total_tokens, prompt_tokens,
                        completion_tokens, estimated_cost_usd, fetcher_type,
                        fetch_elapsed_ms, playwright_elapsed_ms, retry_count,
                        proxy_pool_size, elapsed_ms, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(tenant_id, task_id) DO UPDATE SET
                        status=excluded.status,
                        schema_name=excluded.schema_name,
                        page_type=excluded.page_type,
                        monitor_id=excluded.monitor_id,
                        template_id=excluded.template_id,
                        failure_category=excluded.failure_category,
                        quality_score=excluded.quality_score,
                        field_count=excluded.field_count,
                        filled_field_count=excluded.filled_field_count,
                        empty_field_count=excluded.empty_field_count,
                        total_tokens=excluded.total_tokens,
                        prompt_tokens=excluded.prompt_tokens,
                        completion_tokens=excluded.completion_tokens,
                        estimated_cost_usd=excluded.estimated_cost_usd,
                        fetcher_type=excluded.fetcher_type,
                        fetch_elapsed_ms=excluded.fetch_elapsed_ms,
                        playwright_elapsed_ms=excluded.playwright_elapsed_ms,
                        retry_count=excluded.retry_count,
                        proxy_pool_size=excluded.proxy_pool_size,
                        elapsed_ms=excluded.elapsed_ms,
                        updated_at=excluded.updated_at
                    """,
                    (
                        payload["tenant_id"],
                        payload["task_id"],
                        payload["status"],
                        payload["schema_name"],
                        payload["page_type"],
                        payload["monitor_id"],
                        payload["template_id"],
                        payload["failure_category"],
                        payload["quality_score"],
                        payload["field_count"],
                        payload["filled_field_count"],
                        payload["empty_field_count"],
                        payload["total_tokens"],
                        payload["prompt_tokens"],
                        payload["completion_tokens"],
                        payload["estimated_cost_usd"],
                        payload["fetcher_type"],
                        payload["fetch_elapsed_ms"],
                        payload["playwright_elapsed_ms"],
                        payload["retry_count"],
                        payload["proxy_pool_size"],
                        payload["elapsed_ms"],
                        now,
                        now,
                    ),
                )
                conn.commit()
        previous_tokens = int(previous_metric["total_tokens"] or 0) if previous_metric else 0
        previous_cost = float(previous_metric["estimated_cost_usd"] or 0.0) if previous_metric else 0.0
        previous_playwright = float(previous_metric["playwright_elapsed_ms"] or 0.0) if previous_metric else 0.0
        previous_retry = int(previous_metric["retry_count"] or 0) if previous_metric else 0
        self.record_usage_event(
            tenant_id=normalized_tenant_id,
            total_tokens=max(int(payload["total_tokens"]) - previous_tokens, 0),
            model_cost_usd=max(float(payload["estimated_cost_usd"]) - previous_cost, 0.0),
            playwright_elapsed_ms=max(float(payload["playwright_elapsed_ms"]) - previous_playwright, 0.0),
            retry_count=max(int(payload["retry_count"]) - previous_retry, 0),
        )

    def list_task_operational_metrics(
        self,
        *,
        tenant_id: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM task_operational_metrics
                WHERE tenant_id=?
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                (normalized_tenant_id, max(1, min(int(limit or 100), 1000))),
            ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _normalize_field_feedback_status(value: Any) -> str:
        if isinstance(value, dict):
            value = value.get("status") or value.get("result") or value.get("feedback")
        normalized = str(value or "").strip().lower()
        mapping = {
            "correct": "correct",
            "ok": "correct",
            "true": "correct",
            "right": "correct",
            "正确": "correct",
            "incorrect": "incorrect",
            "wrong": "incorrect",
            "false": "incorrect",
            "错误": "incorrect",
            "缺失": "missing",
            "missing": "missing",
            "empty": "missing",
        }
        return mapping.get(normalized, normalized if normalized else "unknown")

    def record_field_quality_feedback(
        self,
        *,
        task: TaskRecord,
        annotation_id: str,
        field_feedback: dict[str, Any],
        corrected_data: Optional[dict[str, Any]] = None,
        template_id: str = "",
        profile_id: str = "",
        notes: str = "",
        created_by: str = "",
        tenant_id: str = "",
    ) -> list[dict[str, Any]]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id or task.tenant_id)
        task_payload = task.data if isinstance(task.data, dict) else {}
        extracted_payload = (
            task_payload.get("data")
            if isinstance(task_payload.get("data"), dict)
            else task_payload
        )
        corrected = dict(corrected_data or {})
        domain = urlparse(str(task.url or "")).netloc.strip().lower()
        now = self._now_text()
        rows: list[dict[str, Any]] = []
        for field_name, raw_feedback in dict(field_feedback or {}).items():
            normalized_field = str(field_name or "").strip()
            if not normalized_field:
                continue
            status = self._normalize_field_feedback_status(raw_feedback)
            feedback_id = f"fqf-{uuid4().hex[:10]}"
            original_value = (
                extracted_payload.get(normalized_field)
                if isinstance(extracted_payload, dict)
                else None
            )
            corrected_value = corrected.get(normalized_field)
            row_payload = {
                "feedback_id": feedback_id,
                "tenant_id": normalized_tenant_id,
                "task_id": task.task_id,
                "annotation_id": annotation_id,
                "template_id": template_id,
                "profile_id": profile_id,
                "site_domain": domain,
                "field_name": normalized_field,
                "feedback_status": status,
                "original_value": original_value,
                "corrected_value": corrected_value,
                "notes": notes,
                "created_by": created_by,
                "created_at": now,
            }
            rows.append(row_payload)
        if not rows:
            return []
        with self._lock:
            with self._connect() as conn:
                for item in rows:
                    conn.execute(
                        """
                        INSERT INTO field_quality_feedback (
                            feedback_id, tenant_id, task_id, annotation_id, template_id,
                            profile_id, site_domain, field_name, feedback_status,
                            original_value_json, corrected_value_json, notes, created_by, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            item["feedback_id"],
                            item["tenant_id"],
                            item["task_id"],
                            item["annotation_id"],
                            item["template_id"],
                            item["profile_id"],
                            item["site_domain"],
                            item["field_name"],
                            item["feedback_status"],
                            json.dumps(item["original_value"], ensure_ascii=False),
                            json.dumps(item["corrected_value"], ensure_ascii=False),
                            item["notes"],
                            item["created_by"],
                            item["created_at"],
                        ),
                    )
                conn.commit()
        return rows

    def build_field_quality_memory(
        self,
        *,
        tenant_id: str = "",
        template_id: str = "",
        site_domain: str = "",
        limit: int = 200,
    ) -> dict[str, Any]:
        normalized_tenant_id = self._normalize_tenant_id(tenant_id)
        sql = "SELECT * FROM field_quality_feedback WHERE tenant_id=?"
        params: list[Any] = [normalized_tenant_id]
        if str(template_id or "").strip():
            sql += " AND template_id=?"
            params.append(str(template_id).strip())
        if str(site_domain or "").strip():
            sql += " AND site_domain=?"
            params.append(str(site_domain).strip().lower())
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(int(limit or 200), 1000)))
        with self._connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        fields: dict[str, dict[str, Any]] = {}
        for row in rows:
            field_name = str(row["field_name"] or "").strip()
            if not field_name:
                continue
            item = fields.setdefault(
                field_name,
                {"correct": 0, "incorrect": 0, "missing": 0, "unknown": 0},
            )
            status = str(row["feedback_status"] or "unknown")
            item[status if status in item else "unknown"] += 1
        return {
            "tenant_id": normalized_tenant_id,
            "template_id": str(template_id or "").strip(),
            "site_domain": str(site_domain or "").strip().lower(),
            "fields": fields,
            "sample_size": len(rows),
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
