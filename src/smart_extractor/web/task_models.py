"""
Web 任务存储的数据模型。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional


def loads_json_list(raw_value: str | None) -> list[Any]:
    try:
        value = json.loads(raw_value or "[]")
    except json.JSONDecodeError:
        return []
    return value if isinstance(value, list) else []


def loads_json_dict(raw_value: str | None) -> dict[str, Any]:
    try:
        value = json.loads(raw_value or "{}")
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


@dataclass
class TaskRecord:
    """Stored extraction task."""

    db_id: int
    task_id: str
    url: str
    schema_name: str
    storage_format: str
    status: str = "pending"
    created_at: str = ""
    completed_at: str = ""
    elapsed_ms: float = 0.0
    quality_score: float = 0.0
    progress_percent: float = 0.0
    progress_stage: str = ""
    data: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    request_id: str = "-"
    batch_group_id: str = ""
    task_kind: str = "single"
    parent_task_id: str = ""
    total_items: int = 0
    completed_items: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "url": self.url,
            "schema_name": self.schema_name,
            "storage_format": self.storage_format,
            "status": self.status,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "elapsed_ms": self.elapsed_ms,
            "quality_score": self.quality_score,
            "progress_percent": self.progress_percent,
            "progress_stage": self.progress_stage,
            "data": self.data,
            "error": self.error,
            "request_id": self.request_id,
            "batch_group_id": self.batch_group_id,
            "task_kind": self.task_kind,
            "parent_task_id": self.parent_task_id,
            "total_items": self.total_items,
            "completed_items": self.completed_items,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "TaskRecord":
        data_json = row["data_json"] or ""
        data = json.loads(data_json) if data_json else None
        error = row["error"] or None
        return cls(
            db_id=int(row["id"] or 0),
            task_id=row["task_id"],
            url=row["url"],
            schema_name=row["schema_name"],
            storage_format=row["storage_format"],
            status=row["status"],
            created_at=row["created_at"],
            completed_at=row["completed_at"] or "",
            elapsed_ms=float(row["elapsed_ms"] or 0.0),
            quality_score=float(row["quality_score"] or 0.0),
            progress_percent=float(row["progress_percent"] or 0.0),
            progress_stage=row["progress_stage"] or "",
            data=data,
            error=error,
            request_id=row["request_id"] or "-",
            batch_group_id=row["batch_group_id"] or "",
            task_kind=row["task_kind"] or "single",
            parent_task_id=row["parent_task_id"] or "",
            total_items=int(row["total_items"] or 0),
            completed_items=int(row["completed_items"] or 0),
        )


@dataclass
class TemplateRecord:
    db_id: int
    template_id: str
    name: str
    url: str
    page_type: str
    schema_name: str
    storage_format: str
    use_static: bool
    selected_fields: list[str]
    field_labels: dict[str, str]
    profile: dict[str, Any]
    created_at: str
    updated_at: str
    last_used_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "template_id": self.template_id,
            "name": self.name,
            "url": self.url,
            "page_type": self.page_type,
            "schema_name": self.schema_name,
            "storage_format": self.storage_format,
            "use_static": self.use_static,
            "selected_fields": self.selected_fields,
            "field_labels": self.field_labels,
            "profile": self.profile,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_used_at": self.last_used_at,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "TemplateRecord":
        return cls(
            db_id=int(row["id"] or 0),
            template_id=row["template_id"],
            name=row["name"] or "",
            url=row["url"] or "",
            page_type=row["page_type"] or "unknown",
            schema_name=row["schema_name"] or "auto",
            storage_format=row["storage_format"] or "json",
            use_static=bool(row["use_static"] or 0),
            selected_fields=loads_json_list(row["selected_fields_json"]),
            field_labels=loads_json_dict(row["field_labels_json"]),
            profile=loads_json_dict(row["profile_json"]),
            created_at=row["created_at"] or "",
            updated_at=row["updated_at"] or "",
            last_used_at=row["last_used_at"] or "",
        )


@dataclass
class MonitorRecord:
    db_id: int
    monitor_id: str
    name: str
    url: str
    schema_name: str
    storage_format: str
    use_static: bool
    selected_fields: list[str]
    field_labels: dict[str, str]
    profile: dict[str, Any]
    created_at: str
    updated_at: str
    last_task_id: str = ""
    last_checked_at: str = ""
    last_status: str = ""
    last_alert_level: str = ""
    last_alert_message: str = ""
    last_changed_fields: list[dict[str, Any]] | None = None
    last_notification_status: str = ""
    last_notification_message: str = ""
    last_notification_at: str = ""
    last_extraction_strategy: str = ""
    last_learned_profile_id: str = ""
    schedule_enabled: bool = False
    schedule_interval_minutes: int = 60
    schedule_next_run_at: str = ""
    schedule_last_run_at: str = ""
    schedule_paused_at: str = ""
    schedule_claimed_by: str = ""
    schedule_claimed_at: str = ""
    schedule_lease_until: str = ""
    schedule_last_error: str = ""
    schedule_claim_count: int = 0
    last_trigger_source: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "monitor_id": self.monitor_id,
            "name": self.name,
            "url": self.url,
            "schema_name": self.schema_name,
            "storage_format": self.storage_format,
            "use_static": self.use_static,
            "selected_fields": self.selected_fields,
            "field_labels": self.field_labels,
            "profile": self.profile,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_task_id": self.last_task_id,
            "last_checked_at": self.last_checked_at,
            "last_status": self.last_status,
            "last_alert_level": self.last_alert_level,
            "last_alert_message": self.last_alert_message,
            "last_changed_fields": self.last_changed_fields or [],
            "last_notification_status": self.last_notification_status,
            "last_notification_message": self.last_notification_message,
            "last_notification_at": self.last_notification_at,
            "last_extraction_strategy": self.last_extraction_strategy,
            "last_learned_profile_id": self.last_learned_profile_id,
            "schedule_enabled": self.schedule_enabled,
            "schedule_interval_minutes": self.schedule_interval_minutes,
            "schedule_next_run_at": self.schedule_next_run_at,
            "schedule_last_run_at": self.schedule_last_run_at,
            "schedule_paused_at": self.schedule_paused_at,
            "schedule_claimed_by": self.schedule_claimed_by,
            "schedule_claimed_at": self.schedule_claimed_at,
            "schedule_lease_until": self.schedule_lease_until,
            "schedule_last_error": self.schedule_last_error,
            "schedule_claim_count": self.schedule_claim_count,
            "last_trigger_source": self.last_trigger_source,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "MonitorRecord":
        return cls(
            db_id=int(row["id"] or 0),
            monitor_id=row["monitor_id"],
            name=row["name"] or "",
            url=row["url"] or "",
            schema_name=row["schema_name"] or "auto",
            storage_format=row["storage_format"] or "json",
            use_static=bool(row["use_static"] or 0),
            selected_fields=loads_json_list(row["selected_fields_json"]),
            field_labels=loads_json_dict(row["field_labels_json"]),
            profile=loads_json_dict(row["profile_json"]),
            created_at=row["created_at"] or "",
            updated_at=row["updated_at"] or "",
            last_task_id=row["last_task_id"] or "",
            last_checked_at=row["last_checked_at"] or "",
            last_status=row["last_status"] or "",
            last_alert_level=row["last_alert_level"] or "",
            last_alert_message=row["last_alert_message"] or "",
            last_changed_fields=loads_json_list(row["last_changed_fields_json"]),
            last_notification_status=row["last_notification_status"] or "",
            last_notification_message=row["last_notification_message"] or "",
            last_notification_at=row["last_notification_at"] or "",
            last_extraction_strategy=row["last_extraction_strategy"] or "",
            last_learned_profile_id=row["last_learned_profile_id"] or "",
            schedule_enabled=bool(row["schedule_enabled"] or 0),
            schedule_interval_minutes=int(row["schedule_interval_minutes"] or 60),
            schedule_next_run_at=row["schedule_next_run_at"] or "",
            schedule_last_run_at=row["schedule_last_run_at"] or "",
            schedule_paused_at=row["schedule_paused_at"] or "",
            schedule_claimed_by=row["schedule_claimed_by"] or "",
            schedule_claimed_at=row["schedule_claimed_at"] or "",
            schedule_lease_until=row["schedule_lease_until"] or "",
            schedule_last_error=row["schedule_last_error"] or "",
            schedule_claim_count=int(row["schedule_claim_count"] or 0),
            last_trigger_source=row["last_trigger_source"] or "",
        )


@dataclass
class NotificationEventRecord:
    db_id: int
    notification_id: str
    monitor_id: str
    task_id: str
    channel_type: str
    target: str
    event_type: str
    status: str
    status_message: str
    attempt_no: int
    max_attempts: int
    next_retry_at: str
    response_code: Optional[int]
    error_type: str
    error_message: str
    payload_snapshot: dict[str, Any]
    created_at: str
    sent_at: str
    retry_of_notification_id: str = ""
    triggered_by: str = "system"

    def to_dict(self) -> dict[str, Any]:
        return {
            "notification_id": self.notification_id,
            "monitor_id": self.monitor_id,
            "task_id": self.task_id,
            "channel_type": self.channel_type,
            "target": self.target,
            "event_type": self.event_type,
            "status": self.status,
            "status_message": self.status_message,
            "attempt_no": self.attempt_no,
            "max_attempts": self.max_attempts,
            "next_retry_at": self.next_retry_at,
            "response_code": self.response_code,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "payload_snapshot": self.payload_snapshot,
            "created_at": self.created_at,
            "sent_at": self.sent_at,
            "retry_of_notification_id": self.retry_of_notification_id,
            "triggered_by": self.triggered_by,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "NotificationEventRecord":
        response_code = row["response_code"]
        normalized_response_code = (
            int(response_code)
            if response_code is not None and str(response_code).strip() not in {"", "0"}
            else None
        )
        return cls(
            db_id=int(row["id"] or 0),
            notification_id=row["notification_id"] or "",
            monitor_id=row["monitor_id"] or "",
            task_id=row["task_id"] or "",
            channel_type=row["channel_type"] or "webhook",
            target=row["target"] or "",
            event_type=row["event_type"] or "monitor_alert",
            status=row["status"] or "",
            status_message=row["status_message"] or "",
            attempt_no=int(row["attempt_no"] or 1),
            max_attempts=int(row["max_attempts"] or 3),
            next_retry_at=row["next_retry_at"] or "",
            response_code=normalized_response_code,
            error_type=row["error_type"] or "",
            error_message=row["error_message"] or "",
            payload_snapshot=loads_json_dict(row["payload_snapshot_json"]),
            created_at=row["created_at"] or "",
            sent_at=row["sent_at"] or "",
            retry_of_notification_id=row["retry_of_notification_id"] or "",
            triggered_by=row["triggered_by"] or "system",
        )
