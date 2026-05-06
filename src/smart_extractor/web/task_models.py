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
    tenant_id: str
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
            "tenant_id": self.tenant_id,
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
            tenant_id=row["tenant_id"] or "default",
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
    tenant_id: str
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
    use_count: int = 0
    last_used_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "template_id": self.template_id,
            "tenant_id": self.tenant_id,
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
            "use_count": self.use_count,
            "last_used_at": self.last_used_at,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "TemplateRecord":
        return cls(
            db_id=int(row["id"] or 0),
            template_id=row["template_id"],
            tenant_id=row["tenant_id"] or "default",
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
            use_count=int(row["use_count"] or 0),
            last_used_at=row["last_used_at"] or "",
        )


@dataclass
class MonitorRecord:
    db_id: int
    monitor_id: str
    tenant_id: str
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
            "tenant_id": self.tenant_id,
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
            tenant_id=row["tenant_id"] or "default",
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
    tenant_id: str
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
            "tenant_id": self.tenant_id,
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
            tenant_id=row["tenant_id"] or "default",
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


@dataclass
class ActorInstallRecord:
    db_id: int
    actor_instance_id: str
    tenant_id: str
    actor_id: str
    name: str
    version: str
    category: str
    capabilities: list[str]
    config: dict[str, Any]
    linked_template_id: str
    linked_monitor_id: str
    status: str
    created_at: str
    updated_at: str
    last_run_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "actor_instance_id": self.actor_instance_id,
            "tenant_id": self.tenant_id,
            "actor_id": self.actor_id,
            "name": self.name,
            "version": self.version,
            "category": self.category,
            "capabilities": self.capabilities,
            "config": self.config,
            "linked_template_id": self.linked_template_id,
            "linked_monitor_id": self.linked_monitor_id,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_run_at": self.last_run_at,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "ActorInstallRecord":
        return cls(
            db_id=int(row["id"] or 0),
            actor_instance_id=row["actor_instance_id"] or "",
            tenant_id=row["tenant_id"] or "default",
            actor_id=row["actor_id"] or "",
            name=row["name"] or "",
            version=row["version"] or "",
            category=row["category"] or "",
            capabilities=loads_json_list(row["capabilities_json"]),
            config=loads_json_dict(row["config_json"]),
            linked_template_id=row["linked_template_id"] or "",
            linked_monitor_id=row["linked_monitor_id"] or "",
            status=row["status"] or "active",
            created_at=row["created_at"] or "",
            updated_at=row["updated_at"] or "",
            last_run_at=row["last_run_at"] or "",
        )


@dataclass
class WorkerNodeRecord:
    db_id: int
    worker_id: str
    tenant_id: str
    display_name: str
    node_type: str
    status: str
    queue_scope: str
    current_load: int
    capabilities: list[str]
    metadata: dict[str, Any]
    last_seen_at: str
    created_at: str
    updated_at: str
    last_error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "tenant_id": self.tenant_id,
            "display_name": self.display_name,
            "node_type": self.node_type,
            "status": self.status,
            "queue_scope": self.queue_scope,
            "current_load": self.current_load,
            "capabilities": self.capabilities,
            "metadata": self.metadata,
            "last_seen_at": self.last_seen_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_error": self.last_error,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "WorkerNodeRecord":
        return cls(
            db_id=int(row["id"] or 0),
            worker_id=row["worker_id"] or "",
            tenant_id=row["tenant_id"] or "default",
            display_name=row["display_name"] or "",
            node_type=row["node_type"] or "worker",
            status=row["status"] or "idle",
            queue_scope=row["queue_scope"] or "*",
            current_load=int(row["current_load"] or 0),
            capabilities=loads_json_list(row["capabilities_json"]),
            metadata=loads_json_dict(row["metadata_json"]),
            last_seen_at=row["last_seen_at"] or "",
            created_at=row["created_at"] or "",
            updated_at=row["updated_at"] or "",
            last_error=row["last_error"] or "",
        )


@dataclass
class ProxyEndpointRecord:
    db_id: int
    proxy_id: str
    tenant_id: str
    name: str
    proxy_url: str
    provider: str
    status: str
    enabled: bool
    tags: list[str]
    metadata: dict[str, Any]
    success_count: int
    failure_count: int
    last_used_at: str
    created_at: str
    updated_at: str
    last_error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "proxy_id": self.proxy_id,
            "tenant_id": self.tenant_id,
            "name": self.name,
            "proxy_url": self.proxy_url,
            "provider": self.provider,
            "status": self.status,
            "enabled": self.enabled,
            "tags": self.tags,
            "metadata": self.metadata,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "last_used_at": self.last_used_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_error": self.last_error,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "ProxyEndpointRecord":
        return cls(
            db_id=int(row["id"] or 0),
            proxy_id=row["proxy_id"] or "",
            tenant_id=row["tenant_id"] or "default",
            name=row["name"] or "",
            proxy_url=row["proxy_url"] or "",
            provider=row["provider"] or "",
            status=row["status"] or "idle",
            enabled=bool(row["enabled"] or 0),
            tags=loads_json_list(row["tags_json"]),
            metadata=loads_json_dict(row["metadata_json"]),
            success_count=int(row["success_count"] or 0),
            failure_count=int(row["failure_count"] or 0),
            last_used_at=row["last_used_at"] or "",
            created_at=row["created_at"] or "",
            updated_at=row["updated_at"] or "",
            last_error=row["last_error"] or "",
        )


@dataclass
class SitePolicyRecord:
    db_id: int
    policy_id: str
    tenant_id: str
    domain: str
    name: str
    min_interval_seconds: float
    max_concurrency: int
    use_proxy_pool: bool
    preferred_proxy_tags: list[str]
    assigned_worker_group: str
    notes: str
    created_at: str
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "tenant_id": self.tenant_id,
            "domain": self.domain,
            "name": self.name,
            "min_interval_seconds": self.min_interval_seconds,
            "max_concurrency": self.max_concurrency,
            "use_proxy_pool": self.use_proxy_pool,
            "preferred_proxy_tags": self.preferred_proxy_tags,
            "assigned_worker_group": self.assigned_worker_group,
            "notes": self.notes,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "SitePolicyRecord":
        return cls(
            db_id=int(row["id"] or 0),
            policy_id=row["policy_id"] or "",
            tenant_id=row["tenant_id"] or "default",
            domain=row["domain"] or "",
            name=row["name"] or "",
            min_interval_seconds=float(row["min_interval_seconds"] or 0.0),
            max_concurrency=int(row["max_concurrency"] or 1),
            use_proxy_pool=bool(row["use_proxy_pool"] or 0),
            preferred_proxy_tags=loads_json_list(row["preferred_proxy_tags_json"]),
            assigned_worker_group=row["assigned_worker_group"] or "",
            notes=row["notes"] or "",
            created_at=row["created_at"] or "",
            updated_at=row["updated_at"] or "",
        )


@dataclass
class TaskAnnotationRecord:
    db_id: int
    annotation_id: str
    tenant_id: str
    task_id: str
    profile_id: str
    template_id: str
    corrected_data: dict[str, Any]
    field_feedback: dict[str, Any]
    notes: str
    created_by: str
    created_at: str
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "annotation_id": self.annotation_id,
            "tenant_id": self.tenant_id,
            "task_id": self.task_id,
            "profile_id": self.profile_id,
            "template_id": self.template_id,
            "corrected_data": self.corrected_data,
            "field_feedback": self.field_feedback,
            "notes": self.notes,
            "created_by": self.created_by,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "TaskAnnotationRecord":
        return cls(
            db_id=int(row["id"] or 0),
            annotation_id=row["annotation_id"] or "",
            tenant_id=row["tenant_id"] or "default",
            task_id=row["task_id"] or "",
            profile_id=row["profile_id"] or "",
            template_id=row["template_id"] or "",
            corrected_data=loads_json_dict(row["corrected_data_json"]),
            field_feedback=loads_json_dict(row["field_feedback_json"]),
            notes=row["notes"] or "",
            created_by=row["created_by"] or "",
            created_at=row["created_at"] or "",
            updated_at=row["updated_at"] or "",
        )


@dataclass
class RepairSuggestionRecord:
    db_id: int
    repair_id: str
    tenant_id: str
    annotation_id: str
    task_id: str
    profile_id: str
    template_id: str
    status: str
    repair_strategy: str
    suggested_fields: list[str]
    suggested_field_labels: dict[str, str]
    suggested_profile: dict[str, Any]
    reason: str
    created_at: str
    updated_at: str
    applied_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "repair_id": self.repair_id,
            "tenant_id": self.tenant_id,
            "annotation_id": self.annotation_id,
            "task_id": self.task_id,
            "profile_id": self.profile_id,
            "template_id": self.template_id,
            "status": self.status,
            "repair_strategy": self.repair_strategy,
            "suggested_fields": self.suggested_fields,
            "suggested_field_labels": self.suggested_field_labels,
            "suggested_profile": self.suggested_profile,
            "reason": self.reason,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "applied_at": self.applied_at,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "RepairSuggestionRecord":
        return cls(
            db_id=int(row["id"] or 0),
            repair_id=row["repair_id"] or "",
            tenant_id=row["tenant_id"] or "default",
            annotation_id=row["annotation_id"] or "",
            task_id=row["task_id"] or "",
            profile_id=row["profile_id"] or "",
            template_id=row["template_id"] or "",
            status=row["status"] or "suggested",
            repair_strategy=row["repair_strategy"] or "manual_feedback",
            suggested_fields=loads_json_list(row["suggested_fields_json"]),
            suggested_field_labels=loads_json_dict(row["suggested_field_labels_json"]),
            suggested_profile=loads_json_dict(row["suggested_profile_json"]),
            reason=row["reason"] or "",
            created_at=row["created_at"] or "",
            updated_at=row["updated_at"] or "",
            applied_at=row["applied_at"] or "",
        )


@dataclass
class FunnelEventRecord:
    db_id: int
    funnel_event_id: str
    tenant_id: str
    stage: str
    channel: str
    package_type: str
    package_id: str
    package_name: str
    task_id: str
    template_id: str
    monitor_id: str
    actor_instance_id: str
    metadata: dict[str, Any]
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "funnel_event_id": self.funnel_event_id,
            "tenant_id": self.tenant_id,
            "stage": self.stage,
            "channel": self.channel,
            "package_type": self.package_type,
            "package_id": self.package_id,
            "package_name": self.package_name,
            "task_id": self.task_id,
            "template_id": self.template_id,
            "monitor_id": self.monitor_id,
            "actor_instance_id": self.actor_instance_id,
            "metadata": self.metadata,
            "created_at": self.created_at,
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "FunnelEventRecord":
        return cls(
            db_id=int(row["id"] or 0),
            funnel_event_id=row["funnel_event_id"] or "",
            tenant_id=row["tenant_id"] or "default",
            stage=row["stage"] or "",
            channel=row["channel"] or "",
            package_type=row["package_type"] or "",
            package_id=row["package_id"] or "",
            package_name=row["package_name"] or "",
            task_id=row["task_id"] or "",
            template_id=row["template_id"] or "",
            monitor_id=row["monitor_id"] or "",
            actor_instance_id=row["actor_instance_id"] or "",
            metadata=loads_json_dict(row["metadata_json"]),
            created_at=row["created_at"] or "",
        )
