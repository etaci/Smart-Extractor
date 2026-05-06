"""Manual annotation and auto-repair routes."""

from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Request

from smart_extractor.web.api_models import TaskAnnotationRequest
from smart_extractor.web.management_helpers import (
    normalize_field_labels,
    normalize_profile_payload,
    normalize_selected_fields,
    serialize_repair_suggestion,
    serialize_task_annotation,
    serialize_template,
)


def register_annotation_routes(
    router: APIRouter,
    *,
    api_guard: Callable[[Request], Any],
    request_logger: Callable[[Request, str], Any],
    task_store: Any,
    learned_profile_store: Any,
) -> None:
    def _tenant_id(request: Request) -> str:
        identity = getattr(request.state, "identity", None)
        return str(getattr(identity, "tenant_id", "default") or "default")

    def _require(request: Request, permission: str) -> None:
        identity = getattr(request.state, "identity", None)
        require = getattr(identity, "require", None)
        if callable(require):
            require(permission)

    def _actor_name(request: Request) -> str:
        identity = getattr(request.state, "identity", None)
        return str(getattr(identity, "username", "system") or "system")

    @router.get("/api/annotations")
    async def api_annotations(
        request: Request,
        task_id: str = "",
        _: Any = Depends(api_guard),
    ):
        _require(request, "dashboard:read")
        tenant_id = _tenant_id(request)
        return {
            "annotations": [
                serialize_task_annotation(item)
                for item in task_store.list_task_annotations(
                    limit=50,
                    task_id=task_id.strip(),
                    tenant_id=tenant_id,
                )
            ],
            "repairs": [
                serialize_repair_suggestion(item)
                for item in task_store.list_repair_suggestions(
                    limit=50,
                    task_id=task_id.strip(),
                    tenant_id=tenant_id,
                )
            ],
        }

    @router.post("/api/task/{task_id}/annotate")
    async def api_annotate_task(
        task_id: str,
        payload: TaskAnnotationRequest,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "task:review")
        tenant_id = _tenant_id(request)
        task = task_store.get(task_id, tenant_id=tenant_id)
        if task is None:
            raise HTTPException(status_code=404, detail="任务不存在")
        task_data = task.data if isinstance(task.data, dict) else {}
        profile_id = payload.profile_id.strip() or str(task_data.get("learned_profile_id") or "").strip()
        template_id = payload.template_id.strip()
        template = task_store.get_template(template_id, tenant_id=tenant_id) if template_id else None
        corrected_data = dict(payload.corrected_data or {})
        selected_fields = normalize_selected_fields(
            list(corrected_data.keys())
            or list((task_data.get("selected_fields") or []))
        )
        field_labels = normalize_field_labels(
            {
                **(
                    dict(task_data.get("field_labels", {}))
                    if isinstance(task_data.get("field_labels"), dict)
                    else {}
                ),
                **{field: field for field in selected_fields},
            }
        )

        annotation = task_store.create_task_annotation(
            task_id=task_id,
            profile_id=profile_id,
            template_id=template_id,
            corrected_data=corrected_data,
            field_feedback=dict(payload.field_feedback or {}),
            notes=payload.notes.strip(),
            created_by=_actor_name(request),
            tenant_id=tenant_id,
        )

        updated_template = None
        updated_profile = None
        repair_status = "suggested"
        repair_reason = "已根据人工标注生成修复建议"
        if payload.apply_auto_repair:
            repair_status = "applied"
            repair_reason = "已根据人工标注自动修复模板与学习档案"
            if template is not None:
                merged_profile = normalize_profile_payload(
                    {
                        **dict(template.profile or {}),
                        "last_manual_annotation_task_id": task_id,
                        "last_manual_annotation_by": _actor_name(request),
                        "last_manual_annotation_notes": payload.notes.strip(),
                    }
                )
                updated_template = task_store.create_or_update_template(
                    name=template.name,
                    url=template.url,
                    page_type=template.page_type,
                    schema_name=template.schema_name,
                    storage_format=template.storage_format,
                    use_static=template.use_static,
                    selected_fields=selected_fields or list(template.selected_fields or []),
                    field_labels=field_labels or dict(template.field_labels or {}),
                    profile=merged_profile,
                    template_id=template.template_id,
                    tenant_id=tenant_id,
                )
            if profile_id:
                updated_profile = learned_profile_store.apply_manual_feedback(
                    profile_id,
                    selected_fields=selected_fields,
                    field_labels=field_labels,
                    sample_url=task.url,
                    repaired=True,
                    reactivate=True,
                )
        else:
            if profile_id:
                updated_profile = learned_profile_store.apply_manual_feedback(
                    profile_id,
                    selected_fields=selected_fields,
                    field_labels=field_labels,
                    sample_url=task.url,
                    repaired=False,
                    reactivate=False,
                )

        repair = task_store.create_repair_suggestion(
            annotation_id=annotation.annotation_id,
            task_id=task_id,
            profile_id=profile_id,
            template_id=template_id,
            status=repair_status,
            suggested_fields=selected_fields,
            suggested_field_labels=field_labels,
            suggested_profile={
                "source_task_id": task_id,
                "corrected_field_count": len(selected_fields),
                "apply_auto_repair": payload.apply_auto_repair,
            },
            reason=repair_reason,
            tenant_id=tenant_id,
        )
        request_logger(request, task_id).info(
            "Annotate task: task_id={} profile_id={} template_id={} auto_repair={}",
            task_id,
            profile_id or "-",
            template_id or "-",
            payload.apply_auto_repair,
        )
        response_payload: dict[str, Any] = {
            "message": repair_reason,
            "annotation": serialize_task_annotation(annotation),
            "repair": serialize_repair_suggestion(repair),
        }
        if updated_template is not None:
            response_payload["template"] = serialize_template(updated_template)
        if updated_profile is not None:
            response_payload["profile"] = updated_profile.to_dict()
        return response_payload
