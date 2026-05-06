"""监控管理路由。"""

from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Request

from smart_extractor.web.api_models import SaveMonitorRequest
from smart_extractor.web.management_helpers import (
    apply_monitor_notification_defaults,
    normalize_field_labels,
    normalize_profile_payload,
    normalize_selected_fields,
    notification_channels_from_profile,
    serialize_monitor,
)


def register_monitor_routes(
    router: APIRouter,
    *,
    api_guard: Callable[[Request], Any],
    request_logger: Callable[[Request, str], Any],
    get_request_id: Callable[[Request], str],
    task_store: Any,
    learned_profile_store: Any,
    trigger_monitor_run: Callable[..., dict[str, Any] | None],
) -> None:
    def _tenant_id(request: Request) -> str:
        identity = getattr(request.state, "identity", None)
        require = getattr(identity, "require", None)
        if callable(require):
            require("monitor:manage")
        return str(getattr(identity, "tenant_id", "default") or "default")

    @router.get("/api/monitors")
    async def api_monitors(
        request: Request,
        _: Any = Depends(api_guard),
    ):
        tenant_id = _tenant_id(request)
        request_logger(request, "-").info("List monitors")
        return {
            "monitors": [
                serialize_monitor(item, learned_profile_store)
                for item in task_store.list_monitors(limit=30, tenant_id=tenant_id)
            ]
        }

    @router.post("/api/monitors")
    async def api_save_monitor(
        payload: SaveMonitorRequest,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        tenant_id = _tenant_id(request)
        normalized_url = payload.url.strip()
        if not normalized_url.startswith(("http://", "https://")):
            raise HTTPException(status_code=400, detail="url 必须以 http:// 或 https:// 开头")

        normalized_profile = apply_monitor_notification_defaults(
            normalize_profile_payload(payload.profile)
        )
        for channel in notification_channels_from_profile(normalized_profile):
            target = str(channel.get("target") or "").strip()
            if target and not target.startswith(("http://", "https://")):
                raise HTTPException(
                    status_code=400,
                    detail="Webhook 地址必须以 http:// 或 https:// 开头",
                )

        monitor = task_store.create_or_update_monitor(
            name=payload.name.strip(),
            url=normalized_url,
            schema_name=payload.schema_name.strip() or "auto",
            storage_format=payload.storage_format.strip() or "json",
            use_static=payload.use_static,
            selected_fields=normalize_selected_fields(payload.selected_fields),
            field_labels=normalize_field_labels(payload.field_labels),
            profile=normalized_profile,
            monitor_id=payload.monitor_id.strip(),
            schedule_enabled=payload.schedule_enabled,
            schedule_interval_minutes=payload.schedule_interval_minutes,
            tenant_id=tenant_id,
        )
        request_logger(request, "-").info("Save monitor: {}", monitor.monitor_id)
        return {
            "message": "监控已保存",
            "monitor": serialize_monitor(monitor, learned_profile_store),
        }

    @router.post("/api/monitors/{monitor_id}/run")
    async def api_run_monitor(
        monitor_id: str,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        tenant_id = _tenant_id(request)
        monitor = task_store.get_monitor(monitor_id, tenant_id=tenant_id)
        if monitor is None:
            raise HTTPException(status_code=404, detail="监控不存在")

        trigger_result = trigger_monitor_run(
            monitor_id,
            "manual",
            request_id=get_request_id(request),
            tenant_id=tenant_id,
        )
        if trigger_result is None:
            raise HTTPException(status_code=404, detail="监控不存在")

        task_id = str(trigger_result.get("task_id") or "").strip()
        reused_existing_task = bool(trigger_result.get("reused_existing_task"))
        request_logger(request, task_id).info(
            "Run monitor: monitor_id={} url={} reused_existing_task={}",
            monitor_id,
            monitor.url,
            reused_existing_task,
        )
        return {
            "message": (
                "已有运行中的监控任务，已返回现有任务"
                if reused_existing_task
                else "监控检查已启动"
            ),
            "task_id": task_id,
            "monitor_id": monitor_id,
            "reused_existing_task": reused_existing_task,
        }

    @router.post("/api/monitors/{monitor_id}/pause")
    async def api_pause_monitor(
        monitor_id: str,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        tenant_id = _tenant_id(request)
        monitor = task_store.get_monitor(monitor_id, tenant_id=tenant_id)
        if monitor is None:
            raise HTTPException(status_code=404, detail="监控不存在")
        if not monitor.schedule_enabled:
            raise HTTPException(status_code=400, detail="该监控尚未开启自动巡检")

        updated_monitor = task_store.pause_monitor_schedule(
            monitor_id,
            tenant_id=tenant_id,
        )
        if updated_monitor is None:
            raise HTTPException(status_code=404, detail="监控不存在")

        request_logger(request, "-").info("Pause monitor schedule: {}", monitor_id)
        return {
            "message": "已暂停自动巡检",
            "monitor": serialize_monitor(updated_monitor, learned_profile_store),
        }

    @router.post("/api/monitors/{monitor_id}/resume")
    async def api_resume_monitor(
        monitor_id: str,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        tenant_id = _tenant_id(request)
        monitor = task_store.get_monitor(monitor_id, tenant_id=tenant_id)
        if monitor is None:
            raise HTTPException(status_code=404, detail="监控不存在")
        if not monitor.schedule_enabled:
            raise HTTPException(status_code=400, detail="该监控尚未开启自动巡检")

        updated_monitor = task_store.resume_monitor_schedule(
            monitor_id,
            tenant_id=tenant_id,
        )
        if updated_monitor is None:
            raise HTTPException(status_code=404, detail="监控不存在")

        request_logger(request, "-").info("Resume monitor schedule: {}", monitor_id)
        return {
            "message": "已恢复自动巡检",
            "monitor": serialize_monitor(updated_monitor, learned_profile_store),
        }
