"""学习档案管理路由。"""

from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request

from smart_extractor.web.api_models import (
    LearnedProfileActionRequest,
    LearnedProfileBulkActionRequest,
)
from smart_extractor.web.management_helpers import (
    list_risky_active_profiles,
    serialize_learned_profile,
)


def register_learned_profile_routes(
    router: APIRouter,
    *,
    api_guard: Callable[[Request], Any],
    request_logger: Callable[[Request, str], Any],
    get_request_id: Callable[[Request], str],
    task_store: Any,
    learned_profile_store: Any,
    create_background_extraction_task: Callable[..., str],
) -> None:
    def _tenant_id(request: Request) -> str:
        identity = getattr(request.state, "identity", None)
        return str(getattr(identity, "tenant_id", "default") or "default")

    def _require(request: Request, permission: str) -> None:
        identity = getattr(request.state, "identity", None)
        require = getattr(identity, "require", None)
        if callable(require):
            require(permission)

    @router.get("/api/learned_profiles")
    async def api_learned_profiles(
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "dashboard:read")
        tenant_id = _tenant_id(request)
        request_logger(request, "-").info("List learned profiles")
        monitor_hits: dict[str, int] = {}
        for monitor in task_store.list_monitors(limit=200, tenant_id=tenant_id):
            profile_id = str(monitor.last_learned_profile_id or "").strip()
            if profile_id:
                monitor_hits[profile_id] = monitor_hits.get(profile_id, 0) + 1

        profiles = []
        for item in learned_profile_store.list_profiles():
            payload = serialize_learned_profile(item)
            payload["monitor_hits"] = monitor_hits.get(item.profile_id, 0)
            profiles.append(payload)
        profiles.sort(
            key=lambda item: (
                0 if item.get("is_active", True) else 1,
                -(int(item.get("rule_success_count") or 0)),
                -(int(item.get("llm_success_count") or 0)),
                str(item.get("updated_at") or ""),
            )
        )
        return {
            "profiles": profiles[:20],
        }

    @router.post("/api/learned_profiles/{profile_id}/disable")
    async def api_disable_learned_profile(
        profile_id: str,
        payload: LearnedProfileActionRequest,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "monitor:manage")
        profile = learned_profile_store.set_profile_active(
            profile_id,
            is_active=False,
            reason=payload.reason.strip(),
        )
        if profile is None:
            raise HTTPException(status_code=404, detail="学习档案不存在")
        request_logger(request, "-").info("Disable learned profile: {}", profile_id)
        return {
            "message": "学习档案已停用",
            "profile": serialize_learned_profile(profile),
        }

    @router.post("/api/learned_profiles/bulk/disable_risky")
    async def api_disable_risky_learned_profiles(
        payload: LearnedProfileBulkActionRequest,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "monitor:manage")
        risky_profiles = list_risky_active_profiles(learned_profile_store)
        affected_profiles = []
        reason = payload.reason.strip() or "批量停用高风险学习档案"
        for item in risky_profiles:
            updated = learned_profile_store.set_profile_active(
                item.profile_id,
                is_active=False,
                reason=reason,
            )
            if updated is not None:
                affected_profiles.append(serialize_learned_profile(updated))
        request_logger(request, "-").info(
            "Bulk disable risky learned profiles: count={}",
            len(affected_profiles),
        )
        return {
            "message": f"已停用 {len(affected_profiles)} 条高风险学习档案",
            "count": len(affected_profiles),
            "profiles": affected_profiles,
        }

    @router.get("/api/learned_profiles/{profile_id}")
    async def api_learned_profile_detail(
        profile_id: str,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "dashboard:read")
        profile = learned_profile_store.get_profile(profile_id)
        if profile is None:
            raise HTTPException(status_code=404, detail="学习档案不存在")
        activity = task_store.get_learned_profile_activity(
            profile_id,
            task_limit=10,
            tenant_id=_tenant_id(request),
        )
        request_logger(request, "-").info("Get learned profile detail: {}", profile_id)
        return {
            "profile": serialize_learned_profile(profile),
            "activity": activity,
        }

    @router.post("/api/learned_profiles/{profile_id}/relearn")
    async def api_relearn_learned_profile(
        profile_id: str,
        background_tasks: BackgroundTasks,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "task:create")
        profile = learned_profile_store.get_profile(profile_id)
        if profile is None:
            raise HTTPException(status_code=404, detail="学习档案不存在")
        source_url = str(profile.last_matched_url or profile.sample_url or "").strip()
        if not source_url:
            raise HTTPException(status_code=400, detail="学习档案缺少可重学的样本 URL")

        task_id = create_background_extraction_task(
            url=source_url,
            schema_name="auto",
            storage_format="json",
            request_id=get_request_id(request),
            tenant_id=_tenant_id(request),
            background_tasks=background_tasks,
            use_static=False,
            selected_fields=list(profile.selected_fields or []),
            force_strategy="llm",
        )
        request_logger(request, task_id).info(
            "Trigger learned profile relearn: profile_id={} url={}",
            profile_id,
            source_url,
        )
        return {
            "message": "已启动重新学习任务",
            "task_id": task_id,
            "profile_id": profile_id,
            "source_url": source_url,
        }

    @router.post("/api/learned_profiles/bulk/relearn_risky")
    async def api_relearn_risky_learned_profiles(
        payload: LearnedProfileBulkActionRequest,
        background_tasks: BackgroundTasks,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "task:create")
        tenant_id = _tenant_id(request)
        risky_profiles = list_risky_active_profiles(learned_profile_store)
        created_tasks: list[dict[str, str]] = []
        for item in risky_profiles:
            source_url = str(item.last_matched_url or item.sample_url or "").strip()
            if not source_url:
                continue
            task_id = create_background_extraction_task(
                url=source_url,
                schema_name="auto",
                storage_format="json",
                request_id=get_request_id(request),
                tenant_id=tenant_id,
                background_tasks=background_tasks,
                use_static=False,
                selected_fields=list(item.selected_fields or []),
                force_strategy="llm",
            )
            created_tasks.append(
                {
                    "profile_id": item.profile_id,
                    "task_id": task_id,
                    "source_url": source_url,
                }
            )
        request_logger(request, "-").info(
            "Bulk relearn risky learned profiles: requested={} created={}",
            len(risky_profiles),
            len(created_tasks),
        )
        return {
            "message": f"已启动 {len(created_tasks)} 条高风险学习档案的重新学习任务",
            "count": len(created_tasks),
            "tasks": created_tasks,
            "note": payload.reason.strip(),
        }

    @router.post("/api/learned_profiles/{profile_id}/enable")
    async def api_enable_learned_profile(
        profile_id: str,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "monitor:manage")
        profile = learned_profile_store.set_profile_active(profile_id, is_active=True)
        if profile is None:
            raise HTTPException(status_code=404, detail="学习档案不存在")
        request_logger(request, "-").info("Enable learned profile: {}", profile_id)
        return {
            "message": "学习档案已恢复复用",
            "profile": serialize_learned_profile(profile),
        }

    @router.post("/api/learned_profiles/{profile_id}/reset")
    async def api_reset_learned_profile(
        profile_id: str,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "monitor:manage")
        profile = learned_profile_store.reset_profile(profile_id)
        if profile is None:
            raise HTTPException(status_code=404, detail="学习档案不存在")
        request_logger(request, "-").info("Reset learned profile counters: {}", profile_id)
        return {
            "message": "学习档案统计已重置",
            "profile": serialize_learned_profile(profile),
        }

    @router.delete("/api/learned_profiles/{profile_id}")
    async def api_delete_learned_profile(
        profile_id: str,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "monitor:manage")
        deleted = learned_profile_store.delete_profile(profile_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="学习档案不存在")
        request_logger(request, "-").info("Delete learned profile: {}", profile_id)
        return {
            "message": "学习档案已删除",
            "profile_id": profile_id,
        }
