"""管理类 Web 路由。"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Depends, Request

from smart_extractor.web.management_config_routes import register_config_routes
from smart_extractor.web.management_helpers import (
    serialize_task_list_item,
    serialize_template,
    serialize_learned_profile,
    serialize_monitor,
    serialize_notification_event,
)
from smart_extractor.web.management_learned_profile_routes import (
    register_learned_profile_routes,
)
from smart_extractor.web.management_monitor_routes import register_monitor_routes
from smart_extractor.web.management_notification_routes import (
    register_notification_routes,
)
from smart_extractor.web.management_task_routes import register_task_routes
from smart_extractor.web.management_template_routes import (
    register_template_routes,
)
from smart_extractor.web.notification_center import build_notification_digest


def create_management_router(
    *,
    api_guard: Callable[..., Any],
    request_logger: Callable[..., Any],
    get_request_id: Callable[..., str],
    task_store: Any,
    learned_profile_store: Any,
    default_config_path: Path,
    load_config: Callable[..., Any],
    load_raw_yaml_config: Callable[..., dict[str, Any]],
    resolve_local_config_path: Callable[..., Path],
    update_llm_basic_config: Callable[..., Path],
    collect_startup_diagnostics: Callable[..., Any],
    collect_runtime_status: Callable[..., Any],
    list_market_templates: Callable[[], list[dict[str, Any]]],
    get_market_template: Callable[[str], dict[str, Any] | None],
    create_background_extraction_task: Callable[..., str],
    trigger_monitor_run: Callable[..., dict[str, Any] | None],
    send_monitor_notification_fn: Callable[..., Any],
    dispatch_notification_attempt_fn: Callable[..., Any],
    dispatch_digest_notifications_fn: Callable[..., Any],
    build_task_docx: Callable[[dict[str, Any]], bytes],
    build_task_xlsx: Callable[[dict[str, Any]], bytes],
    build_task_markdown: Callable[[dict[str, Any]], str],
) -> APIRouter:
    router = APIRouter()

    register_template_routes(
        router,
        api_guard=api_guard,
        request_logger=request_logger,
        task_store=task_store,
        list_market_templates=list_market_templates,
        get_market_template=get_market_template,
    )
    register_learned_profile_routes(
        router,
        api_guard=api_guard,
        request_logger=request_logger,
        get_request_id=get_request_id,
        task_store=task_store,
        learned_profile_store=learned_profile_store,
        create_background_extraction_task=create_background_extraction_task,
    )
    register_monitor_routes(
        router,
        api_guard=api_guard,
        request_logger=request_logger,
        get_request_id=get_request_id,
        task_store=task_store,
        learned_profile_store=learned_profile_store,
        trigger_monitor_run=trigger_monitor_run,
    )
    register_notification_routes(
        router,
        api_guard=api_guard,
        request_logger=request_logger,
        task_store=task_store,
        send_monitor_notification_fn=send_monitor_notification_fn,
        dispatch_notification_attempt_fn=dispatch_notification_attempt_fn,
        dispatch_digest_notifications_fn=dispatch_digest_notifications_fn,
    )
    register_task_routes(
        router,
        api_guard=api_guard,
        request_logger=request_logger,
        get_request_id=get_request_id,
        task_store=task_store,
        build_task_docx=build_task_docx,
        build_task_xlsx=build_task_xlsx,
        build_task_markdown=build_task_markdown,
        load_config=load_config,
    )
    register_config_routes(
        router,
        api_guard=api_guard,
        request_logger=request_logger,
        default_config_path=default_config_path,
        load_config=load_config,
        load_raw_yaml_config=load_raw_yaml_config,
        resolve_local_config_path=resolve_local_config_path,
        update_llm_basic_config=update_llm_basic_config,
        collect_startup_diagnostics=collect_startup_diagnostics,
        collect_runtime_status=collect_runtime_status,
    )

    @router.get("/api/dashboard")
    async def api_dashboard(
        request: Request,
        task_limit: int = 15,
        notification_limit: int = 12,
        digest_window_hours: int = 24,
        batch_group_id: str = "",
        notification_status: str = "",
        _: None = Depends(api_guard),
    ):
        tasks = task_store.list_all(
            limit=max(1, min(int(task_limit), 50)),
            batch_group_id=str(batch_group_id or "").strip(),
        )
        stats = task_store.stats()
        insights = task_store.build_dashboard_insights()
        templates = [
            serialize_template(item) for item in task_store.list_templates(limit=30)
        ]
        monitors = [
            serialize_monitor(item, learned_profile_store)
            for item in task_store.list_monitors(limit=30)
        ]
        notifications_raw = task_store.list_notification_events(
            limit=max(1, min(int(notification_limit), 50)),
            status=str(notification_status or "").strip().lower(),
        )
        notifications = [serialize_notification_event(item) for item in notifications_raw]
        notification_digest = build_notification_digest(
            task_store=task_store,
            window_hours=max(1, min(int(digest_window_hours), 168)),
        )
        market_templates = list_market_templates()

        monitor_hits: dict[str, int] = {}
        for monitor in task_store.list_monitors(limit=200):
            profile_id = str(monitor.last_learned_profile_id or "").strip()
            if profile_id:
                monitor_hits[profile_id] = monitor_hits.get(profile_id, 0) + 1

        learned_profiles = []
        for item in learned_profile_store.list_profiles():
            payload = serialize_learned_profile(item)
            payload["monitor_hits"] = monitor_hits.get(item.profile_id, 0)
            learned_profiles.append(payload)
        learned_profiles.sort(
            key=lambda item: (
                0 if item.get("is_active", True) else 1,
                -(int(item.get("rule_success_count") or 0)),
                -(int(item.get("llm_success_count") or 0)),
                str(item.get("updated_at") or ""),
            )
        )

        runtime_status = collect_runtime_status(load_config(), app=request.app)
        request.app.state.runtime_status = runtime_status
        request_logger(request, "-").info(
            "Get dashboard payload: tasks={} notifications={} batch_group_id={} notification_status={}",
            len(tasks),
            len(notifications),
            batch_group_id or "-",
            notification_status or "-",
        )
        return {
            "tasks": [serialize_task_list_item(task) for task in tasks],
            "stats": stats,
            "insights": insights,
            "templates": templates,
            "monitors": monitors,
            "notifications": notifications,
            "notification_digest": notification_digest,
            "market_templates": market_templates,
            "learned_profiles": learned_profiles[:20],
            "runtime_status": runtime_status,
        }
    return router
