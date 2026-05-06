"""管理类 Web 路由。"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Depends, Request

from smart_extractor.web.management_config_routes import register_config_routes
from smart_extractor.web.management_funnel_routes import register_funnel_routes
from smart_extractor.web.management_helpers import (
    serialize_actor_install,
    serialize_funnel_event,
    serialize_proxy_endpoint,
    serialize_repair_suggestion,
    serialize_task_list_item,
    serialize_learned_profile,
    serialize_monitor,
    serialize_notification_event,
    serialize_site_policy,
    serialize_task_annotation,
    serialize_template,
    serialize_worker_node,
)
from smart_extractor.web.management_actor_routes import register_actor_routes
from smart_extractor.web.management_annotation_routes import register_annotation_routes
from smart_extractor.web.management_learned_profile_routes import (
    register_learned_profile_routes,
)
from smart_extractor.web.management_monitor_routes import register_monitor_routes
from smart_extractor.web.management_notification_routes import (
    register_notification_routes,
)
from smart_extractor.web.management_runtime_ops_routes import (
    register_runtime_ops_routes,
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
    list_actor_packages: Callable[[], list[dict[str, Any]]],
    get_actor_package: Callable[[str], dict[str, Any] | None],
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
    register_actor_routes(
        router,
        api_guard=api_guard,
        request_logger=request_logger,
        task_store=task_store,
        learned_profile_store=learned_profile_store,
        list_actor_packages=list_actor_packages,
        get_actor_package=get_actor_package,
        get_market_template=get_market_template,
    )
    register_funnel_routes(
        router,
        api_guard=api_guard,
        request_logger=request_logger,
        task_store=task_store,
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
    register_runtime_ops_routes(
        router,
        api_guard=api_guard,
        request_logger=request_logger,
        task_store=task_store,
    )
    register_annotation_routes(
        router,
        api_guard=api_guard,
        request_logger=request_logger,
        task_store=task_store,
        learned_profile_store=learned_profile_store,
    )
    register_task_routes(
        router,
        api_guard=api_guard,
        request_logger=request_logger,
        get_request_id=get_request_id,
        task_store=task_store,
        learned_profile_store=learned_profile_store,
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
        _: Any = Depends(api_guard),
    ):
        identity = getattr(request.state, "identity", None)
        tenant_id = str(getattr(identity, "tenant_id", "default") or "default")
        require = getattr(identity, "require", None)
        if callable(require):
            require("dashboard:read")
        tasks = task_store.list_all(
            limit=max(1, min(int(task_limit), 50)),
            batch_group_id=str(batch_group_id or "").strip(),
            tenant_id=tenant_id,
        )
        stats = task_store.stats(tenant_id=tenant_id)
        insights = task_store.build_dashboard_insights(tenant_id=tenant_id)
        templates = [
            serialize_template(item)
            for item in task_store.list_templates(limit=30, tenant_id=tenant_id)
        ]
        monitors = [
            serialize_monitor(item, learned_profile_store)
            for item in task_store.list_monitors(limit=30, tenant_id=tenant_id)
        ]
        notifications_raw = task_store.list_notification_events(
            limit=max(1, min(int(notification_limit), 50)),
            status=str(notification_status or "").strip().lower(),
            tenant_id=tenant_id,
        )
        notifications = [serialize_notification_event(item) for item in notifications_raw]
        notification_digest = build_notification_digest(
            task_store=task_store,
            window_hours=max(1, min(int(digest_window_hours), 168)),
            tenant_id=tenant_id,
        )
        funnel_events = task_store.list_funnel_events(limit=20, tenant_id=tenant_id)
        funnel_summary = task_store.build_funnel_summary(tenant_id=tenant_id)
        market_templates = list_market_templates()

        monitor_hits: dict[str, int] = {}
        for monitor in task_store.list_monitors(limit=200, tenant_id=tenant_id):
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
            "actors": [
                serialize_actor_install(item)
                for item in task_store.list_actor_installs(limit=30, tenant_id=tenant_id)
            ],
            "monitors": monitors,
            "notifications": notifications,
            "workers": [
                serialize_worker_node(item)
                for item in task_store.list_worker_nodes(limit=30, tenant_id=tenant_id)
            ],
            "proxies": [
                serialize_proxy_endpoint(item)
                for item in task_store.list_proxy_endpoints(limit=30, tenant_id=tenant_id)
            ],
            "site_policies": [
                serialize_site_policy(item)
                for item in task_store.list_site_policies(limit=30, tenant_id=tenant_id)
            ],
            "annotations": [
                serialize_task_annotation(item)
                for item in task_store.list_task_annotations(limit=20, tenant_id=tenant_id)
            ],
            "repairs": [
                serialize_repair_suggestion(item)
                for item in task_store.list_repair_suggestions(limit=20, tenant_id=tenant_id)
            ],
            "notification_digest": notification_digest,
            "funnel_summary": funnel_summary,
            "funnel_events": [serialize_funnel_event(item) for item in funnel_events],
            "market_templates": market_templates,
            "learned_profiles": learned_profiles[:20],
            "runtime_status": runtime_status,
        }
    return router
