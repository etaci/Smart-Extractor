"""Actor/plugin market management routes."""

from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Request

from smart_extractor.web.api_models import InstallActorRequest
from smart_extractor.web.management_helpers import (
    apply_monitor_notification_defaults,
    normalize_profile_payload,
    serialize_actor_install,
    serialize_monitor,
    serialize_template,
)


def register_actor_routes(
    router: APIRouter,
    *,
    api_guard: Callable[[Request], Any],
    request_logger: Callable[[Request, str], Any],
    task_store: Any,
    learned_profile_store: Any,
    list_actor_packages: Callable[[], list[dict[str, Any]]],
    get_actor_package: Callable[[str], dict[str, Any] | None],
    get_market_template: Callable[[str], dict[str, Any] | None],
) -> None:
    def _tenant_id(request: Request) -> str:
        identity = getattr(request.state, "identity", None)
        return str(getattr(identity, "tenant_id", "default") or "default")

    def _require(request: Request, permission: str) -> None:
        identity = getattr(request.state, "identity", None)
        require = getattr(identity, "require", None)
        if callable(require):
            require(permission)

    def _record_funnel_event(
        request: Request,
        *,
        stage: str,
        channel: str,
        package_type: str,
        package_id: str = "",
        package_name: str = "",
        template_id: str = "",
        monitor_id: str = "",
        actor_instance_id: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        task_store.create_funnel_event(
            stage=stage,
            channel=channel,
            package_type=package_type,
            package_id=package_id,
            package_name=package_name,
            template_id=template_id,
            monitor_id=monitor_id,
            actor_instance_id=actor_instance_id,
            metadata=metadata,
            tenant_id=str(
                getattr(getattr(request.state, "identity", None), "tenant_id", "default")
                or "default"
            ),
        )

    @router.get("/api/actor_market")
    async def api_actor_market(
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "dashboard:read")
        actors = list_actor_packages()
        _record_funnel_event(
            request,
            stage="actor_market_list",
            channel="actor_market",
            package_type="actor",
            metadata={"actor_count": len(actors)},
        )
        request_logger(request, "-").info("List actor market packages")
        return {"actors": actors}

    @router.get("/api/actors")
    async def api_installed_actors(
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "dashboard:read")
        tenant_id = _tenant_id(request)
        return {
            "actors": [
                serialize_actor_install(item)
                for item in task_store.list_actor_installs(limit=50, tenant_id=tenant_id)
            ]
        }

    @router.post("/api/actor_market/install")
    async def api_install_actor(
        payload: InstallActorRequest,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "template:manage")
        if payload.create_monitor:
            _require(request, "monitor:manage")
        tenant_id = _tenant_id(request)
        actor_package = get_actor_package(payload.actor_id)
        if actor_package is None:
            raise HTTPException(status_code=404, detail="Actor 包不存在")

        template = None
        monitor = None
        template_package_id = str(actor_package.get("template_package_id") or "").strip()
        template_profile = {}
        market_template = get_market_template(template_package_id) if template_package_id else None
        if payload.create_template and market_template is not None:
            template_profile = normalize_profile_payload(
                {
                    **dict(market_template.get("profile", {})),
                    "installed_actor_id": actor_package["actor_id"],
                    "actor_growth_mode": "actor_market",
                }
            )
            template = task_store.create_or_update_template(
                name=payload.name.strip() or f"{actor_package['name']}模板",
                url=str(market_template.get("sample_url") or ""),
                page_type=str(market_template.get("page_type") or "unknown"),
                schema_name=str(market_template.get("schema_name") or "auto"),
                storage_format=str(market_template.get("storage_format") or "json"),
                use_static=bool(market_template.get("use_static", False)),
                selected_fields=list(market_template.get("selected_fields", [])),
                field_labels=dict(market_template.get("field_labels", {})),
                profile=template_profile,
                tenant_id=tenant_id,
            )
        if payload.create_monitor and market_template is not None:
            recommended_schedule = (
                market_template.get("recommended_schedule")
                if isinstance(market_template.get("recommended_schedule"), dict)
                else {}
            )
            monitor = task_store.create_or_update_monitor(
                name=payload.name.strip() or actor_package["name"],
                url=str(market_template.get("sample_url") or ""),
                schema_name=str(market_template.get("schema_name") or "auto"),
                storage_format=str(market_template.get("storage_format") or "json"),
                use_static=bool(market_template.get("use_static", False)),
                selected_fields=list(market_template.get("selected_fields", [])),
                field_labels=dict(market_template.get("field_labels", {})),
                profile=apply_monitor_notification_defaults(
                    {
                        **normalize_profile_payload(dict(market_template.get("profile", {}))),
                        "installed_actor_id": actor_package["actor_id"],
                        "actor_growth_mode": "actor_market",
                    }
                ),
                schedule_enabled=bool(recommended_schedule.get("enabled", True)),
                schedule_interval_minutes=int(
                    recommended_schedule.get("interval_minutes", 180) or 180
                ),
                tenant_id=tenant_id,
            )

        actor_install = task_store.create_or_update_actor_install(
            actor_id=actor_package["actor_id"],
            name=payload.name.strip() or actor_package["name"],
            version=str(actor_package.get("version") or "1.0.0"),
            category=str(actor_package.get("category") or ""),
            capabilities=list(actor_package.get("capabilities", [])),
            config={
                **dict(actor_package.get("default_config", {})),
                **dict(payload.config or {}),
            },
            linked_template_id=template.template_id if template is not None else "",
            linked_monitor_id=monitor.monitor_id if monitor is not None else "",
            tenant_id=tenant_id,
        )
        request_logger(request, "-").info(
            "Install actor package: actor_id={} actor_instance_id={}",
            actor_package["actor_id"],
            actor_install.actor_instance_id,
        )
        _record_funnel_event(
            request,
            stage="actor_market_install",
            channel="actor_market",
            package_type="actor",
            package_id=actor_package["actor_id"],
            package_name=actor_package["name"],
            template_id=template.template_id if template is not None else "",
            monitor_id=monitor.monitor_id if monitor is not None else "",
            actor_instance_id=actor_install.actor_instance_id,
            metadata={
                "create_template": bool(payload.create_template),
                "create_monitor": bool(payload.create_monitor),
                "template_package_id": template_package_id,
            },
        )
        response_payload = {
            "message": "Actor 包已安装",
            "actor": serialize_actor_install(actor_install),
        }
        if template is not None:
            response_payload["template"] = serialize_template(template)
        if monitor is not None:
            response_payload["monitor"] = serialize_monitor(monitor, learned_profile_store)
        return response_payload
