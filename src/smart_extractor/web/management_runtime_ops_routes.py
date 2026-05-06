"""Distributed worker, proxy pool, and site policy routes."""

from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, Depends, Request

from smart_extractor.web.api_models import (
    SaveProxyEndpointRequest,
    SaveSitePolicyRequest,
    WorkerHeartbeatRequest,
)
from smart_extractor.web.management_helpers import (
    serialize_proxy_endpoint,
    serialize_site_policy,
    serialize_worker_node,
)


def register_runtime_ops_routes(
    router: APIRouter,
    *,
    api_guard: Callable[[Request], Any],
    request_logger: Callable[[Request, str], Any],
    task_store: Any,
) -> None:
    def _tenant_id(request: Request) -> str:
        identity = getattr(request.state, "identity", None)
        return str(getattr(identity, "tenant_id", "default") or "default")

    def _require(request: Request, permission: str) -> None:
        identity = getattr(request.state, "identity", None)
        require = getattr(identity, "require", None)
        if callable(require):
            require(permission)

    @router.get("/api/workers")
    async def api_workers(
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "dashboard:read")
        return {
            "workers": [
                serialize_worker_node(item)
                for item in task_store.list_worker_nodes(limit=100, tenant_id=_tenant_id(request))
            ]
        }

    @router.post("/api/workers/heartbeat")
    async def api_worker_heartbeat(
        payload: WorkerHeartbeatRequest,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "config:manage")
        worker = task_store.heartbeat_worker_node(
            worker_id=payload.worker_id,
            display_name=payload.display_name,
            node_type=payload.node_type,
            status=payload.status,
            queue_scope=payload.queue_scope,
            current_load=payload.current_load,
            capabilities=list(payload.capabilities or []),
            metadata=dict(payload.metadata or {}),
            last_error=payload.last_error.strip(),
            tenant_id=_tenant_id(request),
        )
        request_logger(request, "-").info("Worker heartbeat: {}", payload.worker_id)
        return {"worker": serialize_worker_node(worker)}

    @router.get("/api/proxies")
    async def api_proxies(
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "dashboard:read")
        return {
            "proxies": [
                serialize_proxy_endpoint(item)
                for item in task_store.list_proxy_endpoints(limit=100, tenant_id=_tenant_id(request))
            ]
        }

    @router.post("/api/proxies")
    async def api_save_proxy(
        payload: SaveProxyEndpointRequest,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "config:manage")
        proxy = task_store.create_or_update_proxy_endpoint(
            name=payload.name.strip(),
            proxy_url=payload.proxy_url.strip(),
            provider=payload.provider.strip(),
            status=payload.status.strip() or "idle",
            enabled=payload.enabled,
            tags=list(payload.tags or []),
            metadata=dict(payload.metadata or {}),
            proxy_id=payload.proxy_id.strip(),
            tenant_id=_tenant_id(request),
        )
        request_logger(request, "-").info("Save proxy endpoint: {}", proxy.proxy_id)
        return {"proxy": serialize_proxy_endpoint(proxy)}

    @router.get("/api/site_policies")
    async def api_site_policies(
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "dashboard:read")
        return {
            "policies": [
                serialize_site_policy(item)
                for item in task_store.list_site_policies(limit=100, tenant_id=_tenant_id(request))
            ]
        }

    @router.post("/api/site_policies")
    async def api_save_site_policy(
        payload: SaveSitePolicyRequest,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _require(request, "config:manage")
        policy = task_store.create_or_update_site_policy(
            domain=payload.domain.strip(),
            name=payload.name.strip() or payload.domain.strip(),
            min_interval_seconds=payload.min_interval_seconds,
            max_concurrency=payload.max_concurrency,
            use_proxy_pool=payload.use_proxy_pool,
            preferred_proxy_tags=list(payload.preferred_proxy_tags or []),
            assigned_worker_group=payload.assigned_worker_group.strip(),
            notes=payload.notes.strip(),
            policy_id=payload.policy_id.strip(),
            tenant_id=_tenant_id(request),
        )
        request_logger(request, "-").info("Save site policy: {}", policy.domain)
        return {"policy": serialize_site_policy(policy)}
