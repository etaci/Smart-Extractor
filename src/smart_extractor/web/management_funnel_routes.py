"""Growth funnel management routes."""

from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, Depends, Request

from smart_extractor.web.management_helpers import serialize_funnel_event


def register_funnel_routes(
    router: APIRouter,
    *,
    api_guard: Callable[[Request], Any],
    request_logger: Callable[[Request, str], Any],
    task_store: Any,
) -> None:
    @router.get("/api/funnel")
    async def api_funnel(
        request: Request,
        limit: int = 50,
        stage: str = "",
        package_id: str = "",
        _: Any = Depends(api_guard),
    ):
        identity = getattr(request.state, "identity", None)
        tenant_id = str(getattr(identity, "tenant_id", "default") or "default")
        require = getattr(identity, "require", None)
        if callable(require):
            require("dashboard:read")

        normalized_limit = max(1, min(int(limit), 200))
        events = task_store.list_funnel_events(
            limit=normalized_limit,
            stage=str(stage or "").strip(),
            package_id=str(package_id or "").strip(),
            tenant_id=tenant_id,
        )
        summary = task_store.build_funnel_summary(tenant_id=tenant_id)
        request_logger(request, "-").info(
            "Get funnel payload: limit={} stage={} package_id={} events={}",
            normalized_limit,
            stage or "-",
            package_id or "-",
            len(events),
        )
        return {
            "summary": summary,
            "events": [serialize_funnel_event(item) for item in events],
        }
