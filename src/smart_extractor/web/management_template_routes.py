"""模板管理路由。"""

from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Request

from smart_extractor.web.api_models import InstallMarketTemplateRequest, SaveTemplateRequest
from smart_extractor.web.management_helpers import (
    normalize_field_labels,
    normalize_profile_payload,
    normalize_selected_fields,
    serialize_template,
)


def register_template_routes(
    router: APIRouter,
    *,
    api_guard: Callable[[Request], None],
    request_logger: Callable[[Request, str], Any],
    task_store: Any,
    list_market_templates: Callable[[], list[dict[str, Any]]],
    get_market_template: Callable[[str], dict[str, Any] | None],
) -> None:
    def _tenant_id(request: Request) -> str:
        identity = getattr(request.state, "identity", None)
        require = getattr(identity, "require", None)
        if callable(require):
            require("template:manage")
        return str(getattr(identity, "tenant_id", "default") or "default")

    def _record_funnel_event(
        request: Request,
        *,
        stage: str,
        channel: str,
        package_type: str,
        package_id: str = "",
        package_name: str = "",
        template_id: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        task_store.create_funnel_event(
            stage=stage,
            channel=channel,
            package_type=package_type,
            package_id=package_id,
            package_name=package_name,
            template_id=template_id,
            metadata=metadata,
            tenant_id=str(
                getattr(getattr(request.state, "identity", None), "tenant_id", "default")
                or "default"
            ),
        )

    @router.get("/api/templates")
    async def api_templates(
        request: Request,
        _: Any = Depends(api_guard),
    ):
        request_logger(request, "-").info("List templates")
        return {
            "templates": [
                serialize_template(item)
                for item in task_store.list_templates(
                    limit=30,
                    tenant_id=_tenant_id(request),
                )
            ]
        }

    @router.post("/api/templates")
    async def api_save_template(
        payload: SaveTemplateRequest,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        tenant_id = _tenant_id(request)
        template = task_store.create_or_update_template(
            name=payload.name.strip(),
            url=payload.url.strip(),
            page_type=payload.page_type.strip() or "unknown",
            schema_name=payload.schema_name.strip() or "auto",
            storage_format=payload.storage_format.strip() or "json",
            use_static=payload.use_static,
            selected_fields=normalize_selected_fields(payload.selected_fields),
            field_labels=normalize_field_labels(payload.field_labels),
            profile=normalize_profile_payload(payload.profile),
            template_id=payload.template_id.strip(),
            tenant_id=tenant_id,
        )
        request_logger(request, "-").info("Save template: {}", template.template_id)
        return {
            "message": "模板已保存",
            "template": serialize_template(template),
        }

    @router.get("/api/template_market")
    async def api_template_market(
        request: Request,
        _: Any = Depends(api_guard),
    ):
        _tenant_id(request)
        templates = list_market_templates()
        _record_funnel_event(
            request,
            stage="template_market_list",
            channel="template_market",
            package_type="template",
            metadata={"template_count": len(templates)},
        )
        request_logger(request, "-").info("List market templates")
        return {"templates": templates}

    @router.post("/api/template_market/install")
    async def api_install_market_template(
        payload: InstallMarketTemplateRequest,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        tenant_id = _tenant_id(request)
        market_template = get_market_template(payload.template_id.strip())
        if market_template is None:
            raise HTTPException(status_code=404, detail="模板不存在")

        template = task_store.create_or_update_template(
            name=market_template["name"],
            url=market_template.get("sample_url", ""),
            page_type=market_template.get("page_type", "unknown"),
            schema_name=market_template.get("schema_name", "auto"),
            storage_format=market_template.get("storage_format", "json"),
            use_static=bool(market_template.get("use_static", False)),
            selected_fields=list(market_template.get("selected_fields", [])),
            field_labels=dict(market_template.get("field_labels", {})),
            profile=normalize_profile_payload(dict(market_template.get("profile", {}))),
            tenant_id=tenant_id,
        )
        request_logger(request, "-").info(
            "Install market template: market_id={} template_id={}",
            payload.template_id,
            template.template_id,
        )
        _record_funnel_event(
            request,
            stage="template_market_install",
            channel="template_market",
            package_type="template",
            package_id=market_template["template_id"],
            package_name=market_template["name"],
            template_id=template.template_id,
            metadata={
                "page_type": str(market_template.get("page_type", "")),
                "use_static": bool(market_template.get("use_static", False)),
            },
        )
        return {
            "message": "模板已安装到我的模板",
            "template": serialize_template(template),
        }
