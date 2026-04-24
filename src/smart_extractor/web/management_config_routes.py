"""配置与运行时管理路由。"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Body, Depends, Request

from smart_extractor.web.api_models import BasicLLMConfigPayload
from smart_extractor.web.management_helpers import llm_basic_payload_from_sources


def register_config_routes(
    router: APIRouter,
    *,
    api_guard: Callable[[Request], None],
    request_logger: Callable[[Request, str], Any],
    default_config_path: Path,
    load_config: Callable[..., Any],
    load_raw_yaml_config: Callable[..., dict[str, Any]],
    resolve_local_config_path: Callable[..., Path],
    update_llm_basic_config: Callable[..., Path],
    collect_startup_diagnostics: Callable[..., Any],
    collect_runtime_status: Callable[..., Any],
) -> None:
    @router.get("/api/config/basic")
    async def api_basic_config(
        request: Request,
        _: None = Depends(api_guard),
    ):
        payload = llm_basic_payload_from_sources(
            default_config_path=default_config_path,
            load_config=load_config,
            load_raw_yaml_config=load_raw_yaml_config,
            resolve_local_config_path=resolve_local_config_path,
        )
        request_logger(request, "-").info("Get editable llm config")
        return payload

    @router.post("/api/config/basic")
    async def api_update_basic_config(
        request: Request,
        payload: BasicLLMConfigPayload = Body(...),
        _: None = Depends(api_guard),
    ):
        update_llm_basic_config(
            api_key=payload.api_key.strip(),
            base_url=payload.base_url.strip(),
            model=payload.model.strip(),
            temperature=payload.temperature,
        )
        request.app.state.runtime_status = collect_runtime_status(
            load_config(), app=request.app
        )
        updated_payload = llm_basic_payload_from_sources(
            default_config_path=default_config_path,
            load_config=load_config,
            load_raw_yaml_config=load_raw_yaml_config,
            resolve_local_config_path=resolve_local_config_path,
        )
        request_logger(request, "-").info("Update editable llm config")
        return {
            "message": "基础配置已保存到本地覆盖配置 local.yaml",
            "config": updated_payload,
        }

    @router.get("/api/runtime")
    async def api_runtime_status(
        request: Request,
        _: None = Depends(api_guard),
    ):
        runtime_status = collect_runtime_status(load_config(), app=request.app)
        request.app.state.runtime_status = runtime_status
        request_logger(request, "-").info("Get runtime status")
        return runtime_status
