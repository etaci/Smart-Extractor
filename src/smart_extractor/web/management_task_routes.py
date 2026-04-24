"""任务与分析管理路由。"""

from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response

from smart_extractor.web.api_models import NaturalLanguageTaskRequest


def register_task_routes(
    router: APIRouter,
    *,
    api_guard: Callable[[Request], None],
    request_logger: Callable[[Request, str], Any],
    get_request_id: Callable[[Request], str],
    task_store: Any,
    build_task_docx: Callable[[dict[str, Any]], bytes],
    build_task_xlsx: Callable[[dict[str, Any]], bytes],
    build_task_markdown: Callable[[dict[str, Any]], str],
    load_config: Callable[..., Any],
) -> None:
    @router.post("/api/nl_task")
    async def api_natural_language_task(
        payload: NaturalLanguageTaskRequest,
        request: Request,
        _: None = Depends(api_guard),
    ):
        from smart_extractor.extractor.llm_extractor import LLMExtractor

        extractor = LLMExtractor(load_config().llm)
        plan = extractor.parse_task_request(payload.request_text.strip())
        request_logger(request, "-").info(
            "Parse natural language task: task_type={} urls={} fields={}",
            plan.get("task_type"),
            len(plan.get("urls", [])),
            plan.get("selected_fields", []),
        )
        return {
            "message": "已生成任务草案",
            "plan": plan,
            "request_id": get_request_id(request),
        }

    @router.get("/api/task/{task_id}")
    async def api_task_detail(
        task_id: str,
        request: Request,
        _: None = Depends(api_guard),
    ):
        detail = task_store.get_task_detail_payload(task_id)
        if not detail:
            raise HTTPException(status_code=404, detail="任务不存在")
        request_logger(request, task_id).info("Get task detail")
        return detail

    @router.get("/api/task/{task_id}/export")
    async def api_task_export(
        task_id: str,
        request: Request,
        format: str = "docx",
        _: None = Depends(api_guard),
    ):
        detail = task_store.get_task_detail_payload(task_id)
        if not detail:
            raise HTTPException(status_code=404, detail="任务不存在")

        normalized_format = str(format or "docx").strip().lower()
        if normalized_format == "docx":
            content = build_task_docx(detail)
            media_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            file_name = f"{task_id}.docx"
        elif normalized_format == "xlsx":
            content = build_task_xlsx(detail)
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            file_name = f"{task_id}.xlsx"
        elif normalized_format == "md":
            content = build_task_markdown(detail).encode("utf-8")
            media_type = "text/markdown; charset=utf-8"
            file_name = f"{task_id}.md"
        elif normalized_format == "json":
            import json

            content = json.dumps(detail, ensure_ascii=False, indent=2).encode("utf-8")
            media_type = "application/json; charset=utf-8"
            file_name = f"{task_id}.json"
        else:
            raise HTTPException(status_code=400, detail="仅支持 docx、xlsx、md 或 json")

        request_logger(request, task_id).info("Export task: format={}", normalized_format)
        return Response(
            content=content,
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{file_name}"'},
        )
