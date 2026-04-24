"""网页分析类 API 路由。"""

from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Request
from starlette.concurrency import run_in_threadpool

from smart_extractor.utils.display import get_page_type_label
from smart_extractor.web.api_models import (
    AnalyzeComparePreviewRequest,
    AnalyzeCompareRequest,
    AnalyzeInsightRequest,
    AnalyzePageRequest,
)


def create_analysis_router(
    *,
    api_guard: Callable[[Request], None],
    request_logger: Callable[[Request, str], Any],
    get_request_id: Callable[[Request], str],
    load_config: Callable[..., Any],
) -> APIRouter:
    router = APIRouter()

    @router.post("/api/analyze_page")
    async def api_analyze_page(
        req: AnalyzePageRequest,
        request: Request,
        _: None = Depends(api_guard),
    ):
        req_url = req.url.strip()
        if not req_url:
            raise HTTPException(status_code=400, detail="url 不能为空")
        if not req_url.startswith(("http://", "https://")):
            raise HTTPException(
                status_code=400,
                detail="url 必须以 http:// 或 https:// 开头",
            )

        from smart_extractor.pipeline import ExtractionPipeline

        req_log = request_logger(request)
        req_log.info("Analyze page: url={} static={}", req_url, req.use_static)

        try:

            def _analyze() -> dict:
                with ExtractionPipeline(
                    config=load_config(),
                    use_dynamic_fetcher=not req.use_static,
                ) as pipeline:
                    return pipeline.analyze_page(req_url)

            analysis = await run_in_threadpool(_analyze)

            req_log.info(
                "Page analysis done: page_type={} candidate_fields={}",
                analysis.get("page_type"),
                analysis.get("candidate_fields"),
            )
            return {
                "page_type": analysis.get("page_type", "unknown"),
                "page_type_label": get_page_type_label(
                    analysis.get("page_type", "unknown")
                ),
                "candidate_fields": analysis.get("candidate_fields", []),
                "field_labels": analysis.get("field_labels", {}),
                "preview": analysis.get("preview", ""),
                "learned_profile": analysis.get("learned_profile"),
                "request_id": get_request_id(request),
            }
        except Exception as exc:
            req_log.exception("Page analysis failed: {}", exc)
            raise HTTPException(
                status_code=500,
                detail=f"页面分析失败: {type(exc).__name__}: {exc}",
            ) from exc

    @router.post("/api/analyze_insight")
    async def api_analyze_insight(
        req: AnalyzeInsightRequest,
        request: Request,
        _: None = Depends(api_guard),
    ):
        req_url = req.url.strip()
        if not req_url:
            raise HTTPException(status_code=400, detail="url 不能为空")
        if not req_url.startswith(("http://", "https://")):
            raise HTTPException(
                status_code=400,
                detail="url 必须以 http:// 或 https:// 开头",
            )

        from smart_extractor.pipeline import ExtractionPipeline

        req_log = request_logger(request)
        user_context = {
            "goal": req.goal.strip() or "summary",
            "role": req.role.strip() or "consumer",
            "priority": req.priority.strip(),
            "constraints": req.constraints.strip(),
            "notes": req.notes.strip(),
            "output_format": req.output_format.strip() or "cards",
        }
        req_log.info(
            "Analyze insight: url={} goal={} role={} static={}",
            req_url,
            user_context["goal"],
            user_context["role"],
            req.use_static,
        )

        try:

            def _analyze() -> dict:
                with ExtractionPipeline(
                    config=load_config(),
                    use_dynamic_fetcher=not req.use_static,
                ) as pipeline:
                    return pipeline.analyze_with_context(
                        req_url,
                        user_context=user_context,
                    )

            analysis = await run_in_threadpool(_analyze)
            analysis["request_id"] = get_request_id(request)
            return analysis
        except Exception as exc:
            req_log.exception("Insight analysis failed: {}", exc)
            raise HTTPException(
                status_code=500,
                detail=f"智能分析失败: {type(exc).__name__}: {exc}",
            ) from exc

    @router.post("/api/analyze_compare_preview")
    async def api_analyze_compare_preview(
        req: AnalyzeComparePreviewRequest,
        request: Request,
        _: None = Depends(api_guard),
    ):
        urls = [str(url or "").strip() for url in req.urls if str(url or "").strip()]
        if len(urls) < 2:
            raise HTTPException(status_code=400, detail="至少需要两个 URL 才能生成对比预览")
        for url in urls:
            if not url.startswith(("http://", "https://")):
                raise HTTPException(
                    status_code=400,
                    detail="url 必须以 http:// 或 https:// 开头",
                )

        from smart_extractor.pipeline import ExtractionPipeline

        req_log = request_logger(request)
        req_log.info(
            "Analyze compare preview: count={} static={}",
            len(urls),
            req.use_static,
        )

        try:

            def _analyze_one(url: str) -> dict:
                with ExtractionPipeline(
                    config=load_config(),
                    use_dynamic_fetcher=not req.use_static,
                ) as pipeline:
                    result = pipeline.analyze_page(url)
                    return {
                        "url": url,
                        "page_type": result.get("page_type", "unknown"),
                        "page_type_label": get_page_type_label(
                            result.get("page_type", "unknown")
                        ),
                        "candidate_fields": result.get("candidate_fields", []),
                        "field_labels": result.get("field_labels", {}),
                        "preview": result.get("preview", ""),
                    }

            items = await run_in_threadpool(lambda: [_analyze_one(url) for url in urls])
            return {
                "items": items,
                "request_id": get_request_id(request),
            }
        except Exception as exc:
            req_log.exception("Compare preview failed: {}", exc)
            raise HTTPException(
                status_code=500,
                detail=f"对比预览失败: {type(exc).__name__}: {exc}",
            ) from exc

    @router.post("/api/analyze_compare")
    async def api_analyze_compare(
        req: AnalyzeCompareRequest,
        request: Request,
        _: None = Depends(api_guard),
    ):
        urls = [str(url or "").strip() for url in req.urls if str(url or "").strip()]
        if len(urls) < 2:
            raise HTTPException(status_code=400, detail="至少需要两个 URL 才能进行对比分析")
        for url in urls:
            if not url.startswith(("http://", "https://")):
                raise HTTPException(
                    status_code=400,
                    detail="url 必须以 http:// 或 https:// 开头",
                )

        from smart_extractor.pipeline import ExtractionPipeline

        req_log = request_logger(request)
        user_context = {
            "goal": req.goal.strip() or "comparison",
            "role": req.role.strip() or "consumer",
            "focus": req.focus.strip(),
            "must_have": req.must_have.strip(),
            "elimination": req.elimination.strip(),
            "notes": req.notes.strip(),
            "output_format": req.output_format.strip() or "table",
        }
        req_log.info(
            "Analyze compare: count={} goal={} role={} static={}",
            len(urls),
            user_context["goal"],
            user_context["role"],
            req.use_static,
        )

        try:

            def _analyze() -> dict:
                with ExtractionPipeline(
                    config=load_config(),
                    use_dynamic_fetcher=not req.use_static,
                ) as pipeline:
                    return pipeline.analyze_many_with_context(
                        urls,
                        user_context=user_context,
                    )

            analysis = await run_in_threadpool(_analyze)
            analysis["request_id"] = get_request_id(request)
            return analysis
        except Exception as exc:
            req_log.exception("Compare analysis failed: {}", exc)
            raise HTTPException(
                status_code=500,
                detail=f"对比分析失败: {type(exc).__name__}: {exc}",
            ) from exc

    return router
