"""FastAPI Web 应用入口。"""

from __future__ import annotations

import sys
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from loguru import logger

from smart_extractor import __version__
from smart_extractor.config import load_config
from smart_extractor.utils.logger import setup_logger
from smart_extractor.web.security import (
    collect_runtime_status,
    enforce_csrf_origin,
    run_startup_self_check,
)

if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception as exc:  # pragma: no cover
        logger.debug("Windows 终端编码切换失败，继续使用当前编码: {}", exc)

WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理。"""
    config = load_config()
    setup_logger(config.log)

    task_worker_service = None
    monitor_scheduler_service = None
    notification_retry_service = None
    notification_digest_service = None
    app.state.task_worker_service = None
    app.state.monitor_scheduler_service = None
    app.state.notification_retry_service = None
    app.state.notification_digest_service = None

    if (
        str(config.web.task_dispatch_mode or "").strip().lower() in {"queue", "redis"}
        and config.web.start_builtin_worker
    ):
        from smart_extractor.web.routes import create_managed_task_worker

        task_worker_service = create_managed_task_worker(worker_id="builtin-web")
        task_worker_service.start()
        app.state.task_worker_service = task_worker_service
        logger.info("已启动内置队列 worker")

    if config.web.start_builtin_monitor_scheduler:
        from smart_extractor.web.routes import create_managed_monitor_scheduler

        monitor_scheduler_service = create_managed_monitor_scheduler(
            scheduler_id="builtin-monitor-scheduler"
        )
        monitor_scheduler_service.start()
        app.state.monitor_scheduler_service = monitor_scheduler_service
        logger.info("已启动内置监控调度服务")
    else:
        logger.info("当前实例未启动内置监控调度器")

    if config.web.start_builtin_notification_retry:
        from smart_extractor.web.routes import create_managed_notification_retry

        notification_retry_service = create_managed_notification_retry(
            service_id="builtin-notification-retry"
        )
        notification_retry_service.start()
        app.state.notification_retry_service = notification_retry_service
        logger.info("已启动内置通知自动重试服务")
    else:
        logger.info("当前实例未启动内置通知自动重试服务")

    if config.web.start_builtin_notification_digest:
        from smart_extractor.web.routes import create_managed_notification_digest

        notification_digest_service = create_managed_notification_digest(
            service_id="builtin-notification-digest"
        )
        notification_digest_service.start()
        app.state.notification_digest_service = notification_digest_service
        logger.info("已启动内置通知 Digest 服务")
    else:
        logger.info("当前实例未启动内置通知 Digest 服务")

    startup_status = run_startup_self_check(config, strict=False)
    app.state.runtime_status = collect_runtime_status(config, app=app)
    app.state.runtime_status["ready"] = bool(startup_status.get("ready"))
    app.state.runtime_status["issues"] = list(startup_status.get("issues") or [])
    app.state.runtime_status["warnings"] = list(
        startup_status.get("warnings") or []
    )
    logger.info("Web 应用启动完成")
    yield

    if notification_retry_service is not None:
        notification_retry_service.stop()
        logger.info("已停止内置通知自动重试服务")
    if notification_digest_service is not None:
        notification_digest_service.stop()
        logger.info("已停止内置通知 Digest 服务")
    if monitor_scheduler_service is not None:
        monitor_scheduler_service.stop()
        logger.info("已停止内置监控调度服务")
    if task_worker_service is not None:
        task_worker_service.stop()
        logger.info("已停止内置队列 worker")
    logger.info("Web 应用关闭")


templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _normalize_host(value: str) -> str:
    host = str(value or "").strip().lower()
    if not host:
        return ""
    if host.startswith("[") and "]" in host:
        end = host.find("]")
        return host[: end + 1]
    if ":" in host:
        return host.split(":", 1)[0]
    return host


def register_middlewares(app: FastAPI) -> None:
    """注册中间件。"""

    config = load_config()
    csrf_enabled = bool(config.web.csrf_protection_enabled)
    csrf_allowed_origins = list(config.web.csrf_allowed_origins or [])
    api_token_configured = bool(config.web.api_token)
    allowed_hosts = {
        _normalize_host(item)
        for item in config.web.allowed_hosts or []
        if _normalize_host(item)
    }
    request_max_body_bytes = max(int(config.web.request_max_body_bytes or 0), 0)
    security_headers_enabled = bool(config.web.security_headers_enabled)

    @app.middleware("http")
    async def csrf_guard_middleware(request: Request, call_next):
        if csrf_enabled:
            try:
                enforce_csrf_origin(
                    request,
                    api_token_configured=api_token_configured,
                    allowed_origins=csrf_allowed_origins,
                )
            except HTTPException as exc:
                from fastapi.responses import JSONResponse

                return JSONResponse(
                    status_code=exc.status_code,
                    content={"detail": exc.detail},
                )
        return await call_next(request)

    @app.middleware("http")
    async def host_guard_middleware(request: Request, call_next):
        if allowed_hosts:
            host = _normalize_host(request.headers.get("host", ""))
            if not host or host not in allowed_hosts:
                from fastapi.responses import JSONResponse

                return JSONResponse(
                    status_code=400,
                    content={"detail": "非法 Host 头"},
                )
        return await call_next(request)

    @app.middleware("http")
    async def request_size_guard_middleware(request: Request, call_next):
        if request_max_body_bytes > 0:
            content_length = str(request.headers.get("content-length") or "").strip()
            if content_length:
                try:
                    if int(content_length) > request_max_body_bytes:
                        from fastapi.responses import JSONResponse

                        return JSONResponse(
                            status_code=413,
                            content={
                                "detail": (
                                    f"请求体过大，超过限制 {request_max_body_bytes} bytes"
                                )
                            },
                        )
                except ValueError:
                    pass
        return await call_next(request)

    @app.middleware("http")
    async def request_trace_middleware(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
        request.state.request_id = request_id
        start_at = time.perf_counter()

        with logger.contextualize(request_id=request_id, task_id="-"):
            logger.info("HTTP 请求开始: {} {}", request.method, request.url.path)
            try:
                response = await call_next(request)
            except Exception:
                elapsed_ms = (time.perf_counter() - start_at) * 1000
                logger.exception(
                    "HTTP 请求异常: {} {} elapsed_ms={:.0f}",
                    request.method,
                    request.url.path,
                    elapsed_ms,
                )
                raise

            elapsed_ms = (time.perf_counter() - start_at) * 1000
            response.headers["X-Request-ID"] = request_id
            if security_headers_enabled:
                response.headers.setdefault("X-Content-Type-Options", "nosniff")
                response.headers.setdefault("X-Frame-Options", "DENY")
                response.headers.setdefault(
                    "Referrer-Policy", "strict-origin-when-cross-origin"
                )
                response.headers.setdefault(
                    "Content-Security-Policy",
                    "default-src 'self'; img-src 'self' data:; "
                    "style-src 'self' 'unsafe-inline'; script-src 'self'; "
                    "font-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; "
                    "base-uri 'self'; form-action 'self'",
                )
            logger.info(
                "HTTP 请求结束: {} {} status={} elapsed_ms={:.0f}",
                request.method,
                request.url.path,
                response.status_code,
                elapsed_ms,
            )
            return response


def create_app() -> FastAPI:
    """创建并配置 FastAPI 应用。"""
    app = FastAPI(
        title="Smart Data Extractor",
        description="复杂网页数据智能提取引擎 Web API",
        version=__version__,
        lifespan=lifespan,
    )
    register_middlewares(app)
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    from smart_extractor.web.routes import router  # noqa: WPS433,E402

    app.include_router(router)
    return app


app = create_app()
