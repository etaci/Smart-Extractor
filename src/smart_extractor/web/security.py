"""
Web 安全与启动自检。

包含 API Token 鉴权、基础限流、启动阶段 LLM 可用性校验。
"""

from __future__ import annotations

import secrets
import threading
import time
from collections import defaultdict, deque
from typing import Any

from fastapi import HTTPException, Request
from loguru import logger
from openai import OpenAI

from smart_extractor.config import AppConfig
from smart_extractor.extractor.llm_extractor import _extract_chat_message_content
from smart_extractor.web.database import parse_database_target


class ApiRateLimiter:
    """按客户端维度做每分钟限流"""

    def __init__(self, limit_per_minute: int):
        self._limit = max(0, int(limit_per_minute))
        self._hits: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, client_key: str) -> None:
        if self._limit <= 0:
            return
        now = time.time()
        threshold = now - 60.0
        with self._lock:
            bucket = self._hits[client_key]
            while bucket and bucket[0] < threshold:
                bucket.popleft()
            if len(bucket) >= self._limit:
                raise HTTPException(
                    status_code=429,
                    detail=f"请求过于频繁，请稍后重试（限流={self._limit}/min）",
                )
            bucket.append(now)


def resolve_client_key(request: Request) -> str:
    """提取客户端标识。

    默认使用直连客户端 IP。仅当请求来自可信代理时，才信任
    `X-Forwarded-For` / `X-Real-IP`，避免攻击者伪造头绕过限流。
    """
    return resolve_client_key_with_trusted_proxies(request, trusted_proxy_ips=[])


def _is_trusted_proxy(remote_host: str, trusted_proxy_ips: list[str]) -> bool:
    normalized_remote_host = str(remote_host or "").strip().lower()
    if not normalized_remote_host:
        return False

    normalized_trusted = {
        str(item or "").strip().lower()
        for item in trusted_proxy_ips or []
        if str(item or "").strip()
    }
    return "*" in normalized_trusted or normalized_remote_host in normalized_trusted


def resolve_client_key_with_trusted_proxies(
    request: Request,
    *,
    trusted_proxy_ips: list[str],
) -> str:
    """在可信代理场景下解析真实客户端地址。"""
    remote_host = (
        str(request.client.host).strip() if request.client and request.client.host else ""
    )
    if _is_trusted_proxy(remote_host, trusted_proxy_ips):
        forwarded_for = request.headers.get("x-forwarded-for", "").strip()
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        real_ip = request.headers.get("x-real-ip", "").strip()
        if real_ip:
            return real_ip

    forwarded_for = request.headers.get("x-forwarded-for", "").strip()
    if forwarded_for and not remote_host:
        return forwarded_for.split(",")[0].strip()
    if remote_host:
        return remote_host
    return "unknown"


def extract_token_from_request(request: Request) -> str:
    """从请求头提取 API Token"""
    token = request.headers.get("x-api-token", "").strip()
    if token:
        return token
    auth_header = request.headers.get("authorization", "").strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return ""


def enforce_api_token(request: Request, expected_token: str) -> None:
    """校验 API Token"""
    if not expected_token:
        return
    provided = extract_token_from_request(request)
    if not provided or not secrets.compare_digest(provided, expected_token):
        raise HTTPException(status_code=401, detail="鉴权失败：API Token 无效")


_UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def _normalize_origin(value: str) -> str:
    """保留 scheme+host(+port)，丢弃 path/query；小写 host 以便比较。"""
    text = str(value or "").strip()
    if not text:
        return ""
    from urllib.parse import urlsplit

    parts = urlsplit(text)
    if not parts.scheme or not parts.netloc:
        # 可能只传了 host；退化为原始值做宽松比较
        return text.lower().rstrip("/")
    return f"{parts.scheme.lower()}://{parts.netloc.lower()}"


def _request_expected_origins(request: Request, allowed_origins: list[str]) -> set[str]:
    expected: set[str] = set()

    host_header = str(request.headers.get("host") or "").strip()
    if host_header:
        # 浏览器发来的 Origin 使用实际访问的 scheme；同时允许 http/https 两种
        expected.add(f"http://{host_header.lower()}")
        expected.add(f"https://{host_header.lower()}")

    for raw in allowed_origins or []:
        normalized = _normalize_origin(raw)
        if normalized:
            expected.add(normalized)
    return expected


def enforce_csrf_origin(
    request: Request,
    *,
    api_token_configured: bool,
    allowed_origins: list[str],
) -> None:
    """对浏览器发起的状态变更请求做 Origin/Referer 校验。

    放行条件（任一命中即通过）：
    1. 方法是安全方法（GET/HEAD/OPTIONS 等）
    2. 请求携带了有效 `X-API-Token`——跨源 JS 无法设置自定义 header 除非 CORS
       预检通过，因此这一条天然具备 CSRF 防护
    3. Origin 或 Referer 与当前 Host（或显式 allow list）匹配——同源场景

    任一都不满足则返回 403。
    """
    if request.method.upper() not in _UNSAFE_METHODS:
        return

    if api_token_configured:
        provided = extract_token_from_request(request)
        if provided:
            return

    origin_header = str(request.headers.get("origin") or "").strip()
    referer_header = str(request.headers.get("referer") or "").strip()
    if not origin_header and not referer_header:
        # 浏览器总会附带 Origin 或 Referer；纯 API 客户端（如 curl）通常两者都没有，
        # 但此时要求必须带 token（由上面的判断处理）；到这里代表没 token 也没来源头
        raise HTTPException(
            status_code=403,
            detail="CSRF 守卫：缺少 Origin/Referer 或有效 API Token",
        )

    expected = _request_expected_origins(request, allowed_origins)

    candidate = _normalize_origin(origin_header or referer_header)
    if candidate in expected:
        return

    logger.warning(
        "CSRF 守卫拒绝请求: method={} path={} origin={} referer={} expected={}",
        request.method,
        request.url.path,
        origin_header,
        referer_header,
        sorted(expected),
    )
    raise HTTPException(
        status_code=403,
        detail="CSRF 守卫：请求来源与当前站点不匹配",
    )


def collect_startup_diagnostics(config: AppConfig) -> dict[str, object]:
    """生成启动诊断信息，供 CLI / Web 首屏展示。"""
    issues: list[str] = []
    warnings: list[str] = []

    if not config.llm.api_key.strip():
        issues.append("未配置 LLM API Key，当前只能查看界面，无法提交提取或分析任务。")

    if not config.llm.model.strip():
        issues.append("未配置 llm.model，提取任务无法启动。")

    if not config.web.api_token.strip():
        warnings.append("当前未启用 Web API Token 鉴权，仅建议在本机调试环境使用。")

    if not config.web.startup_check_enabled:
        warnings.append(
            "启动自检已关闭；如需在启动时验证模型连通性，可开启 startup_check_enabled。"
        )

    if not config.security.auth_secret_key.strip():
        warnings.append("未配置 auth_secret_key，账号登录与会话签名能力不可用")

    if not config.security.config_secret_key.strip():
        warnings.append("未配置 config_secret_key，本地 API Key 将以明文形式写入 local.yaml")

    dispatch_mode = str(config.web.task_dispatch_mode or "").strip().lower() or "inline"
    if dispatch_mode == "redis":
        if not str(config.web.redis_url or "").strip():
            issues.append("task_dispatch_mode=redis 但未配置 web.redis_url。")
        try:
            import redis  # noqa: F401
        except ImportError:
            issues.append("task_dispatch_mode=redis 但当前环境未安装 redis 依赖。")

    database_url = str(
        config.storage.task_store_database_url or config.storage.database_url or ""
    ).strip()
    if not database_url:
        warnings.append("未配置 database_url，任务治理数据将回退到本地 SQLite 文件")
    else:
        try:
            database_target = parse_database_target(database_url)
        except ValueError:
            warnings.append(f"database_url 配置无法识别: {database_url}")
        else:
            if database_target.dialect == "sqlite":
                warnings.append("生产环境建议使用 PostgreSQL，而不是 SQLite 作为任务治理数据库")

    return {
        "ready": not issues,
        "issues": issues,
        "warnings": warnings,
        "api_token_required": bool(config.web.api_token.strip()),
        "startup_check_enabled": bool(config.web.startup_check_enabled),
        "startup_check_verify_model": bool(config.web.startup_check_verify_model),
    }


def _task_worker_runtime_payload(config: AppConfig, app: Any | None) -> dict[str, object]:
    service = getattr(getattr(app, "state", None), "task_worker_service", None) if app else None
    dispatch_mode = str(config.web.task_dispatch_mode or "").strip().lower() or "inline"
    enabled = bool(
        dispatch_mode in {"queue", "redis"} and config.web.start_builtin_worker
    )
    return {
        "enabled": enabled,
        "alive": bool(service.is_alive) if service is not None else False,
        "task_dispatch_mode": dispatch_mode,
        "worker_poll_interval_seconds": float(config.web.worker_poll_interval_seconds),
        "worker_stale_after_seconds": float(config.web.worker_stale_after_seconds),
        "worker_queue_scope": str(config.web.worker_queue_scope or "").strip() or "*",
        "redis_enabled": dispatch_mode == "redis",
        "redis_queue_name": str(config.web.redis_queue_name or "").strip(),
    }


def _monitor_scheduler_runtime_payload(
    config: AppConfig, app: Any | None
) -> dict[str, object]:
    service = (
        getattr(getattr(app, "state", None), "monitor_scheduler_service", None)
        if app
        else None
    )
    snapshot = service.runtime_snapshot() if service is not None else {}
    return {
        "enabled": bool(config.web.start_builtin_monitor_scheduler),
        "alive": bool(snapshot.get("is_alive")) if snapshot else False,
        "scheduler_id": str(snapshot.get("scheduler_id") or ""),
        "poll_interval_seconds": float(
            snapshot.get(
                "poll_interval_seconds", config.web.monitor_scheduler_poll_interval_seconds
            )
            or 0.0
        ),
        "batch_size": int(snapshot.get("batch_size") or config.web.monitor_scheduler_batch_size),
        "lease_seconds": float(
            snapshot.get("lease_seconds") or config.web.monitor_scheduler_lease_seconds
        ),
        "active": bool(snapshot.get("active")) if snapshot else False,
        "last_run_started_at": str(snapshot.get("last_run_started_at") or ""),
        "last_run_completed_at": str(snapshot.get("last_run_completed_at") or ""),
        "last_claimed_count": int(snapshot.get("last_claimed_count") or 0),
        "last_triggered_count": int(snapshot.get("last_triggered_count") or 0),
        "last_failed_count": int(snapshot.get("last_failed_count") or 0),
        "last_reclaimed_count": int(snapshot.get("last_reclaimed_count") or 0),
        "last_skipped_active_task_count": int(
            snapshot.get("last_skipped_active_task_count") or 0
        ),
        "last_error": str(snapshot.get("last_error") or ""),
        "total_runs": int(snapshot.get("total_runs") or 0),
        "total_claimed_count": int(snapshot.get("total_claimed_count") or 0),
        "total_triggered_count": int(snapshot.get("total_triggered_count") or 0),
        "total_failed_count": int(snapshot.get("total_failed_count") or 0),
        "total_reclaimed_count": int(snapshot.get("total_reclaimed_count") or 0),
    }


def _notification_retry_runtime_payload(
    config: AppConfig, app: Any | None
) -> dict[str, object]:
    service = (
        getattr(getattr(app, "state", None), "notification_retry_service", None)
        if app
        else None
    )
    snapshot = service.runtime_snapshot() if service is not None else {}
    return {
        "enabled": bool(config.web.start_builtin_notification_retry),
        "alive": bool(snapshot.get("is_alive")) if snapshot else False,
        "service_id": str(snapshot.get("service_id") or ""),
        "poll_interval_seconds": float(
            snapshot.get(
                "poll_interval_seconds",
                config.web.notification_retry_poll_interval_seconds,
            )
            or 0.0
        ),
        "batch_size": int(
            snapshot.get("batch_size") or config.web.notification_retry_batch_size
        ),
        "active": bool(snapshot.get("active")) if snapshot else False,
        "last_run_started_at": str(snapshot.get("last_run_started_at") or ""),
        "last_run_completed_at": str(snapshot.get("last_run_completed_at") or ""),
        "last_claimed_count": int(snapshot.get("last_claimed_count") or 0),
        "last_retried_count": int(snapshot.get("last_retried_count") or 0),
        "last_failed_count": int(snapshot.get("last_failed_count") or 0),
        "last_error": str(snapshot.get("last_error") or ""),
        "total_runs": int(snapshot.get("total_runs") or 0),
        "total_claimed_count": int(snapshot.get("total_claimed_count") or 0),
        "total_retried_count": int(snapshot.get("total_retried_count") or 0),
        "total_failed_count": int(snapshot.get("total_failed_count") or 0),
    }


def _notification_digest_runtime_payload(
    config: AppConfig, app: Any | None
) -> dict[str, object]:
    service = (
        getattr(getattr(app, "state", None), "notification_digest_service", None)
        if app
        else None
    )
    snapshot = service.runtime_snapshot() if service is not None else {}
    return {
        "enabled": bool(config.web.start_builtin_notification_digest),
        "alive": bool(snapshot.get("is_alive")) if snapshot else False,
        "service_id": str(snapshot.get("service_id") or ""),
        "poll_interval_seconds": float(
            snapshot.get(
                "poll_interval_seconds",
                config.web.notification_digest_poll_interval_seconds,
            )
            or 0.0
        ),
        "batch_size": int(
            snapshot.get("batch_size") or config.web.notification_digest_batch_size
        ),
        "active": bool(snapshot.get("active")) if snapshot else False,
        "last_run_started_at": str(snapshot.get("last_run_started_at") or ""),
        "last_run_completed_at": str(snapshot.get("last_run_completed_at") or ""),
        "last_claimed_count": int(snapshot.get("last_claimed_count") or 0),
        "last_sent_count": int(snapshot.get("last_sent_count") or 0),
        "last_retry_pending_count": int(
            snapshot.get("last_retry_pending_count") or 0
        ),
        "last_failed_count": int(snapshot.get("last_failed_count") or 0),
        "last_skipped_sent_today_count": int(
            snapshot.get("last_skipped_sent_today_count") or 0
        ),
        "last_error": str(snapshot.get("last_error") or ""),
        "total_runs": int(snapshot.get("total_runs") or 0),
        "total_claimed_count": int(snapshot.get("total_claimed_count") or 0),
        "total_sent_count": int(snapshot.get("total_sent_count") or 0),
        "total_retry_pending_count": int(
            snapshot.get("total_retry_pending_count") or 0
        ),
        "total_failed_count": int(snapshot.get("total_failed_count") or 0),
    }


def collect_runtime_status(
    config: AppConfig,
    *,
    app: Any | None = None,
) -> dict[str, object]:
    diagnostics = collect_startup_diagnostics(config)
    diagnostics["services"] = {
        "task_worker": _task_worker_runtime_payload(config, app),
        "monitor_scheduler": _monitor_scheduler_runtime_payload(config, app),
        "notification_retry": _notification_retry_runtime_payload(config, app),
        "notification_digest": _notification_digest_runtime_payload(config, app),
    }
    return diagnostics


def run_startup_self_check(config: AppConfig, strict: bool = True) -> dict[str, object]:
    """启动自检：关键配置 + 模型可用性。"""
    diagnostics = collect_startup_diagnostics(config)
    if not config.web.startup_check_enabled:
        logger.warning("启动自检已禁用（web.startup_check_enabled=false）")
        return diagnostics

    if not config.llm.api_key.strip():
        error = "启动自检失败：LLM API Key 为空。请设置 SMART_EXTRACTOR_API_KEY。"
        if strict:
            raise RuntimeError(error)
        diagnostics["issues"] = [*diagnostics["issues"], error]
        return diagnostics

    if not config.llm.model.strip():
        error = "启动自检失败：llm.model 为空。"
        if strict:
            raise RuntimeError(error)
        diagnostics["issues"] = [*diagnostics["issues"], error]
        return diagnostics

    if not config.web.api_token.strip():
        logger.warning("启动自检提示：当前未配置 Web API Token，将以无鉴权模式启动。")

    if not config.web.startup_check_verify_model:
        logger.info("启动自检通过（已跳过模型可用性校验）")
        return diagnostics

    timeout = max(1, int(config.web.startup_check_timeout))
    client = OpenAI(
        api_key=config.llm.api_key,
        base_url=config.llm.base_url,
        timeout=timeout,
    )

    model_name = config.llm.model
    retrieve_error: Exception | None = None
    try:
        client.models.retrieve(model_name)
        logger.info("启动自检通过：模型可用（models.retrieve） model={}", model_name)
        return diagnostics
    except Exception as exc:
        retrieve_error = exc
        logger.warning("models.retrieve 校验失败，尝试回退到最小对话校验：{}", exc)

    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": "ping"}],
            max_tokens=1,
            temperature=0,
        )
        if not _extract_chat_message_content(response):
            raise RuntimeError("模型返回空结果")
        logger.info("启动自检通过：模型可用（chat.completions） model={}", model_name)
    except Exception as chat_error:
        error = (
            "启动自检失败：模型不可用。"
            f" model={model_name}; models.retrieve_error={retrieve_error}; "
            f"chat_completion_error={chat_error}"
        )
        if strict:
            raise RuntimeError(error) from chat_error
        logger.warning(error)
        diagnostics["issues"] = [*diagnostics["issues"], error]
        diagnostics["ready"] = False
        return diagnostics

    return diagnostics
