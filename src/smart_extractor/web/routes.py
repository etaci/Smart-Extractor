"""
Web pages and REST API routes.
"""

from __future__ import annotations

import threading
from inspect import Parameter, signature
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from loguru import logger

from smart_extractor.config import (
    DEFAULT_CONFIG_PATH,
    load_config,
    load_raw_yaml_config,
    resolve_local_config_path,
    update_llm_basic_config,
)
from smart_extractor import __version__
from smart_extractor.extractor.learned_profile_store import LearnedProfileStore
from smart_extractor.web.analysis_routes import create_analysis_router
from smart_extractor.web.api_models import (
    BatchExtractRequest,
    ExtractRequest,
    LoginRequest,
    RegisterRequest,
    TaskReviewRequest,
)
from smart_extractor.web.auth import AuthService, UserIdentity
from smart_extractor.web.governance_store import (
    GovernanceService,
    create_audit_log,
    fetch_audit_logs,
    upsert_task_review,
)
from smart_extractor.web.management_routes import create_management_router
from smart_extractor.web.monitor_scheduler import (
    ManagedMonitorSchedulerThread,
    MonitorScheduler,
)
from smart_extractor.web.notification_digest import (
    ManagedNotificationDigestThread,
    NotificationDigestService,
)
from smart_extractor.web.notification_center import (
    dispatch_digest_notifications,
    dispatch_notification_attempt,
)
from smart_extractor.web.notification_retry import (
    ManagedNotificationRetryThread,
    NotificationRetryService,
)
from smart_extractor.web.security import (
    ApiRateLimiter,
    collect_startup_diagnostics,
    collect_runtime_status,
    resolve_client_key_with_trusted_proxies,
)
from smart_extractor.web.task_store import SQLiteTaskStore
from smart_extractor.web.exporters import (
    build_task_docx,
    build_task_markdown,
    build_task_xlsx,
)
from smart_extractor.web.redis_queue import RedisTaskQueue
from smart_extractor.web.management_helpers import serialize_task_list_item
from smart_extractor.web.notifier import send_monitor_notification, should_notify
from smart_extractor.web.task_dispatcher import (
    ExtractionTaskSpec,
    build_task_dispatcher,
)
from smart_extractor.web.task_execution import (
    run_extraction as execute_extraction_task,
    sync_monitor_notification as execute_monitor_notification,
)
from smart_extractor.web.task_worker import (
    ManagedTaskWorkerThread,
    RedisTaskWorker,
    SQLiteTaskWorker,
)
from smart_extractor.web.template_market import (
    get_market_template,
    list_market_templates,
)
from smart_extractor.web.actor_market import (
    get_actor_package,
    list_actor_packages,
)

router = APIRouter()

_app_config = load_config()
_task_store = SQLiteTaskStore(
    Path(_app_config.storage.output_dir) / "web_tasks.db",
    database_url=(
        _app_config.storage.task_store_database_url
        or _app_config.storage.database_url
    ),
    default_tenant_id=_app_config.security.default_tenant_id,
    sqlite_busy_timeout_ms=_app_config.storage.sqlite_busy_timeout_ms,
    sqlite_enable_wal=_app_config.storage.sqlite_enable_wal,
    sqlite_synchronous=_app_config.storage.sqlite_synchronous,
)
_learned_profile_store = LearnedProfileStore(
    Path(_app_config.storage.output_dir) / "learned_profiles.json"
)
_rate_limiter = ApiRateLimiter(_app_config.web.rate_limit_per_minute)
_auth_service = AuthService(
    connect=_task_store._connect,
    lock=_task_store._lock,
    config=_app_config,
)
_auth_service.ensure_bootstrap_admin()
_governance_service = GovernanceService(
    task_store=_task_store,
    connect=_task_store._connect,
)
_redis_task_queue = RedisTaskQueue(
    redis_url=_app_config.web.redis_url,
    queue_name=_app_config.web.redis_queue_name,
    visibility_timeout_seconds=_app_config.web.redis_visibility_timeout_seconds,
)
_task_dispatcher = build_task_dispatcher(
    task_store=_task_store,
    dispatch_mode=_app_config.web.task_dispatch_mode,
    redis_queue=_redis_task_queue,
)


def _get_templates():
    from smart_extractor.web.app import templates

    return templates


def _get_request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "-")


def _request_logger(request: Request, task_id: str = "-"):
    return logger.bind(request_id=_get_request_id(request), task_id=task_id)


def _get_identity(request: Request) -> UserIdentity:
    identity = getattr(request.state, "identity", None)
    if isinstance(identity, UserIdentity):
        return identity
    return UserIdentity(
        user_id="token-admin",
        username="token-admin",
        role="admin",
        tenant_id=_app_config.security.default_tenant_id,
        display_name="Token Admin",
        auth_mode="token",
    )


def _audit(
    request: Request,
    *,
    action: str,
    resource_type: str,
    resource_id: str = "",
    payload: dict[str, object] | None = None,
) -> None:
    if not _app_config.security.audit_log_enabled:
        return
    identity = _get_identity(request)
    create_audit_log(
        lock=_task_store._lock,
        connect=_task_store._connect,
        tenant_id=identity.tenant_id,
        actor_user_id=identity.user_id,
        actor_role=identity.role,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        request_id=_get_request_id(request),
        http_method=request.method,
        path=request.url.path,
        remote_addr=(
            str(request.client.host).strip()
            if request.client and request.client.host
            else ""
        ),
        auth_mode=identity.auth_mode,
        payload=payload,
    )


def _api_guard(request: Request) -> UserIdentity:
    identity = _auth_service.authenticate_request(
        request,
        expected_api_token=_app_config.web.api_token,
    )
    _rate_limiter.check(
        resolve_client_key_with_trusted_proxies(
            request,
            trusted_proxy_ips=_app_config.web.trusted_proxy_ips,
        )
    )
    return identity


def _build_extraction_task_spec(
    *,
    url: str,
    schema_name: str,
    storage_format: str,
    request_id: str,
    tenant_id: str = "",
    use_static: bool = False,
    selected_fields: Optional[list[str]] = None,
    monitor_id: str = "",
    force_strategy: str = "",
    mode_label_override: str = "",
) -> ExtractionTaskSpec:
    normalized_tenant_id = (
        str(tenant_id or _app_config.security.default_tenant_id).strip()
        or _app_config.security.default_tenant_id
    )
    normalized_schema = str(schema_name or "auto").strip().lower() or "auto"
    mode_label = str(mode_label_override or "").strip()
    if not mode_label:
        mode_label = normalized_schema if normalized_schema != "auto" else "auto"
        if selected_fields and normalized_schema == "auto":
            mode_label = "auto + fields"
        if force_strategy:
            mode_label = f"{mode_label} + {force_strategy}"

    dispatch_context = _resolve_dispatch_context(
        url=url,
        tenant_id=normalized_tenant_id,
        monitor_id=monitor_id,
    )

    task = _task_store.create(
        str(url or "").strip(),
        mode_label,
        str(storage_format or "json").strip() or "json",
        request_id=request_id,
        tenant_id=normalized_tenant_id,
    )
    return ExtractionTaskSpec(
        task_id=task.task_id,
        tenant_id=normalized_tenant_id,
        schema_name=normalized_schema,
        use_static=use_static,
        selected_fields=list(selected_fields or []),
        monitor_id=str(monitor_id or "").strip(),
        force_strategy=str(force_strategy or "").strip(),
        queue_scope=dispatch_context["queue_scope"],
        isolation_key=dispatch_context["isolation_key"],
        site_domain=dispatch_context["site_domain"],
        dispatch_backend=dispatch_context["dispatch_backend"],
    )


def _resolve_dispatch_context(
    *,
    url: str,
    tenant_id: str,
    monitor_id: str = "",
) -> dict[str, str]:
    site_domain = (urlsplit(str(url or "").strip()).hostname or "").strip().lower()
    site_policy = (
        _task_store.get_site_policy_for_url(url, tenant_id=tenant_id)
        if site_domain
        else None
    )
    queue_scope = (
        str(site_policy.assigned_worker_group or "").strip()
        if site_policy is not None
        else ""
    ) or str(_app_config.web.worker_queue_scope or "").strip() or "*"
    dispatch_mode = str(_app_config.web.task_dispatch_mode or "").strip().lower() or "inline"
    dispatch_backend = {
        "queue": "sqlite",
        "redis": "redis",
    }.get(dispatch_mode, "inline")
    isolation_monitor_id = str(monitor_id or "").strip() or "-"
    return {
        "queue_scope": queue_scope,
        "isolation_key": (
            f"tenant:{tenant_id}:domain:{site_domain or '-'}:"
            f"monitor:{isolation_monitor_id}"
        ),
        "site_domain": site_domain,
        "dispatch_backend": dispatch_backend,
    }


def _dispatch_extraction_task(
    *,
    spec: ExtractionTaskSpec,
    background_tasks: BackgroundTasks | None = None,
) -> None:
    dispatch_mode = str(_app_config.web.task_dispatch_mode or "").strip().lower() or "inline"
    if dispatch_mode in {"queue", "redis"}:
        _task_dispatcher.enqueue(
            background_tasks=background_tasks or BackgroundTasks(),
            spec=spec,
            runner=_run_extraction,
        )
        return

    if background_tasks is not None:
        _task_dispatcher.enqueue(
            background_tasks=background_tasks,
            spec=spec,
            runner=_run_extraction,
        )
        return

    runner_signature = signature(_run_extraction)
    supports_var_kwargs = any(
        parameter.kind == Parameter.VAR_KEYWORD
        for parameter in runner_signature.parameters.values()
    )
    runner_kwargs = (
        spec.to_runner_kwargs()
        if supports_var_kwargs
        else {
            key: value
            for key, value in spec.to_runner_kwargs().items()
            if key in runner_signature.parameters
        }
    )

    threading.Thread(
        target=_run_extraction,
        args=tuple(spec.to_runner_args()),
        kwargs=runner_kwargs,
        name=f"smart-extractor-inline-{spec.task_id}",
        daemon=True,
    ).start()


def _create_background_extraction_task(
    *,
    url: str,
    schema_name: str,
    storage_format: str,
    request_id: str,
    tenant_id: str = "",
    background_tasks: BackgroundTasks,
    use_static: bool = False,
    selected_fields: Optional[list[str]] = None,
    monitor_id: str = "",
    force_strategy: str = "",
    mode_label_override: str = "",
    trigger_source: str = "manual",
) -> str:
    spec = _build_extraction_task_spec(
        url=url,
        schema_name=schema_name,
        storage_format=storage_format,
        request_id=request_id,
        tenant_id=tenant_id,
        use_static=use_static,
        selected_fields=selected_fields,
        monitor_id=monitor_id,
        force_strategy=force_strategy,
        mode_label_override=mode_label_override,
    )
    _dispatch_extraction_task(spec=spec, background_tasks=background_tasks)
    if monitor_id:
        _task_store.mark_monitor_run_scheduled(
            monitor_id,
            task_id=spec.task_id,
            trigger_source=trigger_source,
            tenant_id=spec.tenant_id,
        )
    return spec.task_id


def _trigger_monitor_run_with_state(
    monitor_id: str,
    trigger_source: str = "manual",
    *,
    claimed_by: str = "",
    request_id: str = "",
    tenant_id: str = "",
) -> dict[str, object] | None:
    normalized_tenant_id = str(tenant_id or _app_config.security.default_tenant_id).strip() or _app_config.security.default_tenant_id
    monitor = _task_store.get_monitor(monitor_id, tenant_id=normalized_tenant_id)
    if monitor is None:
        return None
    normalized_claimed_by = str(claimed_by or "").strip()
    if normalized_claimed_by and monitor.schedule_claimed_by != normalized_claimed_by:
        raise RuntimeError(f"monitor claim lost: {monitor_id}")
    last_task_id = str(monitor.last_task_id or "").strip()
    if last_task_id:
        last_task = _task_store.get(last_task_id, tenant_id=normalized_tenant_id)
        if last_task is not None and last_task.status in {"pending", "queued", "running"}:
            logger.info(
                "Skip duplicate monitor trigger: monitor_id={} task_id={} trigger_source={}",
                monitor_id,
                last_task_id,
                trigger_source,
            )
            return {
                "task_id": last_task_id,
                "reused_existing_task": True,
            }

    spec = _build_extraction_task_spec(
        url=monitor.url,
        schema_name=monitor.schema_name,
        storage_format=monitor.storage_format,
        request_id=str(request_id or "").strip() or f"monitor-{trigger_source}",
        tenant_id=normalized_tenant_id,
        use_static=monitor.use_static,
        selected_fields=list(monitor.selected_fields or []),
        monitor_id=monitor_id,
        mode_label_override="monitor",
    )
    _dispatch_extraction_task(spec=spec, background_tasks=None)
    _task_store.mark_monitor_run_scheduled(
        monitor_id,
        task_id=spec.task_id,
        trigger_source=trigger_source,
        claimed_by=normalized_claimed_by,
        tenant_id=normalized_tenant_id,
    )
    return {
        "task_id": spec.task_id,
        "reused_existing_task": False,
    }


def trigger_monitor_run(
    monitor_id: str,
    trigger_source: str = "manual",
    *,
    claimed_by: str = "",
    request_id: str = "",
    tenant_id: str = "",
) -> str | None:
    result = _trigger_monitor_run_with_state(
        monitor_id,
        trigger_source,
        claimed_by=claimed_by,
        request_id=request_id,
        tenant_id=tenant_id,
    )
    if result is None:
        return None
    return str(result.get("task_id") or "").strip() or None


def create_task_worker(*, worker_id: str = ""):
    dispatch_mode = str(_app_config.web.task_dispatch_mode or "").strip().lower() or "inline"
    common_kwargs = {
        "task_store": _task_store,
        "runner": _run_extraction,
        "worker_id": worker_id,
        "queue_scope": str(_app_config.web.worker_queue_scope or "").strip() or "*",
        "stale_after_seconds": _app_config.web.worker_stale_after_seconds,
    }
    if dispatch_mode == "redis":
        return RedisTaskWorker(
            **common_kwargs,
            redis_queue=_redis_task_queue,
            visibility_timeout_seconds=_app_config.web.redis_visibility_timeout_seconds,
        )
    return SQLiteTaskWorker(**common_kwargs)


def create_managed_task_worker(*, worker_id: str = "") -> ManagedTaskWorkerThread:
    return ManagedTaskWorkerThread(
        worker=create_task_worker(worker_id=worker_id),
        poll_interval_seconds=_app_config.web.worker_poll_interval_seconds,
    )


def create_monitor_scheduler(*, scheduler_id: str = "") -> MonitorScheduler:
    return MonitorScheduler(
        task_store=_task_store,
        trigger_monitor_run=trigger_monitor_run,
        scheduler_id=scheduler_id,
        batch_size=_app_config.web.monitor_scheduler_batch_size,
        lease_seconds=_app_config.web.monitor_scheduler_lease_seconds,
    )


def create_managed_monitor_scheduler(
    *,
    scheduler_id: str = "",
) -> ManagedMonitorSchedulerThread:
    return ManagedMonitorSchedulerThread(
        scheduler=create_monitor_scheduler(scheduler_id=scheduler_id),
        poll_interval_seconds=_app_config.web.monitor_scheduler_poll_interval_seconds,
    )


def create_notification_retry(*, service_id: str = "") -> NotificationRetryService:
    return NotificationRetryService(
        task_store=_task_store,
        send_monitor_notification_fn=send_monitor_notification,
        service_id=service_id,
        batch_size=_app_config.web.notification_retry_batch_size,
    )


def create_managed_notification_retry(
    *,
    service_id: str = "",
) -> ManagedNotificationRetryThread:
    return ManagedNotificationRetryThread(
        service=create_notification_retry(service_id=service_id),
        poll_interval_seconds=_app_config.web.notification_retry_poll_interval_seconds,
    )


def create_notification_digest(*, service_id: str = "") -> NotificationDigestService:
    return NotificationDigestService(
        task_store=_task_store,
        send_monitor_notification_fn=send_monitor_notification,
        service_id=service_id,
        batch_size=_app_config.web.notification_digest_batch_size,
    )


def create_managed_notification_digest(
    *,
    service_id: str = "",
) -> ManagedNotificationDigestThread:
    return ManagedNotificationDigestThread(
        service=create_notification_digest(service_id=service_id),
        poll_interval_seconds=_app_config.web.notification_digest_poll_interval_seconds,
    )


router.include_router(
    create_management_router(
        api_guard=_api_guard,
        request_logger=_request_logger,
        get_request_id=_get_request_id,
        task_store=_task_store,
        learned_profile_store=_learned_profile_store,
        default_config_path=DEFAULT_CONFIG_PATH,
        load_config=load_config,
        load_raw_yaml_config=load_raw_yaml_config,
        resolve_local_config_path=resolve_local_config_path,
        update_llm_basic_config=update_llm_basic_config,
        collect_startup_diagnostics=collect_startup_diagnostics,
        collect_runtime_status=collect_runtime_status,
        list_actor_packages=list_actor_packages,
        get_actor_package=get_actor_package,
        list_market_templates=list_market_templates,
        get_market_template=get_market_template,
        create_background_extraction_task=_create_background_extraction_task,
        trigger_monitor_run=_trigger_monitor_run_with_state,
        send_monitor_notification_fn=lambda *args, **kwargs: send_monitor_notification(
            *args, **kwargs
        ),
        dispatch_notification_attempt_fn=lambda *args, **kwargs: dispatch_notification_attempt(
            *args,
            **kwargs,
        ),
        dispatch_digest_notifications_fn=lambda *args, **kwargs: dispatch_digest_notifications(
            *args,
            **kwargs,
        ),
        build_task_docx=build_task_docx,
        build_task_xlsx=build_task_xlsx,
        build_task_markdown=build_task_markdown,
    )
)

router.include_router(
    create_analysis_router(
        api_guard=_api_guard,
        request_logger=_request_logger,
        get_request_id=_get_request_id,
        load_config=load_config,
    )
)


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    stats = _task_store.stats()
    tasks = _task_store.list_all(limit=15)
    insights = _task_store.build_dashboard_insights()
    runtime_status = getattr(
        request.app.state,
        "runtime_status",
        collect_runtime_status(_app_config, app=request.app),
    )
    return _get_templates().TemplateResponse(
        request,
        "dashboard.html",
        {
            "stats": stats,
            "tasks": [serialize_task_list_item(task) for task in tasks],
            "insights": insights,
            "app_version": __version__,
            "api_token_required": bool(_app_config.web.api_token),
            "runtime_status": runtime_status,
        },
    )


@router.get("/healthz")
async def healthz(request: Request):
    runtime_status = getattr(
        request.app.state,
        "runtime_status",
        collect_runtime_status(_app_config, app=request.app),
    )
    return {
        "status": "ok",
        "version": __version__,
        "services": runtime_status.get("services", {}),
    }


@router.get("/readyz")
async def readyz(request: Request):
    runtime_status = getattr(
        request.app.state,
        "runtime_status",
        collect_runtime_status(_app_config, app=request.app),
    )
    if runtime_status.get("ready"):
        return {
            "status": "ready",
            "version": __version__,
            "issues": runtime_status.get("issues", []),
            "warnings": runtime_status.get("warnings", []),
        }
    raise HTTPException(
        status_code=503,
        detail={
            "status": "not_ready",
            "version": __version__,
            "issues": runtime_status.get("issues", []),
            "warnings": runtime_status.get("warnings", []),
        },
    )


@router.post("/api/auth/login")
async def api_login(payload: LoginRequest, request: Request):
    result = _auth_service.login(
        username=payload.username.strip(),
        password=payload.password,
        tenant_id=payload.tenant_id.strip(),
    )
    request.state.identity = UserIdentity(
        user_id=result["user"]["user_id"],
        username=result["user"]["username"],
        role=result["user"]["role"],
        tenant_id=result["user"]["tenant_id"],
        display_name=result["user"]["display_name"],
        auth_mode="session",
    )
    _audit(
        request,
        action="auth.login",
        resource_type="session",
        resource_id=result["user"]["user_id"],
        payload={"username": result["user"]["username"]},
    )
    return result


@router.post("/api/auth/register")
async def api_register(payload: RegisterRequest, request: Request):
    result = _auth_service.register(
        username=payload.username.strip(),
        password=payload.password,
        tenant_id=payload.tenant_id.strip(),
        display_name=payload.display_name.strip(),
    )
    request.state.identity = UserIdentity(
        user_id=result["user"]["user_id"],
        username=result["user"]["username"],
        role=result["user"]["role"],
        tenant_id=result["user"]["tenant_id"],
        display_name=result["user"]["display_name"],
        auth_mode="session",
    )
    _audit(
        request,
        action="auth.register",
        resource_type="user",
        resource_id=result["user"]["user_id"],
        payload={"username": result["user"]["username"]},
    )
    return result


@router.get("/api/auth/me")
async def api_me(request: Request, identity: UserIdentity = Depends(_api_guard)):
    return {
        "user_id": identity.user_id,
        "username": identity.username,
        "role": identity.role,
        "tenant_id": identity.tenant_id,
        "display_name": identity.display_name,
        "permissions": sorted(identity.permissions),
        "auth_mode": identity.auth_mode,
    }


@router.get("/api/quality")
async def api_quality_dashboard(
    request: Request,
    recent_limit: int = 200,
    identity: UserIdentity = Depends(_api_guard),
):
    identity.require("dashboard:read")
    payload = _governance_service.build_quality_dashboard(
        tenant_id=identity.tenant_id,
        recent_limit=max(20, min(int(recent_limit or 200), 500)),
    )
    return payload


@router.get("/api/cost")
async def api_cost_dashboard(
    request: Request,
    recent_limit: int = 200,
    identity: UserIdentity = Depends(_api_guard),
):
    identity.require("dashboard:read")
    payload = _governance_service.build_cost_dashboard(
        tenant_id=identity.tenant_id,
        recent_limit=max(20, min(int(recent_limit or 200), 500)),
    )
    return payload


@router.get("/api/audit")
async def api_audit_logs(
    request: Request,
    limit: int = 50,
    identity: UserIdentity = Depends(_api_guard),
):
    identity.require("audit:read")
    return {
        "logs": fetch_audit_logs(
            connect=_task_store._connect,
            tenant_id=identity.tenant_id,
            limit=max(1, min(int(limit or 50), 200)),
        )
    }


@router.post("/api/task/{task_id}/review")
async def api_task_review(
    task_id: str,
    payload: TaskReviewRequest,
    request: Request,
    identity: UserIdentity = Depends(_api_guard),
):
    identity.require("task:review")
    task = _task_store.get(task_id, tenant_id=identity.tenant_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务不存在")
    review = upsert_task_review(
        lock=_task_store._lock,
        connect=_task_store._connect,
        tenant_id=identity.tenant_id,
        task_id=task_id,
        reviewer_user_id=identity.user_id,
        confirmed=payload.confirmed,
        accuracy_score=payload.accuracy_score,
        notes=payload.notes,
    )
    _audit(
        request,
        action="task.review",
        resource_type="task",
        resource_id=task_id,
        payload={
            "confirmed": payload.confirmed,
            "accuracy_score": payload.accuracy_score,
        },
    )
    return {"message": "人工复核结果已保存", "review": review}


@router.get("/task/{task_id}", response_class=HTMLResponse)
async def task_detail(request: Request, task_id: str):
    detail = _task_store.get_task_detail_payload(task_id)
    if not detail:
        raise HTTPException(status_code=404, detail="任务不存在")
    return _get_templates().TemplateResponse(
        request,
        "task_detail.html",
        {
            "task": detail,
            "app_version": __version__,
        },
    )


@router.post("/api/extract")
async def api_extract(
    req: ExtractRequest,
    background_tasks: BackgroundTasks,
    request: Request,
    identity: UserIdentity = Depends(_api_guard),
):
    identity.require("task:create")
    req_url = req.url.strip()
    if not req_url:
        raise HTTPException(status_code=400, detail="url 不能为空")
    task_id = _create_background_extraction_task(
        url=req_url,
        schema_name=req.schema_name,
        storage_format=req.storage_format,
        request_id=_get_request_id(request),
        tenant_id=identity.tenant_id,
        background_tasks=background_tasks,
        use_static=req.use_static,
        selected_fields=req.selected_fields,
    )
    _request_logger(request, task_id).info(
        "Create extraction task: url={} mode={} format={} static={} selected_fields={}",
        req_url,
        str(req.schema_name or "auto").strip().lower() or "auto",
        req.storage_format,
        req.use_static,
        req.selected_fields,
    )
    return {
        "task_id": task_id,
        "status": "pending",
        "message": f"任务已创建: {task_id}",
        "request_id": _get_request_id(request),
    }


@router.post("/api/batch")
async def api_batch(
    req: BatchExtractRequest,
    background_tasks: BackgroundTasks,
    request: Request,
    identity: UserIdentity = Depends(_api_guard),
):
    identity.require("task:create")
    task_ids: list[str] = []
    normalized_schema = str(req.schema_name or "auto").strip().lower() or "auto"
    batch_group_id = (
        str(req.batch_group_id or "").strip() or _task_store.new_batch_group_id()
    )
    parent_task = _task_store.create_batch_root(
        req.urls,
        normalized_schema,
        req.storage_format,
        request_id=_get_request_id(request),
        batch_group_id=batch_group_id,
        tenant_id=identity.tenant_id,
    )
    for url in req.urls:
        task = _task_store.create(
            url,
            normalized_schema,
            req.storage_format,
            request_id=_get_request_id(request),
            batch_group_id=batch_group_id,
            parent_task_id=parent_task.task_id,
            tenant_id=identity.tenant_id,
        )
        task_ids.append(task.task_id)
        dispatch_context = _resolve_dispatch_context(
            url=url,
            tenant_id=identity.tenant_id,
        )
        spec = ExtractionTaskSpec(
            task_id=task.task_id,
            tenant_id=identity.tenant_id,
            schema_name=normalized_schema,
            use_static=False,
            queue_scope=dispatch_context["queue_scope"],
            isolation_key=dispatch_context["isolation_key"],
            site_domain=dispatch_context["site_domain"],
            dispatch_backend=dispatch_context["dispatch_backend"],
        )
        _dispatch_extraction_task(
            spec=spec,
            background_tasks=background_tasks,
        )

    _request_logger(request).info(
        "Create batch extraction tasks: count={} format={} batch_group_id={}",
        len(task_ids),
        req.storage_format,
        batch_group_id,
    )
    return {
        "task_id": parent_task.task_id,
        "task_ids": task_ids,
        "count": len(task_ids),
        "batch_group_id": batch_group_id,
        "message": f"已创建 1 个批量任务，包含 {len(task_ids)} 个 URL",
        "request_id": _get_request_id(request),
    }


def _sync_monitor_notification(
    monitor_id: str,
    task_id: str,
    tenant_id: str = "",
) -> None:
    execute_monitor_notification(
        monitor_id=monitor_id,
        task_id=task_id,
        tenant_id=tenant_id,
        task_store=_task_store,
        should_notify_fn=should_notify,
        send_monitor_notification_fn=send_monitor_notification,
    )


def _run_extraction(
    task_id: str,
    tenant_id: str = "",
    schema_name: str = "auto",
    use_static: bool = False,
    selected_fields: Optional[list[str]] = None,
    monitor_id: str = "",
    force_strategy: str = "",
    worker_id: str = "",
    queue_scope: str = "*",
    isolation_key: str = "",
    site_domain: str = "",
    dispatch_backend: str = "",
):
    execute_extraction_task(
        task_id=task_id,
        tenant_id=tenant_id,
        schema_name=schema_name,
        use_static=use_static,
        selected_fields=selected_fields,
        monitor_id=monitor_id,
        force_strategy=force_strategy,
        worker_id=worker_id,
        queue_scope=queue_scope,
        isolation_key=isolation_key,
        site_domain=site_domain,
        dispatch_backend=dispatch_backend,
        task_store=_task_store,
        load_config_fn=load_config,
        sync_monitor_notification_fn=_sync_monitor_notification,
    )
