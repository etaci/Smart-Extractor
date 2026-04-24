"""
Web pages and REST API routes.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Optional

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
    enforce_api_token,
    resolve_client_key_with_trusted_proxies,
)
from smart_extractor.web.task_store import SQLiteTaskStore
from smart_extractor.web.exporters import (
    build_task_docx,
    build_task_markdown,
    build_task_xlsx,
)
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
from smart_extractor.web.task_worker import ManagedTaskWorkerThread, SQLiteTaskWorker
from smart_extractor.web.template_market import (
    get_market_template,
    list_market_templates,
)

router = APIRouter()

_app_config = load_config()
_task_store = SQLiteTaskStore(
    Path(_app_config.storage.output_dir) / "web_tasks.db",
    sqlite_busy_timeout_ms=_app_config.storage.sqlite_busy_timeout_ms,
    sqlite_enable_wal=_app_config.storage.sqlite_enable_wal,
    sqlite_synchronous=_app_config.storage.sqlite_synchronous,
)
_learned_profile_store = LearnedProfileStore(
    Path(_app_config.storage.output_dir) / "learned_profiles.json"
)
_rate_limiter = ApiRateLimiter(_app_config.web.rate_limit_per_minute)
_task_dispatcher = build_task_dispatcher(
    task_store=_task_store,
    dispatch_mode=_app_config.web.task_dispatch_mode,
)


def _get_templates():
    from smart_extractor.web.app import templates

    return templates


def _get_request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "-")


def _request_logger(request: Request, task_id: str = "-"):
    return logger.bind(request_id=_get_request_id(request), task_id=task_id)


def _api_guard(request: Request) -> None:
    enforce_api_token(request, _app_config.web.api_token)
    _rate_limiter.check(
        resolve_client_key_with_trusted_proxies(
            request,
            trusted_proxy_ips=_app_config.web.trusted_proxy_ips,
        )
    )


def _build_extraction_task_spec(
    *,
    url: str,
    schema_name: str,
    storage_format: str,
    request_id: str,
    use_static: bool = False,
    selected_fields: Optional[list[str]] = None,
    monitor_id: str = "",
    force_strategy: str = "",
    mode_label_override: str = "",
) -> ExtractionTaskSpec:
    normalized_schema = str(schema_name or "auto").strip().lower() or "auto"
    mode_label = str(mode_label_override or "").strip()
    if not mode_label:
        mode_label = normalized_schema if normalized_schema != "auto" else "auto"
        if selected_fields and normalized_schema == "auto":
            mode_label = "auto + fields"
        if force_strategy:
            mode_label = f"{mode_label} + {force_strategy}"

    task = _task_store.create(
        str(url or "").strip(),
        mode_label,
        str(storage_format or "json").strip() or "json",
        request_id=request_id,
    )
    return ExtractionTaskSpec(
        task_id=task.task_id,
        schema_name=normalized_schema,
        use_static=use_static,
        selected_fields=list(selected_fields or []),
        monitor_id=str(monitor_id or "").strip(),
        force_strategy=str(force_strategy or "").strip(),
    )


def _dispatch_extraction_task(
    *,
    spec: ExtractionTaskSpec,
    background_tasks: BackgroundTasks | None = None,
) -> None:
    if str(_app_config.web.task_dispatch_mode or "").strip().lower() == "queue":
        _task_store.enqueue_task_spec(spec)
        return

    if background_tasks is not None:
        _task_dispatcher.enqueue(
            background_tasks=background_tasks,
            spec=spec,
            runner=_run_extraction,
        )
        return

    threading.Thread(
        target=_run_extraction,
        args=tuple(spec.to_runner_args()),
        kwargs=spec.to_runner_kwargs(),
        name=f"smart-extractor-inline-{spec.task_id}",
        daemon=True,
    ).start()


def _create_background_extraction_task(
    *,
    url: str,
    schema_name: str,
    storage_format: str,
    request_id: str,
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
        )
    return spec.task_id


def _trigger_monitor_run_with_state(
    monitor_id: str,
    trigger_source: str = "manual",
    *,
    claimed_by: str = "",
    request_id: str = "",
) -> dict[str, object] | None:
    monitor = _task_store.get_monitor(monitor_id)
    if monitor is None:
        return None
    normalized_claimed_by = str(claimed_by or "").strip()
    if normalized_claimed_by and monitor.schedule_claimed_by != normalized_claimed_by:
        raise RuntimeError(f"monitor claim lost: {monitor_id}")
    last_task_id = str(monitor.last_task_id or "").strip()
    if last_task_id:
        last_task = _task_store.get(last_task_id)
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
) -> str | None:
    result = _trigger_monitor_run_with_state(
        monitor_id,
        trigger_source,
        claimed_by=claimed_by,
        request_id=request_id,
    )
    if result is None:
        return None
    return str(result.get("task_id") or "").strip() or None


def create_task_worker(*, worker_id: str = "") -> SQLiteTaskWorker:
    return SQLiteTaskWorker(
        task_store=_task_store,
        runner=_run_extraction,
        worker_id=worker_id,
        stale_after_seconds=_app_config.web.worker_stale_after_seconds,
    )


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
    _: None = Depends(_api_guard),
):
    req_url = req.url.strip()
    if not req_url:
        raise HTTPException(status_code=400, detail="url 不能为空")
    task_id = _create_background_extraction_task(
        url=req_url,
        schema_name=req.schema_name,
        storage_format=req.storage_format,
        request_id=_get_request_id(request),
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
    _: None = Depends(_api_guard),
):
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
    )
    for url in req.urls:
        task = _task_store.create(
            url,
            normalized_schema,
            req.storage_format,
            request_id=_get_request_id(request),
            batch_group_id=batch_group_id,
            parent_task_id=parent_task.task_id,
        )
        task_ids.append(task.task_id)
        _task_dispatcher.enqueue(
            background_tasks=background_tasks,
            spec=ExtractionTaskSpec(
                task_id=task.task_id,
                schema_name=normalized_schema,
                use_static=False,
            ),
            runner=_run_extraction,
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


def _sync_monitor_notification(monitor_id: str, task_id: str) -> None:
    execute_monitor_notification(
        monitor_id=monitor_id,
        task_id=task_id,
        task_store=_task_store,
        should_notify_fn=should_notify,
        send_monitor_notification_fn=send_monitor_notification,
    )


def _run_extraction(
    task_id: str,
    schema_name: str = "auto",
    use_static: bool = False,
    selected_fields: Optional[list[str]] = None,
    monitor_id: str = "",
    force_strategy: str = "",
):
    execute_extraction_task(
        task_id=task_id,
        schema_name=schema_name,
        use_static=use_static,
        selected_fields=selected_fields,
        monitor_id=monitor_id,
        force_strategy=force_strategy,
        task_store=_task_store,
        load_config_fn=load_config,
        sync_monitor_notification_fn=_sync_monitor_notification,
    )
