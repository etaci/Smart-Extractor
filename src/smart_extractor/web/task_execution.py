"""Web 后台提取任务执行逻辑。"""

from __future__ import annotations

import time
from typing import Callable, Optional
from urllib.parse import urlparse, urlsplit, urlunsplit

from loguru import logger

from smart_extractor.web.management_helpers import notification_channels_from_profile
from smart_extractor.web.notifier import (
    DEFAULT_NOTIFICATION_MAX_ATTEMPTS,
    build_monitor_notification_payload,
    normalize_delivery_result,
)

_PROGRESS_STEPS: tuple[tuple[str, int, str], ...] = (
    ("before_fetch", 14, "正在发起网页请求"),
    ("after_fetch", 36, "网页抓取完成，正在清洗正文"),
    ("after_clean", 58, "正文清洗完成，正在分析字段"),
    ("after_extract", 78, "字段提取完成，正在校验结果"),
    ("after_validate", 90, "结果校验完成，正在保存数据"),
    ("after_store", 96, "数据已保存，正在收尾"),
)


def _tenant_id_of(record, fallback: str = "") -> str:
    return str(getattr(record, "tenant_id", fallback) or fallback)


def _call_with_optional_tenant(
    method: Callable[..., object],
    *args,
    tenant_id: str = "",
    **kwargs,
):
    try:
        return method(*args, tenant_id=tenant_id, **kwargs)
    except TypeError as exc:
        if "tenant_id" not in str(exc):
            raise
        return method(*args, **kwargs)


def _mask_proxy_url(proxy_url: str) -> str:
    normalized_url = str(proxy_url or "").strip()
    if not normalized_url:
        return ""
    parts = urlsplit(normalized_url)
    hostname = parts.hostname or ""
    if not hostname:
        return normalized_url
    credentials = ""
    if parts.username:
        credentials = f"{parts.username}:***@"
    netloc = f"{credentials}{hostname}"
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _build_progress_hook(
    task_store,
    task_id: str,
    percent: int,
    message: str,
    tenant_id: str,
) -> Callable[..., None]:
    def _hook(**_kwargs) -> None:
        _call_with_optional_tenant(
            task_store.update_progress,
            task_id,
            percent,
            message,
            tenant_id=tenant_id,
        )

    return _hook


def _update_monitor_result(
    *,
    task_store,
    monitor_id: str,
    task_id: str,
    tenant_id: str,
    sync_monitor_notification_fn: Callable[..., None],
) -> None:
    if not monitor_id:
        return

    latest_task = _call_with_optional_tenant(
        task_store.get,
        task_id,
        tenant_id=tenant_id,
    )
    if latest_task is None:
        return

    updated_monitor = _call_with_optional_tenant(
        task_store.update_monitor_result,
        monitor_id,
        latest_task,
        tenant_id=_tenant_id_of(latest_task, tenant_id),
    )
    if updated_monitor is not None:
        try:
            sync_monitor_notification_fn(
                monitor_id,
                latest_task.task_id,
                tenant_id=tenant_id,
            )
        except TypeError as exc:
            if "tenant_id" not in str(exc):
                raise
            sync_monitor_notification_fn(monitor_id, latest_task.task_id)


def _aggregate_notification_results(results: list[object]) -> tuple[str, str]:
    normalized_results = list(results or [])
    if not normalized_results:
        return "skipped", "未生成通知事件"

    if len(normalized_results) == 1:
        first = normalized_results[0]
        return str(first.status or "").strip(), str(first.message or "").strip()

    sent_count = sum(1 for item in normalized_results if item.status == "sent")
    failed_count = sum(1 for item in normalized_results if item.status == "failed")
    retry_pending_count = sum(
        1 for item in normalized_results if item.status == "retry_pending"
    )
    skipped_count = sum(1 for item in normalized_results if item.status == "skipped")
    if retry_pending_count > 0:
        status = "retry_pending"
    elif failed_count > 0:
        status = "failed"
    elif sent_count > 0:
        status = "sent"
    else:
        status = "skipped"
    message = (
        f"通知共 {len(normalized_results)} 个通道，成功 {sent_count}，"
        f"失败 {failed_count}，待重试 {retry_pending_count}，跳过 {skipped_count}"
    )
    return status, message


def sync_monitor_notification(
    *,
    monitor_id: str,
    task_id: str,
    tenant_id: str = "",
    task_store,
    should_notify_fn: Callable[..., bool],
    send_monitor_notification_fn: Callable[..., object],
) -> None:
    task = _call_with_optional_tenant(
        task_store.get,
        task_id,
        tenant_id=tenant_id,
    )
    monitor = _call_with_optional_tenant(
        task_store.get_monitor,
        monitor_id,
        tenant_id=tenant_id,
    )
    if monitor is None or task is None:
        return

    monitor_payload = monitor.to_dict()
    task_payload = task.to_dict()
    payload_snapshot = build_monitor_notification_payload(monitor_payload, task_payload)
    profile = (
        monitor_payload.get("profile")
        if isinstance(monitor_payload.get("profile"), dict)
        else {}
    )
    channels = notification_channels_from_profile(profile)

    if not channels:
        skip_message = "未配置通知通道，已跳过发送"
        task_store.create_notification_event(
            monitor_id=monitor_id,
            task_id=task_id,
            channel_type="webhook",
            target="",
            event_type="monitor_alert",
            status="skipped",
            status_message=skip_message,
            payload_snapshot=payload_snapshot,
            error_type="missing_target",
            tenant_id=tenant_id,
        )
        _call_with_optional_tenant(
            task_store.update_monitor_notification,
            monitor_id,
            status="skipped",
            message=skip_message,
            tenant_id=tenant_id,
        )
        return

    notify_policy_result = should_notify_fn(monitor_payload, monitor.last_alert_level)
    if isinstance(notify_policy_result, tuple):
        should_send = bool(notify_policy_result[0])
        skip_message = str(notify_policy_result[1] or "").strip()
    else:
        should_send = bool(notify_policy_result)
        skip_message = ""

    if not should_send:
        skip_message = skip_message or "当前告警级别未命中通知规则，已跳过发送"
        for channel in channels:
            task_store.create_notification_event(
                monitor_id=monitor_id,
                task_id=task_id,
                channel_type=str(channel.get("channel_type") or "webhook"),
                target=str(channel.get("target") or "").strip(),
                event_type="monitor_alert",
                status="skipped",
                status_message=skip_message,
                payload_snapshot=payload_snapshot,
                error_type="rule_filtered",
                tenant_id=tenant_id,
            )
        _call_with_optional_tenant(
            task_store.update_monitor_notification,
            monitor_id,
            status="skipped",
            message=skip_message,
            tenant_id=tenant_id,
        )
        return

    results = []
    for channel in channels:
        channel_type = str(channel.get("channel_type") or "webhook").strip().lower()
        target = str(channel.get("target") or "").strip()
        secret = str(channel.get("secret") or "").strip()
        try:
            raw_result = send_monitor_notification_fn(
                monitor_payload,
                task_payload,
                payload_override=payload_snapshot,
                target_override=target,
                secret_override=secret,
                channel_type_override=channel_type,
                attempt_no=1,
                max_attempts=DEFAULT_NOTIFICATION_MAX_ATTEMPTS,
            )
        except TypeError:
            raw_result = send_monitor_notification_fn(monitor_payload, task_payload)
        result = normalize_delivery_result(
            raw_result,
            payload_snapshot=payload_snapshot,
            target=target,
            channel_type=channel_type,
            attempt_no=1,
            max_attempts=DEFAULT_NOTIFICATION_MAX_ATTEMPTS,
        )
        results.append(result)
        task_store.create_notification_event(
            monitor_id=monitor_id,
            task_id=task_id,
            channel_type=result.channel_type,
            target=result.target,
            event_type="monitor_alert",
            status=result.status,
            status_message=result.message,
            attempt_no=result.attempt_no,
            max_attempts=result.max_attempts,
            next_retry_at=result.next_retry_at,
            response_code=result.response_code,
            error_type=result.error_type,
            error_message=result.error_message,
            payload_snapshot=result.payload_snapshot,
            sent_at=result.sent_at,
            tenant_id=tenant_id,
        )

    monitor_status, monitor_message = _aggregate_notification_results(results)
    _call_with_optional_tenant(
        task_store.update_monitor_notification,
        monitor_id,
        status=monitor_status,
        message=monitor_message,
        tenant_id=tenant_id,
    )


def run_extraction(
    *,
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
    task_store,
    load_config_fn: Callable[[], object],
    sync_monitor_notification_fn: Callable[..., None],
) -> None:
    from smart_extractor.pipeline import ExtractionPipeline

    task = _call_with_optional_tenant(task_store.get, task_id, tenant_id=tenant_id)
    if not task:
        with logger.contextualize(request_id="-", task_id=task_id):
            logger.error("Background task not found: {}", task_id)
        return
    task_tenant_id = _tenant_id_of(task, tenant_id)

    selected_field_list = list(selected_fields or [])
    domain = (
        str(site_domain or "").strip().lower()
        or urlparse(str(task.url or "").strip()).netloc.strip().lower()
    )
    site_policy = (
        _call_with_optional_tenant(
            task_store.get_site_policy_for_url,
            task.url,
            tenant_id=task_tenant_id,
        )
        if hasattr(task_store, "get_site_policy_for_url")
        else None
    )
    selected_proxy = None
    selected_proxies = []
    acquired_site_slot = False
    with logger.contextualize(request_id=task.request_id or "-", task_id=task.task_id):
        runtime_config = load_config_fn()
        if worker_id:
            task_store.heartbeat_worker_node(
                worker_id=worker_id,
                display_name=worker_id,
                status="busy",
                queue_scope=queue_scope,
                current_load=1,
                capabilities=["extract", "queue", dispatch_backend or "inline"],
                metadata={
                    "current_task_id": task.task_id,
                    "queue_scope": queue_scope,
                    "isolation_key": isolation_key,
                    "dispatch_backend": dispatch_backend or "inline",
                    "site_domain": domain,
                },
                tenant_id=task_tenant_id,
            )
        if site_policy is not None and domain:
            while True:
                site_slot = task_store.acquire_site_execution_slot(
                    domain=domain,
                    tenant_id=task_tenant_id,
                    min_interval_seconds=site_policy.min_interval_seconds,
                    max_concurrency=site_policy.max_concurrency,
                )
                if site_slot.get("acquired"):
                    acquired_site_slot = True
                    break
                time.sleep(min(max(float(site_slot.get("wait_seconds", 0.1) or 0.1), 0.1), 2.0))
            if site_policy.use_proxy_pool:
                selected_proxies = task_store.pick_proxy_endpoints(
                    preferred_tags=list(site_policy.preferred_proxy_tags or []),
                    limit=max(
                        int(getattr(runtime_config.fetcher, "fetch_max_attempts", 3) or 3),
                        1,
                    ),
                    tenant_id=task_tenant_id,
                )
                if selected_proxies:
                    selected_proxy = selected_proxies[0]
        _call_with_optional_tenant(
            task_store.mark_running,
            task.task_id,
            tenant_id=task_tenant_id,
        )
        start_at = time.perf_counter()
        logger.info(
            "Background extraction started: url={} mode={} format={} static={} selected_fields={} worker_id={} proxy_id={} proxy_pool_size={}",
            task.url,
            task.schema_name,
            task.storage_format,
            use_static,
            selected_field_list,
            worker_id or "-",
            selected_proxy.proxy_id if selected_proxy is not None else "-",
            len(selected_proxies),
        )

        try:
            fetcher_config = getattr(runtime_config, "fetcher", None)
            if fetcher_config is not None:
                if selected_proxies:
                    fetcher_config.proxy_url = selected_proxies[0].proxy_url
                    fetcher_config.proxy_urls = [
                        item.proxy_url for item in selected_proxies[1:] if item.proxy_url
                    ]
                else:
                    fetcher_config.proxy_url = ""
                    fetcher_config.proxy_urls = []
                if selected_proxy is not None:
                    fetcher_config.proxy_url = selected_proxy.proxy_url
                    logger.info(
                        "Apply proxy pool to fetcher runtime: proxy_id={} proxy_url={} extra_proxy_count={}",
                        selected_proxy.proxy_id,
                        _mask_proxy_url(selected_proxy.proxy_url),
                        len(fetcher_config.proxy_urls),
                    )
            with ExtractionPipeline(
                config=runtime_config,
                use_dynamic_fetcher=not use_static,
            ) as pipeline:
                for hook_name, percent, message in _PROGRESS_STEPS:
                    pipeline.add_hook(
                        hook_name,
                        _build_progress_hook(
                            task_store,
                            task.task_id,
                            percent,
                            message,
                            task_tenant_id,
                        ),
                    )
                result = pipeline.run(
                    url=task.url,
                    schema_name=schema_name or task.schema_name or "auto",
                    storage_format=task.storage_format,
                    collection_name="web_extract",
                    selected_fields=selected_field_list,
                    force_strategy=force_strategy,
                )

            elapsed_ms = result.elapsed_ms or (time.perf_counter() - start_at) * 1000
            if result.success:
                saved_data = result.data.model_dump() if result.data else {}
                extractor_stats = getattr(result, "extractor_stats", {}) or {}
                if extractor_stats:
                    saved_data["_extractor_stats"] = dict(extractor_stats)
                    saved_data["_llm_usage"] = {
                        "total_calls": int(extractor_stats.get("total_calls", 0) or 0),
                        "prompt_tokens": int(extractor_stats.get("prompt_tokens", 0) or 0),
                        "completion_tokens": int(extractor_stats.get("completion_tokens", 0) or 0),
                        "total_tokens": int(extractor_stats.get("total_tokens", 0) or 0),
                        "estimated_cost_usd": float(
                            extractor_stats.get("estimated_cost_usd", 0.0) or 0.0
                        ),
                        "api_usage_calls": int(
                            extractor_stats.get("api_usage_calls", 0) or 0
                        ),
                        "estimated_usage_calls": int(
                            extractor_stats.get("estimated_usage_calls", 0) or 0
                        ),
                        "api_usage_ratio": float(
                            extractor_stats.get("api_usage_ratio", 0.0) or 0.0
                        ),
                    }
                fetch_result = getattr(result, "fetch_result", None)
                fetch_elapsed_ms = (
                    float(fetch_result.elapsed_ms or 0.0)
                    if fetch_result is not None
                    else 0.0
                )
                retry_count = (
                    int(getattr(fetch_result, "retry_count", 0) or 0)
                    if fetch_result is not None
                    else 0
                )
                task_cost = float(
                    saved_data.get("_llm_usage", {}).get("estimated_cost_usd", 0.0)
                    if isinstance(saved_data.get("_llm_usage"), dict)
                    else 0.0
                )
                if fetch_result is not None:
                    saved_data["_runtime_metrics"] = {
                        "fetcher_type": "static" if use_static else "playwright",
                        "fetch_elapsed_ms": fetch_elapsed_ms,
                        "playwright_elapsed_ms": fetch_elapsed_ms if not use_static else 0.0,
                        "retry_count": retry_count,
                        "retry_cost_usd": round(task_cost * max(retry_count, 0), 6),
                        "total_elapsed_ms": float(elapsed_ms or 0.0),
                    }
                has_execution_context = any(
                    [
                        worker_id,
                        site_policy is not None,
                        queue_scope and queue_scope != "*",
                        isolation_key,
                        dispatch_backend,
                        selected_proxy is not None,
                        selected_proxies,
                    ]
                )
                if has_execution_context:
                    saved_data["_execution_context"] = {
                        "worker_id": worker_id,
                        "site_policy_id": site_policy.policy_id if site_policy is not None else "",
                        "site_domain": domain,
                        "queue_scope": queue_scope,
                        "isolation_key": isolation_key,
                        "dispatch_backend": dispatch_backend or "inline",
                        "proxy_id": selected_proxy.proxy_id if selected_proxy is not None else "",
                        "proxy_provider": selected_proxy.provider if selected_proxy is not None else "",
                        "proxy_pool_size": len(selected_proxies),
                        "assigned_worker_group": (
                            site_policy.assigned_worker_group if site_policy is not None else ""
                        ),
                    }
                _call_with_optional_tenant(
                    task_store.mark_success,
                    task.task_id,
                    elapsed_ms=elapsed_ms,
                    quality_score=result.validation.quality_score
                    if result.validation
                    else 0.0,
                    data=saved_data or None,
                    tenant_id=task_tenant_id,
                )
                _update_monitor_result(
                    task_store=task_store,
                    monitor_id=monitor_id,
                    task_id=task.task_id,
                    tenant_id=task_tenant_id,
                    sync_monitor_notification_fn=sync_monitor_notification_fn,
                )
                if selected_proxy is not None:
                    _call_with_optional_tenant(
                        task_store.mark_proxy_endpoint_result,
                        selected_proxy.proxy_id,
                        success=True,
                        tenant_id=task_tenant_id,
                    )
                logger.info(
                    "Background extraction succeeded: elapsed_ms={:.0f}", elapsed_ms
                )
                return

            _call_with_optional_tenant(
                task_store.mark_failed,
                task.task_id,
                elapsed_ms=elapsed_ms,
                error=result.error or "未知错误",
                tenant_id=task_tenant_id,
            )
            _update_monitor_result(
                task_store=task_store,
                monitor_id=monitor_id,
                task_id=task.task_id,
                tenant_id=task_tenant_id,
                sync_monitor_notification_fn=sync_monitor_notification_fn,
            )
            if selected_proxy is not None:
                _call_with_optional_tenant(
                    task_store.mark_proxy_endpoint_result,
                    selected_proxy.proxy_id,
                    success=False,
                    error=result.error or "task_failed",
                    tenant_id=task_tenant_id,
                )
            logger.error("Background extraction failed: {}", result.error)
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start_at) * 1000
            _call_with_optional_tenant(
                task_store.mark_failed,
                task.task_id,
                elapsed_ms=elapsed_ms,
                error=f"{type(exc).__name__}: {exc}",
                tenant_id=task_tenant_id,
            )
            _update_monitor_result(
                task_store=task_store,
                monitor_id=monitor_id,
                task_id=task.task_id,
                tenant_id=task_tenant_id,
                sync_monitor_notification_fn=sync_monitor_notification_fn,
            )
            if selected_proxy is not None:
                _call_with_optional_tenant(
                    task_store.mark_proxy_endpoint_result,
                    selected_proxy.proxy_id,
                    success=False,
                    error=f"{type(exc).__name__}: {exc}",
                    tenant_id=task_tenant_id,
                )
            logger.exception("Background extraction crashed")
        finally:
            if acquired_site_slot and domain:
                _call_with_optional_tenant(
                    task_store.release_site_execution_slot,
                    domain=domain,
                    tenant_id=task_tenant_id,
                )
            if worker_id:
                task_store.heartbeat_worker_node(
                    worker_id=worker_id,
                    display_name=worker_id,
                    status="idle",
                    queue_scope=queue_scope,
                    current_load=0,
                    capabilities=["extract", "queue", dispatch_backend or "inline"],
                    metadata={
                        "queue_scope": queue_scope,
                        "dispatch_backend": dispatch_backend or "inline",
                    },
                    tenant_id=task_tenant_id,
                )
