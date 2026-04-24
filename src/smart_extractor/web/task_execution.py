"""Web 后台提取任务执行逻辑。"""

from __future__ import annotations

import time
from typing import Callable, Optional

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


def _build_progress_hook(
    task_store,
    task_id: str,
    percent: int,
    message: str,
) -> Callable[..., None]:
    def _hook(**_kwargs) -> None:
        task_store.update_progress(task_id, percent, message)

    return _hook


def _update_monitor_result(
    *,
    task_store,
    monitor_id: str,
    task_id: str,
    sync_monitor_notification_fn: Callable[[str, str], None],
) -> None:
    if not monitor_id:
        return

    latest_task = task_store.get(task_id)
    if latest_task is None:
        return

    updated_monitor = task_store.update_monitor_result(monitor_id, latest_task)
    if updated_monitor is not None:
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
    task_store,
    should_notify_fn: Callable[..., bool],
    send_monitor_notification_fn: Callable[..., object],
) -> None:
    monitor = task_store.get_monitor(monitor_id)
    task = task_store.get(task_id)
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
        )
        task_store.update_monitor_notification(
            monitor_id,
            status="skipped",
            message=skip_message,
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
            )
        task_store.update_monitor_notification(
            monitor_id,
            status="skipped",
            message=skip_message,
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
        )

    monitor_status, monitor_message = _aggregate_notification_results(results)
    task_store.update_monitor_notification(
        monitor_id,
        status=monitor_status,
        message=monitor_message,
    )


def run_extraction(
    *,
    task_id: str,
    schema_name: str = "auto",
    use_static: bool = False,
    selected_fields: Optional[list[str]] = None,
    monitor_id: str = "",
    force_strategy: str = "",
    task_store,
    load_config_fn: Callable[[], object],
    sync_monitor_notification_fn: Callable[[str, str], None],
) -> None:
    from smart_extractor.pipeline import ExtractionPipeline

    task = task_store.get(task_id)
    if not task:
        with logger.contextualize(request_id="-", task_id=task_id):
            logger.error("Background task not found: {}", task_id)
        return

    selected_field_list = list(selected_fields or [])
    with logger.contextualize(request_id=task.request_id or "-", task_id=task.task_id):
        task_store.mark_running(task.task_id)
        start_at = time.perf_counter()
        logger.info(
            "Background extraction started: url={} mode={} format={} static={} selected_fields={}",
            task.url,
            task.schema_name,
            task.storage_format,
            use_static,
            selected_field_list,
        )

        try:
            with ExtractionPipeline(
                config=load_config_fn(),
                use_dynamic_fetcher=not use_static,
            ) as pipeline:
                for hook_name, percent, message in _PROGRESS_STEPS:
                    pipeline.add_hook(
                        hook_name,
                        _build_progress_hook(task_store, task.task_id, percent, message),
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
                task_store.mark_success(
                    task.task_id,
                    elapsed_ms=elapsed_ms,
                    quality_score=result.validation.quality_score
                    if result.validation
                    else 0.0,
                    data=saved_data or None,
                )
                _update_monitor_result(
                    task_store=task_store,
                    monitor_id=monitor_id,
                    task_id=task.task_id,
                    sync_monitor_notification_fn=sync_monitor_notification_fn,
                )
                logger.info(
                    "Background extraction succeeded: elapsed_ms={:.0f}", elapsed_ms
                )
                return

            task_store.mark_failed(
                task.task_id,
                elapsed_ms=elapsed_ms,
                error=result.error or "未知错误",
            )
            _update_monitor_result(
                task_store=task_store,
                monitor_id=monitor_id,
                task_id=task.task_id,
                sync_monitor_notification_fn=sync_monitor_notification_fn,
            )
            logger.error("Background extraction failed: {}", result.error)
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start_at) * 1000
            task_store.mark_failed(
                task.task_id,
                elapsed_ms=elapsed_ms,
                error=f"{type(exc).__name__}: {exc}",
            )
            _update_monitor_result(
                task_store=task_store,
                monitor_id=monitor_id,
                task_id=task.task_id,
                sync_monitor_notification_fn=sync_monitor_notification_fn,
            )
            logger.exception("Background extraction crashed")
