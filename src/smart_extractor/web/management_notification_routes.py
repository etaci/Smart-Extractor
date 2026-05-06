"""通知中心管理路由。"""

from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Request

from smart_extractor.web.api_models import NotificationResendRequest
from smart_extractor.web.management_helpers import serialize_notification_event


def register_notification_routes(
    router: APIRouter,
    *,
    api_guard: Callable[[Request], Any],
    request_logger: Callable[[Request, str], Any],
    task_store: Any,
    send_monitor_notification_fn: Callable[..., object],
    dispatch_notification_attempt_fn: Callable[..., Any],
    dispatch_digest_notifications_fn: Callable[..., Any],
) -> None:
    def _tenant_id(request: Request) -> str:
        identity = getattr(request.state, "identity", None)
        require = getattr(identity, "require", None)
        if callable(require):
            require("notification:manage")
        return str(getattr(identity, "tenant_id", "default") or "default")

    @router.get("/api/notifications")
    async def api_notifications(
        request: Request,
        limit: int = 50,
        monitor_id: str = "",
        status: str = "",
        task_id: str = "",
        event_type: str = "",
        _: Any = Depends(api_guard),
    ):
        tenant_id = _tenant_id(request)
        events = task_store.list_notification_events(
            limit=limit,
            monitor_id=str(monitor_id or "").strip(),
            status=str(status or "").strip().lower(),
            task_id=str(task_id or "").strip(),
            event_type=str(event_type or "").strip().lower(),
            tenant_id=tenant_id,
        )
        serialized = [serialize_notification_event(item) for item in events]
        request_logger(request, "-").info(
            "List notifications: limit={} total={} monitor_id={} status={} task_id={} event_type={}",
            limit,
            len(serialized),
            monitor_id or "-",
            status or "-",
            task_id or "-",
            event_type or "-",
        )
        return {
            "notifications": serialized,
        }

    @router.post("/api/notifications/digest/send")
    async def api_send_notification_digest(
        payload: NotificationResendRequest,
        request: Request,
        window_hours: int = 24,
        _: Any = Depends(api_guard),
    ):
        tenant_id = _tenant_id(request)
        events = dispatch_digest_notifications_fn(
            task_store=task_store,
            send_monitor_notification_fn=send_monitor_notification_fn,
            window_hours=window_hours,
            tenant_id=tenant_id,
        )
        request_logger(request, "-").info(
            "Send notification digest: window_hours={} count={} reason={}",
            window_hours,
            len(events),
            payload.reason.strip() or "-",
        )
        return {
            "message": "通知日报已发送",
            "window_hours": window_hours,
            "count": len(events),
            "notifications": [serialize_notification_event(item) for item in events],
        }

    @router.post("/api/notifications/{notification_id}/resend")
    async def api_resend_notification(
        notification_id: str,
        payload: NotificationResendRequest,
        request: Request,
        _: Any = Depends(api_guard),
    ):
        tenant_id = _tenant_id(request)
        source_event = task_store.get_notification_event(
            notification_id,
            tenant_id=tenant_id,
        )
        if source_event is None:
            raise HTTPException(status_code=404, detail="通知记录不存在")
        if not str(source_event.target or "").strip() and not source_event.payload_snapshot:
            raise HTTPException(status_code=400, detail="当前通知缺少可补发信息")

        resent_event = dispatch_notification_attempt_fn(
            source_event=source_event,
            task_store=task_store,
            send_monitor_notification_fn=send_monitor_notification_fn,
            triggered_by="manual",
            reason=payload.reason.strip(),
        )
        request_logger(request, resent_event.notification_id).info(
            "Resend notification: source={} status={} reason={}",
            notification_id,
            resent_event.status,
            payload.reason.strip() or "-",
        )
        return {
            "message": "通知补发完成",
            "notification": serialize_notification_event(resent_event),
            "source_notification_id": notification_id,
        }
