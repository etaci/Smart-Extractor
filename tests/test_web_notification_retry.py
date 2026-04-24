from smart_extractor.web.notification_retry import NotificationRetryService
from smart_extractor.web.notifier import NotificationDeliveryResult
from smart_extractor.web.task_store import SQLiteTaskStore


def test_notification_retry_service_retries_due_event(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)
    monitor = store.create_or_update_monitor(
        name="通知自动重试",
        url="https://example.com/page",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"webhook_url": "https://example.com/webhook"},
    )
    source_event = store.create_notification_event(
        monitor_id=monitor.monitor_id,
        task_id="task-000001",
        channel_type="webhook",
        target="https://example.com/webhook",
        event_type="monitor_alert",
        status="retry_pending",
        status_message="通知目标限流，等待自动重试",
        attempt_no=1,
        max_attempts=3,
        next_retry_at="2000-01-01 00:00:00",
        response_code=429,
        error_type="http_error",
        error_message="429",
        payload_snapshot={"event": "smart_extractor.monitor_alert"},
    )

    captured = {}

    def fake_send_notification(monitor_payload, task_payload, **kwargs):
        captured["kwargs"] = kwargs
        return NotificationDeliveryResult(
            status="sent",
            message="通知已发送到 https://example.com/webhook",
            target="https://example.com/webhook",
            payload_snapshot=kwargs["payload_override"],
            response_code=200,
            sent_at="2026-04-18 10:00:00",
            attempt_no=kwargs["attempt_no"],
            max_attempts=kwargs["max_attempts"],
        )

    service = NotificationRetryService(
        task_store=store,
        send_monitor_notification_fn=fake_send_notification,
        service_id="retry-test",
        batch_size=5,
    )

    assert service.run_once() == 1

    refreshed_source = store.get_notification_event(source_event.notification_id)
    assert refreshed_source is not None
    assert refreshed_source.status == "retried"
    assert refreshed_source.next_retry_at == ""

    events = store.list_notification_events(limit=10, monitor_id=monitor.monitor_id)
    retry_children = [
        item for item in events if item.retry_of_notification_id == source_event.notification_id
    ]
    assert len(retry_children) == 1
    assert retry_children[0].triggered_by == "retry"
    assert retry_children[0].attempt_no == 2
    assert captured["kwargs"]["attempt_no"] == 2

    snapshot = service.runtime_snapshot()
    assert snapshot["last_claimed_count"] == 1
    assert snapshot["last_retried_count"] == 1


def test_notification_retry_service_keeps_channel_scope(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)
    monitor = store.create_or_update_monitor(
        name="多通道重试",
        url="https://example.com/page",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={
            "notification_channels": [
                {
                    "channel_type": "webhook",
                    "name": "默认回调",
                    "target": "https://example.com/webhook",
                },
                {
                    "channel_type": "slack",
                    "name": "运营群",
                    "target": "https://example.com/slack-hook",
                    "secret": "slack-secret",
                },
            ]
        },
    )
    source_event = store.create_notification_event(
        monitor_id=monitor.monitor_id,
        task_id="task-000002",
        channel_type="slack",
        target="https://example.com/slack-hook",
        event_type="monitor_alert",
        status="retry_pending",
        status_message="通知目标限流，等待自动重试",
        attempt_no=1,
        max_attempts=3,
        next_retry_at="2000-01-01 00:00:00",
        response_code=429,
        error_type="http_error",
        error_message="429",
        payload_snapshot={"event": "smart_extractor.monitor_alert"},
    )

    captured = {}

    def fake_send_notification(monitor_payload, task_payload, **kwargs):
        captured["kwargs"] = kwargs
        return NotificationDeliveryResult(
            status="sent",
            message="通知已发送到 https://example.com/slack-hook",
            channel_type="slack",
            target="https://example.com/slack-hook",
            payload_snapshot=kwargs["payload_override"],
            response_code=200,
            sent_at="2026-04-18 10:00:00",
            attempt_no=kwargs["attempt_no"],
            max_attempts=kwargs["max_attempts"],
        )

    service = NotificationRetryService(
        task_store=store,
        send_monitor_notification_fn=fake_send_notification,
        service_id="retry-scope-test",
        batch_size=5,
    )

    assert service.run_once() == 1
    assert captured["kwargs"]["channel_type_override"] == "slack"
    assert captured["kwargs"]["target_override"] == "https://example.com/slack-hook"
    assert captured["kwargs"]["secret_override"] == "slack-secret"
    retry_events = [
        item
        for item in store.list_notification_events(limit=10, monitor_id=monitor.monitor_id)
        if item.retry_of_notification_id == source_event.notification_id
    ]
    assert len(retry_events) == 1
    assert retry_events[0].channel_type == "slack"
