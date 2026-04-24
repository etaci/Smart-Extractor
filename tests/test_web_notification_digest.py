from __future__ import annotations

from datetime import datetime

from smart_extractor.web.notification_center import dispatch_digest_notifications
from smart_extractor.web.notification_digest import NotificationDigestService
from smart_extractor.web.notifier import NotificationDeliveryResult
from smart_extractor.web.task_store import SQLiteTaskStore


def test_dispatch_digest_notifications_filters_target_scope(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)
    monitor_a = store.create_or_update_monitor(
        name="A 监控",
        url="https://example.com/a",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"webhook_url": "https://example.com/webhook-a"},
    )
    monitor_b = store.create_or_update_monitor(
        name="B 监控",
        url="https://example.com/b",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"webhook_url": "https://example.com/webhook-b"},
    )
    store.create_notification_event(
        monitor_id=monitor_a.monitor_id,
        task_id="task-a",
        channel_type="webhook",
        target="https://example.com/webhook-a",
        event_type="monitor_alert",
        status="sent",
        status_message="A changed",
        payload_snapshot={"event": "smart_extractor.monitor_alert"},
    )
    store.create_notification_event(
        monitor_id=monitor_b.monitor_id,
        task_id="task-b",
        channel_type="webhook",
        target="https://example.com/webhook-b",
        event_type="monitor_alert",
        status="failed",
        status_message="B failed",
        payload_snapshot={"event": "smart_extractor.monitor_alert"},
    )

    captured: list[dict[str, object]] = []

    def fake_send_notification(monitor_payload, task_payload, **kwargs):
        captured.append(
            {
                "target": kwargs["target_override"],
                "payload": kwargs["payload_override"],
            }
        )
        return NotificationDeliveryResult(
            status="sent",
            message=f"通知已发送到 {kwargs['target_override']}",
            target=kwargs["target_override"],
            payload_snapshot=kwargs["payload_override"],
            response_code=200,
            sent_at="2026-04-18 10:00:00",
            attempt_no=1,
            max_attempts=3,
        )

    events = dispatch_digest_notifications(
        task_store=store,
        send_monitor_notification_fn=fake_send_notification,
        window_hours=24,
        target_configs=[
            {
                "target": "https://example.com/webhook-a",
                "secret": "",
                "monitor_ids": [monitor_a.monitor_id],
            }
        ],
    )

    assert len(events) == 1
    assert len(captured) == 1
    assert captured[0]["target"] == "https://example.com/webhook-a"
    payload = captured[0]["payload"]
    assert payload["event"] == "smart_extractor.daily_digest"
    assert payload["summary"]["unique_monitors"] == 1
    assert payload["summary"]["failed_count"] == 0
    assert payload["top_monitors"][0]["monitor_id"] == monitor_a.monitor_id


def test_notification_digest_service_sends_once_per_target_per_day(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)
    digest_hour = datetime.now().hour
    monitor = store.create_or_update_monitor(
        name="日报监控",
        url="https://example.com/digest",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={
            "webhook_url": "https://example.com/webhook",
            "digest_enabled": True,
            "digest_hour": digest_hour,
        },
    )
    store.create_notification_event(
        monitor_id=monitor.monitor_id,
        task_id="task-000001",
        channel_type="webhook",
        target="https://example.com/webhook",
        event_type="monitor_alert",
        status="retry_pending",
        status_message="等待重试",
        payload_snapshot={"event": "smart_extractor.monitor_alert"},
    )

    captured: list[dict[str, object]] = []

    def fake_send_notification(monitor_payload, task_payload, **kwargs):
        captured.append(
            {
                "target": kwargs["target_override"],
                "payload": kwargs["payload_override"],
            }
        )
        return NotificationDeliveryResult(
            status="sent",
            message=f"通知已发送到 {kwargs['target_override']}",
            target=kwargs["target_override"],
            payload_snapshot=kwargs["payload_override"],
            response_code=200,
            sent_at="2026-04-18 10:00:00",
            attempt_no=1,
            max_attempts=3,
        )

    service = NotificationDigestService(
        task_store=store,
        send_monitor_notification_fn=fake_send_notification,
        service_id="digest-test",
        batch_size=5,
    )

    assert service.run_once() == 1
    assert service.run_once() == 0

    events = store.list_notification_events(limit=10, event_type="daily_digest")
    assert len(events) == 1
    assert events[0].target == "https://example.com/webhook"
    assert events[0].status == "sent"
    assert captured[0]["payload"]["summary"]["retry_pending_count"] == 1

    snapshot = service.runtime_snapshot()
    assert snapshot["total_runs"] == 2
    assert snapshot["total_sent_count"] == 1
    assert snapshot["last_skipped_sent_today_count"] == 1


def test_dispatch_digest_notifications_supports_multiple_channels(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)
    store.create_or_update_monitor(
        name="Digest 多通道监控",
        url="https://example.com/digest-multi",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={
            "digest_enabled": True,
            "digest_hour": 9,
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
                },
            ],
        },
    )

    captured = []

    def fake_send_notification(monitor_payload, task_payload, **kwargs):
        captured.append(
            (kwargs["channel_type_override"], kwargs["target_override"])
        )
        return NotificationDeliveryResult(
            status="sent",
            message=f"通知已发送到 {kwargs['target_override']}",
            channel_type=kwargs["channel_type_override"],
            target=kwargs["target_override"],
            payload_snapshot=kwargs["payload_override"],
            response_code=200,
            sent_at="2026-04-18 10:00:00",
            attempt_no=1,
            max_attempts=3,
        )

    events = dispatch_digest_notifications(
        task_store=store,
        send_monitor_notification_fn=fake_send_notification,
        window_hours=24,
    )

    assert len(events) == 2
    assert sorted(captured) == [
        ("slack", "https://example.com/slack-hook"),
        ("webhook", "https://example.com/webhook"),
    ]
