from types import SimpleNamespace

import smart_extractor.pipeline as pipeline_module

from smart_extractor.web.notifier import NotificationDeliveryResult
from smart_extractor.web.task_execution import (
    run_extraction,
    sync_monitor_notification,
)


class _DummyTaskStore:
    def __init__(self):
        self.task = SimpleNamespace(
            task_id="task-000001",
            url="https://example.com/article",
            schema_name="news",
            storage_format="json",
            request_id="req-000001",
            to_dict=lambda: {
                "task_id": "task-000001",
                "url": "https://example.com/article",
                "status": "success",
            },
        )
        self.progress_updates = []
        self.success_payload = None
        self.notification_updates = []
        self.monitor_result_updates = []
        self.notification_events = []

    def get(self, task_id: str):
        if task_id != self.task.task_id:
            return None
        return self.task

    def mark_running(self, task_id: str):
        assert task_id == self.task.task_id

    def update_progress(self, task_id: str, percent: int, message: str):
        self.progress_updates.append((task_id, percent, message))

    def mark_success(
        self,
        task_id: str,
        *,
        elapsed_ms: float,
        quality_score: float,
        data,
    ):
        self.success_payload = {
            "task_id": task_id,
            "elapsed_ms": elapsed_ms,
            "quality_score": quality_score,
            "data": data,
        }

    def mark_failed(self, task_id: str, *, elapsed_ms: float, error: str):
        raise AssertionError(f"不应走失败分支: {task_id} {elapsed_ms} {error}")

    def update_monitor_result(self, monitor_id: str, task):
        self.monitor_result_updates.append((monitor_id, task.task_id))
        return {"monitor_id": monitor_id}

    def get_monitor(self, monitor_id: str):
        return SimpleNamespace(
            monitor_id=monitor_id,
            last_alert_level="info",
            to_dict=lambda: {
                "monitor_id": monitor_id,
                "profile": {
                    "notify_on": ["changed"],
                    "webhook_url": "https://example.com/webhook",
                },
            },
        )

    def update_monitor_notification(self, monitor_id: str, *, status: str, message: str):
        self.notification_updates.append(
            {"monitor_id": monitor_id, "status": status, "message": message}
        )

    def create_notification_event(self, **payload):
        self.notification_events.append(payload)
        return payload


def test_run_extraction_executes_pipeline_and_updates_monitor(monkeypatch):
    task_store = _DummyTaskStore()
    sync_calls = []
    captured = {}

    class DummyResultData:
        def model_dump(self):
            return {"title": "示例标题"}

    class DummyPipeline:
        def __init__(self, config=None, use_dynamic_fetcher=True):
            captured["config"] = config
            captured["use_dynamic_fetcher"] = use_dynamic_fetcher
            self._hooks = {}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def add_hook(self, name, hook):
            self._hooks[name] = hook

        def run(self, **kwargs):
            captured["run_kwargs"] = kwargs
            for hook in self._hooks.values():
                hook()
            return SimpleNamespace(
                success=True,
                elapsed_ms=123.0,
                validation=SimpleNamespace(quality_score=0.97),
                data=DummyResultData(),
                error="",
            )

    monkeypatch.setattr(pipeline_module, "ExtractionPipeline", DummyPipeline)

    run_extraction(
        task_id="task-000001",
        schema_name="auto",
        use_static=True,
        selected_fields=["title"],
        monitor_id="mon-000001",
        force_strategy="llm",
        task_store=task_store,
        load_config_fn=lambda: {"llm": {"model": "gpt-test"}},
        sync_monitor_notification_fn=lambda monitor_id, task_id: sync_calls.append(
            (monitor_id, task_id)
        ),
    )

    assert captured["use_dynamic_fetcher"] is False
    assert captured["run_kwargs"]["selected_fields"] == ["title"]
    assert captured["run_kwargs"]["force_strategy"] == "llm"
    assert len(task_store.progress_updates) == 6
    assert task_store.success_payload["quality_score"] == 0.97
    assert task_store.success_payload["data"] == {"title": "示例标题"}
    assert task_store.monitor_result_updates == [("mon-000001", "task-000001")]
    assert sync_calls == [("mon-000001", "task-000001")]


def test_sync_monitor_notification_marks_skipped_when_rule_not_matched():
    task_store = _DummyTaskStore()

    sync_monitor_notification(
        monitor_id="mon-000001",
        task_id="task-000001",
        task_store=task_store,
        should_notify_fn=lambda monitor_payload, last_alert_level: False,
        send_monitor_notification_fn=lambda monitor_payload, task_payload: ("sent", "ok"),
    )

    assert task_store.notification_updates == [
        {
            "monitor_id": "mon-000001",
            "status": "skipped",
            "message": "当前告警级别未命中通知规则，已跳过发送",
        }
    ]
    assert task_store.notification_events[0]["status"] == "skipped"
    assert task_store.notification_events[0]["error_type"] == "rule_filtered"


def test_sync_monitor_notification_records_retry_pending_result():
    task_store = _DummyTaskStore()

    sync_monitor_notification(
        monitor_id="mon-000001",
        task_id="task-000001",
        task_store=task_store,
        should_notify_fn=lambda monitor_payload, last_alert_level: True,
        send_monitor_notification_fn=lambda monitor_payload, task_payload: NotificationDeliveryResult(
            status="retry_pending",
            message="通知目标限流，计划稍后重试",
            target="https://example.com/webhook",
            payload_snapshot={"event": "smart_extractor.monitor_alert"},
            response_code=429,
            error_type="http_error",
            error_message="429",
            should_retry=True,
            next_retry_at="2026-04-18 10:00:00",
            sent_at="2026-04-18 09:59:00",
        ),
    )

    assert task_store.notification_updates[0]["status"] == "retry_pending"
    assert task_store.notification_events[0]["status"] == "retry_pending"
    assert task_store.notification_events[0]["response_code"] == 429


def test_sync_monitor_notification_accepts_policy_reason():
    task_store = _DummyTaskStore()

    sync_monitor_notification(
        monitor_id="mon-000001",
        task_id="task-000001",
        task_store=task_store,
        should_notify_fn=lambda monitor_payload, last_alert_level: (
            False,
            "当前处于静默时段，实时通知已跳过",
        ),
        send_monitor_notification_fn=lambda monitor_payload, task_payload: ("sent", "ok"),
    )

    assert task_store.notification_updates[0]["message"] == "当前处于静默时段，实时通知已跳过"
