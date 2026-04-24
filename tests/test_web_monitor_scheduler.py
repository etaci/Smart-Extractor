from smart_extractor.web.monitor_scheduler import MonitorScheduler
from smart_extractor.web.task_store import SQLiteTaskStore


def test_monitor_scheduler_triggers_due_monitor_and_updates_schedule(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    monitor = store.create_or_update_monitor(
        name="自动巡检监控",
        url="https://example.com/auto",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"scenario_label": "自动巡检"},
        schedule_enabled=True,
        schedule_interval_minutes=15,
    )

    with store._connect() as conn:
        conn.execute(
            "UPDATE monitor_profiles SET schedule_next_run_at='2000-01-01 00:00:00' WHERE monitor_id=?",
            (monitor.monitor_id,),
        )
        conn.commit()

    captured = {}

    def fake_trigger_monitor_run(
        monitor_id: str,
        trigger_source: str,
        *,
        claimed_by: str = "",
    ):
        captured["monitor_id"] = monitor_id
        captured["trigger_source"] = trigger_source
        captured["claimed_by"] = claimed_by
        store.mark_monitor_run_scheduled(
            monitor_id,
            task_id="task-auto-000001",
            trigger_source=trigger_source,
            claimed_by=claimed_by,
        )
        return "task-auto-000001"

    scheduler = MonitorScheduler(
        task_store=store,
        trigger_monitor_run=fake_trigger_monitor_run,
        scheduler_id="scheduler-test",
        batch_size=5,
    )

    assert scheduler.run_once() == 1

    refreshed = store.get_monitor(monitor.monitor_id)
    assert refreshed is not None
    assert refreshed.last_trigger_source == "auto"
    assert refreshed.schedule_last_run_at
    assert refreshed.schedule_next_run_at
    assert refreshed.schedule_next_run_at != "2000-01-01 00:00:00"
    assert captured == {
        "monitor_id": monitor.monitor_id,
        "trigger_source": "auto",
        "claimed_by": "scheduler-test",
    }
    snapshot = scheduler.runtime_snapshot()
    assert snapshot["last_claimed_count"] == 1
    assert snapshot["last_triggered_count"] == 1
    assert snapshot["last_failed_count"] == 0
    assert snapshot["total_runs"] == 1


def test_monitor_scheduler_claim_prevents_duplicate_trigger(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    monitor = store.create_or_update_monitor(
        name="避免重复触发",
        url="https://example.com/claim",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"scenario_label": "自动巡检"},
        schedule_enabled=True,
        schedule_interval_minutes=15,
    )

    with store._connect() as conn:
        conn.execute(
            "UPDATE monitor_profiles SET schedule_next_run_at='2000-01-01 00:00:00' WHERE monitor_id=?",
            (monitor.monitor_id,),
        )
        conn.commit()

    captured_calls = []

    def fake_trigger_monitor_run(
        monitor_id: str,
        trigger_source: str,
        *,
        claimed_by: str = "",
    ):
        captured_calls.append((monitor_id, trigger_source, claimed_by))
        return "task-auto-000002"

    scheduler_a = MonitorScheduler(
        task_store=store,
        trigger_monitor_run=fake_trigger_monitor_run,
        scheduler_id="scheduler-a",
        batch_size=5,
        lease_seconds=120.0,
    )
    scheduler_b = MonitorScheduler(
        task_store=store,
        trigger_monitor_run=fake_trigger_monitor_run,
        scheduler_id="scheduler-b",
        batch_size=5,
        lease_seconds=120.0,
    )

    assert scheduler_a.run_once() == 1
    assert scheduler_b.run_once() == 0
    assert captured_calls == [(monitor.monitor_id, "auto", "scheduler-a")]

    refreshed = store.get_monitor(monitor.monitor_id)
    assert refreshed is not None
    assert refreshed.schedule_claimed_by == "scheduler-a"
    assert refreshed.schedule_lease_until
    assert refreshed.schedule_claim_count == 1
    snapshot = scheduler_a.runtime_snapshot()
    assert snapshot["last_claimed_count"] == 1
    assert snapshot["last_reclaimed_count"] == 0
    assert snapshot["last_skipped_active_task_count"] == 0


def test_monitor_scheduler_records_claim_error_and_waits_recovery(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    monitor = store.create_or_update_monitor(
        name="调度失败监控",
        url="https://example.com/failure",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"scenario_label": "自动巡检"},
        schedule_enabled=True,
        schedule_interval_minutes=15,
    )

    with store._connect() as conn:
        conn.execute(
            "UPDATE monitor_profiles SET schedule_next_run_at='2000-01-01 00:00:00' WHERE monitor_id=?",
            (monitor.monitor_id,),
        )
        conn.commit()

    def fake_trigger_monitor_run(
        monitor_id: str,
        trigger_source: str,
        *,
        claimed_by: str = "",
    ):
        raise RuntimeError("dispatch boom")

    scheduler = MonitorScheduler(
        task_store=store,
        trigger_monitor_run=fake_trigger_monitor_run,
        scheduler_id="scheduler-failure",
        batch_size=5,
        lease_seconds=120.0,
    )

    assert scheduler.run_once() == 0

    refreshed = store.get_monitor(monitor.monitor_id)
    assert refreshed is not None
    assert refreshed.schedule_claimed_by == "scheduler-failure"
    assert refreshed.schedule_lease_until
    assert refreshed.schedule_last_error == "dispatch boom"
    snapshot = scheduler.runtime_snapshot()
    assert snapshot["last_failed_count"] == 1
    assert snapshot["last_error"] == "dispatch boom"
