"""
Web 任务持久化存储测试。
"""

from smart_extractor.web.task_store import SQLiteTaskStore


def test_sqlite_task_store_create_and_persist(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    task = store.create(
        url="https://example.com/a1",
        schema_name="news",
        storage_format="json",
        request_id="req-1",
    )
    assert task.task_id.startswith("task-")
    assert task.status == "pending"
    assert task.request_id == "req-1"
    assert task.batch_group_id == ""
    assert task.progress_percent == 0.0

    store.mark_running(task.task_id)
    running_task = store.get(task.task_id)
    assert running_task is not None
    assert running_task.status == "running"
    assert running_task.progress_percent >= 6.0

    store.mark_success(
        task.task_id,
        elapsed_ms=1234.5,
        quality_score=0.91,
        data={"title": "T"},
    )
    done = store.get(task.task_id)
    assert done is not None
    assert done.status == "success"
    assert done.data == {"title": "T"}
    assert done.quality_score == 0.91
    assert done.progress_percent == 100.0

    # 重建 store，验证数据持久化
    reloaded_store = SQLiteTaskStore(db_path)
    reloaded = reloaded_store.get(task.task_id)
    assert reloaded is not None
    assert reloaded.status == "success"
    assert reloaded.data == {"title": "T"}

    stats = reloaded_store.stats()
    assert stats["total"] == 1
    assert stats["success"] == 1
    assert stats["failed"] == 0


def test_sqlite_task_store_mark_failed(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)
    task = store.create(
        url="https://example.com/fail",
        schema_name="news",
        storage_format="json",
    )
    store.mark_failed(task.task_id, elapsed_ms=500.0, error="timeout")
    failed = store.get(task.task_id)
    assert failed is not None
    assert failed.status == "failed"
    assert failed.error == "timeout"
    assert failed.progress_percent == 100.0


def test_sqlite_task_store_builds_history_and_change_insights(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    first = store.create(
        url="https://example.com/product/1",
        schema_name="auto",
        storage_format="json",
    )
    store.mark_success(
        first.task_id,
        elapsed_ms=110.0,
        quality_score=0.90,
        data={
            "page_type": "product",
            "field_labels": {"price": "价格"},
            "data": {"price": "99", "title": "Phone"},
        },
    )

    second = store.create(
        url="https://example.com/product/1",
        schema_name="auto",
        storage_format="json",
    )
    store.mark_success(
        second.task_id,
        elapsed_ms=120.0,
        quality_score=0.92,
        data={
            "page_type": "product",
            "field_labels": {"price": "价格"},
            "data": {"price": "79", "title": "Phone"},
        },
    )

    detail = store.get_task_detail_payload(second.task_id)
    assert detail is not None
    assert detail["progress"]["percent"] == 100.0
    assert detail["history_summary"]["total_runs"] == 2
    assert detail["comparison"]["has_previous"] is True
    assert detail["comparison"]["changed"] is True
    assert detail["comparison"]["changed_fields_count"] == 1
    assert detail["comparison"]["changed_fields"][0]["field"] == "price"
    assert detail["comparison"]["impact_summary"]
    assert detail["comparison"]["suggested_actions"]

    insights = store.build_dashboard_insights()
    assert insights["summary"]["repeat_urls"] == 1
    assert insights["summary"]["changed_tasks"] == 1
    assert insights["recent_changes"][0]["task_id"] == second.task_id


def test_sqlite_task_store_monitor_profile_and_notification_state(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    monitor = store.create_or_update_monitor(
        name="竞品卖点监控",
        url="https://example.com/competitor",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title", "summary"],
        field_labels={"title": "标题", "summary": "总结"},
        profile={
            "scenario_label": "竞品变化监控",
            "business_goal": "变化后通知市场团队",
            "alert_focus": "标题、总结",
            "notify_on": ["changed", "error"],
            "webhook_url": "https://example.com/webhook",
        },
        schedule_enabled=True,
        schedule_interval_minutes=30,
    )

    assert monitor.profile["scenario_label"] == "竞品变化监控"
    assert monitor.schedule_enabled is True
    assert monitor.schedule_interval_minutes == 30
    assert monitor.schedule_next_run_at

    task = store.create(
        url="https://example.com/competitor",
        schema_name="auto",
        storage_format="json",
    )
    store.mark_failed(task.task_id, elapsed_ms=18.0, error="timeout")
    latest_task = store.get(task.task_id)
    assert latest_task is not None
    updated_monitor = store.update_monitor_result(monitor.monitor_id, latest_task)
    assert updated_monitor is not None
    assert updated_monitor.last_alert_level == "error"

    delivered_monitor = store.update_monitor_notification(
        monitor.monitor_id,
        status="sent",
        message="通知已发送到 https://example.com/webhook",
    )
    assert delivered_monitor is not None
    assert delivered_monitor.last_notification_status == "sent"


def test_sqlite_task_store_persists_notification_events(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    event = store.create_notification_event(
        monitor_id="mon-000001",
        task_id="task-000001",
        channel_type="webhook",
        target="https://example.com/webhook",
        event_type="monitor_alert",
        status="retry_pending",
        status_message="通知目标限流，计划稍后重试",
        attempt_no=1,
        max_attempts=3,
        next_retry_at="2026-04-18 10:00:00",
        response_code=429,
        error_type="http_error",
        error_message="429",
        payload_snapshot={"event": "smart_extractor.monitor_alert"},
    )

    assert event.notification_id.startswith("ntf-")
    assert event.status == "retry_pending"
    assert event.response_code == 429

    listed = store.list_notification_events(limit=10, monitor_id="mon-000001")
    assert len(listed) == 1
    assert listed[0].notification_id == event.notification_id
    assert listed[0].payload_snapshot["event"] == "smart_extractor.monitor_alert"


def test_sqlite_task_store_lists_due_notification_retries(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    event = store.create_notification_event(
        monitor_id="mon-000001",
        task_id="task-000001",
        channel_type="webhook",
        target="https://example.com/webhook",
        event_type="monitor_alert",
        status="retry_pending",
        status_message="等待自动重试",
        next_retry_at="2000-01-01 00:00:00",
        payload_snapshot={"event": "smart_extractor.monitor_alert"},
    )

    due_events = store.list_due_notification_retries(
        due_before="2000-01-01 00:00:01",
        limit=10,
    )
    assert len(due_events) == 1
    assert due_events[0].notification_id == event.notification_id

    store.update_notification_event(
        event.notification_id,
        status="retried",
        next_retry_at="",
    )
    updated = store.get_notification_event(event.notification_id)
    assert updated is not None
    assert updated.status == "retried"
    assert updated.next_retry_at == ""


def test_sqlite_task_store_monitor_schedule_can_pause_and_resume(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    monitor = store.create_or_update_monitor(
        name="自动巡检监控",
        url="https://example.com/scheduled",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"scenario_label": "自动监控"},
        schedule_enabled=True,
        schedule_interval_minutes=15,
    )

    assert monitor.schedule_enabled is True
    assert monitor.schedule_next_run_at

    paused = store.pause_monitor_schedule(monitor.monitor_id)
    assert paused is not None
    assert paused.schedule_paused_at
    assert paused.schedule_next_run_at == ""

    resumed = store.resume_monitor_schedule(monitor.monitor_id)
    assert resumed is not None
    assert resumed.schedule_paused_at == ""
    assert resumed.schedule_next_run_at


def test_sqlite_task_store_lists_due_monitors_without_running_task(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    due_monitor = store.create_or_update_monitor(
        name="到期执行的监控",
        url="https://example.com/due",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"scenario_label": "到期执行"},
        schedule_enabled=True,
        schedule_interval_minutes=15,
    )
    blocked_monitor = store.create_or_update_monitor(
        name="正在运行的监控",
        url="https://example.com/busy",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"scenario_label": "正在运行"},
        schedule_enabled=True,
        schedule_interval_minutes=15,
    )

    with store._connect() as conn:
        conn.execute(
            "UPDATE monitor_profiles SET schedule_next_run_at='2000-01-01 00:00:00' WHERE monitor_id=?",
            (due_monitor.monitor_id,),
        )
        conn.execute(
            "UPDATE monitor_profiles SET schedule_next_run_at='2000-01-01 00:00:00' WHERE monitor_id=?",
            (blocked_monitor.monitor_id,),
        )
        conn.commit()

    task = store.create(
        url=blocked_monitor.url,
        schema_name="auto",
        storage_format="json",
    )
    store.mark_running(task.task_id)
    store.mark_monitor_run_scheduled(
        blocked_monitor.monitor_id,
        task_id=task.task_id,
        trigger_source="auto",
    )

    due_list = store.list_due_monitors(
        due_before="2000-01-01 00:00:01",
        limit=5,
    )
    assert [item.monitor_id for item in due_list] == [due_monitor.monitor_id]


def test_sqlite_task_store_can_claim_due_monitor_once(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    monitor = store.create_or_update_monitor(
        name="可抢占监控",
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

    first_claim = store.claim_due_monitors(
        due_before="2000-01-01 00:00:01",
        claimer_id="scheduler-a",
        lease_seconds=120.0,
        limit=5,
    )
    second_claim = store.claim_due_monitors(
        due_before="2000-01-01 00:00:01",
        claimer_id="scheduler-b",
        lease_seconds=120.0,
        limit=5,
    )

    assert [item.monitor_id for item in first_claim] == [monitor.monitor_id]
    assert second_claim == []

    refreshed = store.get_monitor(monitor.monitor_id)
    assert refreshed is not None
    assert refreshed.schedule_claimed_by == "scheduler-a"
    assert refreshed.schedule_claimed_at
    assert refreshed.schedule_lease_until
    assert refreshed.schedule_claim_count == 1


def test_sqlite_task_store_reclaims_expired_monitor_lease(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    monitor = store.create_or_update_monitor(
        name="过期租约监控",
        url="https://example.com/reclaim",
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
            """
            UPDATE monitor_profiles
            SET schedule_next_run_at='2000-01-01 00:00:00',
                schedule_claimed_by='scheduler-old',
                schedule_claimed_at='1999-12-31 23:50:00',
                schedule_lease_until='2000-01-01 00:00:00'
            WHERE monitor_id=?
            """,
            (monitor.monitor_id,),
        )
        conn.commit()

    claimed = store.claim_due_monitors(
        due_before="2000-01-01 00:00:01",
        claimer_id="scheduler-new",
        lease_seconds=120.0,
        limit=5,
    )

    assert [item.monitor_id for item in claimed] == [monitor.monitor_id]
    refreshed = store.get_monitor(monitor.monitor_id)
    assert refreshed is not None
    assert refreshed.schedule_claimed_by == "scheduler-new"
    assert refreshed.schedule_claim_count == 1


def test_sqlite_task_store_update_progress(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)
    task = store.create(
        url="https://example.com/progress",
        schema_name="auto",
        storage_format="json",
    )

    store.mark_running(task.task_id)
    store.update_progress(task.task_id, 58.0, "正文清洗完成，正在分析字段")

    updated = store.get(task.task_id)
    assert updated is not None
    assert updated.status == "running"
    assert updated.progress_percent == 58.0
    assert updated.progress_stage == "正文清洗完成，正在分析字段"


def test_sqlite_task_store_persists_batch_group_id(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    task = store.create(
        url="https://example.com/grouped",
        schema_name="auto",
        storage_format="json",
        batch_group_id="batch-demo01",
    )

    reloaded = store.get(task.task_id)
    assert reloaded is not None
    assert reloaded.batch_group_id == "batch-demo01"

    listed = store.list_all(limit=10)
    assert listed[0].batch_group_id == "batch-demo01"


def test_sqlite_task_store_can_filter_by_batch_group_id(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    store.create(
        url="https://example.com/a",
        schema_name="auto",
        storage_format="json",
        batch_group_id="batch-a001",
    )
    store.create(
        url="https://example.com/b",
        schema_name="auto",
        storage_format="json",
        batch_group_id="batch-b001",
    )

    filtered = store.list_all(limit=10, batch_group_id="batch-a001")
    assert len(filtered) == 1
    assert filtered[0].batch_group_id == "batch-a001"


def test_sqlite_task_store_batch_root_aggregates_children(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    parent = store.create_batch_root(
        urls=["https://example.com/a", "https://example.com/b"],
        schema_name="auto",
        storage_format="json",
        batch_group_id="batch-root01",
    )
    child_a = store.create(
        url="https://example.com/a",
        schema_name="auto",
        storage_format="json",
        batch_group_id="batch-root01",
        parent_task_id=parent.task_id,
    )
    child_b = store.create(
        url="https://example.com/b",
        schema_name="auto",
        storage_format="json",
        batch_group_id="batch-root01",
        parent_task_id=parent.task_id,
    )

    listed = store.list_all(limit=10, batch_group_id="batch-root01")
    assert len(listed) == 1
    assert listed[0].task_id == parent.task_id
    assert listed[0].task_kind == "batch"

    store.mark_success(
        child_a.task_id, elapsed_ms=10.0, quality_score=0.8, data={"ok": True}
    )
    store.mark_failed(child_b.task_id, elapsed_ms=15.0, error="boom")

    refreshed_parent = store.get(parent.task_id)
    assert refreshed_parent is not None
    assert refreshed_parent.task_kind == "batch"
    assert refreshed_parent.total_items == 2
    assert refreshed_parent.completed_items == 2
    assert refreshed_parent.status == "failed"

    detail = store.get_task_detail_payload(parent.task_id)
    assert detail is not None
    assert len(detail["batch_children"]) == 2
    assert detail["batch_children"][0]["task_id"] == child_a.task_id


def test_sqlite_task_store_insights_include_strategy_summary(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    first = store.create(
        url="https://example.com/product/1",
        schema_name="auto",
        storage_format="json",
    )
    store.mark_success(
        first.task_id,
        elapsed_ms=10.0,
        quality_score=0.93,
        data={
            "page_type": "product",
            "selected_fields": ["title", "price"],
            "field_labels": {"title": "标题", "price": "价格"},
            "data": {"title": "Phone", "price": "99"},
            "extraction_strategy": "llm",
            "learned_profile_id": "lp-000001",
        },
    )

    second = store.create(
        url="https://example.com/product/1",
        schema_name="auto",
        storage_format="json",
    )
    store.mark_success(
        second.task_id,
        elapsed_ms=11.0,
        quality_score=0.95,
        data={
            "page_type": "product",
            "selected_fields": ["title", "price"],
            "field_labels": {"title": "标题", "price": "价格"},
            "data": {"title": "Phone", "price": "79"},
            "extraction_strategy": "rule",
            "learned_profile_id": "lp-000001",
        },
    )

    fallback = store.create(
        url="https://example.com/article/1",
        schema_name="auto",
        storage_format="json",
    )
    store.mark_success(
        fallback.task_id,
        elapsed_ms=9.0,
        quality_score=0.82,
        data={
            "data": {"content": "fallback body"},
            "extraction_strategy": "fallback",
        },
    )

    insights = store.build_dashboard_insights()
    assert insights["summary"]["rule_based_tasks"] == 1
    assert insights["summary"]["fallback_tasks"] == 1
    assert insights["summary"]["learned_profile_hits"] == 2


def test_sqlite_task_store_monitor_persists_strategy_metadata(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    monitor = store.create_or_update_monitor(
        name="商品价格监控",
        url="https://example.com/product/1",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title", "price"],
        field_labels={"title": "标题", "price": "价格"},
        profile={"scenario_label": "价格监控"},
    )

    task = store.create(
        url="https://example.com/product/1",
        schema_name="auto",
        storage_format="json",
    )
    store.mark_success(
        task.task_id,
        elapsed_ms=12.0,
        quality_score=0.94,
        data={
            "page_type": "product",
            "selected_fields": ["title", "price"],
            "field_labels": {"title": "标题", "price": "价格"},
            "data": {"title": "Phone", "price": "79"},
            "extraction_strategy": "rule",
            "learned_profile_id": "lp-000001",
        },
    )

    latest_task = store.get(task.task_id)
    assert latest_task is not None

    updated_monitor = store.update_monitor_result(monitor.monitor_id, latest_task)
    assert updated_monitor is not None
    assert updated_monitor.last_extraction_strategy == "rule"
    assert updated_monitor.last_learned_profile_id == "lp-000001"


def test_sqlite_task_store_can_build_learned_profile_activity(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    monitor = store.create_or_update_monitor(
        name="商品价格监控",
        url="https://example.com/product/1",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title", "price"],
        field_labels={"title": "标题", "price": "价格"},
        profile={"scenario_label": "价格监控"},
    )
    task = store.create(
        url="https://example.com/product/1",
        schema_name="auto",
        storage_format="json",
    )
    store.mark_success(
        task.task_id,
        elapsed_ms=12.0,
        quality_score=0.94,
        data={
            "page_type": "product",
            "selected_fields": ["title", "price"],
            "field_labels": {"title": "标题", "price": "价格"},
            "data": {"title": "Phone", "price": "79"},
            "extraction_strategy": "rule",
            "learned_profile_id": "lp-000001",
        },
    )

    latest_task = store.get(task.task_id)
    assert latest_task is not None
    store.update_monitor_result(monitor.monitor_id, latest_task)

    activity = store.get_learned_profile_activity("lp-000001", task_limit=5)
    assert activity["summary"]["task_hits"] == 1
    assert activity["summary"]["monitor_links"] == 1
    assert activity["summary"]["rule_hits"] == 1
    assert activity["recent_hits"][0]["task_id"] == task.task_id
    assert activity["related_monitors"][0]["monitor_id"] == monitor.monitor_id
    assert "schema_name" not in activity["related_monitors"][0]
    assert "last_task_id" not in activity["related_monitors"][0]


def test_sqlite_task_store_can_enqueue_and_claim_queue_task(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    task = store.create(
        url="https://example.com/queued",
        schema_name="auto",
        storage_format="json",
    )

    class DummySpec:
        task_id = task.task_id

        @staticmethod
        def to_queue_payload():
            return {
                "schema_name": "auto",
                "use_static": True,
                "selected_fields": ["title"],
                "monitor_id": "",
                "force_strategy": "llm",
            }

    store.enqueue_task_spec(DummySpec())

    queued = store.get(task.task_id)
    assert queued is not None
    assert queued.status == "queued"

    claimed = store.claim_next_queued_task(worker_id="worker-test")
    assert claimed is not None
    claimed_task, claimed_payload = claimed
    assert claimed_task.task_id == task.task_id
    assert claimed_payload["selected_fields"] == ["title"]

    store.mark_queue_done(task.task_id)


def test_sqlite_task_store_enables_wal_and_busy_timeout(tmp_path):
    db_path = tmp_path / "web_tasks.db"
    store = SQLiteTaskStore(db_path)

    with store._connect() as conn:
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        busy_timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]
        synchronous = conn.execute("PRAGMA synchronous").fetchone()[0]

    assert str(journal_mode).lower() == "wal"
    assert int(busy_timeout) == 5000
    assert int(synchronous) in {1, 2}
