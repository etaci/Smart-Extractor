import time

from smart_extractor.config import load_raw_yaml_config
from smart_extractor.web.notifier import NotificationDeliveryResult
from tests.web_route_testkit import _build_test_client


def test_api_template_market_lists_templates(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)

    response = client.get("/api/template_market", headers={"X-API-Token": "test-token"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["templates"]
    assert payload["templates"][0]["template_id"].startswith("market-")


def test_api_learned_profiles_returns_profiles(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    routes_module._learned_profile_store.upsert_from_result(
        "https://example.com/product/1",
        page_type="product",
        selected_fields=["title", "price"],
        field_labels={"title": "标题", "price": "价格"},
        strategy="llm",
        completeness=1.0,
    )
    routes_module._learned_profile_store.record_rule_attempt(
        "lp-000001",
        success=True,
        completeness=1.0,
    )
    monitor = routes_module._task_store.create_or_update_monitor(
        name="商品监控",
        url="https://example.com/product/1",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title", "price"],
        field_labels={"title": "标题", "price": "价格"},
        profile={"scenario_label": "价格监控"},
    )
    task = routes_module._task_store.create(
        url="https://example.com/product/1",
        schema_name="auto",
        storage_format="json",
    )
    routes_module._task_store.mark_success(
        task.task_id,
        elapsed_ms=18.0,
        quality_score=0.95,
        data={
            "page_type": "product",
            "field_labels": {"title": "标题", "price": "价格"},
            "data": {"title": "Phone", "price": "79"},
            "extraction_strategy": "rule",
            "learned_profile_id": "lp-000001",
        },
    )
    latest_task = routes_module._task_store.get(task.task_id)
    assert latest_task is not None
    routes_module._task_store.update_monitor_result(monitor.monitor_id, latest_task)

    response = client.get(
        "/api/learned_profiles", headers={"X-API-Token": "test-token"}
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["profiles"]
    assert payload["profiles"][0]["profile_id"] == "lp-000001"
    assert payload["profiles"][0]["status_label"] == "可复用"
    assert payload["profiles"][0]["risk_level"] in {"low", "medium", "high", "paused"}
    assert payload["profiles"][0]["recommended_actions"]
    assert payload["profiles"][0]["monitor_hits"] == 1
    assert len(payload["profiles"]) == 1


def test_api_learned_profiles_support_lifecycle_actions(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    routes_module._learned_profile_store.upsert_from_result(
        "https://example.com/product/2",
        page_type="product",
        selected_fields=["title", "price"],
        field_labels={"title": "标题", "price": "价格"},
        strategy="llm",
        completeness=1.0,
    )

    headers = {"X-API-Token": "test-token"}

    disable_response = client.post(
        "/api/learned_profiles/lp-000001/disable",
        headers=headers,
        json={"reason": "规则命中不稳定"},
    )
    assert disable_response.status_code == 200
    assert disable_response.json()["profile"]["is_active"] is False

    reset_response = client.post(
        "/api/learned_profiles/lp-000001/reset",
        headers=headers,
    )
    assert reset_response.status_code == 200
    assert reset_response.json()["profile"]["llm_success_count"] == 0

    enable_response = client.post(
        "/api/learned_profiles/lp-000001/enable",
        headers=headers,
    )
    assert enable_response.status_code == 200
    assert enable_response.json()["profile"]["is_active"] is True

    delete_response = client.delete(
        "/api/learned_profiles/lp-000001",
        headers=headers,
    )
    assert delete_response.status_code == 200
    assert delete_response.json()["profile_id"] == "lp-000001"


def test_api_learned_profile_detail_returns_activity(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    routes_module._learned_profile_store.upsert_from_result(
        "https://example.com/product/7",
        page_type="product",
        selected_fields=["title", "price"],
        field_labels={"title": "标题", "price": "价格"},
        strategy="llm",
        completeness=1.0,
    )
    monitor = routes_module._task_store.create_or_update_monitor(
        name="商品价格监控",
        url="https://example.com/product/7",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title", "price"],
        field_labels={"title": "标题", "price": "价格"},
        profile={"scenario_label": "价格监控"},
    )
    task = routes_module._task_store.create(
        url="https://example.com/product/7",
        schema_name="auto",
        storage_format="json",
    )
    routes_module._task_store.mark_success(
        task.task_id,
        elapsed_ms=15.0,
        quality_score=0.96,
        data={
            "page_type": "product",
            "field_labels": {"title": "标题", "price": "价格"},
            "data": {"title": "Phone", "price": "79"},
            "extraction_strategy": "rule",
            "learned_profile_id": "lp-000001",
        },
    )
    latest_task = routes_module._task_store.get(task.task_id)
    assert latest_task is not None
    routes_module._task_store.update_monitor_result(monitor.monitor_id, latest_task)

    response = client.get(
        "/api/learned_profiles/lp-000001",
        headers={"X-API-Token": "test-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["profile"]["profile_id"] == "lp-000001"
    assert payload["profile"]["recommended_actions"]
    assert payload["activity"]["summary"]["task_hits"] == 1
    assert payload["activity"]["summary"]["monitor_links"] == 1
    assert payload["activity"]["recent_hits"][0]["task_id"] == task.task_id
    assert payload["activity"]["related_monitors"][0]["monitor_id"] == monitor.monitor_id
    assert "schema_name" not in payload["activity"]["related_monitors"][0]
    assert "last_task_id" not in payload["activity"]["related_monitors"][0]


def test_api_learned_profiles_marks_high_risk_recommendations(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    routes_module._learned_profile_store.upsert_from_result(
        "https://example.com/product/risky",
        page_type="product",
        selected_fields=["title", "price", "stock", "description"],
        field_labels={"title": "标题", "price": "价格"},
        strategy="llm",
        completeness=0.2,
    )
    routes_module._learned_profile_store.record_rule_attempt(
        "lp-000001",
        success=False,
        completeness=0.2,
        source_url="https://example.com/product/risky",
    )
    routes_module._learned_profile_store.record_rule_attempt(
        "lp-000001",
        success=False,
        completeness=0.1,
        source_url="https://example.com/product/risky",
    )

    response = client.get(
        "/api/learned_profiles/lp-000001",
        headers={"X-API-Token": "test-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["profile"]["risk_level"] == "high"
    assert any("停用" in item for item in payload["profile"]["recommended_actions"])


def test_api_learned_profile_relearn_creates_force_llm_task(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    routes_module._learned_profile_store.upsert_from_result(
        "https://example.com/product/relearn",
        page_type="product",
        selected_fields=["title", "price"],
        field_labels={"title": "标题", "price": "价格"},
        strategy="llm",
        completeness=1.0,
    )
    captured = {}

    def fake_run_extraction(
        task_id: str,
        schema_name: str = "auto",
        use_static: bool = False,
        selected_fields=None,
        monitor_id: str = "",
        force_strategy: str = "",
    ):
        captured["task_id"] = task_id
        captured["schema_name"] = schema_name
        captured["selected_fields"] = selected_fields or []
        captured["force_strategy"] = force_strategy
        routes_module._task_store.mark_success(
            task_id=task_id,
            elapsed_ms=21.0,
            quality_score=0.9,
            data={
                "page_type": "product",
                "data": {"title": "Phone", "price": "88"},
                "extraction_strategy": "llm",
                "learned_profile_id": "lp-000001",
            },
        )

    monkeypatch.setattr(routes_module, "_run_extraction", fake_run_extraction)

    response = client.post(
        "/api/learned_profiles/lp-000001/relearn",
        headers={"X-API-Token": "test-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["profile_id"] == "lp-000001"
    assert payload["task_id"].startswith("task-")
    assert captured["force_strategy"] == "llm"
    assert captured["schema_name"] == "auto"
    assert captured["selected_fields"] == ["title", "price"]


def test_api_bulk_disable_risky_learned_profiles(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    headers = {"X-API-Token": "test-token"}

    routes_module._learned_profile_store.upsert_from_result(
        "https://risk-a.example.com/product/risky-1",
        page_type="product",
        selected_fields=["title", "price", "stock", "description"],
        field_labels={"title": "标题", "price": "价格"},
        strategy="llm",
        completeness=0.2,
    )
    routes_module._learned_profile_store.record_rule_attempt(
        "lp-000001",
        success=False,
        completeness=0.2,
        source_url="https://risk-a.example.com/product/risky-1",
    )
    routes_module._learned_profile_store.record_rule_attempt(
        "lp-000001",
        success=False,
        completeness=0.1,
        source_url="https://risk-a.example.com/product/risky-1",
    )

    routes_module._learned_profile_store.upsert_from_result(
        "https://risk-b.example.com/product/risky-2",
        page_type="product",
        selected_fields=["title", "price", "stock", "description"],
        field_labels={"title": "标题", "price": "价格"},
        strategy="llm",
        completeness=0.2,
    )
    routes_module._learned_profile_store.record_rule_attempt(
        "lp-000002",
        success=False,
        completeness=0.2,
        source_url="https://risk-b.example.com/product/risky-2",
    )
    routes_module._learned_profile_store.record_rule_attempt(
        "lp-000002",
        success=False,
        completeness=0.1,
        source_url="https://risk-b.example.com/product/risky-2",
    )

    response = client.post(
        "/api/learned_profiles/bulk/disable_risky",
        headers=headers,
        json={"reason": "批量停用高风险档案"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 2
    assert len(payload["profiles"]) == 2
    assert all(item["is_active"] is False for item in payload["profiles"])
    assert payload["count"] == 2


def test_api_bulk_relearn_risky_learned_profiles(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    headers = {"X-API-Token": "test-token"}
    captured_calls = []

    routes_module._learned_profile_store.upsert_from_result(
        "https://risk-a.example.com/product/risky-relearn-1",
        page_type="product",
        selected_fields=["title", "price", "stock", "description"],
        field_labels={"title": "标题", "price": "价格"},
        strategy="llm",
        completeness=0.2,
    )
    routes_module._learned_profile_store.record_rule_attempt(
        "lp-000001",
        success=False,
        completeness=0.2,
        source_url="https://risk-a.example.com/product/risky-relearn-1",
    )
    routes_module._learned_profile_store.record_rule_attempt(
        "lp-000001",
        success=False,
        completeness=0.1,
        source_url="https://risk-a.example.com/product/risky-relearn-1",
    )

    routes_module._learned_profile_store.upsert_from_result(
        "https://risk-b.example.com/product/risky-relearn-2",
        page_type="product",
        selected_fields=["title", "price", "stock", "description"],
        field_labels={"title": "标题", "price": "价格"},
        strategy="llm",
        completeness=0.2,
    )
    routes_module._learned_profile_store.record_rule_attempt(
        "lp-000002",
        success=False,
        completeness=0.2,
        source_url="https://risk-b.example.com/product/risky-relearn-2",
    )
    routes_module._learned_profile_store.record_rule_attempt(
        "lp-000002",
        success=False,
        completeness=0.1,
        source_url="https://risk-b.example.com/product/risky-relearn-2",
    )

    def fake_run_extraction(
        task_id: str,
        schema_name: str = "auto",
        use_static: bool = False,
        selected_fields=None,
        monitor_id: str = "",
        force_strategy: str = "",
    ):
        captured_calls.append(
            {
                "task_id": task_id,
                "schema_name": schema_name,
                "selected_fields": selected_fields or [],
                "force_strategy": force_strategy,
            }
        )
        routes_module._task_store.mark_success(
            task_id=task_id,
            elapsed_ms=19.0,
            quality_score=0.92,
            data={
                "page_type": "product",
                "data": {"title": "Phone", "price": "88"},
                "extraction_strategy": "llm",
            },
        )

    monkeypatch.setattr(routes_module, "_run_extraction", fake_run_extraction)

    response = client.post(
        "/api/learned_profiles/bulk/relearn_risky",
        headers=headers,
        json={"reason": "批量重新学习高风险档案"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 2
    assert len(payload["tasks"]) == 2
    assert len(captured_calls) == 2
    assert all(item["force_strategy"] == "llm" for item in captured_calls)
    assert all(item["schema_name"] == "auto" for item in captured_calls)
    assert all(item["task_id"].startswith("task-") for item in captured_calls)


def test_api_template_market_can_install_template(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)

    response = client.post(
        "/api/template_market/install",
        headers={"X-API-Token": "test-token"},
        json={"template_id": "market-product-monitor"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["template"]["template_id"].startswith("tpl-")
    assert payload["template"]["selected_fields"]
    assert payload["template"]["profile"]["scenario_label"]


def test_api_save_monitor_persists_profile(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)

    response = client.post(
        "/api/monitors",
        headers={"X-API-Token": "test-token"},
        json={
            "name": "政策更新监控",
            "url": "https://example.com/policy",
            "schema_name": "auto",
            "storage_format": "json",
            "use_static": True,
            "schedule_enabled": True,
            "schedule_interval_minutes": 30,
            "selected_fields": ["title", "publish_date", "content"],
            "field_labels": {
                "title": "标题",
                "publish_date": "发布时间",
                "content": "正文",
            },
            "profile": {
                "scenario_label": "政策更新监控",
                "business_goal": "页面变化时推送给运营负责人",
                "alert_focus": "发布时间、正文",
                "notify_on": ["changed", "error"],
                "webhook_url": "https://example.com/webhook",
                "digest_enabled": True,
                "digest_hour": 8,
                "digest_only": True,
                "quiet_hours_enabled": True,
                "quiet_hours_start": 22,
                "quiet_hours_end": 8,
                "notification_cooldown_minutes": 30,
                "min_change_count": 2,
                "min_change_ratio": 0.4,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["monitor"]["monitor_id"].startswith("mon-")
    assert payload["monitor"]["profile"]["scenario_label"] == "政策更新监控"
    assert payload["monitor"]["profile"]["webhook_url"] == "https://example.com/webhook"
    assert payload["monitor"]["profile"]["digest_enabled"] is True
    assert payload["monitor"]["profile"]["digest_hour"] == 8
    assert payload["monitor"]["profile"]["digest_only"] is True
    assert payload["monitor"]["profile"]["quiet_hours_enabled"] is True
    assert payload["monitor"]["profile"]["notification_cooldown_minutes"] == 30
    assert payload["monitor"]["profile"]["min_change_count"] == 2
    assert payload["monitor"]["profile"]["min_change_ratio"] == 0.4
    assert payload["monitor"]["schedule_enabled"] is True
    assert payload["monitor"]["schedule_interval_minutes"] == 30
    assert payload["monitor"]["schedule_status"] == "active"
    assert payload["monitor"]["schedule_next_run_at"]
    assert payload["monitor"]["profile"]["digest_only"] is True


def test_api_monitor_can_pause_and_resume_schedule(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    monitor = routes_module._task_store.create_or_update_monitor(
        name="自动巡检监控",
        url="https://example.com/scheduled",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"scenario_label": "自动巡检"},
        schedule_enabled=True,
        schedule_interval_minutes=15,
    )

    pause_response = client.post(
        f"/api/monitors/{monitor.monitor_id}/pause",
        headers={"X-API-Token": "test-token"},
    )
    assert pause_response.status_code == 200
    paused_monitor = pause_response.json()["monitor"]
    assert paused_monitor["schedule_status"] == "paused"
    assert paused_monitor["schedule_paused_at"]
    assert paused_monitor["schedule_next_run_at"] == ""

    resume_response = client.post(
        f"/api/monitors/{monitor.monitor_id}/resume",
        headers={"X-API-Token": "test-token"},
    )
    assert resume_response.status_code == 200
    resumed_monitor = resume_response.json()["monitor"]
    assert resumed_monitor["schedule_status"] == "active"
    assert resumed_monitor["schedule_paused_at"] == ""
    assert resumed_monitor["schedule_next_run_at"]


def test_monitor_run_dispatches_notification_when_profile_matches(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    sent_payload = {}

    monitor = routes_module._task_store.create_or_update_monitor(
        name="竞品监控",
        url="https://example.com/competitor",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title", "summary"],
        field_labels={"title": "标题", "summary": "总结"},
        profile={
            "scenario_label": "竞品变化监控",
            "business_goal": "监控竞品文案变化",
            "alert_focus": "标题、总结",
            "notify_on": ["stable", "changed", "error"],
            "webhook_url": "https://example.com/webhook",
        },
        schedule_enabled=True,
        schedule_interval_minutes=30,
    )

    def fake_run_extraction(
        task_id: str,
        schema_name: str = "auto",
        use_static: bool = False,
        selected_fields=None,
        monitor_id: str = "",
    ):
        routes_module._task_store.mark_success(
            task_id=task_id,
            elapsed_ms=42.0,
            quality_score=0.91,
            data={
                "page_type": "article",
                "field_labels": {"summary": "总结"},
                "data": {"summary": "新版卖点"},
                "extraction_strategy": "rule",
                "learned_profile_id": "lp-000001",
            },
        )
        latest_task = routes_module._task_store.get(task_id)
        assert latest_task is not None
        routes_module._task_store.update_monitor_result(monitor_id, latest_task)
        routes_module._sync_monitor_notification(monitor_id, task_id)

    def fake_send_monitor_notification(
        monitor_payload, task_payload, timeout_seconds=10.0, **kwargs
    ):
        sent_payload["monitor"] = monitor_payload
        sent_payload["task"] = task_payload
        sent_payload["kwargs"] = kwargs
        return "sent", "通知已发送到 https://example.com/webhook"

    monkeypatch.setattr(routes_module, "_run_extraction", fake_run_extraction)
    monkeypatch.setattr(
        routes_module, "send_monitor_notification", fake_send_monitor_notification
    )

    response = client.post(
        f"/api/monitors/{monitor.monitor_id}/run",
        headers={"X-API-Token": "test-token"},
    )

    assert response.status_code == 200
    latest_monitor = None
    for _ in range(50):
        latest_monitor = routes_module._task_store.get_monitor(monitor.monitor_id)
        if latest_monitor is not None and latest_monitor.last_notification_status == "sent":
            break
        time.sleep(0.02)
    assert latest_monitor is not None
    assert latest_monitor.last_notification_status == "sent"
    assert latest_monitor.last_extraction_strategy == "rule"
    assert latest_monitor.last_learned_profile_id == "lp-000001"
    assert latest_monitor.last_trigger_source == "manual"
    assert latest_monitor.schedule_last_run_at
    assert latest_monitor.schedule_next_run_at
    assert sent_payload["kwargs"]["target_override"] == "https://example.com/webhook"
    assert sent_payload["monitor"]["profile"]["scenario_label"] == "竞品变化监控"


def test_api_monitors_returns_learned_profile_summary(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    routes_module._learned_profile_store.upsert_from_result(
        "https://example.com/product/9",
        page_type="product",
        selected_fields=["title", "price"],
        field_labels={"title": "标题", "price": "价格"},
        strategy="llm",
        completeness=1.0,
    )
    monitor = routes_module._task_store.create_or_update_monitor(
        name="价格监控",
        url="https://example.com/product/9",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title", "price"],
        field_labels={"title": "标题", "price": "价格"},
        profile={"scenario_label": "价格监控"},
    )
    task = routes_module._task_store.create(
        url="https://example.com/product/9",
        schema_name="auto",
        storage_format="json",
    )
    routes_module._task_store.mark_success(
        task.task_id,
        elapsed_ms=18.0,
        quality_score=0.95,
        data={
            "page_type": "product",
            "field_labels": {"title": "标题", "price": "价格"},
            "data": {"title": "Phone", "price": "79"},
            "extraction_strategy": "rule",
            "learned_profile_id": "lp-000001",
        },
    )
    latest_task = routes_module._task_store.get(task.task_id)
    assert latest_task is not None
    routes_module._task_store.update_monitor_result(monitor.monitor_id, latest_task)

    response = client.get("/api/monitors", headers={"X-API-Token": "test-token"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["monitors"]
    assert payload["monitors"][0]["last_extraction_strategy"] == "rule"
    assert payload["monitors"][0]["learned_profile"]["profile_id"] == "lp-000001"


def test_api_monitors_returns_scheduler_claim_observability(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    monitor = routes_module._task_store.create_or_update_monitor(
        name="调度可观测监控",
        url="https://example.com/claim-state",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"scenario_label": "自动巡检"},
        schedule_enabled=True,
        schedule_interval_minutes=15,
    )

    with routes_module._task_store._connect() as conn:
        conn.execute(
            "UPDATE monitor_profiles SET schedule_next_run_at='2000-01-01 00:00:00' WHERE monitor_id=?",
            (monitor.monitor_id,),
        )
        conn.commit()

    claimed = routes_module._task_store.claim_due_monitors(
        due_before="2000-01-01 00:00:01",
        claimer_id="scheduler-observe",
        lease_seconds=120.0,
        limit=5,
    )
    assert claimed
    routes_module._task_store.fail_monitor_claim(
        monitor.monitor_id,
        error="dispatch boom",
        claimed_by="scheduler-observe",
    )

    response = client.get("/api/monitors", headers={"X-API-Token": "test-token"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["monitors"]
    assert payload["monitors"][0]["schedule_claim_status"] == "claimed"
    assert payload["monitors"][0]["schedule_claim_status_label"] == "调度抢占中"
    assert payload["monitors"][0]["schedule_claimed_by"] == "scheduler-observe"
    assert payload["monitors"][0]["schedule_last_error"] == "dispatch boom"


def test_api_basic_config_returns_editable_llm_fields(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)

    response = client.get("/api/config/basic", headers={"X-API-Token": "test-token"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["base_url"] == "https://api.openai.com/v1"
    assert payload["model"] == "gpt-4o-mini"
    assert payload["temperature"] == 0.0
    assert payload["config_path"].endswith("local.yaml")
    assert payload["has_local_override"] is False


def test_api_basic_config_can_update_local_yaml(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)
    config_path = tmp_path / "config" / "local.yaml"

    response = client.post(
        "/api/config/basic",
        headers={"X-API-Token": "test-token"},
        json={
            "api_key": "new-key-from-dashboard",
            "base_url": "https://example.org/v1",
            "model": "gpt-test",
            "temperature": 0.7,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert "基础配置已保存" in payload["message"]
    assert payload["config"]["config_path"].endswith("local.yaml")
    assert payload["config"]["has_local_override"] is True

    config_data = load_raw_yaml_config(config_path)
    assert config_data["llm"]["api_key"] == "new-key-from-dashboard"
    assert config_data["llm"]["base_url"] == "https://example.org/v1"
    assert config_data["llm"]["model"] == "gpt-test"
    assert config_data["llm"]["temperature"] == 0.7


def test_api_basic_config_update_refreshes_runtime_status(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)
    headers = {"X-API-Token": "test-token"}

    client.app.state.runtime_status = {
        "ready": False,
        "issues": ["未配置 LLM API Key，当前只能查看界面，无法提交提取或分析任务。"],
        "warnings": [],
    }

    update_response = client.post(
        "/api/config/basic",
        headers=headers,
        json={
            "api_key": "runtime-refresh-key",
            "base_url": "https://example.org/v1",
            "model": "gpt-test",
            "temperature": 0.2,
        },
    )
    assert update_response.status_code == 200

    after_response = client.get("/api/runtime", headers=headers)
    assert after_response.status_code == 200
    after_payload = after_response.json()
    assert after_payload["ready"] is True
    assert not any("LLM API Key" in item for item in after_payload["issues"])


def test_api_runtime_returns_monitor_scheduler_status(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)

    with client:
        response = client.get("/api/runtime", headers={"X-API-Token": "test-token"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["services"]["monitor_scheduler"]["enabled"] is True
    assert payload["services"]["monitor_scheduler"]["alive"] is True
    assert (
        payload["services"]["monitor_scheduler"]["scheduler_id"]
        == "builtin-monitor-scheduler"
    )
    assert payload["services"]["task_worker"]["enabled"] is False
    assert payload["services"]["notification_retry"]["enabled"] is True
    assert payload["services"]["notification_retry"]["alive"] is True
    assert payload["services"]["notification_digest"]["enabled"] is True
    assert payload["services"]["notification_digest"]["alive"] is True
    assert (
        payload["services"]["notification_digest"]["service_id"]
        == "builtin-notification-digest"
    )


def test_api_runtime_reflects_disabled_builtin_monitor_scheduler(monkeypatch, tmp_path):
    client, _ = _build_test_client(
        monkeypatch,
        tmp_path,
        start_builtin_monitor_scheduler=False,
        start_builtin_notification_retry=False,
        start_builtin_notification_digest=False,
    )

    with client:
        response = client.get("/api/runtime", headers={"X-API-Token": "test-token"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["services"]["monitor_scheduler"]["enabled"] is False
    assert payload["services"]["monitor_scheduler"]["alive"] is False
    assert payload["services"]["notification_retry"]["enabled"] is False
    assert payload["services"]["notification_retry"]["alive"] is False
    assert payload["services"]["notification_digest"]["enabled"] is False
    assert payload["services"]["notification_digest"]["alive"] is False


def test_api_notifications_returns_notification_history(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)

    routes_module._task_store.create_notification_event(
        monitor_id="mon-000001",
        task_id="task-000001",
        channel_type="webhook",
        target="https://example.com/webhook",
        event_type="monitor_alert",
        status="retry_pending",
        status_message="通知目标限流，计划稍后重试",
        response_code=429,
        error_type="http_error",
        error_message="429",
        payload_snapshot={"event": "smart_extractor.monitor_alert"},
    )

    response = client.get("/api/notifications", headers={"X-API-Token": "test-token"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["notifications"]
    assert payload["notifications"][0]["status"] == "retry_pending"
    assert payload["notifications"][0]["status_label"] == "等待重试"
    assert len(payload["notifications"]) == 1


def test_api_dashboard_returns_aggregated_payload(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)

    routes_module._learned_profile_store.upsert_from_result(
        "https://example.com/product/dashboard",
        page_type="product",
        selected_fields=["title", "price"],
        field_labels={"title": "标题", "price": "价格"},
        strategy="llm",
        completeness=1.0,
    )
    monitor = routes_module._task_store.create_or_update_monitor(
        name="Dashboard 监控",
        url="https://example.com/product/dashboard",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title", "price"],
        field_labels={"title": "标题", "price": "价格"},
        profile={"scenario_label": "价格监控"},
    )
    task = routes_module._task_store.create(
        url="https://example.com/product/dashboard",
        schema_name="auto",
        storage_format="json",
    )
    routes_module._task_store.mark_success(
        task.task_id,
        elapsed_ms=12.0,
        quality_score=0.97,
        data={
            "page_type": "product",
            "field_labels": {"title": "标题", "price": "价格"},
            "data": {"title": "Phone", "price": "99"},
            "extraction_strategy": "rule",
            "learned_profile_id": "lp-000001",
        },
    )
    latest_task = routes_module._task_store.get(task.task_id)
    assert latest_task is not None
    routes_module._task_store.update_monitor_result(monitor.monitor_id, latest_task)
    routes_module._task_store.create_notification_event(
        monitor_id=monitor.monitor_id,
        task_id=task.task_id,
        channel_type="webhook",
        target="https://example.com/webhook",
        event_type="monitor_alert",
        status="retry_pending",
        status_message="等待重试",
    )

    response = client.get(
        "/api/dashboard?notification_status=retry_pending",
        headers={"X-API-Token": "test-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["tasks"]
    assert payload["stats"]["total"] >= 1
    assert payload["insights"]["summary"]["learned_profile_hits"] >= 0
    assert payload["monitors"][0]["monitor_id"] == monitor.monitor_id
    assert payload["notifications"][0]["status"] == "retry_pending"
    assert len(payload["notifications"]) == 1
    assert payload["learned_profiles"][0]["profile_id"] == "lp-000001"
    assert "summary" in payload["notification_digest"]
    assert "ready" in payload["runtime_status"]


def test_api_notification_resend_creates_new_attempt(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    captured = {}

    monitor = routes_module._task_store.create_or_update_monitor(
        name="补发通知测试",
        url="https://example.com/page",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={
            "scenario_label": "补发通知测试",
            "notify_on": ["changed", "error"],
            "webhook_url": "https://example.com/webhook",
        },
    )
    task = routes_module._task_store.create(
        url="https://example.com/page",
        schema_name="auto",
        storage_format="json",
    )
    original_event = routes_module._task_store.create_notification_event(
        monitor_id=monitor.monitor_id,
        task_id=task.task_id,
        channel_type="webhook",
        target="https://example.com/webhook",
        event_type="monitor_alert",
        status="failed",
        status_message="通知请求参数无效（400）",
        attempt_no=1,
        max_attempts=3,
        response_code=400,
        error_type="http_error",
        error_message="400",
        payload_snapshot={"event": "smart_extractor.monitor_alert", "task": {"task_id": task.task_id}},
    )

    def fake_send_monitor_notification(
        monitor_payload,
        task_payload,
        timeout_seconds=10.0,
        **kwargs,
    ):
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

    monkeypatch.setattr(
        routes_module,
        "send_monitor_notification",
        fake_send_monitor_notification,
    )

    response = client.post(
        f"/api/notifications/{original_event.notification_id}/resend",
        headers={"X-API-Token": "test-token"},
        json={"reason": "人工补发验证"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["notification"]["retry_of_notification_id"] == original_event.notification_id
    assert payload["notification"]["triggered_by"] == "manual"
    assert captured["kwargs"]["attempt_no"] == 2
    assert captured["kwargs"]["payload_override"]["event"] == "smart_extractor.monitor_alert"

    latest_monitor = routes_module._task_store.get_monitor(monitor.monitor_id)
    assert latest_monitor is not None
    assert latest_monitor.last_notification_status == "sent"


def test_api_notification_digest_send_dispatches_digest(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)

    routes_module._task_store.create_or_update_monitor(
        name="Digest 发送监控",
        url="https://example.com/digest-send",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"webhook_url": "https://example.com/webhook"},
    )
    captured = {}

    def fake_send_monitor_notification(
        monitor_payload,
        task_payload,
        timeout_seconds=10.0,
        **kwargs,
    ):
        captured["payload"] = kwargs["payload_override"]
        captured["target"] = kwargs["target_override"]
        return NotificationDeliveryResult(
            status="sent",
            message="通知已发送到 https://example.com/webhook",
            target="https://example.com/webhook",
            payload_snapshot=kwargs["payload_override"],
            response_code=200,
            sent_at="2026-04-18 10:00:00",
            attempt_no=1,
            max_attempts=1,
        )

    monkeypatch.setattr(
        routes_module,
        "send_monitor_notification",
        fake_send_monitor_notification,
    )

    response = client.post(
        "/api/notifications/digest/send?window_hours=24",
        headers={"X-API-Token": "test-token"},
        json={"reason": "人工发送日报"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["notifications"][0]["event_type"] == "daily_digest"
    assert captured["payload"]["event"] == "smart_extractor.daily_digest"
    assert captured["target"] == "https://example.com/webhook"


def test_api_save_monitor_supports_notification_channels(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)

    response = client.post(
        "/api/monitors",
        headers={"X-API-Token": "test-token"},
        json={
            "name": "多通道监控",
            "url": "https://example.com/channel-config",
            "schema_name": "auto",
            "storage_format": "json",
            "use_static": True,
            "schedule_enabled": False,
            "schedule_interval_minutes": 60,
            "selected_fields": ["title"],
            "field_labels": {"title": "标题"},
            "profile": {
                "scenario_label": "多通道运营通知",
                "webhook_url": "https://example.com/webhook",
                "notification_channels": [
                    {
                        "channel_type": "slack",
                        "name": "运营群",
                        "target": "https://example.com/slack-hook",
                    }
                ],
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["monitor"]["profile"]["webhook_url"] == "https://example.com/webhook"
    assert len(payload["monitor"]["profile"]["notification_channels"]) == 2
    assert payload["monitor"]["notification_channel_count"] == 2


def test_api_run_monitor_reuses_existing_running_task(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    monitor = routes_module._task_store.create_or_update_monitor(
        name="复用运行中任务",
        url="https://example.com/reuse-running-task",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"scenario_label": "手动检查防重"},
    )
    pending_task = routes_module._task_store.create(
        url=monitor.url,
        schema_name="monitor",
        storage_format="json",
        request_id="existing-run",
    )
    routes_module._task_store.mark_monitor_run_scheduled(
        monitor.monitor_id,
        task_id=pending_task.task_id,
        trigger_source="manual",
    )

    before_tasks = routes_module._task_store.list_all(limit=20)
    response = client.post(
        f"/api/monitors/{monitor.monitor_id}/run",
        headers={"X-API-Token": "test-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["task_id"] == pending_task.task_id
    assert payload["reused_existing_task"] is True
    after_tasks = routes_module._task_store.list_all(limit=20)
    assert len(after_tasks) == len(before_tasks)


def test_monitor_run_dispatches_notifications_to_multiple_channels(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    sent_targets = []

    monitor = routes_module._task_store.create_or_update_monitor(
        name="多通道投递监控",
        url="https://example.com/multi-channel",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title", "summary"],
        field_labels={"title": "标题", "summary": "总结"},
        profile={
            "scenario_label": "多通道变化监控",
            "notify_on": ["stable", "changed", "error"],
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

    def fake_run_extraction(
        task_id: str,
        schema_name: str = "auto",
        use_static: bool = False,
        selected_fields=None,
        monitor_id: str = "",
        force_strategy: str = "",
    ):
        routes_module._task_store.mark_success(
            task_id=task_id,
            elapsed_ms=32.0,
            quality_score=0.93,
            data={
                "page_type": "article",
                "field_labels": {"summary": "总结"},
                "data": {"summary": "更新后的卖点"},
                "extraction_strategy": "rule",
            },
        )
        latest_task = routes_module._task_store.get(task_id)
        assert latest_task is not None
        routes_module._task_store.update_monitor_result(monitor_id, latest_task)
        routes_module._sync_monitor_notification(monitor_id, task_id)

    def fake_send_monitor_notification(
        monitor_payload,
        task_payload,
        timeout_seconds=10.0,
        **kwargs,
    ):
        sent_targets.append(
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

    monkeypatch.setattr(routes_module, "_run_extraction", fake_run_extraction)
    monkeypatch.setattr(
        routes_module, "send_monitor_notification", fake_send_monitor_notification
    )

    response = client.post(
        f"/api/monitors/{monitor.monitor_id}/run",
        headers={"X-API-Token": "test-token"},
    )

    assert response.status_code == 200
    for _ in range(50):
        if len(sent_targets) == 2:
            break
        time.sleep(0.02)
    assert sorted(sent_targets) == [
        ("slack", "https://example.com/slack-hook"),
        ("webhook", "https://example.com/webhook"),
    ]
    alert_events = []
    for _ in range(50):
        events = routes_module._task_store.list_notification_events(
            limit=10,
            monitor_id=monitor.monitor_id,
        )
        alert_events = [item for item in events if item.event_type == "monitor_alert"]
        if len(alert_events) == 2:
            break
        time.sleep(0.02)
    assert len(alert_events) == 2
