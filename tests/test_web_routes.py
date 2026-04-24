"""Web 路由集成测试。"""

from tests.web_route_testkit import _build_test_client


def test_api_requires_token(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)
    response = client.get("/api/dashboard")
    assert response.status_code == 401


def test_api_accepts_token_and_returns_request_id(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)
    response = client.get("/api/dashboard", headers={"X-API-Token": "test-token"})
    assert response.status_code == 200
    assert "X-Request-ID" in response.headers


def test_api_rate_limit(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path, rate_limit=2)
    headers = {"X-API-Token": "test-token"}

    first_response = client.get("/api/dashboard", headers=headers)
    second_response = client.get("/api/dashboard", headers=headers)
    third_response = client.get("/api/dashboard", headers=headers)

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert third_response.status_code == 429


def test_api_rate_limit_honors_trusted_proxy_forwarded_for(monkeypatch, tmp_path):
    client, _ = _build_test_client(
        monkeypatch,
        tmp_path,
        rate_limit=1,
        trusted_proxy_ips=["testclient"],
    )
    headers = {
        "X-API-Token": "test-token",
        "X-Forwarded-For": "198.51.100.8",
    }

    first_response = client.get("/api/dashboard", headers=headers)
    second_response = client.get("/api/dashboard", headers=headers)

    assert first_response.status_code == 200
    assert second_response.status_code == 429


def test_api_blocks_disallowed_host(monkeypatch, tmp_path):
    client, _ = _build_test_client(
        monkeypatch,
        tmp_path,
        allowed_hosts=["allowed.example"],
    )

    response = client.get(
        "/api/dashboard",
        headers={
            "X-API-Token": "test-token",
            "Host": "evil.example",
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "非法 Host 头"


def test_api_rejects_oversized_request_body(monkeypatch, tmp_path):
    client, _ = _build_test_client(
        monkeypatch,
        tmp_path,
        request_max_body_bytes=64,
    )

    response = client.post(
        "/api/extract",
        headers={"X-API-Token": "test-token"},
        json={
            "url": "https://example.com/" + ("a" * 200),
            "schema_name": "auto",
            "storage_format": "json",
        },
    )

    assert response.status_code == 413
    assert "请求体过大" in response.json()["detail"]


def test_task_persistence_across_restart(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)

    def fake_run_extraction(
        task_id: str, use_static: bool = False, selected_fields=None
    ):
        task = routes_module._task_store.get(task_id)
        assert task is not None
        routes_module._task_store.mark_success(
            task_id=task_id,
            elapsed_ms=88.0,
            quality_score=0.95,
            data={"mock": True, "static": use_static},
        )

    monkeypatch.setattr(routes_module, "_run_extraction", fake_run_extraction)

    create_response = client.post(
        "/api/extract",
        headers={"X-API-Token": "test-token"},
        json={
            "url": "https://example.com/article",
            "schema_name": "news",
            "storage_format": "json",
            "use_static": True,
        },
    )
    assert create_response.status_code == 200
    task_id = create_response.json()["task_id"]

    detail_response = client.get(
        f"/api/task/{task_id}", headers={"X-API-Token": "test-token"}
    )
    assert detail_response.status_code == 200
    detail_data = detail_response.json()
    assert detail_data["status"] == "success"
    assert detail_data["progress"]["percent"] == 100.0
    assert detail_data["data"]["mock"] is True
    assert "request_id" not in detail_data
    assert "parent_task_id" not in detail_data

    restarted_client, _ = _build_test_client(monkeypatch, tmp_path)
    restarted_detail = restarted_client.get(
        f"/api/task/{task_id}",
        headers={"X-API-Token": "test-token"},
    )
    assert restarted_detail.status_code == 200
    restarted_data = restarted_detail.json()
    assert restarted_data["status"] == "success"
    assert restarted_data["data"]["mock"] is True


def test_dashboard_payload_includes_history_and_insights(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    call_count = {"value": 0}

    def fake_run_extraction(
        task_id: str, use_static: bool = False, selected_fields=None
    ):
        call_count["value"] += 1
        price = "99" if call_count["value"] == 1 else "79"
        routes_module._task_store.mark_success(
            task_id=task_id,
            elapsed_ms=66.0,
            quality_score=0.96,
            data={
                "page_type": "product",
                "field_labels": {"price": "价格"},
                "data": {"price": price, "title": "Phone"},
            },
        )

    monkeypatch.setattr(routes_module, "_run_extraction", fake_run_extraction)

    headers = {"X-API-Token": "test-token"}
    for _ in range(2):
        response = client.post(
            "/api/extract",
            headers=headers,
            json={
                "url": "https://example.com/product/1",
                "storage_format": "json",
                "use_static": True,
            },
        )
        assert response.status_code == 200
        last_task_id = response.json()["task_id"]

    detail_response = client.get(f"/api/task/{last_task_id}", headers=headers)
    assert detail_response.status_code == 200
    detail_data = detail_response.json()
    assert detail_data["history_summary"]["total_runs"] == 2
    assert detail_data["progress"]["percent"] == 100.0
    assert detail_data["comparison"]["has_previous"] is True
    assert detail_data["comparison"]["changed"] is True
    assert detail_data["comparison"]["changed_fields"][0]["field"] == "price"
    assert "request_id" not in detail_data
    assert "completed_at" not in detail_data

    dashboard_response = client.get("/api/dashboard", headers=headers)
    assert dashboard_response.status_code == 200
    dashboard_data = dashboard_response.json()
    assert dashboard_data["stats"]["total"] == 2
    assert dashboard_data["insights"]["summary"]["repeat_urls"] == 1
    assert dashboard_data["insights"]["summary"]["changed_tasks"] == 1
    assert dashboard_data["insights"]["recent_changes"][0]["task_id"] == last_task_id


def test_dashboard_payload_includes_cost_summary(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    task = routes_module._task_store.create(
        url="https://example.com/costs",
        schema_name="auto",
        storage_format="json",
    )
    routes_module._task_store.mark_success(
        task.task_id,
        elapsed_ms=10.0,
        quality_score=0.95,
        data={
            "extraction_strategy": "rule",
            "learned_profile_id": "lp-000001",
            "_extractor_stats": {
                "total_calls": 2,
                "prompt_tokens": 400,
                "completion_tokens": 80,
                "total_tokens": 480,
                "estimated_cost_usd": 0.0012,
            },
        },
    )

    response = client.get("/api/dashboard", headers={"X-API-Token": "test-token"})
    assert response.status_code == 200
    summary = response.json()["insights"]["summary"]
    assert summary["llm_total_calls"] == 2
    assert summary["llm_total_tokens"] == 480
    assert summary["llm_estimated_cost_usd"] == 0.0012


def test_api_export_supports_markdown_and_json(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    task = routes_module._task_store.create(
        url="https://example.com/export",
        schema_name="auto",
        storage_format="json",
    )
    routes_module._task_store.mark_success(
        task.task_id,
        elapsed_ms=22.0,
        quality_score=0.9,
        data={
            "page_type": "article",
            "data": {"title": "导出标题"},
            "_llm_usage": {
                "total_calls": 1,
                "prompt_tokens": 120,
                "completion_tokens": 30,
                "estimated_cost_usd": 0.000123,
            },
        },
    )

    md_response = client.get(
        f"/api/task/{task.task_id}/export?format=md",
        headers={"X-API-Token": "test-token"},
    )
    assert md_response.status_code == 200
    assert "text/markdown" in md_response.headers["content-type"]
    assert "Smart Extractor" in md_response.text

    json_response = client.get(
        f"/api/task/{task.task_id}/export?format=json",
        headers={"X-API-Token": "test-token"},
    )
    assert json_response.status_code == 200
    payload = json_response.json()
    assert payload["task_id"] == task.task_id


def test_api_batch_returns_and_persists_batch_group_id(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)

    def fake_run_extraction(
        task_id: str, schema_name: str = "auto", use_static: bool = False
    ):
        routes_module._task_store.mark_success(
            task_id=task_id,
            elapsed_ms=20.0,
            quality_score=0.88,
            data={"mock": True},
        )

    monkeypatch.setattr(routes_module, "_run_extraction", fake_run_extraction)

    headers = {"X-API-Token": "test-token"}
    response = client.post(
        "/api/batch",
        headers=headers,
        json={
            "urls": [
                "https://example.com/a",
                "https://example.com/b",
            ],
            "storage_format": "json",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 2
    assert payload["batch_group_id"].startswith("batch-")
    assert payload["task_id"].startswith("task-")

    dashboard_response = client.get(
        f"/api/dashboard?batch_group_id={payload['batch_group_id']}",
        headers=headers,
    )
    assert dashboard_response.status_code == 200
    tasks = dashboard_response.json()["tasks"]
    assert len(tasks) == 1
    assert tasks[0]["batch_group_id"] == payload["batch_group_id"]
    assert tasks[0]["task_kind"] == "batch"
    assert tasks[0]["total_items"] == 2


def test_dashboard_can_filter_by_batch_group_id(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)

    routes_module._task_store.create(
        url="https://example.com/a",
        schema_name="auto",
        storage_format="json",
        batch_group_id="batch-filter01",
    )
    routes_module._task_store.create(
        url="https://example.com/b",
        schema_name="auto",
        storage_format="json",
        batch_group_id="batch-filter02",
    )

    response = client.get(
        "/api/dashboard?batch_group_id=batch-filter01",
        headers={"X-API-Token": "test-token"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["tasks"]) == 1
    assert payload["tasks"][0]["batch_group_id"] == "batch-filter01"


def test_api_batch_can_continue_existing_batch_group(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)

    def fake_run_extraction(
        task_id: str, schema_name: str = "auto", use_static: bool = False
    ):
        routes_module._task_store.mark_success(
            task_id=task_id,
            elapsed_ms=20.0,
            quality_score=0.88,
            data={"mock": True},
        )

    monkeypatch.setattr(routes_module, "_run_extraction", fake_run_extraction)

    headers = {"X-API-Token": "test-token"}
    response = client.post(
        "/api/batch",
        headers=headers,
        json={
            "urls": ["https://example.com/c"],
            "storage_format": "json",
            "batch_group_id": "batch-continue01",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["batch_group_id"] == "batch-continue01"

    dashboard_response = client.get(
        "/api/dashboard?batch_group_id=batch-continue01",
        headers=headers,
    )
    assert dashboard_response.status_code == 200
    tasks = dashboard_response.json()["tasks"]
    assert len(tasks) == 1
    assert tasks[0]["batch_group_id"] == "batch-continue01"
    assert tasks[0]["task_kind"] == "batch"


def test_api_batch_normalizes_and_deduplicates_urls(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)

    def fake_run_extraction(
        task_id: str, schema_name: str = "auto", use_static: bool = False
    ):
        routes_module._task_store.mark_success(
            task_id=task_id,
            elapsed_ms=20.0,
            quality_score=0.88,
            data={"mock": True},
        )

    monkeypatch.setattr(routes_module, "_run_extraction", fake_run_extraction)

    response = client.post(
        "/api/batch",
        headers={"X-API-Token": "test-token"},
        json={
            "urls": [
                " https://example.com/a ",
                "https://example.com/a",
                "https://example.com/b",
            ],
            "storage_format": "json",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 2
