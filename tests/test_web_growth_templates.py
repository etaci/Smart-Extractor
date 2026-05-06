from tests.web_route_testkit import _build_test_client


def _headers() -> dict[str, str]:
    return {"X-API-Token": "test-token"}


def _create_success_task(routes_module, *, url: str, page_type: str, data: dict, selected_fields: list[str], field_labels: dict[str, str], quality_score: float = 0.95):
    task = routes_module._task_store.create(
        url=url,
        schema_name="auto",
        storage_format="json",
    )
    routes_module._task_store.mark_success(
        task.task_id,
        elapsed_ms=24.0,
        quality_score=quality_score,
        data={
            "page_type": page_type,
            "selected_fields": selected_fields,
            "field_labels": field_labels,
            "data": data,
        },
    )
    return task


def test_api_template_market_returns_three_core_packages(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)

    response = client.get("/api/template_market", headers=_headers())

    assert response.status_code == 200
    templates = response.json()["templates"]
    assert {item["template_id"] for item in templates} == {
        "market-product-monitor",
        "market-job-compare",
        "market-policy-watch",
    }
    assert all(item["package_strength"] == "core" for item in templates)
    events = routes_module._task_store.list_funnel_events(limit=10, tenant_id="default")
    assert any(item.stage == "template_market_list" for item in events)


def test_task_detail_includes_growth_entry_for_success_task(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    task = _create_success_task(
        routes_module,
        url="https://example.com/product/1",
        page_type="product",
        data={"name": "Phone", "price": "99", "brand": "OpenAI", "availability": "in stock"},
        selected_fields=["name", "price", "brand", "availability"],
        field_labels={
            "name": "商品名称",
            "price": "价格",
            "brand": "品牌",
            "availability": "库存状态",
        },
    )

    response = client.get(f"/api/task/{task.task_id}", headers=_headers())

    assert response.status_code == 200
    growth_entry = response.json()["growth_entry"]
    assert growth_entry["eligible"] is True
    assert growth_entry["recommended_template_package_id"] == "market-product-monitor"
    assert growth_entry["recommended_template_package_name"]
    assert growth_entry["template_draft"]["selected_fields"] == [
        "name",
        "price",
        "brand",
        "availability",
    ]
    assert growth_entry["monitor_draft"]["schedule_interval_minutes"] == 120
    assert growth_entry["monitor_draft"]["profile"]["notification_strategy_version"] == "v1"
    assert growth_entry["monitor_draft"]["profile"]["notification_setup_status"] == "pending_channel"
    assert growth_entry["monitor_draft"]["profile"]["notification_defaults"]["suggested_channel_types"] == [
        "webhook"
    ]
    events = routes_module._task_store.list_funnel_events(limit=10, tenant_id="default")
    assert any(
        item.stage == "growth_entry_exposed" and item.task_id == task.task_id for item in events
    )


def test_api_task_can_promote_success_task_to_template(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    task = _create_success_task(
        routes_module,
        url="https://example.com/jobs/backend",
        page_type="job",
        data={
            "title": "后端工程师",
            "company": "OpenAI",
            "salary_range": "30k-50k",
            "location": "上海",
            "requirements": "Python/FastAPI",
        },
        selected_fields=["title", "company", "salary_range", "location", "requirements"],
        field_labels={
            "title": "岗位名称",
            "company": "公司",
            "salary_range": "薪资范围",
            "location": "工作地点",
            "requirements": "任职要求",
        },
    )

    response = client.post(
        f"/api/task/{task.task_id}/template",
        headers=_headers(),
        json={"name": "招聘岗位监控模板"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["recommended_template_package_id"] == "market-job-compare"
    assert payload["template"]["template_id"].startswith("tpl-")
    assert payload["template"]["name"] == "招聘岗位监控模板"
    assert payload["template"]["profile"]["growth_stage"] == "task_to_template"
    events = routes_module._task_store.list_funnel_events(limit=10, tenant_id="default")
    assert any(
        item.stage == "task_promote_template" and item.template_id == payload["template"]["template_id"]
        for item in events
    )


def test_api_task_can_promote_success_task_to_template_and_monitor(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    task = _create_success_task(
        routes_module,
        url="https://example.com/policy/notice",
        page_type="news",
        data={
            "title": "关于业务调整的公告",
            "publish_date": "2026-05-06",
            "content": "正文",
            "summary": "摘要",
        },
        selected_fields=["title", "publish_date", "content", "summary"],
        field_labels={
            "title": "标题",
            "publish_date": "发布日期",
            "content": "正文",
            "summary": "摘要",
        },
    )

    response = client.post(
        f"/api/task/{task.task_id}/template",
        headers=_headers(),
        json={
            "name": "公告监控模板",
            "create_monitor": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["template"]["template_id"].startswith("tpl-")
    assert payload["monitor"]["monitor_id"].startswith("mon-")
    assert payload["monitor"]["schedule_enabled"] is True
    assert payload["monitor"]["schedule_interval_minutes"] == 180
    assert payload["monitor"]["profile"]["growth_stage"] == "task_to_monitor"
    assert payload["monitor"]["profile"]["notification_strategy_version"] == "v1"
    assert payload["monitor"]["profile"]["notification_setup_status"] == "pending_channel"
    assert payload["monitor"]["profile"]["notification_defaults"]["channel_required"] is True
    assert len(payload["monitor"]["profile"]["notification_activation_checklist"]) == 3
    events = routes_module._task_store.list_funnel_events(limit=10, tenant_id="default")
    assert any(
        item.stage == "task_promote_monitor" and item.monitor_id == payload["monitor"]["monitor_id"]
        for item in events
    )


def test_api_template_market_install_supports_legacy_alias(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)

    response = client.post(
        "/api/template_market/install",
        headers=_headers(),
        json={"template_id": "market-news-brief"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["template"]["template_id"].startswith("tpl-")
    assert payload["template"]["profile"]["scenario_label"]
    events = routes_module._task_store.list_funnel_events(limit=10, tenant_id="default")
    assert any(
        item.stage == "template_market_install"
        and item.package_id == "market-policy-watch"
        and item.template_id == payload["template"]["template_id"]
        for item in events
    )


def test_api_funnel_returns_summary_and_recent_events(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    task = _create_success_task(
        routes_module,
        url="https://example.com/product/2",
        page_type="product",
        data={"name": "Phone", "price": "99"},
        selected_fields=["name", "price"],
        field_labels={"name": "商品名称", "price": "价格"},
    )

    client.get("/api/task/{0}".format(task.task_id), headers=_headers())
    client.post(
        f"/api/task/{task.task_id}/template",
        headers=_headers(),
        json={"name": "商品价格模板", "create_monitor": True},
    )

    response = client.get("/api/funnel", headers=_headers())

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["total_events"] >= 2
    assert payload["summary"]["by_stage"]["growth_entry_exposed"] >= 1
    assert payload["summary"]["by_stage"]["task_promote_template"] >= 1
    assert any(item["stage"] == "task_promote_monitor" for item in payload["events"])
