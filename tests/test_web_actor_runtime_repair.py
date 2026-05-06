from __future__ import annotations

from types import SimpleNamespace

from tests.web_route_testkit import _build_test_client


def _headers() -> dict[str, str]:
    return {"X-API-Token": "test-token"}


def test_api_actor_market_install_creates_actor_template_and_monitor(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)

    market_response = client.get("/api/actor_market", headers=_headers())
    response = client.post(
        "/api/actor_market/install",
        headers=_headers(),
        json={
            "actor_id": "actor-product-price-watch",
            "name": "商品价格监控 Actor 安装实例",
            "create_template": True,
            "create_monitor": True,
        },
    )

    assert market_response.status_code == 200
    assert response.status_code == 200
    payload = response.json()
    assert payload["actor"]["actor_id"] == "actor-product-price-watch"
    assert payload["actor"]["actor_instance_id"].startswith("actor-")
    assert payload["template"]["template_id"].startswith("tpl-")
    assert payload["monitor"]["monitor_id"].startswith("mon-")
    assert payload["monitor"]["profile"]["notification_strategy_version"] == "v1"
    assert payload["monitor"]["profile"]["notification_setup_status"] == "pending_channel"
    events = routes_module._task_store.list_funnel_events(limit=10, tenant_id="default")
    assert any(item.stage == "actor_market_list" for item in events)
    assert any(
        item.stage == "actor_market_install"
        and item.actor_instance_id == payload["actor"]["actor_instance_id"]
        for item in events
    )


def test_api_runtime_ops_can_save_worker_proxy_and_site_policy(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)
    headers = _headers()

    worker_response = client.post(
        "/api/workers/heartbeat",
        headers=headers,
        json={
            "worker_id": "worker-node-a",
            "display_name": "Worker A",
            "status": "idle",
            "current_load": 0,
            "capabilities": ["extract", "queue"],
        },
    )
    assert worker_response.status_code == 200
    assert worker_response.json()["worker"]["worker_id"] == "worker-node-a"

    proxy_response = client.post(
        "/api/proxies",
        headers=headers,
        json={
            "name": "住宅代理 A",
            "proxy_url": "http://proxy-a.example.com:8000",
            "provider": "demo",
            "tags": ["residential", "cn"],
        },
    )
    assert proxy_response.status_code == 200
    assert proxy_response.json()["proxy"]["proxy_id"].startswith("proxy-")

    policy_response = client.post(
        "/api/site_policies",
        headers=headers,
        json={
            "domain": "example.com",
            "name": "example.com 策略",
            "min_interval_seconds": 0.0,
            "max_concurrency": 1,
            "use_proxy_pool": True,
            "preferred_proxy_tags": ["residential"],
            "assigned_worker_group": "group-a",
        },
    )
    assert policy_response.status_code == 200
    assert policy_response.json()["policy"]["domain"] == "example.com"

    workers = client.get("/api/workers", headers=headers).json()["workers"]
    proxies = client.get("/api/proxies", headers=headers).json()["proxies"]
    policies = client.get("/api/site_policies", headers=headers).json()["policies"]
    assert any(item["worker_id"] == "worker-node-a" for item in workers)
    assert any(item["provider"] == "demo" for item in proxies)
    assert any(item["domain"] == "example.com" for item in policies)


def test_run_extraction_applies_site_policy_and_proxy_context(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    del client
    captured = {}

    routes_module._task_store.create_or_update_proxy_endpoint(
        name="住宅代理 A",
        proxy_url="http://proxy-a.example.com:8000",
        provider="demo",
        tags=["residential"],
        tenant_id="default",
    )
    routes_module._task_store.create_or_update_site_policy(
        domain="example.com",
        name="example.com",
        min_interval_seconds=0.0,
        max_concurrency=1,
        use_proxy_pool=True,
        preferred_proxy_tags=["residential"],
        assigned_worker_group="group-a",
        tenant_id="default",
    )
    task = routes_module._task_store.create(
        url="https://example.com/product/1",
        schema_name="auto",
        storage_format="json",
        tenant_id="default",
    )

    class _FakeData:
        def model_dump(self):
            return {"data": {"title": "Phone", "price": "99"}}

    class _FakePipeline:
        def __init__(self, *args, **kwargs):
            self._hooks = {}
            captured["proxy_url"] = kwargs["config"].fetcher.proxy_url

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def add_hook(self, name, callback):
            self._hooks.setdefault(name, []).append(callback)

        def run(self, **kwargs):
            for callbacks in self._hooks.values():
                for callback in callbacks:
                    callback()
            return SimpleNamespace(
                success=True,
                data=_FakeData(),
                validation=SimpleNamespace(quality_score=0.93),
                elapsed_ms=12.0,
                extractor_stats={},
                fetch_result=SimpleNamespace(elapsed_ms=6.0, retry_count=0),
            )

    monkeypatch.setattr("smart_extractor.pipeline.ExtractionPipeline", _FakePipeline)

    routes_module._run_extraction(
        task.task_id,
        tenant_id="default",
        schema_name="auto",
        use_static=True,
        worker_id="worker-test-a",
    )

    updated = routes_module._task_store.get(task.task_id, tenant_id="default")
    assert updated is not None
    assert updated.status == "success"
    assert updated.data["_execution_context"]["worker_id"] == "worker-test-a"
    assert updated.data["_execution_context"]["site_policy_id"].startswith("site-")
    assert updated.data["_execution_context"]["proxy_id"].startswith("proxy-")
    assert captured["proxy_url"] == "http://proxy-a.example.com:8000"
    proxies = routes_module._task_store.list_proxy_endpoints(limit=10, tenant_id="default")
    assert proxies[0].success_count >= 1


def test_static_fetcher_builds_httpx_client_with_proxy(monkeypatch):
    from smart_extractor.config import FetcherConfig
    from smart_extractor.fetcher.playwright import PlaywrightFetcher
    from smart_extractor.fetcher.static import StaticFetcher

    captured = {}

    class _FakeClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def close(self):
            return None

    monkeypatch.setattr("smart_extractor.fetcher.static.httpx.Client", _FakeClient)

    fetcher = StaticFetcher(FetcherConfig(proxy_url="http://user:pass@proxy.example.com:9000"))

    client = fetcher._ensure_client()

    assert isinstance(client, _FakeClient)
    assert captured["proxy"] == "http://user:pass@proxy.example.com:9000"
    playwright_fetcher = PlaywrightFetcher(
        FetcherConfig(proxy_url="http://user:pass@proxy.example.com:9000")
    )
    assert playwright_fetcher._build_proxy_options() == {
        "server": "http://proxy.example.com:9000",
        "username": "user",
        "password": "pass",
    }


def test_api_task_annotation_creates_repair_and_updates_template_profile(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    profile = routes_module._learned_profile_store.upsert_from_result(
        "https://example.com/product/2",
        page_type="product",
        selected_fields=["title", "price"],
        field_labels={"title": "标题", "price": "价格"},
        strategy="llm",
        completeness=1.0,
    )
    template = routes_module._task_store.create_or_update_template(
        name="旧模板",
        url="https://example.com/product/2",
        page_type="product",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        profile={"scenario_label": "商品价格监控"},
        tenant_id="default",
    )
    task = routes_module._task_store.create(
        url="https://example.com/product/2",
        schema_name="auto",
        storage_format="json",
        tenant_id="default",
    )
    routes_module._task_store.mark_success(
        task.task_id,
        elapsed_ms=10.0,
        quality_score=0.9,
        data={
            "selected_fields": ["title", "price"],
            "field_labels": {"title": "标题", "price": "价格"},
            "learned_profile_id": profile.profile_id,
            "data": {"title": "Phone", "price": ""},
        },
        tenant_id="default",
    )

    response = client.post(
        f"/api/task/{task.task_id}/annotate",
        headers=_headers(),
        json={
            "profile_id": profile.profile_id,
            "template_id": template.template_id,
            "corrected_data": {"title": "Phone", "price": "99"},
            "field_feedback": {"price": {"issue": "missing"}},
            "notes": "补齐价格字段",
            "apply_auto_repair": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["annotation"]["annotation_id"].startswith("ann-")
    assert payload["repair"]["status"] == "applied"
    assert payload["template"]["template_id"] == template.template_id
    assert "price" in payload["template"]["selected_fields"]
    assert payload["profile"]["auto_repair_count"] >= 1
