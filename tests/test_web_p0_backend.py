from __future__ import annotations

from smart_extractor.config import load_config
from smart_extractor.web.auth import hash_password
from smart_extractor.web.database_admin import (
    backup_task_store_database,
    restore_task_store_database,
)
from tests.web_route_testkit import _build_test_client

LOGIN_HEADERS = {"Origin": "http://testserver"}


def _create_user(routes_module, *, tenant_id: str, username: str, password: str, role: str) -> None:
    with routes_module._task_store._lock:
        with routes_module._task_store._connect() as conn:
            conn.execute(
                """
                INSERT INTO web_users (
                    user_id, tenant_id, username, password_hash, role,
                    display_name, is_active, created_at, updated_at, last_login_at
                ) VALUES (?, ?, ?, ?, ?, ?, 1, '2026-05-06 10:00:00', '2026-05-06 10:00:00', '')
                """,
                (
                    f"usr-{username}",
                    tenant_id,
                    username,
                    hash_password(password),
                    role,
                    username,
                ),
            )
            conn.commit()


def test_login_and_bearer_auth(monkeypatch, tmp_path):
    client, _ = _build_test_client(
        monkeypatch,
        tmp_path,
        auth_secret_key="auth-secret",
        bootstrap_admin_password="admin-pass",
    )

    login_response = client.post(
        "/api/auth/login",
        headers=LOGIN_HEADERS,
        json={"username": "admin", "password": "admin-pass", "tenant_id": "default"},
    )

    assert login_response.status_code == 200
    token = login_response.json()["access_token"]

    me_response = client.get(
        "/api/auth/me",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert me_response.status_code == 200
    payload = me_response.json()
    assert payload["username"] == "admin"
    assert payload["role"] == "admin"
    assert payload["tenant_id"] == "default"
    assert payload["auth_mode"] == "session"


def test_quality_and_cost_dashboards_are_tenant_scoped(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(
        monkeypatch,
        tmp_path,
        auth_secret_key="auth-secret",
        bootstrap_admin_password="admin-pass",
    )

    template = routes_module._task_store.create_or_update_template(
        name="default-template",
        url="https://example.com/a",
        page_type="article",
        schema_name="auto",
        storage_format="json",
        use_static=True,
        selected_fields=["title"],
        field_labels={"title": "标题"},
        tenant_id="default",
    )
    routes_module._task_store.mark_template_used(template.template_id, tenant_id="default")

    success_task = routes_module._task_store.create(
        url="https://example.com/a",
        schema_name="auto",
        storage_format="json",
        tenant_id="default",
    )
    routes_module._task_store.mark_success(
        success_task.task_id,
        elapsed_ms=12.0,
        quality_score=0.95,
        data={
            "_llm_usage": {
                "prompt_tokens": 100,
                "completion_tokens": 20,
                "total_tokens": 120,
                "estimated_cost_usd": 0.001,
            },
            "_runtime_metrics": {
                "fetcher_type": "playwright",
                "playwright_elapsed_ms": 222.0,
                "retry_count": 1,
                "retry_cost_usd": 0.001,
            },
        },
        tenant_id="default",
    )
    failed_task = routes_module._task_store.create(
        url="https://example.com/b",
        schema_name="auto",
        storage_format="json",
        tenant_id="default",
    )
    routes_module._task_store.mark_failed(
        failed_task.task_id,
        elapsed_ms=11.0,
        error="timeout while fetching",
        tenant_id="default",
    )
    other_tenant_task = routes_module._task_store.create(
        url="https://other.example.com/c",
        schema_name="auto",
        storage_format="json",
        tenant_id="tenant-b",
    )
    routes_module._task_store.mark_success(
        other_tenant_task.task_id,
        elapsed_ms=9.0,
        quality_score=0.99,
        data={"_llm_usage": {"total_tokens": 999, "estimated_cost_usd": 9.99}},
        tenant_id="tenant-b",
    )

    login_response = client.post(
        "/api/auth/login",
        headers=LOGIN_HEADERS,
        json={"username": "admin", "password": "admin-pass", "tenant_id": "default"},
    )
    token = login_response.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}", "Origin": "http://testserver"}

    review_response = client.post(
        f"/api/task/{success_task.task_id}/review",
        headers=headers,
        json={"confirmed": True, "accuracy_score": 0.9, "notes": "looks good"},
    )
    assert review_response.status_code == 200

    quality_response = client.get("/api/quality", headers=headers)
    assert quality_response.status_code == 200
    quality_payload = quality_response.json()
    assert quality_payload["scenario"]["primary"] == "网页变化监控 + 结构化通知"
    assert quality_payload["summary"]["manual_confirmation_rate"] == 1.0
    assert quality_payload["failure_breakdown"][0]["category"] == "timeout"

    cost_response = client.get("/api/cost", headers=headers)
    assert cost_response.status_code == 200
    cost_payload = cost_response.json()
    assert cost_payload["summary"]["total_tasks"] == 2
    assert cost_payload["summary"]["total_tokens"] == 120
    assert cost_payload["summary"]["total_model_cost_usd"] == 0.001
    assert cost_payload["summary"]["total_retry_count"] == 1


def test_export_permission_restricted_for_viewer(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(
        monkeypatch,
        tmp_path,
        auth_secret_key="auth-secret",
        bootstrap_admin_password="admin-pass",
    )
    _create_user(
        routes_module,
        tenant_id="default",
        username="viewer",
        password="viewer-pass",
        role="viewer",
    )
    task = routes_module._task_store.create(
        url="https://example.com/export",
        schema_name="auto",
        storage_format="json",
        tenant_id="default",
    )
    routes_module._task_store.mark_success(
        task.task_id,
        elapsed_ms=10.0,
        quality_score=0.9,
        data={"data": {"title": "demo"}},
        tenant_id="default",
    )

    login_response = client.post(
        "/api/auth/login",
        headers=LOGIN_HEADERS,
        json={"username": "viewer", "password": "viewer-pass", "tenant_id": "default"},
    )
    token = login_response.json()["access_token"]

    export_response = client.get(
        f"/api/task/{task.task_id}/export?format=json",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert export_response.status_code == 403


def test_ops_alerts_and_db_backup_restore(monkeypatch, tmp_path):
    client, routes_module = _build_test_client(monkeypatch, tmp_path)
    headers = {"X-API-Token": "test-token"}

    task = routes_module._task_store.create(
        url="https://example.com/backup",
        schema_name="auto",
        storage_format="json",
    )
    routes_module._task_store.mark_success(
        task.task_id,
        elapsed_ms=8.0,
        quality_score=0.88,
        data={"data": {"title": "backup"}},
    )

    alert_response = client.get("/api/ops/alerts", headers=headers)
    assert alert_response.status_code == 200
    alert_payload = alert_response.json()
    assert alert_payload["summary"]["warning"] >= 1
    assert any("SQLite" in item["message"] or "database_url" in item["message"] for item in alert_payload["alerts"])

    config = load_config()
    backup_path = backup_task_store_database(config)

    routes_module._task_store.mark_failed(
        task.task_id,
        elapsed_ms=1.0,
        error="mutated",
    )
    restore_task_store_database(config, backup_file=backup_path)

    restored_task = routes_module._task_store.get(task.task_id)
    assert restored_task is not None
    assert restored_task.status == "success"
    assert restored_task.data == {"data": {"title": "backup"}}
