from fastapi.testclient import TestClient

from smart_extractor.web.app import create_app
from tests.web_route_testkit import _build_test_client


def test_create_app_serves_dashboard() -> None:
    client = TestClient(create_app())

    response = client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "config/local.yaml" in response.text
    assert "config/default.yaml" in response.text
    assert "dashboard_task_runtime.js" in response.text
    assert "notification-board" in response.text
    assert "monitor-digest-enabled" in response.text
    assert "monitor-digest-hour" in response.text


def test_create_app_with_startup_check_enabled(monkeypatch, tmp_path) -> None:
    client, _ = _build_test_client(monkeypatch, tmp_path)

    response = client.get("/")

    assert response.status_code == 200
    assert "系统已就绪" in response.text


def test_create_app_adds_security_headers(monkeypatch, tmp_path) -> None:
    client, _ = _build_test_client(monkeypatch, tmp_path)

    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]


def test_healthz_and_readyz(monkeypatch, tmp_path) -> None:
    client, _ = _build_test_client(monkeypatch, tmp_path)

    health_response = client.get("/healthz")
    ready_response = client.get("/readyz")

    assert health_response.status_code == 200
    assert health_response.json()["status"] == "ok"
    assert ready_response.status_code == 200
    assert ready_response.json()["status"] == "ready"
