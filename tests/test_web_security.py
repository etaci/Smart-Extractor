"""
Web 安全与启动自检测试。
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from smart_extractor.web.security import (
    ApiRateLimiter,
    enforce_api_token,
    enforce_csrf_origin,
    extract_token_from_request,
    resolve_client_key_with_trusted_proxies,
    run_startup_self_check,
)


def _mock_request(headers: dict, method: str = "GET", path: str = "/api/extract"):
    request = SimpleNamespace(
        headers=headers,
        client=SimpleNamespace(host="127.0.0.1"),
        method=method,
        url=SimpleNamespace(path=path),
    )
    return request


def test_extract_token_from_request():
    req1 = _mock_request({"x-api-token": "abc"})
    assert extract_token_from_request(req1) == "abc"

    req2 = _mock_request({"authorization": "Bearer xyz"})
    assert extract_token_from_request(req2) == "xyz"


def test_enforce_api_token_success():
    req = _mock_request({"x-api-token": "secret"})
    enforce_api_token(req, "secret")


def test_enforce_api_token_failure():
    req = _mock_request({"x-api-token": "wrong"})
    with pytest.raises(HTTPException) as exc:
        enforce_api_token(req, "secret")
    assert exc.value.status_code == 401


def test_rate_limiter_blocks():
    limiter = ApiRateLimiter(limit_per_minute=2)
    limiter.check("127.0.0.1")
    limiter.check("127.0.0.1")
    with pytest.raises(HTTPException) as exc:
        limiter.check("127.0.0.1")
    assert exc.value.status_code == 429


def test_resolve_client_key_ignores_forwarded_for_from_untrusted_proxy():
    req = _mock_request(
        {
            "x-forwarded-for": "198.51.100.8",
        }
    )
    req.client.host = "203.0.113.9"

    assert (
        resolve_client_key_with_trusted_proxies(req, trusted_proxy_ips=["127.0.0.1"])
        == "203.0.113.9"
    )


def test_resolve_client_key_accepts_forwarded_for_from_trusted_proxy():
    req = _mock_request(
        {
            "x-forwarded-for": "198.51.100.8, 203.0.113.10",
        }
    )
    req.client.host = "127.0.0.1"

    assert (
        resolve_client_key_with_trusted_proxies(req, trusted_proxy_ips=["127.0.0.1"])
        == "198.51.100.8"
    )


def test_startup_self_check_requires_api_key(test_config):
    test_config.llm.api_key = ""
    test_config.web.api_token = "web-token"
    with pytest.raises(RuntimeError, match="API Key 为空"):
        run_startup_self_check(test_config)


def test_startup_self_check_model_reachable_via_models_api(test_config):
    test_config.web.api_token = "web-token"

    mock_client = MagicMock()
    mock_client.models.retrieve.return_value = {"id": test_config.llm.model}

    with patch("smart_extractor.web.security.OpenAI", return_value=mock_client):
        diagnostics = run_startup_self_check(test_config)

    assert diagnostics["ready"] is True
    assert diagnostics["issues"] == []
    assert diagnostics["warnings"] == []


def test_startup_self_check_model_failure(test_config):
    test_config.web.api_token = "web-token"

    mock_client = MagicMock()
    mock_client.models.retrieve.side_effect = RuntimeError("models endpoint down")
    mock_client.chat.completions.create.side_effect = RuntimeError("model unavailable")

    with patch("smart_extractor.web.security.OpenAI", return_value=mock_client):
        with pytest.raises(RuntimeError, match="模型不可用"):
            run_startup_self_check(test_config)


def test_startup_self_check_accepts_sse_text_response(test_config):
    test_config.web.api_token = "web-token"

    mock_client = MagicMock()
    mock_client.models.retrieve.side_effect = RuntimeError("models endpoint down")
    mock_client.chat.completions.create.return_value = (
        'data: {"choices":[{"delta":{"content":"pong"}}]}\n'
        "data: [DONE]"
    )

    with patch("smart_extractor.web.security.OpenAI", return_value=mock_client):
        diagnostics = run_startup_self_check(test_config)

    assert diagnostics["ready"] is True


def test_csrf_origin_skipped_for_safe_methods():
    req = _mock_request({}, method="GET")
    # 应直接放行，不抛异常
    enforce_csrf_origin(req, api_token_configured=True, allowed_origins=[])


def test_csrf_origin_allows_valid_api_token():
    req = _mock_request(
        {"x-api-token": "any-token"}, method="POST"
    )
    enforce_csrf_origin(req, api_token_configured=True, allowed_origins=[])


def test_csrf_origin_allows_same_origin_browser_post():
    req = _mock_request(
        {"host": "dashboard.example", "origin": "https://dashboard.example"},
        method="POST",
    )
    enforce_csrf_origin(req, api_token_configured=False, allowed_origins=[])


def test_csrf_origin_blocks_cross_origin_post():
    req = _mock_request(
        {
            "host": "dashboard.example",
            "origin": "https://evil.example",
        },
        method="POST",
    )
    with pytest.raises(HTTPException) as exc:
        enforce_csrf_origin(req, api_token_configured=True, allowed_origins=[])
    assert exc.value.status_code == 403


def test_csrf_origin_blocks_post_without_token_or_origin():
    req = _mock_request({"host": "dashboard.example"}, method="POST")
    with pytest.raises(HTTPException) as exc:
        enforce_csrf_origin(req, api_token_configured=True, allowed_origins=[])
    assert exc.value.status_code == 403


def test_csrf_origin_honors_explicit_allowlist():
    req = _mock_request(
        {
            "host": "dashboard.example",
            "origin": "https://partner.example",
        },
        method="POST",
    )
    enforce_csrf_origin(
        req,
        api_token_configured=True,
        allowed_origins=["https://partner.example"],
    )
