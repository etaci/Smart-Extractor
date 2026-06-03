from smart_extractor.config import FetcherConfig
from smart_extractor.fetcher.base import FetchResult
from smart_extractor.fetcher.url_preflight import URLPreflightResult
from smart_extractor.fetcher.static import StaticFetcher
import httpx


def test_static_fetcher_escalates_retryable_result_to_dynamic(monkeypatch):
    fetcher = StaticFetcher(FetcherConfig(static_fallback_to_dynamic=True))
    previous = FetchResult(
        url="https://example.com",
        status_code=403,
        error="403",
    )

    monkeypatch.setattr(
        fetcher,
        "_fetch_dynamic_fallback",
        lambda url, prior: FetchResult(
            url=url,
            html="<html><body>real content</body></html>",
            status_code=200,
            retry_count=(prior.retry_count if prior else 0) + 1,
        ),
    )

    assert fetcher._should_escalate_to_dynamic(previous) is True
    result = fetcher._fetch_dynamic_fallback("https://example.com", previous)
    assert result.is_success is True
    assert result.retry_count == 1


def test_static_fetcher_can_disable_dynamic_escalation():
    fetcher = StaticFetcher(FetcherConfig(static_fallback_to_dynamic=False))
    result = FetchResult(
        url="https://example.com",
        status_code=403,
        error="403",
    )

    assert fetcher._should_escalate_to_dynamic(result) is False


def test_static_fetcher_does_not_escalate_hard_404():
    fetcher = StaticFetcher(FetcherConfig(static_fallback_to_dynamic=True))
    result = FetchResult(
        url="https://example.com/missing",
        status_code=404,
        html="<html><body>Not found</body></html>",
    )

    assert fetcher._should_escalate_to_dynamic(result) is False


def test_static_fetcher_headers_include_browser_hints():
    fetcher = StaticFetcher(
        FetcherConfig(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            )
        )
    )

    headers = fetcher._build_headers()

    assert headers["User-Agent"].startswith("Mozilla/5.0")
    assert headers["Accept-Encoding"] == "gzip, deflate, br"
    assert headers["sec-ch-ua-platform"] == '"Windows"'


def test_static_fetcher_aborts_unreachable_url_after_preflight(monkeypatch):
    def fake_preflight(url, **kwargs):
        return URLPreflightResult(
            original_url=url,
            final_url=url,
            status_code=404,
            reachable=False,
            reason="http_404",
        )

    monkeypatch.setattr("smart_extractor.fetcher.static.preflight_url", fake_preflight)
    fetcher = StaticFetcher(FetcherConfig(url_preflight_enabled=True))

    result = fetcher.fetch("https://example.com/missing")

    assert result.status_code == 404
    assert result.error == "unreachable_url: http_404"
    assert result.headers["x-smart-url-preflight"] == "unreachable"


def test_static_fetcher_retries_decode_failure_with_identity_encoding(monkeypatch):
    fetcher = StaticFetcher(FetcherConfig(url_preflight_enabled=False))

    class FakeClient:
        def __init__(self):
            self.calls = []

        def get(self, url, headers):
            self.calls.append(dict(headers))
            if len(self.calls) == 1:
                raise httpx.DecodingError("broken br")
            return httpx.Response(
                200,
                request=httpx.Request("GET", url),
                content=b"<html><body>ok</body></html>",
                headers={"content-type": "text/html"},
            )

    fake_client = FakeClient()
    monkeypatch.setattr(fetcher, "_ensure_client", lambda proxy_url=None: fake_client)

    result = fetcher.fetch("https://example.com")

    assert result.is_success is True
    assert fake_client.calls[1]["Accept-Encoding"] == "identity"
    assert result.diagnostics["failure_reason"].startswith("decode_retry")
    assert result.diagnostics["content_type"] == "text/html"
    assert result.diagnostics["response_headers"]["content-type"] == "text/html"
    assert result.diagnostics["request_accept_encoding"] == "gzip, deflate, br"
    assert "utf-8" in result.diagnostics["decode_attempted_charsets"]
    assert "raw_error" in result.diagnostics


def test_static_fetcher_records_shell_markers_and_content_encoding(monkeypatch):
    fetcher = StaticFetcher(FetcherConfig(url_preflight_enabled=False, static_fallback_to_dynamic=False))

    class FakeClient:
        def get(self, url, headers):
            return httpx.Response(
                200,
                request=httpx.Request("GET", url),
                content=b"<html><title>Just a moment</title><body>Checking your browser</body></html>",
                headers={"content-type": "text/html", "content-encoding": "br", "cf-ray": "abc"},
            )

    monkeypatch.setattr(fetcher, "_ensure_client", lambda proxy_url=None: FakeClient())

    result = fetcher.fetch("https://example.com/product/widget")

    assert result.is_shell_page is True
    assert result.diagnostics["content_encoding"] == "br"
    assert result.diagnostics["response_headers"]["content-encoding"] == "br"
    assert result.diagnostics["response_headers"]["cf-ray"] == "abc"
    assert "cloudflare" in result.diagnostics["shell_markers"]


def test_static_fetcher_dynamic_fallback_records_static_decode_context(monkeypatch):
    fetcher = StaticFetcher(FetcherConfig(static_fallback_to_dynamic=True))
    previous = FetchResult(
        url="https://example.com",
        status_code=200,
        html="<html><body>Loading</body></html>",
        diagnostics={
            "failure_reason": "decode_error",
            "content_encoding": "br",
            "raw_error": "DecodingError: broken br",
        },
    )

    class FakeDynamicFetcher:
        def __init__(self, config):
            self.config = config

        def fetch(self, url):
            return FetchResult(
                url=url,
                html="<html><body>real content</body></html>",
                status_code=200,
                diagnostics={"failure_stage": "fetch", "failure_reason": ""},
            )

        def close(self):
            return None

    monkeypatch.setattr("smart_extractor.fetcher.playwright.PlaywrightFetcher", FakeDynamicFetcher)

    result = fetcher._fetch_dynamic_fallback("https://example.com", previous)

    assert result.diagnostics["playwright_fallback"] is True
    assert result.diagnostics["static_failure_reason"] == "decode_error"
    assert result.diagnostics["static_content_encoding"] == "br"
    assert result.diagnostics["static_raw_error"] == "DecodingError: broken br"


def test_static_fetcher_uses_rss_fallback_for_shell_page(monkeypatch):
    fetcher = StaticFetcher(FetcherConfig(url_preflight_enabled=False, static_fallback_to_dynamic=False))

    class FakeClient:
        def get(self, url, headers):
            if str(url).endswith("/article/widget"):
                return httpx.Response(
                    200,
                    request=httpx.Request("GET", url),
                    content=b"<html><body>Loading</body></html>",
                    headers={"content-type": "text/html"},
                )
            return httpx.Response(
                200,
                request=httpx.Request("GET", url),
                content=(
                    "<rss><channel><item>"
                    "<title>Widget News</title>"
                    "<link>https://example.com/article/widget</link>"
                    "<description>Real article body</description>"
                    "</item></channel></rss>"
                ).encode("utf-8"),
                headers={"content-type": "application/rss+xml"},
            )

    monkeypatch.setattr(fetcher, "_ensure_client", lambda proxy_url=None: FakeClient())

    result = fetcher.fetch("https://example.com/article/widget")

    assert result.is_success is True
    assert "Widget News" in result.html
    assert result.headers["x-smart-fetch-rescue"] == "rss_fallback"
    assert result.diagnostics["failure_reason"] == "rss_fallback"
