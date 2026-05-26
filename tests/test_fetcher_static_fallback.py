from smart_extractor.config import FetcherConfig
from smart_extractor.fetcher.base import FetchResult
from smart_extractor.fetcher.url_preflight import URLPreflightResult
from smart_extractor.fetcher.static import StaticFetcher


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
