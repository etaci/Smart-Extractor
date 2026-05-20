from smart_extractor.config import FetcherConfig
from smart_extractor.fetcher.base import FetchResult
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
