import asyncio
import time
from unittest.mock import MagicMock

from smart_extractor.fetcher.base import FetchResult
from smart_extractor.pipeline import ExtractionPipeline


class TrackingSyncFetcher:
    def __init__(self, html: str):
        self._html = html
        self.calls: list[str] = []
        self.closed = False

    def fetch(self, url: str) -> FetchResult:
        time.sleep(0.05)
        self.calls.append(url)
        return FetchResult(url=url, html=self._html, status_code=200)

    def close(self) -> None:
        self.closed = True


class TrackingAsyncFetcher:
    def __init__(self, html: str):
        self._html = html
        self.calls: list[str] = []
        self.closed = False

    async def fetch(self, url: str) -> FetchResult:
        await asyncio.sleep(0.01)
        self.calls.append(url)
        return FetchResult(url=url, html=self._html, status_code=200)

    async def close(self) -> None:
        self.closed = True


def _mock_extractor(sample_article):
    extractor = MagicMock()
    extractor.extract.return_value = sample_article
    extractor.get_stats.return_value = {}
    return extractor


def test_run_batch_uses_worker_fetcher_factory(test_config, sample_article):
    parent_fetcher = MagicMock()
    parent_fetcher.close.return_value = None
    created_fetchers: list[TrackingSyncFetcher] = []
    html = "<html><body><h1>标题</h1><p>正文内容足够长，用于通过清洗与校验。</p></body></html>"

    def factory():
        fetcher = TrackingSyncFetcher(html)
        created_fetchers.append(fetcher)
        return fetcher

    pipeline = ExtractionPipeline(
        config=test_config,
        fetcher=parent_fetcher,
        fetcher_factory=factory,
    )
    pipeline._extractor = _mock_extractor(sample_article)
    pipeline._schema_registry.register("news", type(sample_article))

    results = pipeline.run_batch(
        urls=[f"https://example.com/{i}" for i in range(4)],
        schema_name="news",
        skip_storage=True,
        max_workers=2,
    )

    assert len(results) == 4
    assert all(result.success for result in results)
    assert parent_fetcher.fetch.call_count == 0
    assert 1 <= len(created_fetchers) <= 2
    assert sum(len(fetcher.calls) for fetcher in created_fetchers) == 4
    assert all(fetcher.closed for fetcher in created_fetchers)
    pipeline.close()


def test_run_batch_async_uses_async_fetcher_factory(test_config, sample_article):
    parent_fetcher = MagicMock()
    parent_fetcher.close.return_value = None
    created_fetchers: list[TrackingAsyncFetcher] = []
    html = "<html><body><h1>标题</h1><p>正文内容足够长，用于通过清洗与校验。</p></body></html>"

    def async_factory():
        fetcher = TrackingAsyncFetcher(html)
        created_fetchers.append(fetcher)
        return fetcher

    pipeline = ExtractionPipeline(
        config=test_config,
        fetcher=parent_fetcher,
        async_fetcher_factory=async_factory,
    )
    pipeline._extractor = _mock_extractor(sample_article)
    pipeline._schema_registry.register("news", type(sample_article))

    results = asyncio.run(
        pipeline.run_batch_async(
            urls=[f"https://example.com/async-{i}" for i in range(4)],
            schema_name="news",
            skip_storage=True,
            max_concurrency=2,
        )
    )

    assert len(results) == 4
    assert all(result.success for result in results)
    assert parent_fetcher.fetch.call_count == 0
    assert 1 <= len(created_fetchers) <= 2
    assert sum(len(fetcher.calls) for fetcher in created_fetchers) == 4
    assert all(fetcher.closed for fetcher in created_fetchers)
    pipeline.close()
