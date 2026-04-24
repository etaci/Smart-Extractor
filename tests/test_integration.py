"""
端到端 Pipeline 集成测试。

通过 mock fetcher 和 extractor 覆盖抓取、清洗、校验、存储的主流程。
"""

import json
from pathlib import Path
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from pydantic import Field

from smart_extractor.config import (
    AppConfig,
    CleanerConfig,
    FetcherConfig,
    LLMConfig,
    LogConfig,
    SchedulerConfig,
    StorageConfig,
)
from smart_extractor.fetcher.base import FetchResult
from smart_extractor.models.base import BaseExtractModel
from smart_extractor.pipeline import ExtractionPipeline


class NewsArticle(BaseExtractModel):
    title: str = Field(default="", description="标题")
    author: str = Field(default="", description="作者")
    content: str = Field(default="", description="正文")
    tags: List[str] = Field(default_factory=list, description="标签")


class ProductDetail(BaseExtractModel):
    name: str = Field(default="", description="商品名称")
    price: float = Field(default=0.0, description="价格")
    brand: str = Field(default="", description="品牌")


@pytest.fixture
def tmp_config(tmp_path):
    return AppConfig(
        llm=LLMConfig(api_key="test-key", model="gpt-4o-mini"),
        fetcher=FetcherConfig(timeout=5000),
        cleaner=CleanerConfig(max_text_length=4000),
        storage=StorageConfig(output_dir=str(tmp_path / "output")),
        scheduler=SchedulerConfig(
            max_concurrency=2,
            request_delay_min=0.0,
            request_delay_max=0.0,
        ),
        log=LogConfig(level="WARNING", log_dir=str(tmp_path / "logs")),
    )


@pytest.fixture
def news_html():
    return """
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head><title>测试新闻</title></head>
    <body>
        <h1>Python 3.13 正式发布</h1>
        <p>作者：李四 | 2026-03-24</p>
        <article>
            Python 3.13 带来了全新的 JIT 编译器，性能提升明显。
            开发者社区对此表示欢迎。
        </article>
        <ul><li>JIT</li><li>性能</li><li>Python</li></ul>
    </body>
    </html>
    """


def _make_pipeline_with_mocks(config, html, extracted_model):
    mock_fetcher = MagicMock()
    mock_fetcher.fetch.return_value = FetchResult(
        url="https://example.com",
        html=html,
        status_code=200,
    )
    mock_fetcher.close = MagicMock()

    with patch("smart_extractor.pipeline.LLMExtractor"):
        pipeline = ExtractionPipeline(config=config, fetcher=mock_fetcher)
        pipeline._extractor = MagicMock()
        pipeline._extractor.extract.return_value = extracted_model
        pipeline._extractor.get_stats.return_value = {}
        return pipeline


def _valid_news_article(title: str) -> NewsArticle:
    return NewsArticle(
        title=title,
        author="测试作者",
        content="这是一段完整的新闻正文，用来保证当前质量校验能够稳定通过。",
        tags=["测试", "集成"],
    )


class TestEndToEndSingle:
    def test_successful_news_extraction_json(self, tmp_config, news_html):
        pipeline = _make_pipeline_with_mocks(
            tmp_config,
            news_html,
            _valid_news_article("Python 3.13 正式发布"),
        )
        pipeline._schema_registry.register("news", NewsArticle)

        result = pipeline.run(
            url="https://example.com/news",
            schema_name="news",
            storage_format="json",
            collection_name="test_news",
        )

        assert result.success
        assert result.data is not None
        assert result.data.title == "Python 3.13 正式发布"
        assert result.validation is not None
        assert result.elapsed_ms > 0

        output_file = Path(tmp_config.storage.output_dir) / "test_news.json"
        assert output_file.exists()
        saved = json.loads(output_file.read_text(encoding="utf-8"))
        assert len(saved) == 1
        assert saved[0]["title"] == "Python 3.13 正式发布"
        pipeline.close()

    def test_successful_extraction_sqlite(self, tmp_config, news_html):
        pipeline = _make_pipeline_with_mocks(
            tmp_config,
            news_html,
            _valid_news_article("SQLite 测试文章"),
        )
        pipeline._schema_registry.register("news", NewsArticle)

        result = pipeline.run(
            url="https://example.com/sqlite",
            schema_name="news",
            storage_format="sqlite",
            collection_name="test_table",
        )

        assert result.success
        assert result.storage_path != ""
        pipeline.close()

    def test_skip_storage(self, tmp_config, news_html):
        pipeline = _make_pipeline_with_mocks(
            tmp_config,
            news_html,
            _valid_news_article("跳过存储"),
        )
        pipeline._schema_registry.register("news", NewsArticle)

        result = pipeline.run(
            url="https://example.com/skip",
            schema_name="news",
            skip_storage=True,
        )

        assert result.success
        assert result.storage_path == ""
        pipeline.close()

    def test_fetch_failure(self, tmp_config):
        mock_fetcher = MagicMock()
        mock_fetcher.fetch.return_value = FetchResult(
            url="https://fail.com",
            html="",
            status_code=503,
            error="服务不可用",
        )
        mock_fetcher.close = MagicMock()

        with patch("smart_extractor.pipeline.LLMExtractor"):
            pipeline = ExtractionPipeline(config=tmp_config, fetcher=mock_fetcher)
            pipeline._schema_registry.register("news", NewsArticle)

        result = pipeline.run(url="https://fail.com", schema_name="news")

        assert not result.success
        assert result.error is not None
        assert "抓取失败" in result.error
        pipeline.close()

    def test_unknown_schema(self, tmp_config, news_html):
        pipeline = _make_pipeline_with_mocks(tmp_config, news_html, NewsArticle())

        result = pipeline.run(
            url="https://example.com",
            schema_name="nonexistent_schema_xyz",
        )

        assert not result.success
        assert "未找到 Schema" in result.error
        pipeline.close()

    def test_repeated_url_runs_are_allowed(self, tmp_config, news_html):
        pipeline = _make_pipeline_with_mocks(
            tmp_config,
            news_html,
            _valid_news_article("重复 URL 测试"),
        )
        pipeline._schema_registry.register("news", NewsArticle)

        url = "https://example.com/dedup"

        first = pipeline.run(url=url, schema_name="news", skip_storage=True)
        second = pipeline.run(url=url, schema_name="news", skip_storage=True)

        assert first.success
        assert second.success
        pipeline.close()

    def test_hook_execution_order(self, tmp_config, news_html):
        order = []
        pipeline = _make_pipeline_with_mocks(
            tmp_config,
            news_html,
            _valid_news_article("钩子测试"),
        )
        pipeline._schema_registry.register("news", NewsArticle)

        pipeline.add_hook("before_fetch", lambda **kw: order.append("before_fetch"))
        pipeline.add_hook("after_fetch", lambda **kw: order.append("after_fetch"))
        pipeline.add_hook("after_clean", lambda **kw: order.append("after_clean"))
        pipeline.add_hook("after_extract", lambda **kw: order.append("after_extract"))

        pipeline.run(url="https://hook.example.com", schema_name="news", skip_storage=True)

        assert order == ["before_fetch", "after_fetch", "after_clean", "after_extract"]
        pipeline.close()


class TestEndToEndBatch:
    def _batch_pipeline(self, config, html, extracted_model):
        return _make_pipeline_with_mocks(config, html, extracted_model)

    def test_batch_all_success(self, tmp_config, news_html):
        pipeline = self._batch_pipeline(
            tmp_config,
            news_html,
            _valid_news_article("批量文章"),
        )
        pipeline._schema_registry.register("news", NewsArticle)

        urls = [
            "https://example.com/a1",
            "https://example.com/a2",
            "https://example.com/a3",
        ]

        results = pipeline.run_batch(
            urls=urls,
            schema_name="news",
            skip_storage=True,
            max_workers=2,
        )

        assert len(results) == 3
        assert all(result.success for result in results)
        pipeline.close()

    def test_batch_dedup_skips_duplicate(self, tmp_config, news_html):
        pipeline = self._batch_pipeline(
            tmp_config,
            news_html,
            _valid_news_article("批量去重"),
        )
        pipeline._schema_registry.register("news", NewsArticle)

        urls = [
            "https://example.com/dup",
            "https://example.com/dup",
        ]

        results = pipeline.run_batch(
            urls=urls,
            schema_name="news",
            skip_storage=True,
            max_workers=1,
        )

        assert len(results) == 2
        assert all(result.success for result in results)
        pipeline.close()

    def test_batch_empty_urls(self, tmp_config, news_html):
        pipeline = self._batch_pipeline(tmp_config, news_html, NewsArticle())
        results = pipeline.run_batch(urls=[], schema_name="news")
        assert results == []
        pipeline.close()

    def test_batch_result_order_preserved(self, tmp_config, news_html):
        pipeline = self._batch_pipeline(
            tmp_config,
            news_html,
            _valid_news_article("顺序测试"),
        )
        pipeline._schema_registry.register("news", NewsArticle)

        urls = [f"https://example.com/page{i}" for i in range(5)]
        results = pipeline.run_batch(
            urls=urls,
            schema_name="news",
            skip_storage=True,
            max_workers=3,
        )

        assert len(results) == 5
        for index, result in enumerate(results):
            assert result.url == urls[index]
        pipeline.close()

    def test_run_batch_async_returns_ordered_results(self, tmp_config, news_html):
        import asyncio

        pipeline = self._batch_pipeline(
            tmp_config,
            news_html,
            _valid_news_article("async 顺序测试"),
        )
        pipeline._schema_registry.register("news", NewsArticle)

        urls = [f"https://example.com/async{i}" for i in range(4)]
        results = asyncio.run(
            pipeline.run_batch_async(
                urls=urls,
                schema_name="news",
                skip_storage=True,
                max_concurrency=2,
            )
        )

        assert len(results) == 4
        for index, result in enumerate(results):
            assert result.url == urls[index]
            assert result.success
        pipeline.close()


class TestStorageIntegration:
    def test_json_accumulation(self, tmp_config, news_html):
        pipeline = _make_pipeline_with_mocks(
            tmp_config,
            news_html,
            _valid_news_article("第一条"),
        )
        pipeline._schema_registry.register("news", NewsArticle)

        pipeline.run(
            url="https://example.com/p1",
            schema_name="news",
            storage_format="json",
            collection_name="acc_test",
        )

        pipeline._extractor.extract.return_value = _valid_news_article("第二条")
        pipeline.run(
            url="https://example.com/p2",
            schema_name="news",
            storage_format="json",
            collection_name="acc_test",
        )

        output_file = Path(tmp_config.storage.output_dir) / "acc_test.json"
        data = json.loads(output_file.read_text(encoding="utf-8"))
        assert len(data) == 2
        titles = [item["title"] for item in data]
        assert "第一条" in titles
        assert "第二条" in titles
        pipeline.close()
