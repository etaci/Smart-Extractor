"""
Pipeline 与 Fetcher 单元测试

使用 mock 避免真实网络请求和 LLM 调用。
"""

import pytest
from unittest.mock import MagicMock, patch
from typing import List
from pydantic import Field

from smart_extractor.config import AppConfig
from smart_extractor.models.base import BaseExtractModel, DynamicExtractResult, ExtractionMeta
from smart_extractor.extractor.learned_profile_store import LearnedProfile, LearnedProfileStore
from smart_extractor.fetcher.base import FetchResult
from smart_extractor.pipeline import ExtractionPipeline, PipelineResult


class MockArticle(BaseExtractModel):
    """测试用文章模型"""

    title: str = Field(default="", description="标题")
    content: str = Field(default="", description="内容")


# ===== PipelineResult 测试 =====


class TestPipelineResult:
    """PipelineResult 数据类测试"""

    def test_default_state(self):
        """测试默认状态"""
        r = PipelineResult()
        assert r.success is False
        assert r.url == ""
        assert r.data is None
        assert r.error is None

    def test_success_summary(self):
        """测试成功结果摘要"""
        r = PipelineResult()
        r.success = True
        r.url = "https://example.com"
        r.elapsed_ms = 1500
        summary = r.summary
        assert "PASS" in summary
        assert "example.com" in summary

    def test_failure_summary(self):
        """测试失败结果摘要"""
        r = PipelineResult()
        r.success = False
        r.url = "https://example.com"
        r.error = "连接超时"
        summary = r.summary
        assert "FAIL" in summary
        assert "连接超时" in summary


# ===== FetchResult 测试 =====


class TestFetchResult:
    """FetchResult 数据模型测试"""

    def test_success_result(self):
        """测试成功的 FetchResult"""
        r = FetchResult(
            url="https://example.com",
            html="<html>content</html>",
            status_code=200,
        )
        assert r.is_success
        assert r.url == "https://example.com"
        assert r.html == "<html>content</html>"

    def test_failure_result(self):
        """测试失败的 FetchResult"""
        r = FetchResult(
            url="https://example.com",
            status_code=0,
            error="连接超时",
        )
        assert not r.is_success
        assert r.error == "连接超时"

    def test_non_200_status(self):
        """测试非 200 状态码"""
        r = FetchResult(
            url="https://example.com",
            html="Not Found",
            status_code=404,
        )
        # 404 也算获取到了页面，is_success 应为 True（有 html 内容）
        # 具体行为取决于实现，这里测试不会报错
        assert r.status_code == 404


# ===== Pipeline Mock 测试 =====


class TestPipelineWithMock:
    """使用 mock 的 Pipeline 集成测试"""

    def _create_mock_pipeline(
        self,
        test_config,
        html_content="<html><body><h1>标题</h1><p>正文</p></body></html>",
    ):
        """创建一个使用 mock fetcher 的 pipeline"""
        mock_fetcher = MagicMock()
        mock_fetcher.fetch.return_value = FetchResult(
            url="https://example.com",
            html=html_content,
            status_code=200,
        )
        mock_fetcher.close.return_value = None

        pipeline = ExtractionPipeline(
            config=test_config,
            fetcher=mock_fetcher,
        )
        return pipeline, mock_fetcher

    @patch("smart_extractor.pipeline.LLMExtractor")
    def test_pipeline_calls_fetcher(self, MockExtractor, test_config):
        """测试 Pipeline 调用了 fetcher"""
        mock_extractor_instance = MagicMock()
        mock_extractor_instance.extract.return_value = MockArticle(
            title="标题", content="正文"
        )
        MockExtractor.return_value = mock_extractor_instance

        pipeline, mock_fetcher = self._create_mock_pipeline(test_config)
        result = pipeline.run(
            url="https://example.com",
            schema_name="news",
            skip_storage=True,
        )
        mock_fetcher.fetch.assert_called_once_with("https://example.com")
        pipeline.close()

    @patch("smart_extractor.pipeline.LLMExtractor")
    def test_pipeline_handles_fetch_failure(self, MockExtractor, test_config):
        """测试 Pipeline 处理抓取失败"""
        mock_fetcher = MagicMock()
        mock_fetcher.fetch.return_value = FetchResult(
            url="https://example.com",
            status_code=0,
            error="连接被拒绝",
        )
        mock_fetcher.close.return_value = None

        pipeline = ExtractionPipeline(
            config=test_config,
            fetcher=mock_fetcher,
        )
        result = pipeline.run(
            url="https://example.com",
            schema_name="news",
            skip_storage=True,
        )
        assert result.success is False
        assert "抓取失败" in result.error
        pipeline.close()

    @patch("smart_extractor.pipeline.LLMExtractor")
    def test_pipeline_invalid_schema(self, MockExtractor, test_config):
        """测试 Pipeline 处理无效 Schema"""
        pipeline, _ = self._create_mock_pipeline(test_config)
        result = pipeline.run(
            url="https://example.com",
            schema_name="nonexistent_schema",
            skip_storage=True,
        )
        assert result.success is False
        assert "未找到 Schema" in result.error
        pipeline.close()

    def test_pipeline_hook_system(self, test_config):
        """测试 Pipeline 钩子系统"""
        hook_called = {"count": 0}

        def my_hook(**kwargs):
            hook_called["count"] += 1

        with patch("smart_extractor.pipeline.LLMExtractor"):
            pipeline, _ = self._create_mock_pipeline(test_config)
            pipeline.add_hook("before_fetch", my_hook)
            pipeline.add_hook("after_fetch", my_hook)

            # 模拟 extractor
            pipeline._extractor = MagicMock()
            pipeline._extractor.extract.return_value = MockArticle(
                title="T", content="C"
            )

            result = pipeline.run(
                url="https://example.com",
                schema_name="news",
                skip_storage=True,
            )

            assert hook_called["count"] >= 2
            pipeline.close()

    @patch("smart_extractor.pipeline.LLMExtractor")
    def test_pipeline_fails_on_verification_page(self, MockExtractor, test_config):
        mock_fetcher = MagicMock()
        mock_fetcher.fetch.return_value = FetchResult(
            url="https://example.com",
            html="<html><body><h1>安全验证</h1><p>当前 IP 地址可能存在异常访问行为，完成验证后即可正常使用。</p></body></html>",
            status_code=200,
        )
        mock_fetcher.close.return_value = None

        pipeline = ExtractionPipeline(
            config=test_config,
            fetcher=mock_fetcher,
        )

        result = pipeline.run(
            url="https://example.com",
            schema_name="auto",
            skip_storage=True,
        )

        assert result.success is False
        assert "安全验证" in result.error
        pipeline.close()

    def test_pipeline_fails_on_shell_page(self, test_config):
        mock_fetcher = MagicMock()
        mock_fetcher.fetch.return_value = FetchResult(
            url="https://example.com",
            html="<html><body>加载中，请稍候</body></html>",
            status_code=200,
            is_shell_page=True,
        )
        mock_fetcher.close.return_value = None

        pipeline = ExtractionPipeline(
            config=test_config,
            fetcher=mock_fetcher,
        )

        result = pipeline.run(
            url="https://example.com",
            schema_name="auto",
            skip_storage=True,
        )

        assert result.success is False
        assert "壳页" in result.error
        pipeline.close()

    def test_pipeline_normalizes_unsupported_image_input_error(self, test_config):
        mock_fetcher = MagicMock()
        mock_fetcher.fetch.return_value = FetchResult(
            url="https://example.com",
            status_code=0,
            error='ERROR: Cannot read "image.png" (this model does not support image input). Inform the user.',
        )
        mock_fetcher.close.return_value = None

        pipeline = ExtractionPipeline(
            config=test_config,
            fetcher=mock_fetcher,
        )

        result = pipeline.run(
            url="https://example.com",
            schema_name="auto",
            skip_storage=True,
        )

        assert result.success is False
        assert "当前模型不支持图片输入" in result.error
        assert "image.png" in result.error
        pipeline.close()

    def test_pipeline_marks_invalid_extraction_as_failure(self, test_config):
        with patch("smart_extractor.pipeline.LLMExtractor"):
            pipeline, _ = self._create_mock_pipeline(test_config)
            pipeline._extractor = MagicMock()

            invalid_result = MagicMock()
            invalid_result.page_type = "listing"
            invalid_result.candidate_fields = []
            invalid_result.selected_fields = [
                "featured_section",
                "category_groups",
                "platform_features",
            ]
            invalid_result.field_labels = {}
            invalid_result.data = {
                "featured_section": "",
                "category_groups": "",
                "platform_features": "",
            }
            invalid_result.formatted_text = ""
            invalid_result.completeness_score.return_value = 0.0

            pipeline._extractor.extract_dynamic.return_value = invalid_result

            result = pipeline.run(
                url="https://example.com",
                schema_name="auto",
                skip_storage=True,
            )

            assert result.success is False
            assert result.validation is not None
            assert result.validation.is_valid is False
            assert "质量校验" in result.error
            pipeline.close()

    def test_pipeline_schema_registry(self, test_config):
        """测试 Pipeline 的 Schema 注册表"""
        with patch("smart_extractor.pipeline.LLMExtractor"):
            pipeline, _ = self._create_mock_pipeline(test_config)
            registry = pipeline.get_schema_registry()
            assert "news" in registry.list_schemas()
            assert "product" in registry.list_schemas()
            assert "job" in registry.list_schemas()
            pipeline.close()

    @patch("smart_extractor.pipeline.LLMExtractor")
    def test_pipeline_reuses_learned_profile_before_falling_back_to_llm(
        self, MockExtractor, test_config
    ):
        mock_extractor_instance = MagicMock()
        mock_extractor_instance.extract_dynamic.return_value = DynamicExtractResult(
            page_type="product",
            candidate_fields=["title", "price"],
            selected_fields=["title", "price"],
            field_labels={"title": "标题", "price": "价格"},
            data={"title": "Phone", "price": "99"},
            formatted_text="标题：Phone\n价格：99",
            extraction_strategy="llm",
        )
        MockExtractor.return_value = mock_extractor_instance

        pipeline, _ = self._create_mock_pipeline(test_config)
        pipeline._learned_profile_store = MagicMock()
        pipeline._rule_extractor = MagicMock()

        learned_profile = LearnedProfile(
            profile_id="lp-000001",
            domain="example.com",
            path_prefix="/product/1",
            page_type="product",
            selected_fields=["title", "price"],
            field_labels={"title": "标题", "price": "价格"},
            sample_url="https://example.com/product/1",
        )
        pipeline._learned_profile_store.find_best_match.side_effect = [
            None,
            learned_profile,
        ]
        pipeline._learned_profile_store.upsert_from_result.return_value = learned_profile
        pipeline._rule_extractor.extract.return_value = DynamicExtractResult(
            page_type="product",
            candidate_fields=["title", "price"],
            selected_fields=["title", "price"],
            field_labels={"title": "标题", "price": "价格"},
            data={"title": "Phone", "price": "79"},
            formatted_text="标题：Phone\n价格：79",
            extraction_strategy="rule",
            learned_profile_id="lp-000001",
            strategy_details={"profile_id": "lp-000001"},
        )

        first_result = pipeline.run(
            url="https://example.com/product/1",
            schema_name="auto",
            skip_storage=True,
            selected_fields=["title", "price"],
        )
        second_result = pipeline.run(
            url="https://example.com/product/1",
            schema_name="auto",
            skip_storage=True,
            selected_fields=["title", "price"],
        )

        assert first_result.success is True
        assert second_result.success is True
        assert mock_extractor_instance.extract_dynamic.call_count == 1
        pipeline._rule_extractor.extract.assert_called_once()
        assert second_result.data is not None
        assert second_result.data.extraction_strategy == "rule"
        assert second_result.data.learned_profile_id == "lp-000001"
        pipeline.close()

    def test_learned_profile_store_skips_disabled_profiles(self, tmp_path):
        store = LearnedProfileStore(tmp_path / "learned_profiles.json")
        store.upsert_from_result(
            "https://example.com/product/3",
            page_type="product",
            selected_fields=["title", "price"],
            field_labels={"title": "标题", "price": "价格"},
            strategy="llm",
            completeness=1.0,
        )
        store.set_profile_active(
            "lp-000001",
            is_active=False,
            reason="规则命中不稳定",
        )

        assert (
            store.find_best_match(
                "https://example.com/product/3", ["title", "price"]
            )
            is None
        )

    @patch("smart_extractor.pipeline.LLMExtractor")
    def test_pipeline_force_llm_skips_rule_reuse(self, MockExtractor, test_config):
        mock_extractor_instance = MagicMock()
        mock_extractor_instance.extract_dynamic.return_value = DynamicExtractResult(
            page_type="product",
            candidate_fields=["title", "price"],
            selected_fields=["title", "price"],
            field_labels={"title": "标题", "price": "价格"},
            data={"title": "Phone", "price": "99"},
            formatted_text="标题：Phone\n价格：99",
            extraction_strategy="llm",
        )
        MockExtractor.return_value = mock_extractor_instance

        pipeline, _ = self._create_mock_pipeline(test_config)
        pipeline._learned_profile_store = MagicMock()
        pipeline._rule_extractor = MagicMock()
        pipeline._learned_profile_store.find_best_match.return_value = LearnedProfile(
            profile_id="lp-000001",
            domain="example.com",
            path_prefix="/product/8",
            page_type="product",
            selected_fields=["title", "price"],
            field_labels={"title": "标题", "price": "价格"},
            sample_url="https://example.com/product/8",
        )
        pipeline._learned_profile_store.upsert_from_result.return_value = LearnedProfile(
            profile_id="lp-000001",
            domain="example.com",
            path_prefix="/product/8",
            page_type="product",
            selected_fields=["title", "price"],
            field_labels={"title": "标题", "price": "价格"},
            sample_url="https://example.com/product/8",
        )

        result = pipeline.run(
            url="https://example.com/product/8",
            schema_name="auto",
            skip_storage=True,
            selected_fields=["title", "price"],
            force_strategy="llm",
        )

        assert result.success is True
        pipeline._rule_extractor.extract.assert_not_called()
        assert mock_extractor_instance.extract_dynamic.call_count == 1
        pipeline.close()
