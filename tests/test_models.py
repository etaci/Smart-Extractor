"""
数据模型层单元测试

测试 BaseExtractModel、Schema 注册表、YAML 动态加载等。
"""

import pytest
from typing import List
from pathlib import Path
from pydantic import Field

from smart_extractor.models.base import BaseExtractModel, ExtractionMeta
from smart_extractor.models.news import NewsArticle
from smart_extractor.models.product import ProductDetail
from smart_extractor.models.job import JobPosting
from smart_extractor.models.custom import SchemaRegistry, load_schema_from_yaml


# ===== BaseExtractModel 测试 =====

class TestBaseExtractModel:
    """BaseExtractModel 基础功能测试"""

    def test_completeness_all_filled(self, sample_article):
        """测试全部填充时的完整度"""
        score = sample_article.completeness_score()
        assert score > 0.8

    def test_completeness_all_empty(self, empty_article):
        """测试全部为空时的完整度"""
        score = empty_article.completeness_score()
        assert score < 0.3

    def test_to_flat_dict_without_meta(self, sample_article):
        """测试扁平化字典（无元数据）"""
        d = sample_article.to_flat_dict()
        assert "title" in d
        assert "author" in d
        assert d["title"] == "测试文章"

    def test_to_flat_dict_with_meta(self, sample_article, sample_meta):
        """测试扁平化字典（带元数据）"""
        d = sample_article.to_flat_dict(meta=sample_meta)
        assert "_source_url" in d
        assert "_extracted_at" in d
        assert d["_source_url"] == "https://example.com/test"


class TestExtractionMeta:
    """ExtractionMeta 测试"""

    def test_default_values(self):
        """测试默认值"""
        meta = ExtractionMeta()
        assert meta.source_url == ""
        assert meta.extractor_model == ""
        assert meta.confidence_score == 0.0

    def test_custom_values(self, sample_meta):
        """测试自定义值"""
        assert sample_meta.source_url == "https://example.com/test"
        assert sample_meta.extractor_model == "test-model"
        assert sample_meta.raw_text_length == 500


# ===== 预置 Schema 测试 =====

class TestPrebuiltSchemas:
    """预置 Schema 模型字段测试"""

    def test_news_article_fields(self):
        """测试 NewsArticle 字段"""
        article = NewsArticle(title="测试新闻", content="测试内容")
        assert article.title == "测试新闻"
        assert article.content == "测试内容"
        assert hasattr(article, "author")
        assert hasattr(article, "publish_date")
        assert hasattr(article, "tags")

    def test_product_detail_fields(self):
        """测试 ProductDetail 字段"""
        product = ProductDetail(name="测试商品")
        assert product.name == "测试商品"
        assert hasattr(product, "price")
        assert hasattr(product, "brand")
        assert hasattr(product, "rating")

    def test_job_posting_fields(self):
        """测试 JobPosting 字段"""
        job = JobPosting(title="Python 工程师")
        assert job.title == "Python 工程师"
        assert hasattr(job, "company")
        assert hasattr(job, "salary_range")
        assert hasattr(job, "requirements")

    def test_news_article_defaults(self):
        """测试 NewsArticle 可选字段的默认值"""
        article = NewsArticle(title="标题", content="内容")
        assert article.author == ""
        assert article.tags == []
        assert article.summary == ""

    def test_schema_inheritance(self):
        """测试 Schema 继承自 BaseExtractModel"""
        assert issubclass(NewsArticle, BaseExtractModel)
        assert issubclass(ProductDetail, BaseExtractModel)
        assert issubclass(JobPosting, BaseExtractModel)


# ===== Schema 注册表测试 =====

class TestSchemaRegistry:
    """SchemaRegistry 核心功能测试"""

    def test_builtin_schemas(self):
        """测试内置 Schema 已注册"""
        registry = SchemaRegistry()
        schemas = registry.list_schemas()
        assert "news" in schemas
        assert "product" in schemas
        assert "job" in schemas

    def test_get_builtin(self):
        """测试获取内置 Schema"""
        registry = SchemaRegistry()
        news = registry.get("news")
        assert news is not None
        assert news.__name__ == "NewsArticle"

    def test_get_case_insensitive(self):
        """测试名称不区分大小写"""
        registry = SchemaRegistry()
        assert registry.get("NEWS") is not None
        assert registry.get("News") is not None
        assert registry.get("news") is not None

    def test_get_nonexistent(self):
        """测试获取不存在的 Schema"""
        registry = SchemaRegistry()
        assert registry.get("nonexistent") is None

    def test_register_custom(self):
        """测试注册自定义 Schema"""

        class CustomModel(BaseExtractModel):
            custom_field: str = ""

        registry = SchemaRegistry()
        registry.register("custom", CustomModel)
        assert registry.get("custom") is CustomModel

    def test_load_from_directory(self, schema_dir):
        """测试从目录加载 YAML Schema"""
        registry = SchemaRegistry()
        count = registry.load_from_directory(schema_dir)
        assert count > 0
        assert registry.get("test_person") is not None

    def test_load_from_nonexistent_dir(self, tmp_path):
        """测试加载不存在的目录"""
        registry = SchemaRegistry()
        count = registry.load_from_directory(tmp_path / "nope")
        assert count == 0


# ===== YAML Schema 动态加载测试 =====

class TestYAMLSchemaLoader:
    """load_schema_from_yaml 功能测试"""

    def test_load_valid_yaml(self, schema_dir):
        """测试加载有效 YAML"""
        yaml_path = schema_dir / "test_person.yaml"
        model = load_schema_from_yaml(yaml_path)
        assert model.__name__ == "TestPerson"
        # 验证字段存在
        assert "name" in model.model_fields
        assert "age" in model.model_fields
        assert "hobbies" in model.model_fields

    def test_instantiate_loaded_model(self, schema_dir):
        """测试实例化动态加载的模型"""
        yaml_path = schema_dir / "test_person.yaml"
        Model = load_schema_from_yaml(yaml_path)
        instance = Model(name="测试用户")
        assert instance.name == "测试用户"
        assert instance.age == 0
        assert instance.hobbies == []

    def test_load_nonexistent_file(self):
        """测试加载不存在的文件"""
        with pytest.raises(FileNotFoundError):
            load_schema_from_yaml("/no/such/file.yaml")

    def test_load_invalid_yaml(self, tmp_path):
        """测试加载无效 YAML（缺少 fields）"""
        bad_yaml = tmp_path / "bad.yaml"
        bad_yaml.write_text("name: Bad\n", encoding="utf-8")
        with pytest.raises(ValueError, match="fields"):
            load_schema_from_yaml(bad_yaml)
