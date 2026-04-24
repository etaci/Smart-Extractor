"""
DataValidator 单元测试

测试数据验证、完整度评分、格式校验等功能。
"""

import pytest
from typing import List, Optional
from pydantic import Field

from smart_extractor.validator.data_validator import DataValidator, ValidationResult
from smart_extractor.models.base import BaseExtractModel


class RichModel(BaseExtractModel):
    """用于测试的多字段模型"""
    title: str = Field(default="", description="标题")
    url: str = Field(default="", description="链接")
    publish_date: str = Field(default="", description="发布日期")
    price: str = Field(default="", description="价格")
    content: str = Field(default="", description="内容")
    tags: List[str] = Field(default_factory=list, description="标签")


class TestValidationResult:
    """ValidationResult 基础测试"""

    def test_initial_state(self):
        """测试初始状态"""
        r = ValidationResult()
        assert r.is_valid is True
        assert r.warnings == []
        assert r.errors == []

    def test_add_warning(self):
        """测试添加警告"""
        r = ValidationResult()
        r.add_warning("测试警告")
        assert len(r.warnings) == 1
        assert r.is_valid is True  # 警告不影响有效性

    def test_add_error(self):
        """测试添加错误"""
        r = ValidationResult()
        r.add_error("测试错误")
        assert len(r.errors) == 1
        assert r.is_valid is False  # 错误导致无效

    def test_summary_format(self):
        """测试摘要格式"""
        r = ValidationResult()
        r.completeness_score = 0.8
        r.quality_score = 0.75
        summary = r.summary
        assert "PASS" in summary
        assert "80.0%" in summary


class TestDataValidator:
    """DataValidator 核心功能测试"""

    def setup_method(self):
        self.validator = DataValidator()

    def test_full_data_validation(self):
        """测试完整数据通过验证"""
        data = RichModel(
            title="测试标题",
            url="https://example.com",
            publish_date="2024-01-15",
            price="$29.99",
            content="这是文章内容",
            tags=["tag1", "tag2"],
        )
        result = self.validator.validate(data)
        assert result.is_valid
        assert result.completeness_score > 0.8

    def test_empty_data_validation(self):
        """测试空数据验证"""
        data = RichModel()
        result = self.validator.validate(data)
        # 空数据完整度应该很低
        assert result.completeness_score < 0.3

    def test_partial_data_validation(self):
        """测试部分填充的数据"""
        data = RichModel(title="标题", content="内容")
        result = self.validator.validate(data)
        assert 0.2 < result.completeness_score < 0.8

    def test_required_fields_check(self):
        """测试必填字段校验"""
        data = RichModel(content="有内容但无标题")
        result = self.validator.validate(data, required_fields=["title"])
        assert result.is_valid is False
        assert any("title" in e for e in result.errors)

    def test_url_format_valid(self):
        """测试有效 URL 格式"""
        data = RichModel(url="https://example.com/page")
        result = self.validator.validate(data)
        # 有效 URL 不应产生 URL 相关警告
        url_warnings = [w for w in result.warnings if "url" in w.lower()]
        assert len(url_warnings) == 0

    def test_url_format_invalid(self):
        """测试无效 URL 格式"""
        data = RichModel(url="not-a-valid-url")
        result = self.validator.validate(data)
        assert any("URL" in w or "url" in w for w in result.warnings)

    def test_date_format_valid(self):
        """测试有效日期格式"""
        data = RichModel(publish_date="2024-01-15")
        result = self.validator.validate(data)
        date_warnings = [w for w in result.warnings if "date" in w.lower()]
        assert len(date_warnings) == 0

    def test_date_format_invalid(self):
        """测试无效日期格式"""
        data = RichModel(publish_date="上周三")
        result = self.validator.validate(data)
        assert any("date" in w.lower() for w in result.warnings)

    def test_price_format_valid(self):
        """测试有效价格格式"""
        data = RichModel(price="$29.99")
        result = self.validator.validate(data)
        price_warnings = [w for w in result.warnings if "price" in w.lower()]
        assert len(price_warnings) == 0

    def test_quality_score_range(self):
        """测试质量分范围在 [0, 1]"""
        data = RichModel()
        result = self.validator.validate(data)
        assert 0.0 <= result.quality_score <= 1.0

    def test_quality_score_decrease_with_errors(self):
        """测试错误会降低质量分"""
        full_data = RichModel(
            title="标题", url="https://example.com",
            publish_date="2024-01-01", content="内容",
        )
        empty_data = RichModel()
        result_full = self.validator.validate(full_data)
        result_empty = self.validator.validate(empty_data)
        assert result_full.quality_score > result_empty.quality_score
