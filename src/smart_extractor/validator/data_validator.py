"""
数据质量验证器。
"""

from __future__ import annotations

import re
from typing import Optional

from loguru import logger

from smart_extractor.models.base import BaseExtractModel, DynamicExtractResult


class ValidationResult:
    """验证结果。"""

    def __init__(self):
        self.is_valid = True
        self.warnings: list[str] = []
        self.errors: list[str] = []
        self.completeness_score: float = 0.0
        self.quality_score: float = 0.0

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)
        self.is_valid = False

    @property
    def summary(self) -> str:
        status = "[PASS] 通过" if self.is_valid else "[FAIL] 未通过"
        parts = [
            f"验证状态: {status}",
            f"完整度: {self.completeness_score:.1%}",
            f"质量分: {self.quality_score:.1%}",
        ]
        if self.warnings:
            parts.append(f"警告({len(self.warnings)}): " + "; ".join(self.warnings))
        if self.errors:
            parts.append(f"错误({len(self.errors)}): " + "; ".join(self.errors))
        return " | ".join(parts)


class DataValidator:
    """支持固定字段与动态字段结果的通用验证器。"""

    _URL_PATTERN = re.compile(r"^https?://[^\s<>\"]+$", re.IGNORECASE)
    _DATE_PATTERN = re.compile(r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}")
    _PRICE_PATTERN = re.compile(
        r"^[￥$€£]?\s*[\d,]+(?:\.\d+)?(?:\s*[-~至]\s*[￥$€£]?\s*[\d,]+(?:\.\d+)?)?"
    )

    _PAGE_REQUIRED_FIELDS = {
        "news": ["title", "content"],
        "article": ["title", "content"],
        "job": ["title", "company"],
        "product": ["name", "price"],
    }

    def validate(
        self,
        data: BaseExtractModel,
        required_fields: Optional[list[str]] = None,
    ) -> ValidationResult:
        result = ValidationResult()
        result.completeness_score = data.completeness_score()

        if result.completeness_score < 0.3:
            result.add_error(f"数据完整度过低 ({result.completeness_score:.1%})")
        elif result.completeness_score < 0.5:
            result.add_warning(f"数据完整度较低 ({result.completeness_score:.1%})")

        resolved_required = required_fields or self._resolve_required_fields(data)
        self._validate_required_fields(data, result, resolved_required)
        self._validate_formats(data, result)
        result.quality_score = self._calculate_quality_score(result)

        logger.info("数据验证完成: {}", result.summary)
        return result

    def _resolve_required_fields(self, data: BaseExtractModel) -> list[str]:
        if isinstance(data, DynamicExtractResult):
            required = self._PAGE_REQUIRED_FIELDS.get(data.page_type, [])
            # 仅当大模型主动挑了这些内置的关键字段时，我们才要求它们不能为空。不再随便瞎截取前三个字段导致可选内容由于空置引发验证大爆炸
            return [f for f in required if f in (data.selected_fields or [])]
        return []

    def _validate_required_fields(
        self,
        data: BaseExtractModel,
        result: ValidationResult,
        required_fields: list[str],
    ) -> None:
        if not required_fields:
            return

        if isinstance(data, DynamicExtractResult):
            payload = data.data
            for field_name in required_fields:
                if payload.get(field_name) in (None, "", [], {}):
                    result.add_error(f"关键字段 '{field_name}' 为空")
            return

        for field_name in required_fields:
            value = getattr(data, field_name, None)
            if value in (None, "", [], {}):
                result.add_error(f"关键字段 '{field_name}' 为空")

    def _validate_formats(self, data: BaseExtractModel, result: ValidationResult) -> None:
        values = data.data if isinstance(data, DynamicExtractResult) else data.model_dump()

        for field_name, value in values.items():
            if not value or not isinstance(value, str):
                continue

            field_name_lower = field_name.lower()
            if "url" in field_name_lower and not self._URL_PATTERN.match(value):
                result.add_warning(f"字段 '{field_name}' 不是标准 URL: {value[:60]}")
            if "date" in field_name_lower and not self._DATE_PATTERN.match(value):
                result.add_warning(f"字段 '{field_name}' 日期格式可能不标准: {value}")
            if ("price" in field_name_lower or "salary" in field_name_lower) and not self._PRICE_PATTERN.match(value.strip()):
                result.add_warning(f"字段 '{field_name}' 数值格式可能不标准: {value}")

    def _calculate_quality_score(self, result: ValidationResult) -> float:
        score = result.completeness_score
        score -= len(result.errors) * 0.2
        score -= len(result.warnings) * 0.05
        return max(0.0, min(1.0, score))
