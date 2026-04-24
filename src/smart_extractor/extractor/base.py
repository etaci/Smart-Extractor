"""
抽取器抽象接口。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Type

from smart_extractor.models.base import BaseExtractModel, DynamicExtractResult


class BaseExtractor(ABC):
    """结构化抽取器基类。"""

    @abstractmethod
    def extract(
        self,
        text: str,
        schema: Type[BaseExtractModel],
        prompt_template: str | None = None,
    ) -> BaseExtractModel:
        """按固定模型抽取。"""

    @abstractmethod
    def extract_dynamic(
        self,
        text: str,
        source_url: str,
        selected_fields: list[str] | None = None,
    ) -> DynamicExtractResult:
        """自动识别页面类型并抽取动态字段。"""

    @abstractmethod
    def extract_batch(
        self,
        texts: list[str],
        schema: Type[BaseExtractModel],
        *,
        max_workers: int | None = None,
    ) -> list[BaseExtractModel]:
        """批量按固定模型抽取。"""
