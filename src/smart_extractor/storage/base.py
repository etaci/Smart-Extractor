"""
存储基类

定义数据持久化的抽象接口。
"""

from abc import ABC, abstractmethod
import re
from typing import Any

from smart_extractor.models.base import BaseExtractModel, ExtractionMeta


class BaseStorage(ABC):
    """
    数据存储抽象基类。

    所有存储实现（CSV、JSON、SQLite）都需要继承此类。
    """

    def _normalize_collection_name(self, collection_name: str) -> str:
        """规范化集合名，避免路径穿越和动态 SQL 标识符注入"""
        raw_name = str(collection_name or "").strip()
        normalized = re.sub(r"[^\w-]+", "_", raw_name, flags=re.UNICODE).strip("._-")
        return normalized or "default"

    @abstractmethod
    def save(
        self,
        data: BaseExtractModel | list[BaseExtractModel],
        meta: ExtractionMeta | list[ExtractionMeta] | None = None,
        collection_name: str = "default",
    ) -> str:
        """
        保存提取结果。

        Args:
            data: 单条或多条提取结果
            meta: 对应的元数据
            collection_name: 集合/表/文件名称

        Returns:
            存储路径或标识
        """
        ...

    @abstractmethod
    def load(
        self,
        collection_name: str = "default",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        加载已保存的数据。

        Args:
            collection_name: 集合/表/文件名称
            limit: 最大返回数量
            offset: 偏移量

        Returns:
            数据字典列表
        """
        ...

    @abstractmethod
    def count(self, collection_name: str = "default") -> int:
        """返回集合中的数据数量"""
        ...
