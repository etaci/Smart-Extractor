"""
存储基类

定义数据持久化的抽象接口。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
import re
import threading
from typing import Any

from smart_extractor.models.base import BaseExtractModel, ExtractionMeta


class BaseStorage(ABC):
    """数据存储抽象基类。"""

    _RESOURCE_LOCKS: dict[str, threading.RLock] = {}
    _RESOURCE_LOCKS_GUARD = threading.Lock()

    def _normalize_collection_name(self, collection_name: str) -> str:
        """规范化集合名，避免路径穿越和动态 SQL 标识符注入。"""
        raw_name = str(collection_name or "").strip()
        normalized = re.sub(r"[^\w-]+", "_", raw_name, flags=re.UNICODE).strip("._-")
        return normalized or "default"

    @classmethod
    def _lock_for_resource(cls, resource: str | Path) -> threading.RLock:
        key = str(Path(resource).resolve())
        with cls._RESOURCE_LOCKS_GUARD:
            lock = cls._RESOURCE_LOCKS.get(key)
            if lock is None:
                lock = threading.RLock()
                cls._RESOURCE_LOCKS[key] = lock
            return lock

    @abstractmethod
    def save(
        self,
        data: BaseExtractModel | list[BaseExtractModel],
        meta: ExtractionMeta | list[ExtractionMeta] | None = None,
        collection_name: str = "default",
    ) -> str:
        """保存提取结果。"""

    @abstractmethod
    def load(
        self,
        collection_name: str = "default",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """加载已保存的数据。"""

    @abstractmethod
    def count(self, collection_name: str = "default") -> int:
        """返回集合中的数据数量。"""
