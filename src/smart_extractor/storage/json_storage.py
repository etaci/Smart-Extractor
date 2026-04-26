"""
JSON 文件存储。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from smart_extractor.config import StorageConfig
from smart_extractor.models.base import BaseExtractModel, ExtractionMeta
from smart_extractor.storage.base import BaseStorage


class JSONStorage(BaseStorage):
    """将提取结果保存为格式化 JSON 文件。"""

    def __init__(self, config: Optional[StorageConfig] = None):
        self._config = config or StorageConfig()
        self._output_dir = Path(self._config.output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def _get_file_path(self, collection_name: str) -> Path:
        safe_name = self._normalize_collection_name(collection_name)
        return self._output_dir / f"{safe_name}.json"

    def save(
        self,
        data: BaseExtractModel | list[BaseExtractModel],
        meta: ExtractionMeta | list[ExtractionMeta] | None = None,
        collection_name: str = "default",
    ) -> str:
        filepath = self._get_file_path(collection_name)
        lock = self._lock_for_resource(filepath)

        data_list = data if isinstance(data, list) else [data]
        meta_list = meta if isinstance(meta, list) else ([meta] if meta is not None else None)

        with lock:
            existing: list[dict[str, Any]] = []
            if filepath.exists():
                try:
                    existing = json.loads(filepath.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    existing = []

            for index, item in enumerate(data_list):
                record = item.model_dump()
                if meta_list and index < len(meta_list) and meta_list[index] is not None:
                    record["_meta"] = meta_list[index].model_dump()
                    record["_meta"]["extracted_at"] = record["_meta"]["extracted_at"].isoformat()
                existing.append(record)

            filepath.write_text(
                json.dumps(existing, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        logger.info("JSON 存储: {} 条数据 -> {}", len(data_list), filepath)
        return str(filepath)

    def load(
        self,
        collection_name: str = "default",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        filepath = self._get_file_path(collection_name)
        if not filepath.exists():
            logger.warning("JSON 文件不存在: {}", filepath)
            return []

        with self._lock_for_resource(filepath):
            data = json.loads(filepath.read_text(encoding="utf-8"))
        return data[offset : offset + limit]

    def count(self, collection_name: str = "default") -> int:
        filepath = self._get_file_path(collection_name)
        if not filepath.exists():
            return 0

        with self._lock_for_resource(filepath):
            data = json.loads(filepath.read_text(encoding="utf-8"))
        return len(data)
