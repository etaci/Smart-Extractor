"""
JSON 文件存储

将提取结果保存为格式化的 JSON 文件。
"""

import json
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from smart_extractor.config import StorageConfig
from smart_extractor.models.base import BaseExtractModel, ExtractionMeta
from smart_extractor.storage.base import BaseStorage


class JSONStorage(BaseStorage):
    """
    JSON 文件存储器。

    每个集合对应一个 JSON 文件，数据以列表形式追加。
    """

    def __init__(self, config: Optional[StorageConfig] = None):
        self._config = config or StorageConfig()
        self._output_dir = Path(self._config.output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def _get_file_path(self, collection_name: str) -> Path:
        """获取集合对应的文件路径"""
        safe_name = self._normalize_collection_name(collection_name)
        return self._output_dir / f"{safe_name}.json"

    def save(
        self,
        data: BaseExtractModel | list[BaseExtractModel],
        meta: ExtractionMeta | list[ExtractionMeta] | None = None,
        collection_name: str = "default",
    ) -> str:
        """
        保存提取结果到 JSON 文件。

        Args:
            data: 单条或多条提取结果
            meta: 对应的元数据
            collection_name: 文件名称（不含扩展名）

        Returns:
            保存的文件路径
        """
        filepath = self._get_file_path(collection_name)

        # 统一为列表处理
        data_list = data if isinstance(data, list) else [data]
        meta_list = None
        if meta is not None:
            meta_list = meta if isinstance(meta, list) else [meta]

        # 加载现有数据
        existing = []
        if filepath.exists():
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            except (json.JSONDecodeError, Exception):
                existing = []

        # 追加新数据
        for i, item in enumerate(data_list):
            record = item.model_dump()
            # 合并元数据
            if meta_list and i < len(meta_list):
                record["_meta"] = meta_list[i].model_dump()
                # datetime 序列化
                record["_meta"]["extracted_at"] = record["_meta"]["extracted_at"].isoformat()
            existing.append(record)

        # 写入文件
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)

        logger.info("JSON 存储: {} 条数据 → {}", len(data_list), filepath)
        return str(filepath)

    def load(
        self,
        collection_name: str = "default",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """加载 JSON 文件中的数据"""
        filepath = self._get_file_path(collection_name)

        if not filepath.exists():
            logger.warning("JSON 文件不存在: {}", filepath)
            return []

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        return data[offset:offset + limit]

    def count(self, collection_name: str = "default") -> int:
        """返回 JSON 文件中的数据数量"""
        filepath = self._get_file_path(collection_name)

        if not filepath.exists():
            return 0

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        return len(data)
