"""
CSV 文件存储

将提取结果保存为 CSV 文件，方便 Excel 打开分析。
"""

import csv
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from smart_extractor.config import StorageConfig
from smart_extractor.models.base import BaseExtractModel, ExtractionMeta
from smart_extractor.storage.base import BaseStorage


class CSVStorage(BaseStorage):
    """
    CSV 文件存储器。

    每个集合对应一个 CSV 文件，自动创建表头，支持追加写入。
    使用 utf-8-sig 编码确保 Excel 正确显示中文。
    """

    def __init__(self, config: Optional[StorageConfig] = None):
        self._config = config or StorageConfig()
        self._output_dir = Path(self._config.output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def _get_file_path(self, collection_name: str) -> Path:
        safe_name = self._normalize_collection_name(collection_name)
        return self._output_dir / f"{safe_name}.csv"

    def save(
        self,
        data: BaseExtractModel | list[BaseExtractModel],
        meta: ExtractionMeta | list[ExtractionMeta] | None = None,
        collection_name: str = "default",
    ) -> str:
        """
        保存提取结果到 CSV 文件。

        Args:
            data: 单条或多条提取结果
            meta: 对应的元数据
            collection_name: 文件名称（不含扩展名）

        Returns:
            保存的文件路径
        """
        filepath = self._get_file_path(collection_name)
        data_list = data if isinstance(data, list) else [data]
        meta_list = None
        if meta is not None:
            meta_list = meta if isinstance(meta, list) else [meta]

        # 将数据转为扁平字典
        rows = []
        for i, item in enumerate(data_list):
            m = meta_list[i] if meta_list and i < len(meta_list) else None
            rows.append(item.to_flat_dict(meta=m))

        if not rows:
            logger.warning("没有数据需要保存")
            return str(filepath)

        # 获取所有字段名
        fieldnames = list(rows[0].keys())

        # 判断是否需要写表头（文件不存在或为空）
        write_header = not filepath.exists() or filepath.stat().st_size == 0

        encoding = self._config.csv_encoding

        with open(filepath, "a", encoding=encoding, newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            writer.writerows(rows)

        logger.info("CSV 存储: {} 条数据 → {}", len(rows), filepath)
        return str(filepath)

    def load(
        self,
        collection_name: str = "default",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """加载 CSV 文件中的数据"""
        filepath = self._get_file_path(collection_name)

        if not filepath.exists():
            logger.warning("CSV 文件不存在: {}", filepath)
            return []

        encoding = self._config.csv_encoding
        rows = []
        with open(filepath, "r", encoding=encoding) as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i < offset:
                    continue
                if len(rows) >= limit:
                    break
                rows.append(dict(row))

        return rows

    def count(self, collection_name: str = "default") -> int:
        """返回 CSV 文件中的数据行数（不含表头）"""
        filepath = self._get_file_path(collection_name)

        if not filepath.exists():
            return 0

        encoding = self._config.csv_encoding
        with open(filepath, "r", encoding=encoding) as f:
            # 减去表头行
            return max(0, sum(1 for _ in f) - 1)
