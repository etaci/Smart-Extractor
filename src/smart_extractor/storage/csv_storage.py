"""
CSV 文件存储。
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from smart_extractor.config import StorageConfig
from smart_extractor.models.base import BaseExtractModel, ExtractionMeta
from smart_extractor.storage.base import BaseStorage


class CSVStorage(BaseStorage):
    """将提取结果保存为 CSV 文件。"""

    def __init__(self, config: Optional[StorageConfig] = None):
        self._config = config or StorageConfig()
        self._output_dir = Path(self._config.output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def _get_file_path(self, collection_name: str) -> Path:
        safe_name = self._normalize_collection_name(collection_name)
        return self._output_dir / f"{safe_name}.csv"

    @staticmethod
    def _normalize_row(row: dict[str, Any], fieldnames: list[str]) -> dict[str, Any]:
        return {field: row.get(field, "") for field in fieldnames}

    def _read_existing_rows(self, filepath: Path) -> tuple[list[str], list[dict[str, Any]]]:
        if not filepath.exists() or filepath.stat().st_size == 0:
            return [], []

        with open(filepath, "r", encoding=self._config.csv_encoding, newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = list(reader.fieldnames or [])
            rows = [dict(row) for row in reader]
        return fieldnames, rows

    def _merge_fieldnames(
        self,
        existing_fieldnames: list[str],
        rows: list[dict[str, Any]],
    ) -> list[str]:
        merged = list(existing_fieldnames)
        for row in rows:
            for field in row.keys():
                if field not in merged:
                    merged.append(field)
        return merged

    def _rewrite_csv(
        self,
        filepath: Path,
        *,
        fieldnames: list[str],
        existing_rows: list[dict[str, Any]],
        new_rows: list[dict[str, Any]],
    ) -> None:
        tmp_path = filepath.with_suffix(filepath.suffix + ".tmp")
        with open(tmp_path, "w", encoding=self._config.csv_encoding, newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self._normalize_row(row, fieldnames) for row in existing_rows)
            writer.writerows(self._normalize_row(row, fieldnames) for row in new_rows)
        tmp_path.replace(filepath)

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

        rows: list[dict[str, Any]] = []
        for index, item in enumerate(data_list):
            current_meta = meta_list[index] if meta_list and index < len(meta_list) else None
            rows.append(item.to_flat_dict(meta=current_meta))

        if not rows:
            logger.warning("没有数据需要保存")
            return str(filepath)

        encoding = self._config.csv_encoding
        with lock:
            existing_fieldnames, existing_rows = self._read_existing_rows(filepath)
            merged_fieldnames = self._merge_fieldnames(existing_fieldnames, rows)

            if not existing_fieldnames:
                with open(filepath, "w", encoding=encoding, newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=merged_fieldnames)
                    writer.writeheader()
                    writer.writerows(
                        self._normalize_row(row, merged_fieldnames) for row in rows
                    )
            elif merged_fieldnames != existing_fieldnames:
                self._rewrite_csv(
                    filepath,
                    fieldnames=merged_fieldnames,
                    existing_rows=existing_rows,
                    new_rows=rows,
                )
            else:
                with open(filepath, "a", encoding=encoding, newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=merged_fieldnames)
                    writer.writerows(
                        self._normalize_row(row, merged_fieldnames) for row in rows
                    )

        logger.info("CSV 存储: {} 条数据 -> {}", len(rows), filepath)
        return str(filepath)

    def load(
        self,
        collection_name: str = "default",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        filepath = self._get_file_path(collection_name)
        if not filepath.exists():
            logger.warning("CSV 文件不存在: {}", filepath)
            return []

        rows = []
        with self._lock_for_resource(filepath):
            with open(filepath, "r", encoding=self._config.csv_encoding, newline="") as handle:
                reader = csv.DictReader(handle)
                for index, row in enumerate(reader):
                    if index < offset:
                        continue
                    if len(rows) >= limit:
                        break
                    rows.append(dict(row))
        return rows

    def count(self, collection_name: str = "default") -> int:
        filepath = self._get_file_path(collection_name)
        if not filepath.exists():
            return 0

        with self._lock_for_resource(filepath):
            with open(filepath, "r", encoding=self._config.csv_encoding) as handle:
                return max(0, sum(1 for _ in handle) - 1)
