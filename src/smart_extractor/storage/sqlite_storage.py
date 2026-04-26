"""
SQLite 数据库存储。
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from smart_extractor.config import StorageConfig
from smart_extractor.models.base import BaseExtractModel, ExtractionMeta
from smart_extractor.storage.base import BaseStorage


class SQLiteStorage(BaseStorage):
    """使用 SQLite 进行本地持久化。"""

    def __init__(self, config: Optional[StorageConfig] = None):
        self._config = config or StorageConfig()
        output_dir = Path(self._config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = output_dir / self._config.sqlite_db_name
        self._conn: Optional[sqlite3.Connection] = None
        self._created_tables: set[str] = set()
        self._lock = self._lock_for_resource(self._db_path)

    def _get_conn(self) -> sqlite3.Connection:
        with self._lock:
            if self._conn is None:
                timeout_seconds = max(self._config.sqlite_busy_timeout_ms, 0) / 1000
                self._conn = sqlite3.connect(
                    str(self._db_path),
                    timeout=timeout_seconds,
                    check_same_thread=False,
                )
                self._conn.row_factory = sqlite3.Row
                self._conn.execute(f"PRAGMA busy_timeout = {int(self._config.sqlite_busy_timeout_ms)}")
                if self._config.sqlite_enable_wal:
                    self._conn.execute("PRAGMA journal_mode = WAL")
                if self._config.sqlite_synchronous:
                    self._conn.execute(
                        f"PRAGMA synchronous = {str(self._config.sqlite_synchronous).strip()}"
                    )
                logger.info("SQLite 数据库已连接: {}", self._db_path)
            return self._conn

    def _ensure_table(self, collection_name: str, data: BaseExtractModel) -> None:
        collection_name = self._normalize_collection_name(collection_name)
        with self._lock:
            if collection_name in self._created_tables:
                return

            conn = self._get_conn()
            columns = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]

            for field_name, field_info in type(data).model_fields.items():
                annotation = field_info.annotation
                type_name = getattr(annotation, "__name__", str(annotation)).lower()
                if "list" in type_name or "list" in str(annotation):
                    sqlite_type = "TEXT"
                elif "dict" in type_name or "dict" in str(annotation):
                    sqlite_type = "TEXT"
                elif "int" in type_name:
                    sqlite_type = "INTEGER"
                elif "float" in type_name:
                    sqlite_type = "REAL"
                elif "bool" in type_name:
                    sqlite_type = "INTEGER"
                else:
                    sqlite_type = "TEXT"
                columns.append(f"{field_name} {sqlite_type}")

            columns.extend(
                [
                    "_source_url TEXT",
                    "_extracted_at TEXT",
                    "_model TEXT",
                    "_confidence REAL",
                ]
            )

            columns_sql = ",\n  ".join(columns)
            create_sql = (
                f"CREATE TABLE IF NOT EXISTS [{collection_name}] (\n  {columns_sql}\n)"
            )  # nosec B608
            conn.execute(create_sql)
            conn.commit()
            self._created_tables.add(collection_name)
            logger.debug("确保数据表存在: {}", collection_name)

    def save(
        self,
        data: BaseExtractModel | list[BaseExtractModel],
        meta: ExtractionMeta | list[ExtractionMeta] | None = None,
        collection_name: str = "default",
    ) -> str:
        data_list = data if isinstance(data, list) else [data]
        meta_list = meta if isinstance(meta, list) else ([meta] if meta is not None else None)

        if not data_list:
            return str(self._db_path)

        collection_name = self._normalize_collection_name(collection_name)
        self._ensure_table(collection_name, data_list[0])

        with self._lock:
            conn = self._get_conn()
            for index, item in enumerate(data_list):
                row = item.to_flat_dict(
                    meta=meta_list[index] if meta_list and index < len(meta_list) else None
                )
                for key, value in row.items():
                    if isinstance(value, (list, dict)):
                        row[key] = json.dumps(value, ensure_ascii=False)
                    elif isinstance(value, datetime):
                        row[key] = value.isoformat()

                columns = ", ".join(row.keys())
                placeholders = ", ".join(["?"] * len(row))
                insert_sql = (
                    f"INSERT INTO [{collection_name}] ({columns}) VALUES ({placeholders})"
                )  # nosec B608

                try:
                    conn.execute(insert_sql, list(row.values()))
                except sqlite3.OperationalError as exc:
                    logger.warning("插入数据时报错，尝试补列后重试: {}", exc)
                    self._add_missing_columns(collection_name, row)
                    conn.execute(insert_sql, list(row.values()))

            conn.commit()

        logger.info("SQLite 存储: {} 条数据 -> {} (表: {})", len(data_list), self._db_path, collection_name)
        return str(self._db_path)

    def _add_missing_columns(self, table_name: str, row: dict[str, Any]) -> None:
        table_name = self._normalize_collection_name(table_name)
        conn = self._get_conn()
        cursor = conn.execute(f"PRAGMA table_info([{table_name}])")  # nosec B608
        existing_columns = {col[1] for col in cursor.fetchall()}

        for column_name in row.keys():
            if column_name in existing_columns:
                continue
            try:
                conn.execute(f"ALTER TABLE [{table_name}] ADD COLUMN {column_name} TEXT")  # nosec B608
                logger.debug("为表 {} 添加新列: {}", table_name, column_name)
            except sqlite3.OperationalError:
                continue

    def load(
        self,
        collection_name: str = "default",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        collection_name = self._normalize_collection_name(collection_name)
        with self._lock:
            conn = self._get_conn()
            try:
                cursor = conn.execute(
                    f"SELECT * FROM [{collection_name}] LIMIT ? OFFSET ?",
                    (limit, offset),
                )  # nosec B608
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
            except sqlite3.OperationalError:
                logger.warning("表 {} 不存在", collection_name)
                return []

    def count(self, collection_name: str = "default") -> int:
        collection_name = self._normalize_collection_name(collection_name)
        with self._lock:
            conn = self._get_conn()
            try:
                cursor = conn.execute(f"SELECT COUNT(*) FROM [{collection_name}]")  # nosec B608
                return int(cursor.fetchone()[0])
            except sqlite3.OperationalError:
                return 0

    def close(self) -> None:
        with self._lock:
            if self._conn is None:
                return
            self._conn.close()
            self._conn = None
            logger.debug("SQLite 连接已关闭")

    def __del__(self):
        try:
            self.close()
        except Exception:
            return
