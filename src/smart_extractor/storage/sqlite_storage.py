"""
SQLite 数据库存储

使用 SQLite 实现本地持久化，自动根据 Schema 建表，
支持分页查询和数据统计。
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Type

from loguru import logger

from smart_extractor.config import StorageConfig
from smart_extractor.models.base import BaseExtractModel, ExtractionMeta
from smart_extractor.storage.base import BaseStorage


# Pydantic 类型到 SQLite 类型的映射
_SQLITE_TYPE_MAP = {
    "str": "TEXT",
    "int": "INTEGER",
    "float": "REAL",
    "bool": "INTEGER",
    "list": "TEXT",  # 列表序列化为 JSON 字符串
    "dict": "TEXT",  # 字典序列化为 JSON 字符串
}


class SQLiteStorage(BaseStorage):
    """
    SQLite 数据库存储器。

    功能：
    - 自动根据 Pydantic Schema 创建表
    - 支持追加写入和批量插入
    - 支持分页查询
    - 包含元数据列（来源URL、提取时间等）
    """

    def __init__(self, config: Optional[StorageConfig] = None):
        self._config = config or StorageConfig()
        output_dir = Path(self._config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = output_dir / self._config.sqlite_db_name
        self._conn: Optional[sqlite3.Connection] = None
        self._created_tables: set[str] = set()

    def _get_conn(self) -> sqlite3.Connection:
        """获取数据库连接（延迟初始化）"""
        if self._conn is None:
            self._conn = sqlite3.connect(str(self._db_path))
            self._conn.row_factory = sqlite3.Row
            logger.info("SQLite 数据库已连接: {}", self._db_path)
        return self._conn

    def _ensure_table(self, collection_name: str, data: BaseExtractModel) -> None:
        """确保数据表存在，不存在则根据 Schema 自动创建"""
        collection_name = self._normalize_collection_name(collection_name)
        if collection_name in self._created_tables:
            return

        conn = self._get_conn()

        # 根据 Pydantic 模型的字段生成 DDL
        columns = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]

        for field_name, field_info in type(data).model_fields.items():
            # 获取字段类型名
            annotation = field_info.annotation
            type_name = getattr(annotation, "__name__", str(annotation)).lower()

            # 处理 Optional 和复合类型
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

        # 添加元数据列
        columns.extend([
            "_source_url TEXT",
            "_extracted_at TEXT",
            "_model TEXT",
            "_confidence REAL",
        ])

        columns_sql = ",\n  ".join(columns)
        create_sql = f"CREATE TABLE IF NOT EXISTS [{collection_name}] (\n  {columns_sql}\n)"  # nosec B608

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
        """
        保存提取结果到 SQLite 数据库。

        Args:
            data: 单条或多条提取结果
            meta: 对应的元数据
            collection_name: 表名

        Returns:
            数据库文件路径
        """
        data_list = data if isinstance(data, list) else [data]
        meta_list = None
        if meta is not None:
            meta_list = meta if isinstance(meta, list) else [meta]

        if not data_list:
            return str(self._db_path)

        collection_name = self._normalize_collection_name(collection_name)

        # 确保表存在
        self._ensure_table(collection_name, data_list[0])

        conn = self._get_conn()

        for i, item in enumerate(data_list):
            row = item.to_flat_dict(
                meta=meta_list[i] if meta_list and i < len(meta_list) else None
            )

            # 序列化复杂类型
            for key, value in row.items():
                if isinstance(value, (list, dict)):
                    row[key] = json.dumps(value, ensure_ascii=False)
                elif isinstance(value, datetime):
                    row[key] = value.isoformat()

            columns = ", ".join(row.keys())
            placeholders = ", ".join(["?"] * len(row))
            insert_sql = f"INSERT INTO [{collection_name}] ({columns}) VALUES ({placeholders})"  # nosec B608

            try:
                conn.execute(insert_sql, list(row.values()))
            except sqlite3.OperationalError as e:
                logger.warning("插入数据时出错（可能有新字段未在表中）: {}", e)
                # 尝试添加新列后重试
                self._add_missing_columns(collection_name, row)
                conn.execute(insert_sql, list(row.values()))

        conn.commit()
        logger.info("SQLite 存储: {} 条数据 → {} (表: {})", len(data_list), self._db_path, collection_name)
        return str(self._db_path)

    def _add_missing_columns(self, table_name: str, row: dict) -> None:
        """为表添加缺失的列"""
        table_name = self._normalize_collection_name(table_name)
        conn = self._get_conn()
        cursor = conn.execute(f"PRAGMA table_info([{table_name}])")  # nosec B608
        existing_columns = {col[1] for col in cursor.fetchall()}

        for col_name in row.keys():
            if col_name not in existing_columns:
                try:
                    conn.execute(f"ALTER TABLE [{table_name}] ADD COLUMN {col_name} TEXT")  # nosec B608
                    logger.debug("为表 {} 添加新列: {}", table_name, col_name)
                except sqlite3.OperationalError:
                    pass

    def load(
        self,
        collection_name: str = "default",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """从 SQLite 加载数据"""
        collection_name = self._normalize_collection_name(collection_name)
        conn = self._get_conn()

        try:
            cursor = conn.execute(
                f"SELECT * FROM [{collection_name}] LIMIT ? OFFSET ?",  # nosec B608
                (limit, offset),
            )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except sqlite3.OperationalError:
            logger.warning("表 {} 不存在", collection_name)
            return []

    def count(self, collection_name: str = "default") -> int:
        """返回表中的数据行数"""
        collection_name = self._normalize_collection_name(collection_name)
        conn = self._get_conn()

        try:
            cursor = conn.execute(f"SELECT COUNT(*) FROM [{collection_name}]")  # nosec B608
            return cursor.fetchone()[0]
        except sqlite3.OperationalError:
            return 0

    def close(self) -> None:
        """关闭数据库连接"""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.debug("SQLite 连接已关闭")

    def __del__(self):
        self.close()
