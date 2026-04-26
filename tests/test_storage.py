"""
数据存储模块单元测试

测试 JSON / CSV / SQLite 三种存储器的写入、读取、统计功能。
"""

import csv
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List

import pytest
from pydantic import Field

from smart_extractor.config import StorageConfig
from smart_extractor.models.base import BaseExtractModel, ExtractionMeta
from smart_extractor.storage.json_storage import JSONStorage
from smart_extractor.storage.csv_storage import CSVStorage
from smart_extractor.storage.sqlite_storage import SQLiteStorage


class StorageTestModel(BaseExtractModel):
    """存储测试用模型"""
    title: str = Field(default="", description="标题")
    score: float = Field(default=0.0, description="评分")
    tags: List[str] = Field(default_factory=list, description="标签")


class StorageExtendedTestModel(StorageTestModel):
    category: str = Field(default="", description="分类")


def _make_config(tmp_path) -> StorageConfig:
    """创建使用临时目录的存储配置"""
    return StorageConfig(
        output_dir=str(tmp_path),
        default_format="json",
        sqlite_db_name="test_storage.db",
    )


def _make_sample_data():
    """创建测试数据"""
    return StorageTestModel(title="测试文章", score=9.5, tags=["python", "ai"])


def _make_sample_meta():
    """创建测试元数据"""
    return ExtractionMeta(
        source_url="https://example.com",
        extractor_model="test-model",
        raw_text_length=100,
    )


# ===== JSON Storage 测试 =====

class TestJSONStorage:
    """JSONStorage 单元测试"""

    def test_save_single(self, tmp_path):
        """测试保存单条数据"""
        storage = JSONStorage(_make_config(tmp_path))
        data = _make_sample_data()
        path = storage.save(data, collection_name="test_json")
        assert Path(path).exists()

    def test_save_with_meta(self, tmp_path):
        """测试保存带元数据的数据"""
        storage = JSONStorage(_make_config(tmp_path))
        data = _make_sample_data()
        meta = _make_sample_meta()
        path = storage.save(data, meta=meta, collection_name="test_meta")
        assert Path(path).exists()

    def test_save_list(self, tmp_path):
        """测试保存数据列表"""
        storage = JSONStorage(_make_config(tmp_path))
        data = [_make_sample_data() for _ in range(3)]
        path = storage.save(data, collection_name="test_list")
        assert Path(path).exists()

    def test_load(self, tmp_path):
        """测试读取数据"""
        storage = JSONStorage(_make_config(tmp_path))
        data = _make_sample_data()
        storage.save(data, collection_name="test_load")
        loaded = storage.load(collection_name="test_load")
        assert len(loaded) > 0

    def test_count(self, tmp_path):
        """测试计数"""
        storage = JSONStorage(_make_config(tmp_path))
        data = [_make_sample_data() for _ in range(5)]
        storage.save(data, collection_name="test_count")
        assert storage.count(collection_name="test_count") == 5

    def test_empty_load(self, tmp_path):
        """测试加载不存在的集合"""
        storage = JSONStorage(_make_config(tmp_path))
        loaded = storage.load(collection_name="nonexistent")
        assert loaded == []

    def test_collection_name_is_sanitized_for_json_path(self, tmp_path):
        storage = JSONStorage(_make_config(tmp_path))
        data = _make_sample_data()
        path = Path(storage.save(data, collection_name="../unsafe\\name"))
        assert path.parent == Path(tmp_path)
        assert path.name == "unsafe_name.json"

    def test_concurrent_saves_preserve_all_rows(self, tmp_path):
        storage = JSONStorage(_make_config(tmp_path))

        def _save(index: int):
            storage.save(
                StorageTestModel(title=f"json-{index}", score=float(index)),
                collection_name="test_concurrent_json",
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(_save, range(20)))

        loaded = storage.load(collection_name="test_concurrent_json", limit=100)
        assert len(loaded) == 20
        assert len({item["title"] for item in loaded}) == 20


# ===== CSV Storage 测试 =====

class TestCSVStorage:
    """CSVStorage 单元测试"""

    def test_save_single(self, tmp_path):
        """测试保存单条数据到 CSV"""
        storage = CSVStorage(_make_config(tmp_path))
        data = _make_sample_data()
        path = storage.save(data, collection_name="test_csv")
        assert Path(path).exists()
        assert path.endswith(".csv")

    def test_save_multiple(self, tmp_path):
        """测试保存多条数据到 CSV"""
        storage = CSVStorage(_make_config(tmp_path))
        data = [_make_sample_data() for _ in range(3)]
        path = storage.save(data, collection_name="test_multi")
        assert Path(path).exists()

    def test_load(self, tmp_path):
        """测试从 CSV 读取"""
        storage = CSVStorage(_make_config(tmp_path))
        data = _make_sample_data()
        storage.save(data, collection_name="test_csv_load")
        loaded = storage.load(collection_name="test_csv_load")
        assert len(loaded) > 0

    def test_count(self, tmp_path):
        """测试 CSV 计数"""
        storage = CSVStorage(_make_config(tmp_path))
        data = [_make_sample_data() for _ in range(4)]
        storage.save(data, collection_name="test_csv_count")
        assert storage.count(collection_name="test_csv_count") == 4

    def test_append_mode(self, tmp_path):
        """测试追加写入"""
        storage = CSVStorage(_make_config(tmp_path))
        data1 = _make_sample_data()
        data2 = StorageTestModel(title="第二篇", score=8.0, tags=["test"])
        storage.save(data1, collection_name="test_append")
        storage.save(data2, collection_name="test_append")
        assert storage.count(collection_name="test_append") == 2

    def test_collection_name_is_sanitized_for_csv_path(self, tmp_path):
        storage = CSVStorage(_make_config(tmp_path))
        path = Path(storage.save(_make_sample_data(), collection_name="..\\unsafe/name"))
        assert path.parent == Path(tmp_path)
        assert path.name == "unsafe_name.csv"

    def test_concurrent_appends_keep_all_rows(self, tmp_path):
        storage = CSVStorage(_make_config(tmp_path))

        def _save(index: int):
            storage.save(
                StorageTestModel(title=f"csv-{index}", score=float(index)),
                collection_name="test_concurrent_csv",
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(_save, range(20)))

        assert storage.count(collection_name="test_concurrent_csv") == 20

    def test_schema_evolution_rewrites_header_without_dropping_fields(self, tmp_path):
        storage = CSVStorage(_make_config(tmp_path))
        storage.save(
            StorageTestModel(title="first", score=1.0, tags=["a"]),
            collection_name="test_csv_schema_evolution",
        )
        storage.save(
            StorageExtendedTestModel(
                title="second",
                score=2.0,
                tags=["b"],
                category="news",
            ),
            collection_name="test_csv_schema_evolution",
        )

        csv_path = Path(tmp_path) / "test_csv_schema_evolution.csv"
        with open(csv_path, "r", encoding=storage._config.csv_encoding, newline="") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
            assert reader.fieldnames == ["title", "score", "tags", "category"]

        assert len(rows) == 2
        assert rows[0]["title"] == "first"
        assert rows[0]["category"] == ""
        assert rows[1]["title"] == "second"
        assert rows[1]["category"] == "news"


# ===== SQLite Storage 测试 =====

class TestSQLiteStorage:
    """SQLiteStorage 单元测试"""

    def test_save_single(self, tmp_path):
        """测试保存到 SQLite"""
        storage = SQLiteStorage(_make_config(tmp_path))
        data = _make_sample_data()
        path = storage.save(data, collection_name="test_sqlite")
        assert Path(path).exists()
        storage.close()

    def test_save_with_meta(self, tmp_path):
        """测试带元数据保存"""
        storage = SQLiteStorage(_make_config(tmp_path))
        data = _make_sample_data()
        meta = _make_sample_meta()
        storage.save(data, meta=meta, collection_name="test_meta")
        storage.close()

    def test_load(self, tmp_path):
        """测试从 SQLite 读取"""
        storage = SQLiteStorage(_make_config(tmp_path))
        data = _make_sample_data()
        storage.save(data, collection_name="test_load")
        loaded = storage.load(collection_name="test_load")
        assert len(loaded) == 1
        assert loaded[0]["title"] == "测试文章"
        storage.close()

    def test_count(self, tmp_path):
        """测试 SQLite 计数"""
        storage = SQLiteStorage(_make_config(tmp_path))
        for _ in range(3):
            storage.save(_make_sample_data(), collection_name="test_count")
        assert storage.count(collection_name="test_count") == 3
        storage.close()

    def test_pagination(self, tmp_path):
        """测试分页查询"""
        storage = SQLiteStorage(_make_config(tmp_path))
        for i in range(10):
            d = StorageTestModel(title=f"文章{i}", score=float(i))
            storage.save(d, collection_name="test_page")
        page1 = storage.load(collection_name="test_page", limit=3, offset=0)
        page2 = storage.load(collection_name="test_page", limit=3, offset=3)
        assert len(page1) == 3
        assert len(page2) == 3
        assert page1[0]["title"] != page2[0]["title"]
        storage.close()

    def test_load_nonexistent_table(self, tmp_path):
        """测试加载不存在的表"""
        storage = SQLiteStorage(_make_config(tmp_path))
        loaded = storage.load(collection_name="no_such_table")
        assert loaded == []
        storage.close()

    def test_count_nonexistent_table(self, tmp_path):
        """测试统计不存在的表"""
        storage = SQLiteStorage(_make_config(tmp_path))
        assert storage.count(collection_name="no_such_table") == 0
        storage.close()

    def test_list_serialization(self, tmp_path):
        """测试列表字段正确序列化"""
        storage = SQLiteStorage(_make_config(tmp_path))
        data = StorageTestModel(title="带标签", tags=["a", "b", "c"])
        storage.save(data, collection_name="test_list")
        loaded = storage.load(collection_name="test_list")
        # tags 应被序列化为 JSON 字符串
        assert "a" in loaded[0]["tags"]
        storage.close()

    def test_collection_name_is_sanitized_for_sqlite_table(self, tmp_path):
        storage = SQLiteStorage(_make_config(tmp_path))
        storage.save(_make_sample_data(), collection_name='unsafe"]; DROP TABLE test; --')
        loaded = storage.load(collection_name='unsafe"]; DROP TABLE test; --')
        assert len(loaded) == 1
        assert storage.count(collection_name='unsafe"]; DROP TABLE test; --') == 1
        storage.close()

    def test_concurrent_saves_are_thread_safe(self, tmp_path):
        storage = SQLiteStorage(_make_config(tmp_path))

        def _save(index: int):
            storage.save(
                StorageTestModel(title=f"sqlite-{index}", score=float(index)),
                collection_name="test_concurrent_sqlite",
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(_save, range(20)))

        assert storage.count(collection_name="test_concurrent_sqlite") == 20
        storage.close()
