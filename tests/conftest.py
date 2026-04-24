"""
测试共用 Fixtures

提供所有测试共用的 mock 数据、临时目录、配置等。
"""

import os
import json
import tempfile
from pathlib import Path
from typing import List

import pytest
from pydantic import Field

from smart_extractor.config import (
    AppConfig, LLMConfig, FetcherConfig, CleanerConfig,
    StorageConfig, SchedulerConfig, LogConfig,
)
from smart_extractor.models.base import BaseExtractModel, ExtractionMeta


# ===== 测试用数据模型 =====

class SimpleArticle(BaseExtractModel):
    """测试用的简单文章模型"""
    title: str = Field(default="", description="标题")
    author: str = Field(default="", description="作者")
    content: str = Field(default="", description="正文")
    tags: List[str] = Field(default_factory=list, description="标签")


# ===== 共用 Fixtures =====

@pytest.fixture
def sample_html():
    """提供一段典型的 HTML 测试数据"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>测试页面</title>
        <script>var x = 1;</script>
        <style>body { color: red; }</style>
    </head>
    <body>
        <nav>导航栏</nav>
        <header>页头</header>
        <main>
            <h1>测试文章标题</h1>
            <p>作者：张三</p>
            <p>这是文章正文内容。包含了一些重要的信息，用于测试数据提取功能。</p>
            <ul>
                <li>要点一</li>
                <li>要点二</li>
                <li>要点三</li>
            </ul>
            <table>
                <tr><th>列1</th><th>列2</th></tr>
                <tr><td>数据A</td><td>数据B</td></tr>
            </table>
        </main>
        <footer>页脚内容</footer>
        <aside>侧边栏</aside>
    </body>
    </html>
    """


@pytest.fixture
def sample_html_minimal():
    """最小化 HTML"""
    return "<html><body><h1>标题</h1><p>正文内容</p></body></html>"


@pytest.fixture
def sample_article():
    """提供一个填充好的 SimpleArticle 实例"""
    return SimpleArticle(
        title="测试文章",
        author="张三",
        content="这是一篇测试文章的正文内容。",
        tags=["测试", "示例"],
    )


@pytest.fixture
def sample_meta():
    """提供一个 ExtractionMeta 实例"""
    return ExtractionMeta(
        source_url="https://example.com/test",
        extractor_model="test-model",
        raw_text_length=500,
        confidence_score=0.85,
    )


@pytest.fixture
def empty_article():
    """提供一个空的 SimpleArticle 实例"""
    return SimpleArticle()


@pytest.fixture
def test_config(tmp_path):
    """提供一个使用临时目录的测试配置"""
    return AppConfig(
        llm=LLMConfig(
            api_key="test-key-for-testing-only",
            base_url="https://example.com/v1",
            model="test-model",
            temperature=0.0,
            max_tokens=1024,
            max_retries=1,
            timeout=10,
        ),
        fetcher=FetcherConfig(
            headless=True,
            timeout=5000,
            wait_after_load=100,
        ),
        cleaner=CleanerConfig(
            remove_tags=["script", "style", "nav", "footer", "header", "aside"],
            max_text_length=4000,
            keep_structure=True,
        ),
        storage=StorageConfig(
            output_dir=str(tmp_path / "output"),
            default_format="json",
            sqlite_db_name="test.db",
        ),
        scheduler=SchedulerConfig(
            max_concurrency=2,
            request_delay_min=0.01,
            request_delay_max=0.02,
            max_retries=1,
        ),
        log=LogConfig(
            level="DEBUG",
            log_dir=str(tmp_path / "logs"),
        ),
    )


@pytest.fixture
def output_dir(tmp_path):
    """提供一个干净的临时输出目录"""
    d = tmp_path / "output"
    d.mkdir()
    return d


@pytest.fixture
def schema_dir(tmp_path):
    """提供一个包含测试 Schema YAML 的临时目录"""
    d = tmp_path / "schemas"
    d.mkdir()

    # 创建一个测试用 Schema YAML
    schema_yaml = {
        "name": "TestPerson",
        "description": "测试用人物模型",
        "fields": {
            "name": {
                "type": "str",
                "description": "姓名",
                "required": True,
            },
            "age": {
                "type": "int",
                "description": "年龄",
                "default": 0,
            },
            "hobbies": {
                "type": "list[str]",
                "description": "爱好",
                "default": [],
            },
        },
    }

    import yaml
    yaml_path = d / "test_person.yaml"
    with open(yaml_path, "w", encoding="utf-8") as f:
        yaml.dump(schema_yaml, f, allow_unicode=True)

    return d
