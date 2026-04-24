"""
动态 Schema 加载器

支持从 YAML 配置文件动态创建 Pydantic 模型，
用户无需编写 Python 代码即可定义自定义提取模板。
"""

from pathlib import Path
from typing import Any, Optional, Type

import yaml
from pydantic import create_model, Field
from loguru import logger

from smart_extractor.models.base import BaseExtractModel
from smart_extractor.config import DEFAULT_SCHEMA_DIR


# 支持的字段类型映射
_TYPE_MAP: dict[str, type] = {
    "str": str,
    "string": str,
    "int": int,
    "integer": int,
    "float": float,
    "number": float,
    "bool": bool,
    "boolean": bool,
    "list[str]": list[str],
    "list[int]": list[int],
    "list[float]": list[float],
    "dict": dict[str, str],
    "dict[str,str]": dict[str, str],
}


def _parse_field_type(type_str: str) -> type:
    """
    将 YAML 中的类型字符串解析为 Python 类型。

    Args:
        type_str: 类型字符串，如 "str", "int", "list[str]"
    """
    type_str = type_str.strip().lower()
    if type_str in _TYPE_MAP:
        return _TYPE_MAP[type_str]
    logger.warning("未知字段类型 '{}'，将使用 str", type_str)
    return str


def load_schema_from_yaml(yaml_path: str | Path) -> Type[BaseExtractModel]:
    """
    从 YAML 文件动态创建 Pydantic 模型。

    YAML 格式示例:
    ```yaml
    name: "NewsArticle"
    description: "新闻文章提取模板"
    fields:
      title:
        type: str
        description: "文章标题"
        required: true
      author:
        type: str
        description: "作者"
        default: ""
      tags:
        type: list[str]
        description: "标签列表"
        default: []
    ```

    Args:
        yaml_path: YAML Schema 文件路径

    Returns:
        动态创建的 Pydantic 模型类
    """
    yaml_path = Path(yaml_path)

    if not yaml_path.exists():
        raise FileNotFoundError(f"Schema 文件不存在: {yaml_path}")

    with open(yaml_path, "r", encoding="utf-8") as f:
        schema_data = yaml.safe_load(f)

    if not schema_data or "fields" not in schema_data:
        raise ValueError(f"Schema 文件格式错误，缺少 'fields' 定义: {yaml_path}")

    model_name = schema_data.get("name", yaml_path.stem.title().replace("_", ""))
    model_description = schema_data.get("description", f"从 {yaml_path.name} 加载的自定义模型")

    # 构建字段定义
    field_definitions: dict[str, Any] = {}

    for field_name, field_config in schema_data["fields"].items():
        field_type = _parse_field_type(field_config.get("type", "str"))
        field_desc = field_config.get("description", "")
        is_required = field_config.get("required", False)
        default_value = field_config.get("default", None)

        if is_required:
            # 必填字段：(类型, Field(...))
            field_definitions[field_name] = (
                field_type,
                Field(description=field_desc),
            )
        else:
            # 可选字段：(类型, Field(default=xxx))
            if default_value is None:
                # 基于类型设置默认值
                if field_type in (list, list[str], list[int], list[float]):
                    default_value = []
                elif field_type in (dict, dict[str, str]):
                    default_value = {}
                elif field_type == str:
                    default_value = ""
                elif field_type in (int, float):
                    default_value = 0

            field_definitions[field_name] = (
                field_type,
                Field(default=default_value, description=field_desc),
            )

    # 动态创建模型
    dynamic_model = create_model(
        model_name,
        __base__=BaseExtractModel,
        **field_definitions,
    )

    # 设置模型描述
    dynamic_model.__doc__ = model_description

    logger.info("从 {} 加载自定义 Schema: {} ({}个字段)", yaml_path.name, model_name, len(field_definitions))
    return dynamic_model


class SchemaRegistry:
    """
    Schema 注册表

    管理所有内置和自定义的 Schema，提供按名称查找功能。
    """

    def __init__(self):
        self._schemas: dict[str, Type[BaseExtractModel]] = {}
        # 注册内置 Schema
        self._register_builtin()

    def _register_builtin(self) -> None:
        """注册内置 Schema"""
        from smart_extractor.models.news import NewsArticle
        from smart_extractor.models.product import ProductDetail
        from smart_extractor.models.job import JobPosting

        self.register("news", NewsArticle)
        self.register("product", ProductDetail)
        self.register("job", JobPosting)

    def register(self, name: str, schema: Type[BaseExtractModel]) -> None:
        """注册一个 Schema"""
        self._schemas[name.lower()] = schema
        logger.debug("注册 Schema: {}", name)

    def get(self, name: str) -> Optional[Type[BaseExtractModel]]:
        """按名称获取 Schema"""
        return self._schemas.get(name.lower())

    def list_schemas(self) -> list[str]:
        """列出所有可用的 Schema 名称"""
        return list(self._schemas.keys())

    def load_from_directory(self, schema_dir: str | Path | None = None) -> int:
        """
        从目录批量加载 YAML Schema 文件。

        Args:
            schema_dir: Schema 文件目录，默认使用 config/schemas/

        Returns:
            成功加载的 Schema 数量
        """
        if schema_dir is None:
            schema_dir = DEFAULT_SCHEMA_DIR

        schema_dir = Path(schema_dir)
        if not schema_dir.exists():
            logger.warning("Schema 目录不存在: {}", schema_dir)
            return 0

        loaded = 0
        for yaml_file in schema_dir.glob("*.yaml"):
            try:
                schema = load_schema_from_yaml(yaml_file)
                name = yaml_file.stem
                self.register(name, schema)
                loaded += 1
            except Exception as e:
                logger.error("加载 Schema 文件 {} 失败: {}", yaml_file.name, e)

        logger.info("从 {} 目录加载了 {} 个自定义 Schema", schema_dir, loaded)
        return loaded
