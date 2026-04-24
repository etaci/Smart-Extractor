"""
新闻文章数据模型

定义新闻/文章类内容的提取 Schema。
"""

from datetime import datetime
from typing import Optional

from pydantic import ConfigDict, Field

from smart_extractor.models.base import BaseExtractModel


class NewsArticle(BaseExtractModel):
    """新闻文章 / 博客文章提取 Schema"""

    model_config = ConfigDict(
        json_schema_extra={"description": "新闻文章或博客文章的结构化数据模型"},
    )

    title: str = Field(
        description="文章标题"
    )
    author: str = Field(
        default="",
        description="文章作者"
    )
    publish_date: str = Field(
        default="",
        description="发布日期，格式如 '2024-01-15' 或原始文本"
    )
    content: str = Field(
        description="文章正文内容（摘要或全文）"
    )
    summary: str = Field(
        default="",
        description="文章摘要，不超过 200 字"
    )
    tags: list[str] = Field(
        default_factory=list,
        description="文章标签/关键词列表"
    )
    category: str = Field(
        default="",
        description="文章分类，如：科技、财经、体育等"
    )
    source: str = Field(
        default="",
        description="文章来源/媒体名称"
    )
    image_urls: list[str] = Field(
        default_factory=list,
        description="文章中的图片 URL 列表"
    )
