"""数据模型模块。"""

from smart_extractor.models.base import BaseExtractModel, DynamicExtractResult, ExtractionMeta
from smart_extractor.models.job import JobPosting
from smart_extractor.models.news import NewsArticle
from smart_extractor.models.product import ProductDetail

__all__ = [
    "BaseExtractModel",
    "DynamicExtractResult",
    "ExtractionMeta",
    "NewsArticle",
    "ProductDetail",
    "JobPosting",
]
