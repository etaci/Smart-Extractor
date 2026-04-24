"""
基础数据模型。
提供固定字段模型与动态字段抽取结果共用的基础能力。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ExtractionMeta(BaseModel):
    """提取元数据。"""

    source_url: str = Field(default="", description="数据来源 URL")
    extracted_at: datetime = Field(default_factory=datetime.now, description="提取时间")
    extractor_model: str = Field(default="", description="使用的模型名称")
    confidence_score: float = Field(
        default=0.0, ge=0.0, le=1.0, description="提取置信度"
    )
    raw_text_length: int = Field(default=0, description="清洗后文本长度")


class BaseExtractModel(BaseModel):
    """所有结构化提取模型的基类。"""

    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={"description": "基础提取模型"},
    )

    @model_validator(mode="after")
    def _remove_content_newlines(self) -> "BaseExtractModel":
        """统一清理 content 字段中的换行，兼容旧数据模型。"""
        if hasattr(self, "content") and isinstance(self.content, str):
            self.content = self.content.replace("\n", "").replace("\r", "")
        return self

    def to_flat_dict(self, meta: Optional[ExtractionMeta] = None) -> dict[str, Any]:
        """转换为适合 CSV / SQLite 的扁平字典。"""
        data: dict[str, Any] = {}

        for field_name, value in self.model_dump().items():
            if isinstance(value, list):
                data[field_name] = ", ".join(str(item) for item in value)
            elif isinstance(value, dict):
                import json

                data[field_name] = json.dumps(value, ensure_ascii=False)
            else:
                data[field_name] = value

        if meta:
            data["_source_url"] = meta.source_url
            data["_extracted_at"] = meta.extracted_at.isoformat()
            data["_model"] = meta.extractor_model
            data["_confidence"] = meta.confidence_score

        return data

    def completeness_score(self) -> float:
        """按非空字段占比计算完整度。"""
        fields = type(self).model_fields
        if not fields:
            return 0.0

        filled = 0
        for field_name in fields:
            value = getattr(self, field_name, None)
            if value not in (None, "", [], {}):
                filled += 1

        return filled / len(fields)


class DynamicExtractResult(BaseExtractModel):
    """无 Schema 模式的统一抽取结果。"""

    model_config = ConfigDict(
        extra="ignore",
        json_schema_extra={"description": "动态字段抽取结果"},
    )

    page_type: str = Field(default="unknown", description="页面类型")
    candidate_fields: list[str] = Field(default_factory=list, description="候选字段")
    selected_fields: list[str] = Field(default_factory=list, description="本次抽取字段")
    field_labels: dict[str, str] = Field(default_factory=dict, description="字段显示名")
    data: dict[str, Any] = Field(default_factory=dict, description="字段值")
    formatted_text: str = Field(default="", description="润色后的结果文本")
    extraction_strategy: str = Field(default="llm", description="提取策略")
    learned_profile_id: str = Field(default="", description="学习档案 ID")
    strategy_details: dict[str, Any] = Field(
        default_factory=dict, description="策略补充信息"
    )

    def completeness_score(self) -> float:
        fields = self.selected_fields or self.candidate_fields
        if not fields:
            return 1.0 if self.data else 0.0

        filled = 0
        for field_name in fields:
            if self.data.get(field_name) not in (None, "", [], {}):
                filled += 1
        return filled / len(fields)

    def to_flat_dict(self, meta: Optional[ExtractionMeta] = None) -> dict[str, Any]:
        data: dict[str, Any] = {
            "page_type": self.page_type,
            "candidate_fields": ", ".join(self.candidate_fields),
            "selected_fields": ", ".join(self.selected_fields),
            "formatted_text": self.formatted_text,
            "extraction_strategy": self.extraction_strategy,
            "learned_profile_id": self.learned_profile_id,
        }
        data.update(self.data)

        if self.field_labels:
            import json

            data["_field_labels"] = json.dumps(self.field_labels, ensure_ascii=False)
        if self.strategy_details:
            import json

            data["_strategy_details"] = json.dumps(
                self.strategy_details, ensure_ascii=False
            )

        if meta:
            data["_source_url"] = meta.source_url
            data["_extracted_at"] = meta.extracted_at.isoformat()
            data["_model"] = meta.extractor_model
            data["_confidence"] = meta.confidence_score

        return data
