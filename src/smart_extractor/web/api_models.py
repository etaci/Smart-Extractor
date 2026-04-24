"""
Web API 请求模型。
"""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


MAX_URL_LENGTH = 2048
MAX_BATCH_URLS = 100
MAX_SELECTED_FIELDS = 50
MAX_FIELD_NAME_LENGTH = 64


def _normalize_string_list(
    values: list[str],
    *,
    max_item_length: int,
) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in values or []:
        text = str(item or "").strip()
        if not text:
            continue
        if len(text) > max_item_length:
            raise ValueError(f"列表项长度不能超过 {max_item_length}")
        if text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


class ExtractRequest(BaseModel):
    """Single extraction request."""

    url: str = Field(min_length=1, max_length=MAX_URL_LENGTH, description="Target URL")
    schema_name: str = Field(
        default="auto",
        min_length=1,
        max_length=64,
        description="Schema name or auto",
    )
    storage_format: str = Field(
        default="json",
        min_length=1,
        max_length=32,
        description="Storage format",
    )
    use_static: bool = Field(default=False, description="Use static fetcher")
    selected_fields: list[str] = Field(
        default_factory=list,
        max_length=MAX_SELECTED_FIELDS,
        description="Requested fields",
    )

    @field_validator("url", "schema_name", "storage_format")
    @classmethod
    def _strip_text_fields(cls, value: str) -> str:
        return str(value or "").strip()

    @field_validator("selected_fields")
    @classmethod
    def _normalize_selected_fields(cls, value: list[str]) -> list[str]:
        return _normalize_string_list(
            value,
            max_item_length=MAX_FIELD_NAME_LENGTH,
        )


class BatchExtractRequest(BaseModel):
    """Batch extraction request."""

    urls: list[str] = Field(
        min_length=1,
        max_length=MAX_BATCH_URLS,
        description="Target URLs",
    )
    schema_name: str = Field(
        default="auto",
        min_length=1,
        max_length=64,
        description="Schema name or auto",
    )
    storage_format: str = Field(
        default="json",
        min_length=1,
        max_length=32,
        description="Storage format",
    )
    batch_group_id: str = Field(
        default="",
        max_length=128,
        description="Existing batch group id",
    )

    @field_validator("urls")
    @classmethod
    def _normalize_urls(cls, value: list[str]) -> list[str]:
        normalized = _normalize_string_list(value, max_item_length=MAX_URL_LENGTH)
        if not normalized:
            raise ValueError("urls 不能为空")
        return normalized

    @field_validator("schema_name", "storage_format", "batch_group_id")
    @classmethod
    def _strip_batch_text_fields(cls, value: str) -> str:
        return str(value or "").strip()


class AnalyzePageRequest(BaseModel):
    """Page analysis request."""

    url: str = Field(description="Target URL")
    use_static: bool = Field(default=False, description="Use static fetcher")


class AnalyzeInsightRequest(BaseModel):
    """Context-aware page insight request."""

    url: str = Field(description="Target URL")
    use_static: bool = Field(default=False, description="Use static fetcher")
    goal: str = Field(default="summary", description="Analysis goal")
    role: str = Field(default="consumer", description="User role")
    priority: str = Field(default="", description="Priority focus")
    constraints: str = Field(default="", description="User constraints")
    notes: str = Field(default="", description="Additional context")
    output_format: str = Field(default="cards", description="Requested output style")


class AnalyzeComparePreviewRequest(BaseModel):
    urls: list[str] = Field(description="Target URLs")
    use_static: bool = Field(default=False, description="Use static fetcher")


class AnalyzeCompareRequest(BaseModel):
    urls: list[str] = Field(description="Target URLs")
    use_static: bool = Field(default=False, description="Use static fetcher")
    goal: str = Field(default="comparison", description="Comparison goal")
    role: str = Field(default="consumer", description="User role")
    focus: str = Field(default="", description="Comparison focus")
    must_have: str = Field(default="", description="Must-have conditions")
    elimination: str = Field(default="", description="Elimination conditions")
    notes: str = Field(default="", description="Additional compare context")
    output_format: str = Field(default="table", description="Requested output style")


class BasicLLMConfigPayload(BaseModel):
    api_key: str = Field(default="", description="LLM API key")
    base_url: str = Field(min_length=1, description="LLM base url")
    model: str = Field(min_length=1, description="Model name")
    temperature: float = Field(ge=0.0, le=2.0, description="Generation temperature")


class SaveTemplateRequest(BaseModel):
    name: str = Field(min_length=1, description="Template name")
    url: str = Field(default="", description="Template source url")
    page_type: str = Field(default="unknown", description="Detected page type")
    schema_name: str = Field(default="auto", description="Schema name or auto")
    storage_format: str = Field(default="json", description="Storage format")
    use_static: bool = Field(default=False, description="Use static fetcher")
    selected_fields: list[str] = Field(default_factory=list, description="Saved fields")
    field_labels: dict[str, str] = Field(default_factory=dict, description="Field labels")
    profile: dict[str, object] = Field(default_factory=dict, description="Template profile")
    template_id: str = Field(default="", description="Existing template id")


class SaveMonitorRequest(BaseModel):
    name: str = Field(min_length=1, description="Monitor name")
    url: str = Field(description="Monitor url")
    schema_name: str = Field(default="auto", description="Schema name or auto")
    storage_format: str = Field(default="json", description="Storage format")
    use_static: bool = Field(default=False, description="Use static fetcher")
    selected_fields: list[str] = Field(default_factory=list, description="Tracked fields")
    field_labels: dict[str, str] = Field(default_factory=dict, description="Field labels")
    profile: dict[str, object] = Field(default_factory=dict, description="Monitor profile")
    schedule_enabled: bool = Field(default=False, description="Enable automatic monitor schedule")
    schedule_interval_minutes: int = Field(
        default=60,
        description="Automatic monitor interval in minutes",
    )
    monitor_id: str = Field(default="", description="Existing monitor id")


class NaturalLanguageTaskRequest(BaseModel):
    request_text: str = Field(min_length=1, description="Natural language task request")


class InstallMarketTemplateRequest(BaseModel):
    template_id: str = Field(min_length=1, description="Market template id")


class LearnedProfileActionRequest(BaseModel):
    reason: str = Field(default="", description="Optional operator note")


class LearnedProfileBulkActionRequest(BaseModel):
    reason: str = Field(default="", description="Optional operator note")


class NotificationResendRequest(BaseModel):
    reason: str = Field(default="", description="Optional resend note")


class SaveTaskTemplateRequest(BaseModel):
    name: str = Field(min_length=1, description="Template name")
    template_id: str = Field(default="", description="Existing template id")
    profile: dict[str, object] = Field(default_factory=dict, description="Template profile")
