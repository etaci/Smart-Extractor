"""自然语言任务草案的规范化辅助函数。"""

from __future__ import annotations

from typing import Any

from smart_extractor.extractor.llm_response import (
    _normalize_field_list,
    _normalize_url_list,
)

_VALID_TASK_TYPES = {
    "single_extract",
    "batch_extract",
    "monitor",
    "compare_analysis",
}
_VALID_STORAGE_FORMATS = {"json", "csv", "sqlite"}


def normalize_task_plan_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "task_type": str(payload.get("task_type") or "single_extract").strip(),
        "summary": str(payload.get("summary") or "").strip(),
        "urls": _normalize_url_list(payload.get("urls")),
        "selected_fields": _normalize_field_list(payload.get("selected_fields")),
        "use_static": bool(payload.get("use_static", False)),
        "storage_format": (
            str(payload.get("storage_format") or "json").strip().lower() or "json"
        ),
        "schema_name": (
            str(payload.get("schema_name") or "auto").strip().lower() or "auto"
        ),
        "name": str(payload.get("name") or "自然语言任务").strip() or "自然语言任务",
        "confidence": str(payload.get("confidence") or "medium").strip() or "medium",
        "warnings": [
            str(item).strip() for item in payload.get("warnings", []) if str(item).strip()
        ],
    }
    if normalized["task_type"] not in _VALID_TASK_TYPES:
        normalized["task_type"] = "single_extract"
    if normalized["storage_format"] not in _VALID_STORAGE_FORMATS:
        normalized["storage_format"] = "json"
    if not normalized["summary"]:
        normalized["summary"] = "已根据自然语言需求生成任务草案。"
    return normalized
