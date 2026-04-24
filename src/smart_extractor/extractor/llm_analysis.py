"""LLM 分析结果的组装与规范化辅助函数。"""

from __future__ import annotations

from typing import Any

from smart_extractor.models.base import DynamicExtractResult
from smart_extractor.utils.display import (
    build_field_labels,
    get_page_type_label,
)


def _normalize_text_list(values: Any) -> list[str]:
    return [str(item).strip() for item in values or [] if str(item).strip()]


def _normalize_evidence_spans(values: Any) -> list[dict[str, str]]:
    return [
        {
            "label": str(item.get("label") or "证据"),
            "snippet": str(item.get("snippet") or "").strip(),
        }
        for item in values or []
        if isinstance(item, dict) and str(item.get("snippet") or "").strip()
    ]


def build_page_analysis_summary(
    page_result: DynamicExtractResult,
) -> dict[str, Any]:
    return {
        "page_type": page_result.page_type,
        "candidate_fields": page_result.candidate_fields,
        "field_labels": build_field_labels(
            page_result.candidate_fields,
            page_result.field_labels,
        ),
        "preview": page_result.formatted_text,
    }


def build_context_prompt_payload(
    page_result: DynamicExtractResult,
    *,
    source_url: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    labels = build_field_labels(
        page_result.candidate_fields,
        page_result.field_labels,
    )
    page_payload = {
        "url": source_url,
        "page_type": page_result.page_type,
        "candidate_fields": page_result.candidate_fields,
        "field_labels": labels,
        "data": page_result.data,
        "preview": page_result.formatted_text,
    }
    result = {
        "page_type": page_result.page_type,
        "page_type_label": get_page_type_label(page_result.page_type),
        "page_preview": page_result.formatted_text,
        "candidate_fields": page_result.candidate_fields,
        "field_labels": labels,
    }
    return page_payload, result


def normalize_context_analysis(analysis: dict[str, Any]) -> dict[str, Any]:
    return {
        "headline": str(analysis.get("headline") or "网页智能分析"),
        "summary": str(
            analysis.get("summary")
            or "当前信息可用于初步判断，但仍建议结合更多上下文继续确认。"
        ),
        "confidence": str(analysis.get("confidence") or "medium"),
        "key_points": _normalize_text_list(analysis.get("key_points")),
        "risks": _normalize_text_list(analysis.get("risks")),
        "recommended_actions": _normalize_text_list(
            analysis.get("recommended_actions")
        ),
        "missing_information": _normalize_text_list(
            analysis.get("missing_information")
        ),
        "evidence_spans": _normalize_evidence_spans(analysis.get("evidence_spans")),
    }


def build_compare_prompt_payloads(
    pages: list[tuple[str, DynamicExtractResult]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    page_payloads: list[dict[str, Any]] = []
    preview_items: list[dict[str, Any]] = []

    for url, page_result in pages:
        labels = build_field_labels(
            page_result.candidate_fields,
            page_result.field_labels,
        )
        preview_items.append(
            {
                "url": url,
                "page_type": page_result.page_type,
                "page_type_label": get_page_type_label(page_result.page_type),
                "candidate_fields": page_result.candidate_fields,
                "field_labels": labels,
                "preview": page_result.formatted_text[:280],
                "data": page_result.data,
            }
        )
        page_payloads.append(
            {
                "url": url,
                "page_type": page_result.page_type,
                "field_labels": labels,
                "candidate_fields": page_result.candidate_fields,
                "data": page_result.data,
                "preview": page_result.formatted_text,
            }
        )
    return page_payloads, preview_items


def normalize_compare_analysis(analysis: dict[str, Any]) -> dict[str, Any]:
    return {
        "comparison_matrix": [
            {
                "label": str(item.get("label") or "比较维度"),
                "summary": str(item.get("summary") or "").strip(),
            }
            for item in analysis.get("comparison_matrix", [])
            if isinstance(item, dict) and str(item.get("summary") or "").strip()
        ],
        "analysis": {
            "headline": str(analysis.get("headline") or "多 URL 对比分析"),
            "summary": str(
                analysis.get("summary") or "系统已输出初步横向比较结论。"
            ),
            "confidence": str(analysis.get("confidence") or "medium"),
            "key_points": _normalize_text_list(analysis.get("key_points")),
            "risks": _normalize_text_list(analysis.get("risks")),
            "recommended_actions": _normalize_text_list(
                analysis.get("recommended_actions")
            ),
            "missing_information": _normalize_text_list(
                analysis.get("missing_information")
            ),
            "evidence_spans": _normalize_evidence_spans(analysis.get("evidence_spans")),
        },
    }
