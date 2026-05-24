"""Rule-first specialized extractors for common business page types."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from smart_extractor.extractor.field_normalizer import normalize_dynamic_result
from smart_extractor.extractor.llm_response import _format_dynamic_text
from smart_extractor.models.base import DynamicExtractResult
from smart_extractor.utils.display import build_field_labels


_SPECIALIZED_FIELDS = {
    "product": ["name", "price", "brand", "availability", "description"],
    "pricing": ["plan", "price", "billing_period", "description", "summary"],
    "job": ["title", "company", "salary_range", "location", "requirements"],
    "news": ["title", "publish_date", "author", "summary", "content"],
    "notice": ["title", "publish_date", "agency", "summary", "content"],
    "policy": ["title", "publish_date", "agency", "policy_number", "content"],
}

_ALL_HINT_FIELDS = {
    "agency",
    "author",
    "availability",
    "billing_period",
    "brand",
    "company",
    "content",
    "date",
    "description",
    "location",
    "name",
    "organization",
    "plan",
    "policy_number",
    "price",
    "product",
    "publish_date",
    "salary_range",
    "summary",
    "title",
}


@dataclass(slots=True)
class SpecializedExtraction:
    page_type: str
    data: dict[str, Any]
    confidence: float
    source_fields: dict[str, str]


class SpecializedPageExtractor:
    """Extract high-confidence fields before the generic LLM pass."""

    def extract(
        self,
        text: str,
        *,
        source_url: str,
        page_type: str,
        selected_fields: list[str] | None = None,
    ) -> DynamicExtractResult | None:
        normalized_type = _normalize_page_type(page_type, text)
        if normalized_type not in _SPECIALIZED_FIELDS:
            return None

        fields = [
            field.strip()
            for field in (selected_fields or _SPECIALIZED_FIELDS[normalized_type])
            if str(field or "").strip()
        ] or list(_SPECIALIZED_FIELDS[normalized_type])

        hints = _parse_structured_hints(text)
        lines = _body_lines(text)
        extraction = self._extract_for_type(
            normalized_type,
            hints=hints,
            lines=lines,
            text=text,
        )
        filtered_data = {
            field: extraction.data.get(field, "")
            for field in fields
            if extraction.data.get(field, "") not in (None, "", [], {})
        }
        if not self._is_usable(normalized_type, filtered_data, fields, extraction.confidence):
            return None

        labels = build_field_labels(fields)
        result = DynamicExtractResult(
            page_type=normalized_type,
            candidate_fields=list(_SPECIALIZED_FIELDS[normalized_type]),
            selected_fields=fields,
            field_labels=labels,
            data=filtered_data,
            formatted_text=_format_dynamic_text(labels, filtered_data),
            extraction_strategy="specialized_rule",
            strategy_details={
                "mode": "specialized_rule",
                "source_url": source_url,
                "page_type": normalized_type,
                "confidence": extraction.confidence,
                "source_fields": extraction.source_fields,
            },
        )
        return normalize_dynamic_result(result)

    def _extract_for_type(
        self,
        page_type: str,
        *,
        hints: dict[str, str],
        lines: list[str],
        text: str,
    ) -> SpecializedExtraction:
        if page_type == "product":
            return _extract_product(hints, lines, text)
        if page_type == "pricing":
            return _extract_pricing(hints, lines, text)
        if page_type == "job":
            return _extract_job(hints, lines, text)
        if page_type == "policy":
            return _extract_policy(hints, lines, text)
        return _extract_article_like(page_type, hints, lines, text)

    @staticmethod
    def _is_usable(
        page_type: str,
        data: dict[str, Any],
        fields: list[str],
        confidence: float,
    ) -> bool:
        if not data:
            return False
        if confidence >= 0.85:
            return True
        if page_type == "product":
            return bool((data.get("name") or data.get("title")) and data.get("price"))
        if page_type == "pricing":
            return bool((data.get("plan") or data.get("title")) and data.get("price"))
        if page_type == "job":
            return bool(data.get("title") and (data.get("company") or data.get("location")))
        if page_type in {"news", "notice", "policy"}:
            return bool(
                data.get("title")
                and (data.get("publish_date") or data.get("content") or data.get("summary"))
            )
        return len(data) / max(len(fields), 1) >= 0.5


def _normalize_page_type(page_type: str, text: str) -> str:
    normalized = str(page_type or "").strip().lower()
    if normalized in _SPECIALIZED_FIELDS:
        return normalized
    if normalized in {"article", "blog"}:
        lowered = text.lower()
        if any(marker in lowered for marker in ("policy", "regulation", "公告", "政策", "通知")):
            return "policy"
        return "news"
    lowered = text.lower()
    if any(marker in lowered for marker in ("jobposting", "hiringorganization", "salary", "招聘", "岗位")):
        return "job"
    if any(marker in lowered for marker in ("pricing", "per seat", "per month", "套餐", "订阅")):
        return "pricing"
    if any(marker in lowered for marker in ("product", "offers", "price", "availability", "商品")):
        return "product"
    return normalized


def _parse_structured_hints(text: str) -> dict[str, str]:
    hints: dict[str, str] = {}
    in_hints = False
    for raw in str(text or "").splitlines():
        line = raw.strip()
        if not line:
            if in_hints:
                break
            continue
        if line.lower().startswith("structured extraction hints"):
            in_hints = True
            continue
        if not in_hints or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip().lower()
        value = value.strip()
        if key and value and key not in hints:
            hints[key] = value
    return hints


def _body_lines(text: str) -> list[str]:
    lines: list[str] = []
    in_hints = False
    for raw in str(text or "").splitlines():
        line = re.sub(r"\s+", " ", raw.strip())
        if not line:
            if in_hints:
                in_hints = False
            continue
        if line.lower().startswith("structured extraction hints"):
            in_hints = True
            continue
        if in_hints and ":" in line and line.split(":", 1)[0].strip().lower() in _ALL_HINT_FIELDS:
            continue
        lines.append(line)
    return lines


def _extract_product(hints: dict[str, str], lines: list[str], text: str) -> SpecializedExtraction:
    data = {
        "name": hints.get("name")
        or hints.get("product")
        or _labeled(lines, ("product name", "name", "商品名称", "商品名", "名称"))
        or _pick_title(lines),
        "price": hints.get("price") or _labeled(lines, ("price", "价格", "售价")) or _price_from_text(text),
        "brand": hints.get("brand") or _labeled(lines, ("brand", "品牌")),
        "plan": hints.get("plan") or _labeled(lines, ("plan", "tier", "套餐", "版本")),
        "billing_period": hints.get("billing_period")
        or _regex_first(
            r"per\s+(?:seat|user)?/?\s*(?:month|year)|/mo\b|/month\b|monthly|annually|/yr|/year|每月|每年|月付|年付",
            text,
        ),
        "availability": hints.get("availability")
        or _regex_first(
            r"\b(?:in stock|out of stock|sold out|available|pre[-\s]?order)\b|有货|无货|缺货|现货|售罄",
            text,
        ),
        "description": hints.get("summary") or hints.get("description") or _long_line(lines),
    }
    return _result("product", data, hints)


def _extract_pricing(hints: dict[str, str], lines: list[str], text: str) -> SpecializedExtraction:
    data = {
        "plan": hints.get("plan")
        or _labeled(lines, ("plan", "tier", "套餐", "版本"))
        or _pick_title(lines),
        "price": hints.get("price") or _price_from_text(text),
        "billing_period": hints.get("billing_period")
        or _regex_first(
            r"per\s+(?:seat|user)?/?\s*(?:month|year)|/mo\b|/month\b|monthly|annually|/yr|/year|每月|每年|月付|年付",
            text,
        ),
        "description": hints.get("summary") or hints.get("description") or _long_line(lines),
        "summary": hints.get("summary") or _long_line(lines, limit=120),
    }
    return _result("pricing", data, hints)


def _extract_job(hints: dict[str, str], lines: list[str], text: str) -> SpecializedExtraction:
    data = {
        "title": hints.get("title") or hints.get("name") or _pick_title(lines),
        "company": hints.get("company")
        or hints.get("organization")
        or _labeled(lines, ("company", "hiring organization", "公司", "招聘方")),
        "salary_range": hints.get("salary_range")
        or _regex_first(
            r"(?:[$€£¥]\s?\d[\d,.]*\s*[-~至]\s*[$€£¥]?\s?\d[\d,.]*(?:/[^\s]+)?|\d+(?:k|K|千|万)?\s*[-~至]\s*\d+(?:k|K|千|万)?(?:/[^\s]+)?)",
            text,
        ),
        "location": hints.get("location")
        or _labeled(lines, ("location", "地点", "城市", "工作地点")),
        "requirements": hints.get("requirements")
        or _labeled(lines, ("requirements", "任职要求", "岗位要求", "要求"))
        or _section(lines, ("requirements", "任职要求", "岗位要求", "要求")),
    }
    return _result("job", data, hints)


def _extract_article_like(
    page_type: str,
    hints: dict[str, str],
    lines: list[str],
    text: str,
) -> SpecializedExtraction:
    data = {
        "title": hints.get("title") or _pick_title(lines),
        "publish_date": hints.get("publish_date")
        or hints.get("date")
        or _regex_first(
            r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4}\b|\b(?:19|20)\d{2}[-/.年]\d{1,2}[-/.月]\d{1,2}",
            text,
        ),
        "author": hints.get("author") or _labeled(lines, ("author", "作者", "发布者")),
        "summary": hints.get("summary") or hints.get("description") or _long_line(lines, limit=160),
        "content": hints.get("content")
        or "\n".join([line for line in lines if len(line) > 24][:8])[:1600],
    }
    return _result(page_type, data, hints)


def _extract_policy(hints: dict[str, str], lines: list[str], text: str) -> SpecializedExtraction:
    base = _extract_article_like("policy", hints, lines, text).data
    base.update(
        {
            "agency": hints.get("agency")
            or hints.get("organization")
            or _labeled(lines, ("agency", "department", "机构", "部门", "发布机关")),
            "policy_number": hints.get("policy_number")
            or _regex_first(r"(?:No\.?|编号|文号)[:：\s]*([A-Za-z0-9][A-Za-z0-9\-〔〕\[\]号]+)", text),
        }
    )
    return _result("policy", base, hints)


def _result(page_type: str, data: dict[str, Any], hints: dict[str, str]) -> SpecializedExtraction:
    cleaned = {key: value for key, value in data.items() if value not in (None, "", [], {})}
    hint_hits = {
        key: "structured"
        for key in cleaned
        if key in hints or (key == "name" and "product" in hints)
    }
    confidence = min(0.98, 0.45 + len(cleaned) * 0.12 + len(hint_hits) * 0.08)
    return SpecializedExtraction(
        page_type=page_type,
        data=cleaned,
        confidence=confidence,
        source_fields=hint_hits,
    )


def _pick_title(lines: list[str]) -> str:
    for line in lines[:10]:
        stripped = re.sub(r"^[#>*\-\u2022\s]+", "", line).strip()
        if 4 <= len(stripped) <= 120 and not _is_noise(stripped):
            return stripped
    return ""


def _long_line(lines: list[str], *, limit: int = 240) -> str:
    for line in lines:
        if len(line) >= 20 and not _is_noise(line):
            return line[:limit]
    return ""


def _labeled(lines: list[str], aliases: tuple[str, ...]) -> str:
    for line in lines:
        for alias in aliases:
            matched = re.search(rf"^{re.escape(alias)}\s*[:：]\s*(.+)$", line, flags=re.I)
            if matched:
                return matched.group(1).strip()
    return ""


def _section(lines: list[str], aliases: tuple[str, ...]) -> str:
    for index, line in enumerate(lines):
        if any(alias.lower() in line.lower() for alias in aliases):
            return "\n".join(lines[index + 1 : index + 5])[:800]
    return ""


def _price_from_text(text: str) -> str:
    return _regex_first(
        r"(?:from\s+|starting\s+at\s+|starts\s+at\s+)?(?:[$€£¥]\s?\d[\d,.]*|\d[\d,.]*\s?(?:USD|EUR|GBP|CNY|RMB))(?:\s*(?:/mo|/month|per month|per seat/month|monthly|/yr|/year|annually))?",
        text,
    )


def _regex_first(pattern: str, text: str) -> str:
    matched = re.search(pattern, str(text or ""), flags=re.I)
    if not matched:
        return ""
    return (matched.group(1) if matched.lastindex else matched.group(0)).strip()


def _is_noise(value: str) -> bool:
    lowered = value.lower()
    return any(
        marker in lowered
        for marker in ("cookie", "privacy", "sign in", "subscribe", "menu", "search")
    )
