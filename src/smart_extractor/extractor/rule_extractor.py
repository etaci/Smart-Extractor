"""
Rule-first dynamic extractor for repeated pages.
"""

from __future__ import annotations

import re
from typing import Any

from smart_extractor.extractor.learned_profile_store import LearnedProfile
from smart_extractor.models.base import DynamicExtractResult
from smart_extractor.utils.display import get_field_label


_FIELD_ALIASES: dict[str, list[str]] = {
    "title": ["标题", "主题", "标题名称", "title"],
    "name": ["名称", "商品名", "产品名", "name"],
    "price": ["价格", "售价", "到手价", "price"],
    "brand": ["品牌", "brand"],
    "company": ["公司", "企业", "招聘方", "company"],
    "location": ["地点", "城市", "工作地点", "location"],
    "publish_date": ["发布时间", "发布日期", "更新时间", "日期", "publish date"],
    "author": ["作者", "发布者", "作者信息", "author"],
    "salary_range": ["薪资", "薪酬", "工资", "salary"],
    "stock": ["库存", "现货", "有货", "缺货", "售罄", "stock"],
    "requirements": ["任职要求", "岗位要求", "职位要求", "要求", "requirements"],
    "description": ["描述", "简介", "说明", "description"],
    "summary": ["摘要", "总结", "核心结论", "summary"],
    "content": ["正文", "内容", "详情", "介绍", "content"],
}

_DATE_RE = re.compile(
    r"(20\d{2}[-/年]\d{1,2}[-/月]\d{1,2}(?:日)?(?:\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)?)"
)
_PRICE_RE = re.compile(
    r"((?:¥|￥|\$)?\s?\d{1,6}(?:[.,]\d{1,2})?\s*(?:元|美元|USD|usd|CNY|RMB)?(?:\s*[-~至到]\s*(?:¥|￥|\$)?\s?\d{1,6}(?:[.,]\d{1,2})?\s*(?:元|/月|/年|美元|USD)?)?)"
)
_SALARY_RE = re.compile(
    r"((?:\d{1,2}(?:\.\d+)?[kKwW万千]?[\-/~至到]\d{1,2}(?:\.\d+)?[kKwW万千]?|(?:¥|￥)?\d{1,6}(?:[.,]\d{1,2})?\s*[-~至到]\s*(?:¥|￥)?\d{1,6}(?:[.,]\d{1,2})?)(?:\s*/\s*(?:月|年))?)"
)


def _clean_line(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _split_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw in re.split(r"[\r\n]+", str(text or "")):
        item = _clean_line(raw)
        if item:
            lines.append(item)
    return lines


class RuleBasedDynamicExtractor:
    """Simple heuristics that reuse learned field plans."""

    def extract(
        self,
        text: str,
        *,
        source_url: str,
        profile: LearnedProfile,
        selected_fields: list[str] | None = None,
    ) -> DynamicExtractResult:
        lines = _split_lines(text)
        fields = selected_fields or profile.selected_fields
        normalized_fields = [field for field in fields if str(field or "").strip()]
        data: dict[str, Any] = {}

        title_like = self._pick_title(lines)
        if title_like and "title" in normalized_fields:
            data["title"] = title_like
        if title_like and "name" in normalized_fields:
            data.setdefault("name", title_like)

        for field in normalized_fields:
            value = data.get(field) or self._extract_field(field, lines, text)
            if value not in (None, "", [], {}):
                data[field] = value

        if "description" in normalized_fields and not data.get("description"):
            data["description"] = self._pick_description(lines, title_like)
        if "content" in normalized_fields and not data.get("content"):
            data["content"] = self._pick_content(lines, title_like)
        if "summary" in normalized_fields and not data.get("summary"):
            data["summary"] = self._pick_summary(
                data.get("description") or data.get("content") or text
            )

        filtered_data = {
            field: data.get(field, "")
            for field in normalized_fields
            if data.get(field, "") not in ("", None, [], {})
        }
        field_labels = {
            field: get_field_label(field, profile.field_labels)
            for field in normalized_fields
        }

        formatted_lines = []
        for field in normalized_fields:
            value = filtered_data.get(field)
            if value in ("", None, [], {}):
                continue
            formatted_lines.append(f"{field_labels.get(field, field)}：{value}")

        return DynamicExtractResult(
            page_type=profile.page_type or "unknown",
            candidate_fields=normalized_fields,
            selected_fields=normalized_fields,
            field_labels=field_labels,
            data=filtered_data,
            formatted_text="\n".join(formatted_lines),
            extraction_strategy="rule",
            learned_profile_id=profile.profile_id,
            strategy_details={
                "profile_id": profile.profile_id,
                "path_prefix": profile.path_prefix,
                "domain": profile.domain,
                "matched_fields": len(filtered_data),
                "expected_fields": len(normalized_fields),
                "sample_url": profile.sample_url,
                "source_url": source_url,
            },
        )

    def _extract_field(self, field: str, lines: list[str], text: str) -> str:
        field_name = str(field or "").strip().lower()
        if not field_name:
            return ""
        if field_name == "price":
            return self._extract_first_regex(_PRICE_RE, text)
        if field_name == "salary_range":
            return self._extract_first_regex(_SALARY_RE, text)
        if field_name == "publish_date":
            return self._extract_first_regex(_DATE_RE, text)
        if field_name == "stock":
            return self._extract_stock(text)

        aliases = _FIELD_ALIASES.get(field_name, [])
        labeled = self._extract_labeled_value(lines, aliases)
        if labeled:
            return labeled

        if field_name == "author":
            return self._extract_author(lines)
        if field_name == "location":
            return self._extract_location(lines)
        if field_name == "brand":
            return self._extract_brand(lines)
        if field_name == "company":
            return self._extract_company(lines)
        return ""

    @staticmethod
    def _extract_labeled_value(lines: list[str], aliases: list[str]) -> str:
        if not aliases:
            return ""
        patterns = [
            re.compile(rf"^(?:{re.escape(alias)})\s*[:：]\s*(.+)$", re.IGNORECASE)
            for alias in aliases
        ]
        for line in lines:
            for pattern in patterns:
                matched = pattern.search(line)
                if matched:
                    return _clean_line(matched.group(1))
        return ""

    @staticmethod
    def _extract_first_regex(pattern: re.Pattern[str], text: str) -> str:
        matched = pattern.search(str(text or ""))
        if not matched:
            return ""
        return _clean_line(matched.group(1))

    @staticmethod
    def _pick_title(lines: list[str]) -> str:
        for line in lines[:8]:
            if 4 <= len(line) <= 80:
                return line
        return lines[0] if lines else ""

    @staticmethod
    def _pick_description(lines: list[str], title_like: str) -> str:
        for line in lines:
            if line == title_like:
                continue
            if len(line) >= 18:
                return line[:240]
        return ""

    @staticmethod
    def _pick_content(lines: list[str], title_like: str) -> str:
        paragraphs = [line for line in lines if line != title_like and len(line) >= 16]
        if not paragraphs:
            return ""
        return "\n".join(paragraphs[:6])[:1200]

    @staticmethod
    def _pick_summary(text: str) -> str:
        normalized = _clean_line(text)
        if not normalized:
            return ""
        return normalized[:120]

    @staticmethod
    def _extract_stock(text: str) -> str:
        normalized = str(text or "")
        for marker in ("现货", "有货", "库存充足", "可预订", "售罄", "缺货", "无货"):
            if marker in normalized:
                return marker
        return ""

    @staticmethod
    def _extract_author(lines: list[str]) -> str:
        for line in lines:
            matched = re.search(r"(?:作者|author)\s*[:：]\s*(.+)$", line, re.IGNORECASE)
            if matched:
                return _clean_line(matched.group(1))
        return ""

    @staticmethod
    def _extract_location(lines: list[str]) -> str:
        for line in lines:
            matched = re.search(
                r"(?:地点|城市|工作地点|location)\s*[:：]\s*(.+)$", line, re.IGNORECASE
            )
            if matched:
                return _clean_line(matched.group(1))
        return ""

    @staticmethod
    def _extract_brand(lines: list[str]) -> str:
        for line in lines:
            matched = re.search(r"(?:品牌|brand)\s*[:：]\s*(.+)$", line, re.IGNORECASE)
            if matched:
                return _clean_line(matched.group(1))
        return ""

    @staticmethod
    def _extract_company(lines: list[str]) -> str:
        for line in lines:
            matched = re.search(
                r"(?:公司|企业|招聘方|company)\s*[:：]\s*(.+)$", line, re.IGNORECASE
            )
            if matched:
                return _clean_line(matched.group(1))
        return ""
