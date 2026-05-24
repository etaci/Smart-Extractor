"""Field-level normalization for dynamic extraction results."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from smart_extractor.extractor.llm_response import _format_dynamic_text
from smart_extractor.models.base import DynamicExtractResult


_CURRENCY_SYMBOLS = {
    "$": "USD",
    "US$": "USD",
    "€": "EUR",
    "£": "GBP",
    "¥": "CNY",
    "￥": "CNY",
    "RMB": "CNY",
    "CNY": "CNY",
    "USD": "USD",
    "EUR": "EUR",
    "GBP": "GBP",
}

_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

_PRICE_FIELDS = {
    "price",
    "amount",
    "monthly_price",
    "annual_price",
    "sale_price",
    "regular_price",
}
_DATE_FIELDS = {
    "publish_date",
    "date",
    "posted_at",
    "published_at",
    "updated_at",
    "valid_through",
    "deadline",
}
_PERIOD_FIELDS = {"billing_period", "period", "billing", "billing_cycle"}


def normalize_dynamic_result(result: DynamicExtractResult) -> DynamicExtractResult:
    """Normalize extracted values in place and refresh formatted text."""

    normalized_data = {
        field: normalize_field_value(field, value)
        for field, value in (result.data or {}).items()
    }
    result.data = normalized_data
    result.formatted_text = _format_dynamic_text(result.field_labels or {}, normalized_data)
    details = result.strategy_details if isinstance(result.strategy_details, dict) else {}
    result.strategy_details = {
        **details,
        "normalization_version": "v1",
    }
    return result


def normalize_field_value(field: str, value: Any) -> Any:
    if value in (None, "", [], {}):
        return value
    field_name = str(field or "").strip().lower()
    if isinstance(value, list):
        return [normalize_field_value(field_name, item) for item in value]
    if isinstance(value, dict):
        return value

    text = _clean_text(value)
    if not text:
        return ""

    if field_name in _PRICE_FIELDS or field_name.endswith("_price"):
        return normalize_price(text)
    if field_name in _DATE_FIELDS or field_name.endswith("_date"):
        return normalize_date(text)
    if field_name in _PERIOD_FIELDS:
        return normalize_billing_period(text)
    if field_name in {"availability", "stock"}:
        return normalize_availability(text)
    if field_name in {"salary", "salary_range", "compensation"}:
        return normalize_salary(text)
    return text


def normalize_price(value: str) -> str:
    text = _clean_text(value)
    text = re.sub(r"^(?:from|starting at|starts at|as low as)\s+", "", text, flags=re.I)
    currency = _detect_currency(text)
    matched = re.search(
        r"(\d{1,9}(?:[,\s]\d{3})*(?:[.]\d{1,4})?|\d+(?:[.]\d{1,4})?)",
        text,
    )
    if not matched:
        return text
    amount = matched.group(1).replace(",", "").replace(" ", "")
    if "." in amount:
        amount = amount.rstrip("0").rstrip(".")
    period = normalize_billing_period(text)
    parts = [currency, amount, period]
    return " ".join(part for part in parts if part).strip()


def normalize_date(value: str) -> str:
    text = _clean_text(value)
    iso = re.search(r"\b((?:19|20)\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})", text)
    if iso:
        return _safe_date(int(iso.group(1)), int(iso.group(2)), int(iso.group(3))) or text

    chinese = re.search(r"\b((?:19|20)\d{2})年(\d{1,2})月(\d{1,2})日?", text)
    if chinese:
        return _safe_date(int(chinese.group(1)), int(chinese.group(2)), int(chinese.group(3))) or text

    day_month_year = re.search(
        r"\b(\d{1,2})\s+([A-Za-z]{3,9})\.?,?\s+(\d{2,4})\b",
        text,
        flags=re.I,
    )
    if day_month_year:
        year = _normalize_year(int(day_month_year.group(3)))
        month = _MONTHS.get(day_month_year.group(2).lower().rstrip("."))
        if month:
            return _safe_date(year, month, int(day_month_year.group(1))) or text

    month_day_year = re.search(
        r"\b([A-Za-z]{3,9})\.?\s+(\d{1,2}),?\s+(\d{2,4})\b",
        text,
        flags=re.I,
    )
    if month_day_year:
        year = _normalize_year(int(month_day_year.group(3)))
        month = _MONTHS.get(month_day_year.group(1).lower().rstrip("."))
        if month:
            return _safe_date(year, month, int(month_day_year.group(2))) or text
    return text


def normalize_billing_period(value: str) -> str:
    text = _clean_text(value).lower()
    if not text:
        return ""
    unit = ""
    if re.search(r"\b(seat|user|member)\b", text):
        unit = "seat" if "seat" in text else "user"
    if re.search(r"per\s+seat\s*/\s*month|per\s+seat\s+per\s+month", text):
        return "per seat/month"
    if re.search(r"per\s+user\s*/\s*month|per\s+user\s+per\s+month", text):
        return "per user/month"
    if re.search(r"/mo\b|/month\b|per\s+month|monthly|每月|月付", text):
        return f"per {unit}/month" if unit else "per month"
    if re.search(r"/yr\b|/year\b|per\s+year|annually|annual|每年|年付", text):
        return f"per {unit}/year" if unit else "per year"
    return ""


def normalize_availability(value: str) -> str:
    text = _clean_text(value)
    lowered = text.lower()
    if re.search(r"out\s+of\s+stock|sold\s+out|缺货|无货|售罄", lowered):
        return "out_of_stock"
    if re.search(r"in\s*stock|instock|available|有货|现货|可购买", lowered):
        return "in_stock"
    if re.search(r"pre[-\s]?order|预订|预约", lowered):
        return "preorder"
    return text


def normalize_salary(value: str) -> str:
    text = _clean_text(value)
    text = re.sub(r"\s*/\s*", "/", text)
    text = re.sub(r"\s*[-~至到]\s*", "-", text)
    return text


def _detect_currency(text: str) -> str:
    lowered = text.lower()
    for marker, code in _CURRENCY_SYMBOLS.items():
        if marker.lower() in lowered:
            return code
    return ""


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _normalize_year(year: int) -> int:
    if year < 100:
        return 2000 + year if year < 70 else 1900 + year
    return year


def _safe_date(year: int, month: int, day: int) -> str:
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return ""
