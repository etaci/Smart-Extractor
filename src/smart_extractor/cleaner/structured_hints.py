"""Extract high-confidence field hints from raw HTML before text cleaning."""

from __future__ import annotations

import json
import re
from typing import Any

from bs4 import BeautifulSoup


_MAX_HINT_VALUE_LENGTH = 240


def build_structured_hints(html: str, selected_fields: list[str] | None = None) -> str:
    """Return labeled, high-confidence hints from metadata and JSON-LD.

    Cleaned page text often starts with navigation, cookie text, or footnotes. These
    hints give the rule fallback and LLM a compact, label-first view of canonical
    page metadata before the noisy body text.
    """
    if not html or not html.strip():
        return ""
    fields = [str(field).strip().lower() for field in selected_fields or [] if str(field).strip()]
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return ""

    hints: dict[str, str] = {}
    _merge_hints(hints, _extract_meta_hints(soup))
    for payload in _iter_json_ld_payloads(soup):
        _merge_hints(hints, _extract_json_ld_hints(payload))
    for payload in _iter_hydration_payloads(soup):
        _merge_hints(hints, _extract_hydration_hints(payload))
    _merge_hints(hints, _extract_microdata_hints(soup))

    if fields:
        ordered_fields = fields + [field for field in hints if field not in fields]
    else:
        ordered_fields = list(hints)

    lines = []
    for field in ordered_fields:
        value = hints.get(field)
        if not value:
            continue
        lines.append(f"{field}: {value}")
    if not lines:
        return ""
    return "Structured extraction hints:\n" + "\n".join(lines)


def _merge_hints(target: dict[str, str], source: dict[str, str]) -> None:
    for key, value in source.items():
        normalized = _normalize_value(value)
        if not normalized or key in target:
            continue
        target[key] = normalized


def _normalize_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        value = next((item for item in value if item not in (None, "", [], {})), "")
    if isinstance(value, dict):
        value = value.get("name") or value.get("@id") or value.get("url") or ""
    text = re.sub(r"\s+", " ", str(value).strip())
    if not text:
        return ""
    return text[:_MAX_HINT_VALUE_LENGTH]


def _extract_meta_hints(soup: BeautifulSoup) -> dict[str, str]:
    hints: dict[str, str] = {}
    title = soup.find("title")
    if title:
        hints["title"] = title.get_text(" ", strip=True)

    mapping = {
        "og:title": "title",
        "twitter:title": "title",
        "description": "summary",
        "og:description": "summary",
        "twitter:description": "summary",
        "article:published_time": "publish_date",
        "article:modified_time": "date",
        "date": "date",
        "pubdate": "publish_date",
        "author": "author",
        "product:price:amount": "price",
        "product:price:currency": "price_currency",
        "product:availability": "availability",
        "og:price:amount": "price",
        "og:price:currency": "price_currency",
        "article:author": "author",
        "article:section": "category",
    }
    for tag in soup.find_all("meta"):
        key = (
            tag.get("property")
            or tag.get("name")
            or tag.get("itemprop")
            or ""
        )
        content = tag.get("content") or ""
        field = mapping.get(str(key).strip().lower())
        if field and content:
            hints[field] = content
    if hints.get("price") and hints.get("price_currency") and not _has_currency(hints["price"]):
        hints["price"] = f"{hints['price_currency']} {hints['price']}"
    hints.pop("price_currency", None)
    return hints


def _iter_json_ld_payloads(soup: BeautifulSoup) -> list[Any]:
    payloads: list[Any] = []
    for script in soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)}):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            payloads.append(json.loads(raw))
        except Exception:
            continue
    return payloads


def _iter_hydration_payloads(soup: BeautifulSoup) -> list[Any]:
    payloads: list[Any] = []
    for script in soup.find_all("script"):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        if script.has_attr("data-smart-captured-response"):
            parsed = _safe_json_loads(raw)
            if parsed is not None:
                payloads.append(parsed)
            continue
        if str(script.get("id") or "").strip() == "__NEXT_DATA__":
            parsed = _safe_json_loads(raw)
            if parsed is not None:
                payloads.append(parsed)
            continue
        if "__NUXT_DATA__" in raw:
            parsed = _safe_json_loads(raw)
            if parsed is not None:
                payloads.append(parsed)
            continue
        if not any(
            marker in raw
            for marker in (
                "__NEXT_DATA__",
                "__NUXT__",
                "__APOLLO_STATE__",
                "__INITIAL_STATE__",
                "__PRELOADED_STATE__",
                "__REACT_QUERY_STATE__",
            )
        ):
            continue
        for marker in (
            "__NEXT_DATA__",
            "__NUXT__",
            "__APOLLO_STATE__",
            "__INITIAL_STATE__",
            "__PRELOADED_STATE__",
            "__REACT_QUERY_STATE__",
        ):
            for candidate in _extract_json_assignments(raw, marker):
                parsed = _safe_json_loads(candidate)
                if parsed is not None:
                    payloads.append(parsed)
    return payloads


def _extract_json_assignments(raw: str, marker: str) -> list[str]:
    payloads: list[str] = []
    search_from = 0
    while True:
        marker_pos = raw.find(marker, search_from)
        if marker_pos < 0:
            return payloads
        equals_pos = raw.find("=", marker_pos)
        if equals_pos < 0:
            search_from = marker_pos + len(marker)
            continue
        start = -1
        for index in range(equals_pos + 1, min(len(raw), equals_pos + 200)):
            if raw[index] in "{[":
                start = index
                break
            if raw[index] not in " \t\r\n":
                break
        if start < 0:
            search_from = equals_pos + 1
            continue
        end = _find_balanced_json_end(raw, start)
        if end > start:
            payloads.append(raw[start:end])
            search_from = end
        else:
            search_from = start + 1


def _find_balanced_json_end(raw: str, start: int) -> int:
    opener = raw[start]
    closer = "}" if opener == "{" else "]"
    stack = [closer]
    in_string = False
    escape = False
    for index in range(start + 1, len(raw)):
        char = raw[index]
        if escape:
            escape = False
            continue
        if char == "\\":
            escape = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if char in "{[":
            stack.append("}" if char == "{" else "]")
        elif stack and char == stack[-1]:
            stack.pop()
            if not stack:
                return index + 1
    return -1


def _safe_json_loads(raw: str) -> Any | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _extract_json_ld_hints(payload: Any) -> dict[str, str]:
    hints: dict[str, str] = {}
    for item in _walk_json_ld(payload):
        if not isinstance(item, dict):
            continue
        item_type = _json_type(item)
        if item.get("title"):
            hints.setdefault("title", item.get("title"))
        if item.get("name"):
            if _type_matches(item_type, {"product", "softwareapplication", "service", "offer"}):
                hints.setdefault("name", item.get("name"))
                hints.setdefault("product", item.get("name"))
            elif _type_matches(item_type, {"organization", "governmentorganization", "corporation"}):
                hints.setdefault("organization", item.get("name"))
            else:
                hints.setdefault("title", item.get("name"))
        if item.get("headline"):
            hints.setdefault("title", item.get("headline"))
        if item.get("description"):
            hints.setdefault("summary", item.get("description"))
        if item.get("articleBody"):
            hints.setdefault("content", item.get("articleBody"))
        if item.get("datePublished"):
            hints.setdefault("publish_date", item.get("datePublished"))
        if item.get("dateModified"):
            hints.setdefault("date", item.get("dateModified"))
        if item.get("validThrough"):
            hints.setdefault("valid_through", item.get("validThrough"))
        if item.get("author"):
            hints.setdefault("author", _normalize_value(item.get("author")))
        if item.get("publisher"):
            hints.setdefault("agency", _normalize_value(item.get("publisher")))
        if item.get("brand"):
            hints.setdefault("brand", _normalize_value(item.get("brand")))
        if item.get("hiringOrganization"):
            hints.setdefault("company", _normalize_value(item.get("hiringOrganization")))
        if item.get("jobLocation"):
            hints.setdefault("location", _normalize_value(item.get("jobLocation")))
        if item.get("baseSalary"):
            hints.setdefault("salary_range", _normalize_salary(item.get("baseSalary")))
        offer = item.get("offers")
        if not offer and _type_matches(
            item_type,
            {"offer", "aggregateoffer", "pricespecification", "unitpricespecification"},
        ):
            offer = item
        if offer:
            _merge_hints(hints, _extract_offer_hints(offer))
        if _type_matches(item_type, {"governmentorganization", "organization"}):
            organization = item.get("name") or item.get("legalName")
            if organization:
                hints.setdefault("agency", organization)
        if _type_matches(item_type, {"legislation", "governmentservice", "report"}):
            policy_number = item.get("legislationIdentifier") or item.get("identifier")
            if policy_number:
                hints.setdefault("policy_number", _normalize_value(policy_number))
    return hints


def _extract_hydration_hints(payload: Any) -> dict[str, str]:
    hints: dict[str, str] = {}
    for item in _walk_any_payload(payload):
        if not isinstance(item, dict):
            continue
        normalized_keys = {str(key).strip().lower(): key for key in item.keys()}
        item_type = _json_type(item)
        if _type_matches(
            item_type,
            {
                "product",
                "offer",
                "aggregateoffer",
                "pricespecification",
                "jobposting",
                "article",
                "newsarticle",
                "legislation",
                "governmentservice",
                "report",
            },
        ):
            _merge_hints(hints, _extract_json_ld_hints(item))

        name_value = _first_key_value(item, normalized_keys, ("name", "productname", "title"))
        if name_value:
            product_like = _type_matches(item_type, {"product", "offer"}) or any(
                key in normalized_keys
                for key in ("price", "saleprice", "currentprice", "productname", "availability")
            )
            if product_like:
                hints.setdefault("name", name_value)
                hints.setdefault("product", name_value)
            else:
                hints.setdefault("title", name_value)

        headline = _first_key_value(item, normalized_keys, ("headline", "heading"))
        if headline:
            hints.setdefault("title", headline)
        summary = _first_key_value(item, normalized_keys, ("description", "summary", "excerpt", "subtitle"))
        if summary:
            hints.setdefault("summary", summary)
        price = _first_key_value(
            item,
            normalized_keys,
            ("price", "saleprice", "currentprice", "amount", "lowprice", "monthlyprice", "annualprice"),
        )
        currency = _first_key_value(item, normalized_keys, ("currency", "pricecurrency"))
        if price:
            hints.setdefault("price", f"{currency} {price}".strip() if currency else price)
        plan = _first_key_value(item, normalized_keys, ("plan", "planname", "tier", "tiername", "package"))
        if plan:
            hints.setdefault("plan", plan)
        billing_period = _first_key_value(
            item,
            normalized_keys,
            ("billingperiod", "billingcycle", "priceperiod", "unittext", "recurringinterval"),
        )
        if billing_period:
            hints.setdefault("billing_period", billing_period)
        company = _first_key_value(
            item,
            normalized_keys,
            ("company", "companyname", "hiringorganization", "organization"),
        )
        if company:
            hints.setdefault("company", _normalize_value(company))
        location = _first_key_value(item, normalized_keys, ("location", "joblocation", "address"))
        if location:
            hints.setdefault("location", _normalize_value(location))
        publish_date = _first_key_value(
            item,
            normalized_keys,
            ("datepublished", "publishedate", "publishedat", "createdat", "date", "releasedate"),
        )
        if publish_date:
            hints.setdefault("publish_date", publish_date)
        author = _first_key_value(item, normalized_keys, ("author", "byline"))
        if author:
            hints.setdefault("author", _normalize_value(author))
        agency = _first_key_value(item, normalized_keys, ("agency", "department", "publisher", "issuer"))
        if agency:
            hints.setdefault("agency", _normalize_value(agency))
        policy_number = _first_key_value(
            item,
            normalized_keys,
            ("policynumber", "documentnumber", "filenumber", "identifier", "legislationidentifier"),
        )
        if policy_number:
            hints.setdefault("policy_number", _normalize_value(policy_number))
        content = _first_key_value(item, normalized_keys, ("articlebody", "body", "content", "text"))
        if content:
            hints.setdefault("content", content)
        availability = _first_key_value(item, normalized_keys, ("availability", "stock", "inventory"))
        if availability:
            hints.setdefault("availability", _normalize_value(availability).split("/")[-1])
    return hints


def _walk_any_payload(payload: Any, *, limit: int = 800) -> list[Any]:
    items: list[Any] = []
    stack = [payload]
    while stack and len(items) < limit:
        current = stack.pop()
        items.append(current)
        if isinstance(current, dict):
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return items


def _first_key_value(
    item: dict[str, Any],
    normalized_keys: dict[str, Any],
    names: tuple[str, ...],
) -> str:
    for name in names:
        key = normalized_keys.get(name)
        if key is not None:
            value = _normalize_value(item.get(key))
            if value:
                return value
    return ""


def _walk_json_ld(payload: Any) -> list[Any]:
    items: list[Any] = []
    if isinstance(payload, list):
        for item in payload:
            items.extend(_walk_json_ld(item))
    elif isinstance(payload, dict):
        items.append(payload)
        for key in ("@graph", "itemListElement", "mainEntity", "offers"):
            if key in payload:
                items.extend(_walk_json_ld(payload[key]))
    return items


def _json_type(item: dict[str, Any]) -> set[str]:
    raw = item.get("@type") or item.get("type") or ""
    if isinstance(raw, list):
        values = raw
    else:
        values = [raw]
    return {str(value).strip().lower() for value in values if str(value).strip()}


def _type_matches(item_type: set[str], expected: set[str]) -> bool:
    return bool(item_type & expected)


def _extract_offer_hints(offer_payload: Any) -> dict[str, str]:
    hints: dict[str, str] = {}
    offers = offer_payload if isinstance(offer_payload, list) else [offer_payload]
    for offer in offers:
        if not isinstance(offer, dict):
            continue
        price = offer.get("price") or offer.get("lowPrice") or offer.get("highPrice")
        currency = offer.get("priceCurrency") or offer.get("currency")
        if price not in (None, ""):
            hints.setdefault("price", f"{currency} {price}".strip() if currency else str(price))
        availability = offer.get("availability") or offer.get("availabilityStarts")
        if availability:
            hints.setdefault("availability", str(availability).split("/")[-1])
        if offer.get("name"):
            hints.setdefault("plan", offer.get("name"))
        if offer.get("priceSpecification"):
            _merge_hints(hints, _extract_offer_hints(offer.get("priceSpecification")))
        billing_period = offer.get("billingDuration") or offer.get("unitText") or offer.get("billingPeriod")
        if billing_period:
            hints.setdefault("billing_period", _normalize_value(billing_period))
    return hints


def _normalize_salary(value: Any) -> str:
    if isinstance(value, dict):
        amount = value.get("value") or {}
        if isinstance(amount, dict):
            minimum = amount.get("minValue")
            maximum = amount.get("maxValue")
            unit = amount.get("unitText") or ""
            currency = amount.get("currency") or value.get("currency") or ""
            if minimum and maximum:
                return f"{currency} {minimum}-{maximum} {unit}".strip()
            if amount.get("value"):
                return f"{currency} {amount.get('value')} {unit}".strip()
    return _normalize_value(value)


def _extract_microdata_hints(soup: BeautifulSoup) -> dict[str, str]:
    mapping = {
        "name": "name",
        "headline": "title",
        "price": "price",
        "pricecurrency": "price_currency",
        "availability": "availability",
        "datepublished": "publish_date",
        "datemodified": "date",
        "author": "author",
        "brand": "brand",
        "jobtitle": "title",
        "hiringorganization": "company",
        "joblocation": "location",
        "basesalary": "salary_range",
        "articlebody": "content",
        "publisher": "agency",
    }
    hints: dict[str, str] = {}
    for tag in soup.find_all(attrs={"itemprop": True}):
        prop = str(tag.get("itemprop") or "").strip().lower()
        field = mapping.get(prop)
        if not field:
            continue
        value = tag.get("content") or tag.get("value") or tag.get_text(" ", strip=True)
        if value:
            hints.setdefault(field, value)
    if hints.get("price") and hints.get("price_currency") and not _has_currency(hints["price"]):
        hints["price"] = f"{hints['price_currency']} {hints['price']}"
    hints.pop("price_currency", None)
    return hints


def _has_currency(value: str) -> bool:
    return bool(re.search(r"[$€£¥]|USD|EUR|GBP|CNY|RMB", str(value), re.I))
