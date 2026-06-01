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
    candidates: dict[str, list[str]] = {}
    for payload in _iter_json_ld_payloads(soup):
        _merge_hints(hints, _extract_json_ld_hints(payload), candidates=candidates)
    for payload in _iter_hydration_payloads(soup):
        _merge_hints(hints, _extract_hydration_hints(payload), candidates=candidates)
    _merge_hints(hints, _extract_microdata_hints(soup), candidates=candidates)
    _merge_hints(hints, _extract_meta_hints(soup), candidates=candidates)

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
    for field in ("price", "publish_date", "availability", "billing_period", "salary_range"):
        values = candidates.get(field) or []
        if len(values) > 1:
            lines.append(f"{field}_candidates: {' | '.join(values[:5])}")
    if not lines:
        return ""
    return "Structured extraction hints:\n" + "\n".join(lines)


def _merge_hints(
    target: dict[str, str],
    source: dict[str, str],
    *,
    candidates: dict[str, list[str]] | None = None,
) -> None:
    for key, value in source.items():
        normalized = _normalize_value(value)
        if not normalized:
            continue
        if candidates is not None:
            bucket = candidates.setdefault(key, [])
            if normalized not in bucket:
                bucket.append(normalized)
        if key not in target:
            target[key] = normalized


def _normalize_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        value = next((item for item in value if item not in (None, "", [], {})), "")
    if isinstance(value, dict):
        value = (
            value.get("name")
            or value.get("title")
            or value.get("text")
            or value.get("label")
            or value.get("displayName")
            or value.get("locationsText")
            or value.get("addressLocality")
            or value.get("addressRegion")
            or value.get("@id")
            or value.get("url")
            or ""
        )
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
        script_type = str(script.get("type") or "").strip().lower()
        if script_type in {"application/json", "application/ld+json+raw"}:
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
                "ShopifyAnalytics",
                "BigCommerce",
                "__SFCC",
                "variants",
                "greenhouse",
                "lever",
                "ashby",
                "workday",
                "smartrecruiters",
                "jobPostingTitle",
                "jobRequisitionId",
                "externalPath",
                "locationsText",
                "requisitionId",
                "refNumber",
                "opening",
                "jobPosting",
                "jobReqId",
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
            "ShopifyAnalytics.meta.product",
            "ShopifyAnalytics.meta",
            "window.Shopify",
            "BigCommerce",
            "__SFCC",
            "window.__JOB_DATA__",
            "window.__INITIAL_JOB__",
            "window.__CAREERS_DATA__",
            "window.__JOB_POSTING__",
            "window.__ASHBY_DATA__",
            "window.__GREENHOUSE_DATA__",
            "window.__LEVER_DATA__",
            "jobPosting",
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
        if item.get("sku"):
            hints.setdefault("sku", _normalize_value(item.get("sku")))
        for gtin_key in ("gtin", "gtin8", "gtin12", "gtin13", "gtin14"):
            if item.get(gtin_key):
                hints.setdefault("gtin", _normalize_value(item.get(gtin_key)))
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
        authoritative_job_payload = any(
            key in normalized_keys
            for key in ("jobpostingtitle", "jobrequisitionid", "locationstext")
        )
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

        name_value = _first_key_value(
            item,
            normalized_keys,
            ("name", "productname", "title", "jobpostingtitle", "text"),
        )
        if name_value:
            product_like = _type_matches(item_type, {"product", "offer"}) or any(
                key in normalized_keys
                for key in ("price", "saleprice", "currentprice", "productname", "availability", "variants")
            )
            if product_like:
                hints.setdefault("name", name_value)
                hints.setdefault("product", name_value)
            elif authoritative_job_payload:
                hints["title"] = name_value
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
        if not price and isinstance(item.get("variants"), list):
            for variant in item.get("variants") or []:
                if not isinstance(variant, dict):
                    continue
                variant_keys = {str(key).strip().lower(): key for key in variant.keys()}
                price = _first_key_value(
                    variant,
                    variant_keys,
                    ("price", "saleprice", "currentprice", "amount"),
                )
                if price:
                    break
        currency = _first_key_value(item, normalized_keys, ("currency", "pricecurrency"))
        if price:
            hints.setdefault("price", f"{currency} {price}".strip() if currency else price)
        plan = _first_key_value(item, normalized_keys, ("plan", "planname", "tier", "tiername", "package"))
        if plan:
            hints.setdefault("plan", plan)
        brand = _first_key_value(item, normalized_keys, ("brand", "vendor", "manufacturer"))
        if brand:
            hints.setdefault("brand", _normalize_value(brand))
        sku = _first_key_value(item, normalized_keys, ("sku", "productsku", "mpn"))
        if sku:
            hints.setdefault("sku", sku)
        gtin = _first_key_value(item, normalized_keys, ("gtin", "gtin8", "gtin12", "gtin13", "gtin14", "barcode"))
        if gtin:
            hints.setdefault("gtin", gtin)
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
            ("company", "companyname", "hiringorganization", "organization", "department", "office"),
        )
        if not company and isinstance(item.get("company"), dict):
            company = _normalize_value(item.get("company"))
        if not company and isinstance(item.get("hiringOrganization"), dict):
            company = _normalize_value(item.get("hiringOrganization"))
        if company:
            hints.setdefault("company", _normalize_value(company))
        location = _first_key_value(
            item,
            normalized_keys,
            (
                "location",
                "joblocation",
                "address",
                "worklocation",
                "primarylocation",
                "locations",
                "locationstext",
            ),
        )
        if not location and isinstance(item.get("categories"), dict):
            location = _normalize_value(item.get("categories", {}).get("location"))
        if not location and isinstance(item.get("offices"), list):
            location = _normalize_value(item.get("offices"))
        if not location and isinstance(item.get("location"), dict):
            location = _normalize_value(item.get("location"))
        if not location and isinstance(item.get("locations"), list):
            location = _normalize_value(item.get("locations"))
        if location:
            if authoritative_job_payload:
                hints["location"] = _normalize_value(location)
            else:
                hints.setdefault("location", _normalize_value(location))
        employment_type = _first_key_value(
            item,
            normalized_keys,
            ("employmenttype", "commitment", "jobtype", "type", "employment", "worktype"),
        )
        if not employment_type and isinstance(item.get("categories"), dict):
            employment_type = _normalize_value(
                item.get("categories", {}).get("commitment")
                or item.get("categories", {}).get("employmentType")
            )
        if employment_type:
            hints.setdefault("employment_type", employment_type)
        req_id = _first_key_value(
            item,
            normalized_keys,
            (
                "requisitionid",
                "jobrequisitionid",
                "jobreqid",
                "reqid",
                "refnumber",
                "jobid",
                "externalpath",
                "id",
            ),
        )
        if req_id:
            if authoritative_job_payload:
                hints["job_id"] = req_id
            else:
                hints.setdefault("job_id", req_id)
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
        content = _first_key_value(
            item,
            normalized_keys,
            ("articlebody", "body", "content", "text", "description", "jobdescription"),
        )
        if content:
            if authoritative_job_payload:
                hints["content"] = content
                hints["requirements"] = content
            else:
                hints.setdefault("content", content)
                hints.setdefault("requirements", content)
        availability = _first_key_value(item, normalized_keys, ("availability", "stock", "inventory"))
        if not availability and isinstance(item.get("variants"), list):
            for variant in item.get("variants") or []:
                if not isinstance(variant, dict):
                    continue
                variant_keys = {str(key).strip().lower(): key for key in variant.keys()}
                availability = _first_key_value(
                    variant,
                    variant_keys,
                    ("availability", "stock", "inventory", "available"),
                )
                if availability:
                    break
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
