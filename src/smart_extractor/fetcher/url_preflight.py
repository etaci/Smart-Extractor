"""Lightweight URL health check before expensive extraction fetches."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx
from bs4 import BeautifulSoup


@dataclass(slots=True)
class URLPreflightResult:
    original_url: str
    final_url: str = ""
    status_code: int = 0
    reachable: bool = True
    reason: str = ""
    canonical_url: str = ""
    headers: dict[str, str] = field(default_factory=dict)
    redirect_chain: list[str] = field(default_factory=list)
    repair_reason: str = ""

    @property
    def target_url(self) -> str:
        return self.canonical_url or self.final_url or self.original_url


def preflight_url(
    url: str,
    *,
    timeout_ms: int = 5000,
    headers: dict[str, str] | None = None,
    verify_ssl: bool = True,
    sitemap_fallback_enabled: bool = True,
) -> URLPreflightResult:
    """Resolve redirects and obvious unreachable URLs without loading a browser."""

    normalized_url = str(url or "").strip()
    result = URLPreflightResult(original_url=normalized_url, final_url=normalized_url)
    if not normalized_url:
        result.reachable = False
        result.reason = "empty_url"
        return result

    timeout = max(float(timeout_ms or 5000) / 1000.0, 0.5)
    request_headers = dict(headers or {})
    try:
        with httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            verify=verify_ssl,
        ) as client:
            response = _head_or_probe_get(client, normalized_url, request_headers)
            result.status_code = int(response.status_code or 0)
            result.final_url = str(response.url)
            result.headers = dict(response.headers)
            result.redirect_chain = [str(item.url) for item in getattr(response, "history", [])] + [result.final_url]
            if result.status_code in {404, 410}:
                canonical_from_error = _extract_canonical_url(
                    response.text,
                    base_url=result.final_url,
                ) or _extract_og_url(response.text, base_url=result.final_url)
                if (
                    canonical_from_error
                    and canonical_from_error != normalized_url
                    and _is_safe_repair_candidate(normalized_url, canonical_from_error)
                ):
                    canonical_response = _head_or_probe_get(client, canonical_from_error, request_headers)
                    if 200 <= int(canonical_response.status_code or 0) < 400:
                        result.status_code = int(canonical_response.status_code or 0)
                        result.final_url = str(canonical_response.url)
                        result.canonical_url = canonical_from_error
                        result.headers = {
                            **dict(canonical_response.headers),
                            "x-smart-url-preflight-repaired-from": normalized_url,
                        }
                        result.redirect_chain = [normalized_url, result.final_url]
                        result.reason = "canonical_fallback"
                        result.repair_reason = "canonical_fallback"
                        return result
                repaired = (
                    _resolve_sitemap_fallback(client, normalized_url, request_headers)
                    if sitemap_fallback_enabled
                    else ""
                )
                if repaired and _is_safe_repair_candidate(normalized_url, repaired):
                    repaired_response = _head_or_probe_get(client, repaired, request_headers)
                    result.status_code = int(repaired_response.status_code or 0)
                    result.final_url = str(repaired_response.url)
                    result.headers = {
                        **dict(repaired_response.headers),
                        "x-smart-url-preflight-repaired-from": normalized_url,
                    }
                    if 200 <= result.status_code < 400:
                        result.canonical_url = _extract_canonical_url(
                            repaired_response.text,
                            base_url=result.final_url,
                        )
                        result.reason = "sitemap_fallback"
                        result.repair_reason = "sitemap_fallback"
                        return result
                variant = _resolve_url_variant(client, normalized_url, request_headers)
                if variant:
                    variant_response = _head_or_probe_get(client, variant, request_headers)
                    if 200 <= int(variant_response.status_code or 0) < 400:
                        result.status_code = int(variant_response.status_code or 0)
                        result.final_url = str(variant_response.url)
                        result.headers = {
                            **dict(variant_response.headers),
                            "x-smart-url-preflight-repaired-from": normalized_url,
                        }
                        result.redirect_chain = [normalized_url, result.final_url]
                        result.reason = "url_variant_fallback"
                        result.repair_reason = "url_variant_fallback"
                        return result
                result.reachable = False
                result.reason = f"http_{result.status_code}"
                return result
            if result.status_code == 0:
                result.reachable = False
                result.reason = "empty_response"
                return result
            if 200 <= result.status_code < 400:
                canonical_url = _extract_canonical_url(
                    response.text,
                    base_url=result.final_url,
                )
                if canonical_url and _is_safe_repair_candidate(normalized_url, canonical_url):
                    result.canonical_url = canonical_url
                elif canonical_url:
                    result.headers["x-smart-canonical-rejected"] = canonical_url
            return result
    except httpx.UnsupportedProtocol:
        result.reachable = False
        result.reason = "unsupported_protocol"
    except httpx.InvalidURL:
        result.reachable = False
        result.reason = "invalid_url"
    except httpx.ConnectError as exc:
        result.reachable = False
        result.reason = f"network: {exc}"
    except httpx.TimeoutException:
        result.reachable = False
        result.reason = "timeout"
    except Exception as exc:
        result.reachable = False
        result.reason = f"{type(exc).__name__}: {exc}"
    return result


def _head_or_probe_get(
    client: httpx.Client,
    url: str,
    headers: dict[str, str],
) -> httpx.Response:
    head_response: httpx.Response | None = None
    try:
        response = client.head(url, headers=headers)
        head_response = response
        if response.status_code not in {405, 403, 404, 410, 429}:
            return response
    except httpx.HTTPError:
        pass

    try:
        with client.stream("GET", url, headers={**headers, "Range": "bytes=0-65535"}) as response:
            content = response.read()
            return httpx.Response(
                status_code=response.status_code,
                headers=response.headers,
                content=content,
                request=response.request,
                extensions=response.extensions,
            )
    except AttributeError:
        if head_response is not None:
            return head_response
        raise


def _extract_canonical_url(html: str, *, base_url: str) -> str:
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html[:200_000], "lxml")
    except Exception:
        return ""
    canonical = soup.find("link", attrs={"rel": lambda value: value and "canonical" in value})
    href = canonical.get("href") if canonical else ""
    if not href:
        return ""
    return urljoin(base_url, str(href).strip())


def _extract_og_url(html: str, *, base_url: str) -> str:
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html[:200_000], "lxml")
    except Exception:
        return ""
    tag = soup.find("meta", attrs={"property": "og:url"}) or soup.find(
        "meta",
        attrs={"name": "og:url"},
    )
    content = tag.get("content") if tag else ""
    return urljoin(base_url, str(content).strip()) if content else ""


def _resolve_sitemap_fallback(
    client: httpx.Client,
    url: str,
    headers: dict[str, str],
) -> str:
    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        return ""
    slug = PathLikeSlug(parts.path)
    if not slug:
        return ""
    candidates: list[str] = []
    for sitemap_path in ("/sitemap.xml", "/sitemap_index.xml"):
        sitemap_url = urlunsplit((parts.scheme, parts.netloc, sitemap_path, "", ""))
        try:
            response = client.get(sitemap_url, headers=headers)
        except Exception:
            continue
        if response.status_code >= 400:
            continue
        candidates.extend(re.findall(r"<loc>\s*([^<]+)\s*</loc>", response.text or "", flags=re.I))
    if not candidates:
        return ""
    normalized_slug = slug.lower()
    for candidate in candidates[:1000]:
        candidate_url = candidate.strip()
        candidate_slug = PathLikeSlug(urlsplit(candidate_url).path).lower()
        if candidate_slug and candidate_slug == normalized_slug:
            return candidate_url
    return ""


def _resolve_url_variant(
    client: httpx.Client,
    url: str,
    headers: dict[str, str],
) -> str:
    for candidate in _iter_url_variants(url):
        try:
            response = client.head(candidate, headers=headers)
        except Exception:
            continue
        if 200 <= int(response.status_code or 0) < 400:
            return candidate
    return ""


def _iter_url_variants(url: str) -> list[str]:
    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        return []
    variants: list[str] = []
    path = parts.path or "/"
    if path != "/":
        toggled_path = path.rstrip("/") if path.endswith("/") else f"{path}/"
        variants.append(urlunsplit((parts.scheme, parts.netloc, toggled_path, parts.query, parts.fragment)))
        variants.append(urlunsplit((parts.scheme, parts.netloc, f"/m{path}", parts.query, parts.fragment)))
    host = parts.netloc
    alt_host = host[4:] if host.startswith("www.") else f"www.{host}"
    variants.append(urlunsplit((parts.scheme, alt_host, path, parts.query, parts.fragment)))
    seen: set[str] = set()
    ordered: list[str] = []
    for variant in variants:
        if variant != url and variant not in seen:
            seen.add(variant)
            ordered.append(variant)
    return ordered


def _is_safe_repair_candidate(original_url: str, candidate_url: str) -> bool:
    original = urlsplit(str(original_url or ""))
    candidate = urlsplit(str(candidate_url or ""))
    if not candidate.scheme or not candidate.netloc:
        return False
    original_host = (original.hostname or "").lower().removeprefix("www.")
    candidate_host = (candidate.hostname or "").lower().removeprefix("www.")
    if original_host and candidate_host and original_host != candidate_host:
        return False
    original_slug = PathLikeSlug(original.path).lower()
    candidate_slug = PathLikeSlug(candidate.path).lower()
    if original_slug and candidate_slug == original_slug:
        return True
    original_tokens = _path_tokens(original.path)
    candidate_tokens = _path_tokens(candidate.path)
    if original_tokens and len(original_tokens & candidate_tokens) >= min(2, len(original_tokens)):
        return True
    return False


def _path_tokens(path: str) -> set[str]:
    return {
        token.lower()
        for token in re.split(r"[^A-Za-z0-9]+", str(path or ""))
        if len(token) >= 3
    }


def PathLikeSlug(path: str) -> str:
    normalized = str(path or "").strip().rstrip("/")
    if not normalized:
        return ""
    return normalized.rsplit("/", 1)[-1].strip()
