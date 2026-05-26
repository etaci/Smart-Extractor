"""Lightweight URL health check before expensive extraction fetches."""

from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urljoin

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

    @property
    def target_url(self) -> str:
        return self.canonical_url or self.final_url or self.original_url


def preflight_url(
    url: str,
    *,
    timeout_ms: int = 5000,
    headers: dict[str, str] | None = None,
    verify_ssl: bool = True,
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
            if result.status_code in {404, 410}:
                result.reachable = False
                result.reason = f"http_{result.status_code}"
                return result
            if result.status_code == 0:
                result.reachable = False
                result.reason = "empty_response"
                return result
            if 200 <= result.status_code < 400:
                result.canonical_url = _extract_canonical_url(
                    response.text,
                    base_url=result.final_url,
                )
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
    try:
        response = client.head(url, headers=headers)
        if response.status_code not in {405, 403, 429}:
            return response
    except httpx.HTTPError:
        pass

    with client.stream("GET", url, headers={**headers, "Range": "bytes=0-65535"}) as response:
        content = response.read()
        return httpx.Response(
            status_code=response.status_code,
            headers=response.headers,
            content=content,
            request=response.request,
            extensions=response.extensions,
        )


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
