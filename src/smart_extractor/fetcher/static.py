"""Static HTTP fetcher with proxy rotation and challenge-aware retry."""

from __future__ import annotations

import re
import time
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

import httpx
from loguru import logger
from bs4 import BeautifulSoup

from smart_extractor.config import FetcherConfig
from smart_extractor.fetcher.base import BaseFetcher, FetchResult
from smart_extractor.fetcher.url_preflight import preflight_url
from smart_extractor.utils.anti_detect import (
    assess_challenge,
    build_access_attempts,
    get_random_user_agent,
    mask_proxy_url,
)


def _classify_transport_error(exc: Exception) -> str:
    if isinstance(exc, httpx.ConnectTimeout):
        return "connect_timeout"
    if isinstance(exc, httpx.ReadTimeout):
        return "read_timeout"
    if isinstance(exc, httpx.PoolTimeout):
        return "pool_timeout"
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.DecodingError):
        return "decode_error"
    if isinstance(exc, httpx.ConnectError):
        return "network"
    return type(exc).__name__


class StaticFetcher(BaseFetcher):
    """HTTPX-based fetcher with proxy-pool retry and challenge detection."""

    def __init__(self, config: Optional[FetcherConfig] = None):
        self._config = config or FetcherConfig()
        self._clients: dict[str, httpx.Client] = {}

    def _client_key(self, proxy_url: str | None = None) -> str:
        return str(proxy_url or self._config.proxy_url or "").strip()

    def _ensure_client(self, proxy_url: str | None = None) -> httpx.Client:
        key = self._client_key(proxy_url)
        client = self._clients.get(key)
        if client is not None:
            return client
        if not self._config.verify_ssl:
            logger.warning("StaticFetcher 已关闭 HTTPS 证书校验，仅建议在受控环境排障时使用")
        client_kwargs = {
            "timeout": self._config.timeout / 1000,
            "follow_redirects": True,
            "verify": self._config.verify_ssl,
        }
        if key:
            client_kwargs["proxy"] = key
            logger.info("StaticFetcher 使用代理: {}", mask_proxy_url(key))
        client = httpx.Client(**client_kwargs)
        self._clients[key] = client
        return client

    def _build_headers(self, *, mobile: bool = False) -> dict[str, str]:
        user_agent = self._config.user_agent or get_random_user_agent(mobile=mobile)
        chrome_like = "Chrome/" in user_agent or "Edg/" in user_agent
        headers = {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": f"{self._config.locale},{self._config.locale.split('-')[0]};q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "DNT": "1",
        }
        if chrome_like:
            headers.update(
                {
                    "sec-ch-ua": '"Chromium";v="131", "Google Chrome";v="131", "Not_A Brand";v="24"',
                    "sec-ch-ua-mobile": "?1" if mobile else "?0",
                    "sec-ch-ua-platform": '"Android"' if mobile else '"Windows"',
                }
            )
        return headers

    @staticmethod
    def _identity_headers(headers: dict[str, str]) -> dict[str, str]:
        updated = dict(headers)
        updated["Accept-Encoding"] = "identity"
        return updated

    def _build_diagnostics(
        self,
        *,
        stage: str,
        reason: str,
        status_code: int = 0,
        final_url: str = "",
        original_url: str = "",
        headers: dict[str, str] | None = None,
        body_size: int = 0,
        is_shell_page: bool = False,
        retry_count: int = 0,
        redirect_chain: list[str] | None = None,
    ) -> dict[str, object]:
        content_type = str((headers or {}).get("content-type") or "")
        return {
            "failure_stage": stage,
            "failure_reason": reason,
            "http_status": int(status_code or 0),
            "original_url": original_url,
            "final_url": final_url,
            "redirect_chain": list(redirect_chain or []),
            "content_type": content_type,
            "body_size": int(body_size or 0),
            "is_shell_page": bool(is_shell_page),
            "retry_count": int(retry_count or 0),
        }

    @staticmethod
    def _read_response_text(response: httpx.Response) -> tuple[str, str]:
        content = response.content or b""
        if not content:
            return "", ""
        encodings: list[str] = []
        if response.encoding:
            encodings.append(str(response.encoding))
        content_type = str(response.headers.get("content-type") or "")
        match = re.search(r"charset=([A-Za-z0-9_.-]+)", content_type, re.I)
        if match:
            encodings.append(match.group(1))
        encodings.extend(["utf-8", "utf-8-sig", "gb18030", "big5", "latin-1"])
        seen: set[str] = set()
        first_error = ""
        for encoding in encodings:
            normalized = encoding.strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            try:
                text = content.decode(encoding)
            except UnicodeError as exc:
                first_error = first_error or f"decode_error: {type(exc).__name__}"
                continue
            replacement_ratio = text.count("\ufffd") / max(len(text), 1)
            if replacement_ratio <= 0.01:
                return text, "" if not first_error else f"decode_recovered:{encoding}"
        try:
            import charset_normalizer

            detected = charset_normalizer.from_bytes(content).best()
            if detected is not None:
                return str(detected), "decode_recovered:charset_normalizer"
        except Exception:
            pass
        try:
            return content.decode("utf-8", errors="replace"), first_error or "decode_error: fallback_replace"
        except Exception as exc:
            return "", f"decode_error: {type(exc).__name__}: {exc}"

    def _get_with_decode_fallback(
        self,
        client: httpx.Client,
        url: str,
        headers: dict[str, str],
    ) -> tuple[httpx.Response, str]:
        try:
            return client.get(url, headers=headers), ""
        except (httpx.DecodingError, UnicodeError) as exc:
            logger.warning("StaticFetcher decode failed, retrying with identity encoding: {}", exc)
            response = client.get(url, headers=self._identity_headers(headers))
            return response, f"decode_retry: {type(exc).__name__}"

    def _fetch_rss_fallback(self, url: str, previous: FetchResult | None) -> FetchResult | None:
        parts = urlsplit(url)
        if not parts.scheme or not parts.netloc:
            return None
        slug = _path_slug(parts.path)
        if not slug:
            return None
        client = self._ensure_client()
        headers = self._build_headers()
        for feed_path in ("/feed", "/rss", "/rss.xml", "/atom.xml"):
            feed_url = urlunsplit((parts.scheme, parts.netloc, feed_path, "", ""))
            try:
                response, _ = self._get_with_decode_fallback(client, feed_url, headers)
            except Exception:
                continue
            if response.status_code >= 400:
                continue
            html = _extract_feed_item_html(response.text, slug)
            if not html:
                continue
            diagnostics = self._build_diagnostics(
                stage="feed_fallback",
                reason="rss_fallback",
                status_code=response.status_code,
                final_url=feed_url,
                headers=dict(response.headers),
                body_size=len(html),
                retry_count=(previous.retry_count if previous else 0) + 1,
            )
            return FetchResult(
                url=url,
                html=html,
                status_code=200,
                headers={
                    **dict(response.headers),
                    "x-smart-fetch-rescue": "rss_fallback",
                    "x-smart-final-url": feed_url,
                },
                elapsed_ms=previous.elapsed_ms if previous else 0.0,
                retry_count=(previous.retry_count if previous else 0) + 1,
                diagnostics=diagnostics,
            )
        return None

    def _should_escalate_to_dynamic(self, result: FetchResult | None) -> bool:
        if not bool(getattr(self._config, "static_fallback_to_dynamic", True)):
            return False
        if result is None:
            return True
        if result.is_shell_page:
            return True
        if result.status_code == 0 and result.error:
            return True
        return result.status_code in {401, 403, 429}

    def _fetch_dynamic_fallback(self, url: str, previous: FetchResult | None) -> FetchResult:
        try:
            from smart_extractor.fetcher.playwright import PlaywrightFetcher
        except Exception as exc:
            logger.warning("StaticFetcher dynamic fallback unavailable: {}", exc)
            return previous or FetchResult(url=url, status_code=0, error=str(exc))

        dynamic_config = self._config.model_copy(
            update={
                "static_fallback_to_dynamic": False,
                "challenge_fallback_to_static": False,
            }
        )
        logger.info("StaticFetcher escalating to Playwright: {}", url)
        fetcher = PlaywrightFetcher(dynamic_config)
        try:
            result = fetcher.fetch(url)
            if previous is not None:
                result.retry_count += previous.retry_count + 1
                result.headers = {
                    **(result.headers or {}),
                    "x-smart-static-html-length": str(len(previous.html or "")),
                    "x-smart-dynamic-html-length": str(len(result.html or "")),
                    "x-smart-fetch-rescue": "static_to_dynamic",
                }
            return result
        except Exception as exc:
            logger.warning("StaticFetcher Playwright fallback failed: {}", exc)
            if previous is not None:
                return previous
            return FetchResult(url=url, status_code=0, error=f"{type(exc).__name__}: {exc}")
        finally:
            fetcher.close()

    def fetch(self, url: str) -> FetchResult:
        overall_start = time.perf_counter()
        preflight = None
        if bool(getattr(self._config, "url_preflight_enabled", True)):
            headers = self._build_headers()
            preflight = preflight_url(
                url,
                timeout_ms=int(getattr(self._config, "url_preflight_timeout_ms", 5000) or 5000),
                headers=headers,
                verify_ssl=bool(self._config.verify_ssl),
                sitemap_fallback_enabled=bool(getattr(self._config, "sitemap_fallback_enabled", True)),
            )
            if (
                not preflight.reachable
                and bool(getattr(self._config, "url_preflight_abort_unreachable", True))
            ):
                return FetchResult(
                    url=url,
                    status_code=preflight.status_code,
                    headers={
                        **preflight.headers,
                        "x-smart-url-preflight": "unreachable",
                        "x-smart-url-preflight-reason": preflight.reason,
                        "x-smart-url-preflight-repair-reason": preflight.repair_reason,
                        "x-smart-final-url": preflight.final_url,
                    },
                    error=f"unreachable_url: {preflight.reason}",
                    elapsed_ms=(time.perf_counter() - overall_start) * 1000,
                    diagnostics=self._build_diagnostics(
                        stage="preflight",
                        reason=preflight.reason,
                        status_code=preflight.status_code,
                        final_url=preflight.final_url,
                        original_url=preflight.original_url,
                        headers=preflight.headers,
                        redirect_chain=preflight.redirect_chain,
                    ),
                )
            if preflight.target_url and preflight.target_url != url:
                logger.info("StaticFetcher URL preflight resolved: {} -> {}", url, preflight.target_url)
                url = preflight.target_url
        attempts = build_access_attempts(url, self._config, prefer_dynamic=False)
        last_result: FetchResult | None = None

        for attempt_index, attempt in enumerate(attempts, start=1):
            attempt_start = time.perf_counter()
            logger.info(
                "StaticFetcher 抓取开始: url={} attempt={} proxy={}",
                url,
                attempt_index,
                attempt.masked_proxy_url or "direct",
            )
            try:
                client = self._ensure_client(attempt.proxy_url or None)
                request_headers = self._build_headers(mobile=bool(attempt.mobile_user_agent))
                response, decode_retry = self._get_with_decode_fallback(client, url, request_headers)
                html, decode_error = self._read_response_text(response)
                decode_error = decode_error or decode_retry
                headers = dict(response.headers)
                if decode_error:
                    headers["x-smart-decode-fallback"] = decode_error
                headers.update(
                    {
                        "x-smart-url-preflight": "ok" if preflight else "skipped",
                        "x-smart-final-url": preflight.final_url if preflight else str(response.url),
                        "x-smart-canonical-url": preflight.canonical_url if preflight else "",
                        "x-smart-url-preflight-repair-reason": preflight.repair_reason if preflight else "",
                        "x-smart-fetch-attempt-reason": attempt.reason,
                        "x-smart-fetch-mobile-ua": "1" if attempt.mobile_user_agent else "0",
                        "x-smart-fetch-html-length": str(len(html or "")),
                    }
                )
                assessment = assess_challenge(
                    text=html[:4000],
                    headers=headers,
                    status_code=response.status_code,
                )
                last_result = FetchResult(
                    url=url,
                    html=html,
                    status_code=response.status_code,
                    headers=headers,
                    elapsed_ms=(time.perf_counter() - overall_start) * 1000,
                    is_shell_page=assessment.shell_page or assessment.challenge,
                    retry_count=attempt_index - 1,
                    diagnostics=self._build_diagnostics(
                        stage="fetch",
                        reason=assessment.reason or decode_error or "",
                        status_code=response.status_code,
                        original_url=preflight.original_url if preflight else url,
                        final_url=str(response.url),
                        headers=headers,
                        body_size=len(html or ""),
                        is_shell_page=assessment.shell_page or assessment.challenge,
                        retry_count=attempt_index - 1,
                        redirect_chain=[str(item.url) for item in getattr(response, "history", [])] + [str(response.url)],
                    ),
                )
                if assessment.retryable and attempt_index < len(attempts):
                    logger.warning(
                        "StaticFetcher 命中挑战/壳页，继续下一个尝试: url={} attempt={} reason={}",
                        url,
                        attempt_index,
                        assessment.reason or "retryable",
                    )
                    continue
                if assessment.retryable:
                    rss_result = self._fetch_rss_fallback(url, last_result)
                    if rss_result is not None:
                        return rss_result
                if assessment.retryable and self._should_escalate_to_dynamic(last_result):
                    return self._fetch_dynamic_fallback(url, last_result)
                return last_result
            except Exception as exc:
                error_message = f"{type(exc).__name__}: {exc}"
                assessment = assess_challenge(error=error_message)
                last_result = FetchResult(
                    url=url,
                    status_code=0,
                    error=error_message,
                    elapsed_ms=(time.perf_counter() - overall_start) * 1000,
                    retry_count=attempt_index - 1,
                    diagnostics=self._build_diagnostics(
                        stage="transport",
                        reason=_classify_transport_error(exc),
                        retry_count=attempt_index - 1,
                    ),
                )
                logger.warning(
                    "StaticFetcher 抓取失败: url={} attempt={} proxy={} error={}",
                    url,
                    attempt_index,
                    attempt.masked_proxy_url or "direct",
                    error_message,
                )
                if assessment.retryable and attempt_index < len(attempts):
                    continue
                if assessment.retryable and self._should_escalate_to_dynamic(last_result):
                    rss_result = self._fetch_rss_fallback(url, last_result)
                    if rss_result is not None:
                        return rss_result
                    return self._fetch_dynamic_fallback(url, last_result)
                return last_result
            finally:
                logger.debug(
                    "StaticFetcher 单次尝试结束: url={} attempt={} elapsed_ms={:.0f}",
                    url,
                    attempt_index,
                    (time.perf_counter() - attempt_start) * 1000,
                )

        if self._should_escalate_to_dynamic(last_result):
            rss_result = self._fetch_rss_fallback(url, last_result)
            if rss_result is not None:
                return rss_result
            return self._fetch_dynamic_fallback(url, last_result)

        return last_result or FetchResult(
            url=url,
            status_code=0,
            error="StaticFetcher 未生成任何结果",
            elapsed_ms=(time.perf_counter() - overall_start) * 1000,
        )

    def close(self) -> None:
        for client in list(self._clients.values()):
            try:
                client.close()
            except Exception as exc:
                logger.debug("关闭 httpx 客户端失败，已忽略: {}", exc)
        self._clients.clear()


def _path_slug(path: str) -> str:
    normalized = str(path or "").strip().rstrip("/")
    return normalized.rsplit("/", 1)[-1].lower() if normalized else ""


def _extract_feed_item_html(feed_text: str, slug: str) -> str:
    if not feed_text or not slug:
        return ""
    try:
        soup = BeautifulSoup(feed_text, "xml")
    except Exception:
        soup = BeautifulSoup(feed_text, "lxml")
    for item in soup.find_all(["item", "entry"]):
        link_node = item.find("link")
        link = ""
        if link_node is not None:
            link = link_node.get("href") or link_node.get_text(" ", strip=True)
        if slug not in str(link).lower():
            continue
        title = item.find("title")
        summary = item.find("description") or item.find("summary") or item.find("content")
        published = item.find("pubDate") or item.find("published") or item.find("updated")
        parts = [
            f"<h1>{title.get_text(' ', strip=True)}</h1>" if title else "",
            f"<time>{published.get_text(' ', strip=True)}</time>" if published else "",
            f"<article>{summary.get_text(' ', strip=True)}</article>" if summary else "",
            f'<link rel="canonical" href="{link}">' if link else "",
        ]
        return "<html><body>" + "\n".join(part for part in parts if part) + "</body></html>"
    return ""
