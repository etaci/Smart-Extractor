"""Static HTTP fetcher with proxy rotation and challenge-aware retry."""

from __future__ import annotations

import time
from typing import Optional

import httpx
from loguru import logger

from smart_extractor.config import FetcherConfig
from smart_extractor.fetcher.base import BaseFetcher, FetchResult
from smart_extractor.utils.anti_detect import (
    assess_challenge,
    build_access_attempts,
    get_random_user_agent,
    mask_proxy_url,
)


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

    def _build_headers(self) -> dict[str, str]:
        user_agent = self._config.user_agent or get_random_user_agent()
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
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                }
            )
        return headers

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
                response = client.get(url, headers=self._build_headers())
                html = response.text
                headers = dict(response.headers)
                headers.update(
                    {
                        "x-smart-fetch-attempt-reason": attempt.reason,
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
                )
                if assessment.retryable and attempt_index < len(attempts):
                    logger.warning(
                        "StaticFetcher 命中挑战/壳页，继续下一个尝试: url={} attempt={} reason={}",
                        url,
                        attempt_index,
                        assessment.reason or "retryable",
                    )
                    continue
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
