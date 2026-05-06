"""
静态网页抓取器

使用 httpx 进行轻量级的静态页面抓取，
适用于不需要 JavaScript 渲染的简单页面。
"""

import time
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

import httpx
from loguru import logger

from smart_extractor.config import FetcherConfig
from smart_extractor.fetcher.base import BaseFetcher, FetchResult
from smart_extractor.utils.anti_detect import get_random_user_agent


class StaticFetcher(BaseFetcher):
    """
    基于 httpx 的轻量级静态页面抓取器。

    相比 Playwright 更快速、资源消耗更低，
    但不支持 JavaScript 渲染。
    """

    def __init__(self, config: Optional[FetcherConfig] = None):
        self._config = config or FetcherConfig()
        self._client: Optional[httpx.Client] = None

    @staticmethod
    def _mask_proxy_url(proxy_url: str) -> str:
        normalized_url = str(proxy_url or "").strip()
        if not normalized_url:
            return ""
        parts = urlsplit(normalized_url)
        hostname = parts.hostname or ""
        if not hostname:
            return normalized_url
        credentials = ""
        if parts.username:
            credentials = f"{parts.username}:***@"
        netloc = f"{credentials}{hostname}"
        if parts.port:
            netloc = f"{netloc}:{parts.port}"
        return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))

    def _ensure_client(self) -> httpx.Client:
        """确保 httpx 客户端已初始化"""
        if self._client is None:
            if not self._config.verify_ssl:
                logger.warning("静态抓取器已关闭 HTTPS 证书校验，仅建议用于受控环境排障")
            client_kwargs = {
                "timeout": self._config.timeout / 1000,
                "follow_redirects": True,
                "verify": self._config.verify_ssl,
            }
            proxy_url = str(self._config.proxy_url or "").strip()
            if proxy_url:
                logger.info("静态抓取使用代理: {}", self._mask_proxy_url(proxy_url))
                client_kwargs["proxy"] = proxy_url
            self._client = httpx.Client(**client_kwargs)
        return self._client

    def fetch(self, url: str) -> FetchResult:
        """
        抓取指定 URL 的静态页面内容。

        Args:
            url: 目标网页 URL

        Returns:
            FetchResult 包含 HTML 和状态信息
        """
        start_time = time.time()

        try:
            client = self._ensure_client()
            user_agent = self._config.user_agent or get_random_user_agent()

            logger.info("正在抓取（静态模式）: {}", url)

            response = client.get(
                url,
                headers={
                    "User-Agent": user_agent,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Accept-Encoding": "gzip, deflate",
                },
            )

            elapsed = (time.time() - start_time) * 1000
            html = response.text
            headers = dict(response.headers)

            logger.info(
                "抓取成功: {} (状态码={}, HTML长度={}, 耗时={:.0f}ms)",
                url, response.status_code, len(html), elapsed
            )

            return FetchResult(
                url=url,
                html=html,
                status_code=response.status_code,
                headers=headers,
                elapsed_ms=elapsed,
                retry_count=0,
            )

        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            error_msg = f"{type(e).__name__}: {e}"
            logger.error("抓取失败: {} — {}", url, error_msg)

            return FetchResult(
                url=url,
                status_code=0,
                error=error_msg,
                elapsed_ms=elapsed,
                retry_count=0,
            )

    def close(self) -> None:
        """关闭 httpx 客户端"""
        if self._client:
            try:
                self._client.close()
            except Exception as exc:
                logger.debug("关闭 httpx 客户端失败，已忽略: {}", exc)
            self._client = None
            logger.debug("httpx 客户端已关闭")
