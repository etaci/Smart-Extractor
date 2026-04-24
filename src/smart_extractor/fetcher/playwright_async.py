"""Playwright 动态抓取器的异步版本。

使用 `playwright.async_api` 的事件循环集成，配合 `pipeline.run_batch_async`
可在单进程内把 N 个 URL 的抓取真正并发化（每个任务不占独立 OS 线程）。

对外接口与 `PlaywrightFetcher` 一致，差别在于 `fetch` 为协程，需在事件循环中 await。
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)

from smart_extractor.config import FetcherConfig
from smart_extractor.fetcher.base import FetchResult
from smart_extractor.utils.anti_detect import get_random_user_agent


_DEFAULT_HEADERS = {
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}

_LOADING_MARKERS = ("加载中", "请稍候", "loading")
_SHELL_TEXT_MAX_LENGTH = 40

_ANTI_DETECT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
Object.defineProperty(navigator, 'language', { get: () => 'zh-CN' });
Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh', 'en-US', 'en'] });
window.chrome = window.chrome || { runtime: {} };
"""


class AsyncPlaywrightFetcher:
    """基于 playwright.async_api 的动态网页抓取器。

    生命周期：首次 `fetch` 懒加载浏览器；`close` 释放资源；支持 `async with`。
    """

    def __init__(self, config: Optional[FetcherConfig] = None):
        self._config = config or FetcherConfig()
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._initialized = False
        self._uses_persistent_context = False
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "AsyncPlaywrightFetcher":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def _ensure_browser(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            logger.info(
                "启动 AsyncPlaywright 浏览器，headless={}", self._config.headless
            )
            self._playwright = await async_playwright().start()
            user_agent = self._config.user_agent or get_random_user_agent()
            persistent_dir = self._resolve_persistent_context_dir()

            if persistent_dir:
                persistent_dir.mkdir(parents=True, exist_ok=True)
                self._uses_persistent_context = True
                self._context = (
                    await self._playwright.chromium.launch_persistent_context(
                        user_data_dir=str(persistent_dir),
                        headless=self._config.headless,
                        args=self._build_launch_args(),
                        **self._build_context_options(
                            user_agent, include_storage_state=False
                        ),
                    )
                )
                await self._context.add_init_script(_ANTI_DETECT_SCRIPT)
                self._browser = self._context.browser
                logger.info("AsyncPlaywright 持久化 Profile 已启用: {}", persistent_dir)
            else:
                self._browser = await self._playwright.chromium.launch(
                    headless=self._config.headless,
                    args=self._build_launch_args(),
                )
            self._initialized = True

    def _resolve_persistent_context_dir(self) -> Optional[Path]:
        if not self._config.persistent_context_dir:
            return None
        return Path(self._config.persistent_context_dir)

    def _resolve_storage_state_path(self) -> Optional[Path]:
        if not self._config.storage_state_path:
            return None
        return Path(self._config.storage_state_path)

    def _build_context_options(
        self, user_agent: str, include_storage_state: bool = True
    ) -> dict:
        options: dict = {
            "viewport": {
                "width": self._config.viewport_width,
                "height": self._config.viewport_height,
            },
            "user_agent": user_agent,
            "locale": self._config.locale,
            "timezone_id": self._config.timezone_id,
            "extra_http_headers": dict(_DEFAULT_HEADERS),
            "ignore_https_errors": not self._config.verify_ssl,
        }
        storage_state_path = self._resolve_storage_state_path()
        if (
            include_storage_state
            and storage_state_path
            and storage_state_path.exists()
        ):
            options["storage_state"] = str(storage_state_path)
        return options

    def _build_launch_args(self) -> list[str]:
        return [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-default-browser-check",
            "--disable-features=IsolateOrigins,site-per-process",
        ]

    async def _get_context(self) -> BrowserContext:
        if self._context:
            return self._context
        await self._ensure_browser()
        if self._browser is None:
            raise RuntimeError("AsyncPlaywright 浏览器未成功初始化")
        user_agent = self._config.user_agent or get_random_user_agent()
        self._context = await self._browser.new_context(
            **self._build_context_options(user_agent)
        )
        await self._context.add_init_script(_ANTI_DETECT_SCRIPT)
        return self._context

    async def fetch(self, url: str) -> FetchResult:
        start_time = time.time()
        page: Optional[Page] = None
        try:
            await self._ensure_browser()
            context = await self._get_context()
            page = await context.new_page()
            logger.info("正在异步抓取: {}", url)

            response = await page.goto(
                url,
                timeout=self._config.timeout,
                wait_until="domcontentloaded",
            )

            try:
                await page.wait_for_load_state(
                    "networkidle",
                    timeout=min(self._config.timeout, 5000),
                )
            except Exception as exc:
                logger.debug("异步抓取等待 networkidle 超时: {}", exc)

            if self._config.wait_after_load > 0:
                await page.wait_for_timeout(self._config.wait_after_load)

            try:
                await page.wait_for_selector("body", timeout=5000)
            except Exception as exc:
                logger.debug("异步抓取等待 body 超时: {}", exc)

            status_code = response.status if response else 0
            headers = dict(response.headers) if response else {}

            body_text = ""
            try:
                body_text = (
                    await page.locator("body").inner_text(timeout=1000)
                ).strip()
            except Exception as exc:
                logger.debug("异步读取 body 文本失败: {}", exc)

            is_shell_page = (
                len(body_text) <= _SHELL_TEXT_MAX_LENGTH
                and any(marker in body_text.lower() for marker in _LOADING_MARKERS)
            )

            html = await page.content()
            await self._persist_storage_state()
            elapsed = (time.time() - start_time) * 1000

            logger.info(
                "异步抓取成功: {} (状态码={}, HTML长度={}, 耗时={:.0f}ms)",
                url,
                status_code,
                len(html),
                elapsed,
            )
            return FetchResult(
                url=url,
                html=html,
                status_code=status_code,
                headers=headers,
                elapsed_ms=elapsed,
                is_shell_page=is_shell_page,
            )
        except Exception as exc:
            elapsed = (time.time() - start_time) * 1000
            error_msg = f"{type(exc).__name__}: {exc}"
            logger.error("异步抓取失败: {} - {}", url, error_msg)
            return FetchResult(
                url=url,
                status_code=0,
                error=error_msg,
                elapsed_ms=elapsed,
            )
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception as exc:
                    logger.debug("关闭异步 page 失败: {}", exc)

    async def _persist_storage_state(self) -> None:
        storage_state_path = self._resolve_storage_state_path()
        if not storage_state_path or not self._context:
            return
        try:
            storage_state_path.parent.mkdir(parents=True, exist_ok=True)
            await self._context.storage_state(path=str(storage_state_path))
        except Exception as exc:
            logger.warning("保存异步 storage_state 失败: {}", exc)

    async def close(self) -> None:
        if self._context:
            try:
                await self._persist_storage_state()
                await self._context.close()
            except Exception as exc:
                logger.debug("关闭异步 context 失败: {}", exc)
            self._context = None

        if self._browser and not self._uses_persistent_context:
            try:
                await self._browser.close()
            except Exception as exc:
                logger.debug("关闭异步 browser 失败: {}", exc)
            self._browser = None
        else:
            self._browser = None

        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception as exc:
                logger.debug("停止异步 playwright 失败: {}", exc)
            self._playwright = None

        self._initialized = False
        self._uses_persistent_context = False
        logger.debug("AsyncPlaywright 资源已释放")
