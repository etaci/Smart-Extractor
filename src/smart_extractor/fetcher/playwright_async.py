from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Optional

from loguru import logger
from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

from smart_extractor.config import FetcherConfig
from smart_extractor.fetcher.base import FetchResult
from smart_extractor.utils.anti_detect import get_random_user_agent, headers_indicate_challenge, looks_like_challenge_text, looks_like_loading_text

_DEFAULT_HEADERS = {
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}
_SHELL_TEXT_MAX_LENGTH = 40
_ANTI_DETECT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
Object.defineProperty(navigator, 'language', { get: () => 'zh-CN' });
Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh', 'en-US', 'en'] });
window.chrome = window.chrome || { runtime: {} };
"""


class AsyncPlaywrightFetcher:
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

    def _resolve_persistent_context_dir(self) -> Optional[Path]:
        return Path(self._config.persistent_context_dir) if self._config.persistent_context_dir else None

    def _resolve_storage_state_path(self) -> Optional[Path]:
        return Path(self._config.storage_state_path) if self._config.storage_state_path else None

    def _build_context_options(self, user_agent: str, include_storage_state: bool = True) -> dict:
        options: dict = {
            "viewport": {"width": self._config.viewport_width, "height": self._config.viewport_height},
            "user_agent": user_agent,
            "locale": self._config.locale,
            "timezone_id": self._config.timezone_id,
            "extra_http_headers": dict(_DEFAULT_HEADERS),
            "ignore_https_errors": not self._config.verify_ssl,
        }
        storage_state_path = self._resolve_storage_state_path()
        if include_storage_state and storage_state_path and storage_state_path.exists():
            options["storage_state"] = str(storage_state_path)
        return options

    def _build_launch_args(self) -> list[str]:
        return [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-default-browser-check",
            "--disable-features=IsolateOrigins,site-per-process",
        ]

    async def _ensure_browser(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            self._playwright = await async_playwright().start()
            user_agent = self._config.user_agent or get_random_user_agent()
            persistent_dir = self._resolve_persistent_context_dir()
            if persistent_dir:
                persistent_dir.mkdir(parents=True, exist_ok=True)
                self._uses_persistent_context = True
                self._context = await self._playwright.chromium.launch_persistent_context(
                    user_data_dir=str(persistent_dir),
                    headless=self._config.headless,
                    args=self._build_launch_args(),
                    **self._build_context_options(user_agent, include_storage_state=False),
                )
                await self._context.add_init_script(_ANTI_DETECT_SCRIPT)
                self._browser = self._context.browser
            else:
                self._browser = await self._playwright.chromium.launch(
                    headless=self._config.headless,
                    args=self._build_launch_args(),
                )
            self._initialized = True

    async def _get_context(self) -> BrowserContext:
        if self._context:
            return self._context
        await self._ensure_browser()
        if self._browser is None:
            raise RuntimeError("AsyncPlaywright 浏览器未成功初始化")
        user_agent = self._config.user_agent or get_random_user_agent()
        self._context = await self._browser.new_context(**self._build_context_options(user_agent))
        await self._context.add_init_script(_ANTI_DETECT_SCRIPT)
        return self._context

    @staticmethod
    async def _extract_body_text(page: Page) -> str:
        try:
            return (await page.locator("body").inner_text(timeout=1000)).strip()
        except Exception:
            return ""

    @staticmethod
    async def _extract_title_text(page: Page) -> str:
        try:
            return (await page.title()).strip()
        except Exception:
            return ""

    async def _looks_like_shell_page(self, page: Page) -> bool:
        body_text = await self._extract_body_text(page)
        return not body_text or looks_like_loading_text(body_text, max_length=_SHELL_TEXT_MAX_LENGTH)

    async def _looks_like_challenge_page(self, page: Page, *, status_code: int = 0, headers: dict[str, str] | None = None) -> bool:
        title_text = await self._extract_title_text(page)
        body_text = await self._extract_body_text(page)
        combined_text = "\n".join(part for part in (title_text, body_text) if part)
        if looks_like_challenge_text(combined_text):
            return True
        if headers_indicate_challenge(headers):
            return True
        return status_code in {401, 403, 429} and len(body_text) <= 400

    @staticmethod
    async def _warm_up_page(page: Page) -> None:
        try:
            await page.mouse.move(240, 180)
        except Exception:
            pass
        try:
            await page.evaluate(
                """
                () => {
                    const top = Math.min(window.innerHeight * 0.8, 640);
                    window.scrollTo(0, top);
                    window.scrollTo(0, 0);
                }
                """
            )
        except Exception:
            pass
        try:
            await page.wait_for_timeout(600)
        except Exception:
            pass

    async def _wait_for_meaningful_content(self, page: Page) -> None:
        deadline = time.time() + min(max(self._config.wait_after_load / 1000, 2), 12)
        while time.time() < deadline:
            body_text = await self._extract_body_text(page)
            if len(body_text) >= 80 and not looks_like_loading_text(body_text):
                return
            await page.wait_for_timeout(500)

    async def _stabilize_page(self, page: Page, url: str, *, status_code: int = 0, headers: dict[str, str] | None = None) -> None:
        max_attempts = max(1, int(self._config.challenge_retry_attempts))
        for attempt in range(1, max_attempts + 1):
            await self._wait_for_meaningful_content(page)
            is_shell_page = await self._looks_like_shell_page(page)
            is_challenge_page = await self._looks_like_challenge_page(page, status_code=status_code, headers=headers)
            if not is_shell_page and not is_challenge_page:
                return
            if attempt >= max_attempts:
                logger.warning("页面仍处于壳页或挑战页状态: {}", url)
                return
            await self._warm_up_page(page)
            if self._config.challenge_retry_backoff_ms > 0:
                await page.wait_for_timeout(self._config.challenge_retry_backoff_ms)
            await page.reload(timeout=self._config.timeout, wait_until="domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=min(self._config.timeout, 5000))
            except Exception:
                pass

    async def fetch(self, url: str) -> FetchResult:
        start_time = time.time()
        page: Optional[Page] = None
        try:
            context = await self._get_context()
            page = await context.new_page()
            response = await page.goto(url, timeout=self._config.timeout, wait_until="domcontentloaded")
            status_code = response.status if response else 0
            headers = dict(response.headers) if response else {}
            try:
                await page.wait_for_load_state("networkidle", timeout=min(self._config.timeout, 5000))
            except Exception:
                pass
            await self._stabilize_page(page, url, status_code=status_code, headers=headers)
            if self._config.wait_after_load > 0:
                await page.wait_for_timeout(self._config.wait_after_load)
            try:
                await page.wait_for_selector("body", timeout=5000)
            except Exception:
                pass
            is_shell_page = await self._looks_like_shell_page(page) or await self._looks_like_challenge_page(page, status_code=status_code, headers=headers)
            html = await page.content()
            await self._persist_storage_state()
            elapsed = (time.time() - start_time) * 1000
            return FetchResult(url=url, html=html, status_code=status_code, headers=headers, elapsed_ms=elapsed, is_shell_page=is_shell_page)
        except Exception as exc:
            elapsed = (time.time() - start_time) * 1000
            return FetchResult(url=url, status_code=0, error=f"{type(exc).__name__}: {exc}", elapsed_ms=elapsed)
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass

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
            except Exception:
                pass
            self._context = None
        if self._browser and not self._uses_persistent_context:
            try:
                await self._browser.close()
            except Exception:
                pass
            self._browser = None
        else:
            self._browser = None
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass
            self._playwright = None
        self._initialized = False
        self._uses_persistent_context = False
