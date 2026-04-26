"""
Playwright 动态网页抓取器。

优先复用持久化浏览器 Profile，其次复用 storage_state，以提升真实页面命中率。
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger
from playwright.sync_api import Browser, BrowserContext, Page, Playwright, sync_playwright

from smart_extractor.config import FetcherConfig
from smart_extractor.fetcher.base import BaseFetcher, FetchResult
from smart_extractor.utils.anti_detect import (
    get_random_user_agent,
    headers_indicate_challenge,
    looks_like_challenge_text,
    looks_like_loading_text,
)


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


class PlaywrightFetcher(BaseFetcher):
    """基于 Playwright 的动态网页抓取器。"""

    def __init__(self, config: Optional[FetcherConfig] = None):
        self._config = config or FetcherConfig()
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._initialized = False
        self._uses_persistent_context = False

    def _resolve_storage_state_path(self) -> Optional[Path]:
        if not self._config.storage_state_path:
            return None
        return Path(self._config.storage_state_path)

    def _resolve_persistent_context_dir(self) -> Optional[Path]:
        if not self._config.persistent_context_dir:
            return None
        return Path(self._config.persistent_context_dir)

    def _build_context_options(self, user_agent: str, include_storage_state: bool = True) -> dict:
        options = {
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

    def _ensure_browser(self) -> Optional[Browser]:
        if self._initialized:
            return self._browser

        logger.info("启动 Playwright 浏览器，headless={}", self._config.headless)
        if self._playwright is None:
            self._playwright = sync_playwright().start()
        user_agent = self._config.user_agent or get_random_user_agent()
        persistent_dir = self._resolve_persistent_context_dir()

        if persistent_dir:
            persistent_dir.mkdir(parents=True, exist_ok=True)
            self._uses_persistent_context = True
            self._context = self._playwright.chromium.launch_persistent_context(
                user_data_dir=str(persistent_dir),
                headless=self._config.headless,
                args=self._build_launch_args(),
                **self._build_context_options(user_agent, include_storage_state=False),
            )
            self._context.add_init_script(_ANTI_DETECT_SCRIPT)
            self._browser = self._context.browser
            logger.info("Playwright 持久化 Profile 已启用: {}", persistent_dir)
        else:
            self._browser = self._playwright.chromium.launch(
                headless=self._config.headless,
                args=self._build_launch_args(),
            )

        self._initialized = True
        logger.info("Playwright 浏览器启动成功")
        return self._browser

    def _get_context(self) -> BrowserContext:
        if self._context:
            return self._context

        browser = self._ensure_browser()
        if browser is None:
            raise RuntimeError("Playwright 浏览器未成功初始化")

        user_agent = self._config.user_agent or get_random_user_agent()
        self._context = browser.new_context(**self._build_context_options(user_agent))
        self._context.add_init_script(_ANTI_DETECT_SCRIPT)
        return self._context

    def _create_page(self) -> Page:
        return self._get_context().new_page()

    def _persist_storage_state(self) -> None:
        storage_state_path = self._resolve_storage_state_path()
        if not storage_state_path or not self._context:
            return

        try:
            storage_state_path.parent.mkdir(parents=True, exist_ok=True)
            self._context.storage_state(path=str(storage_state_path))
        except Exception as exc:
            logger.warning("保存 Playwright storage_state 失败: {}", exc)

    @staticmethod
    def _extract_body_text(page: Page) -> str:
        try:
            return page.locator("body").inner_text(timeout=1000).strip()
        except Exception as exc:
            logger.debug("读取 body 文本失败，按空字符串处理: {}", exc)
            return ""

    @staticmethod
    def _extract_title_text(page: Page) -> str:
        try:
            return page.title().strip()
        except Exception as exc:
            logger.debug("读取页面标题失败，按空字符串处理: {}", exc)
            return ""

    def _looks_like_shell_page(self, page: Page) -> bool:
        body_text = self._extract_body_text(page)
        if not body_text:
            return True

        return looks_like_loading_text(body_text, max_length=_SHELL_TEXT_MAX_LENGTH)

    def _looks_like_challenge_page(
        self,
        page: Page,
        *,
        status_code: int = 0,
        headers: dict[str, str] | None = None,
    ) -> bool:
        title_text = self._extract_title_text(page)
        body_text = self._extract_body_text(page)
        combined_text = "\n".join(part for part in (title_text, body_text) if part)

        if looks_like_challenge_text(combined_text):
            return True
        if headers_indicate_challenge(headers):
            return True
        return status_code in {401, 403, 429} and len(body_text) <= 400

    @staticmethod
    def _warm_up_page(page: Page) -> None:
        try:
            page.mouse.move(240, 180)
        except Exception as exc:
            logger.debug("页面鼠标预热失败: {}", exc)

        try:
            page.evaluate(
                """
                () => {
                    const top = Math.min(window.innerHeight * 0.8, 640);
                    window.scrollTo(0, top);
                    window.scrollTo(0, 0);
                }
                """
            )
        except Exception as exc:
            logger.debug("页面滚动预热失败: {}", exc)

        try:
            page.wait_for_timeout(600)
        except Exception as exc:
            logger.debug("页面预热等待失败: {}", exc)

    def _wait_for_meaningful_content(self, page: Page) -> None:
        deadline = time.time() + min(max(self._config.wait_after_load / 1000, 2), 12)

        while time.time() < deadline:
            body_text = self._extract_body_text(page)
            if len(body_text) >= 80 and not looks_like_loading_text(body_text):
                return

            page.wait_for_timeout(500)

    def _stabilize_page(
        self,
        page: Page,
        url: str,
        *,
        status_code: int = 0,
        headers: dict[str, str] | None = None,
    ) -> None:
        max_attempts = max(1, int(self._config.challenge_retry_attempts))
        for attempt in range(1, max_attempts + 1):
            self._wait_for_meaningful_content(page)
            if not self._looks_like_shell_page(page) and not self._looks_like_challenge_page(
                page,
                status_code=status_code,
                headers=headers,
            ):
                return

            if attempt >= max_attempts:
                logger.warning("页面仍处于壳页或挑战页状态: {}", url)
                return

            logger.info("检测到页面仍为壳页或挑战页，执行预热并重试: {} attempt={}", url, attempt + 1)
            self._warm_up_page(page)
            if self._config.challenge_retry_backoff_ms > 0:
                page.wait_for_timeout(self._config.challenge_retry_backoff_ms)
            page.reload(timeout=self._config.timeout, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=min(self._config.timeout, 5000))
            except Exception:
                logger.debug("页面重载后等待 networkidle 超时，继续处理")

    def fetch(self, url: str) -> FetchResult:
        start_time = time.time()
        page = None

        try:
            page = self._create_page()
            logger.info("正在抓取: {}", url)

            response = page.goto(
                url,
                timeout=self._config.timeout,
                wait_until="domcontentloaded",
            )
            status_code = response.status if response else 0
            headers = dict(response.headers) if response else {}

            try:
                page.wait_for_load_state("networkidle", timeout=min(self._config.timeout, 5000))
            except Exception:
                logger.debug("等待 networkidle 超时，继续处理")

            self._stabilize_page(page, url, status_code=status_code, headers=headers)

            if self._config.wait_after_load > 0:
                page.wait_for_timeout(self._config.wait_after_load)

            try:
                page.wait_for_selector("body", timeout=5000)
            except Exception:
                logger.debug("等待 body 超时，继续处理")

            is_shell_page = self._looks_like_shell_page(page) or self._looks_like_challenge_page(
                page,
                status_code=status_code,
                headers=headers,
            )

            if self._config.screenshot:
                self._save_screenshot(page, url)

            html = page.content()
            self._persist_storage_state()
            elapsed = (time.time() - start_time) * 1000

            logger.info(
                "抓取成功: {} (状态码={}, HTML长度={}, 耗时={:.0f}ms)",
                url,
                status_code,
                len(html),
                elapsed,
            )
            if is_shell_page:
                logger.warning("抓取完成但页面仍疑似壳页: {}", url)

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
            logger.error("抓取失败: {} - {}", url, error_msg)
            return FetchResult(
                url=url,
                status_code=0,
                error=error_msg,
                elapsed_ms=elapsed,
            )
        finally:
            if page:
                try:
                    page.close()
                except Exception as exc:
                    logger.debug("关闭 Playwright 页面失败，已忽略: {}", exc)

    def _save_screenshot(self, page: Page, url: str) -> None:
        try:
            screenshot_dir = Path(self._config.screenshot_dir)
            screenshot_dir.mkdir(parents=True, exist_ok=True)

            import hashlib

            url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = screenshot_dir / f"{timestamp}_{url_hash}.png"
            page.screenshot(path=str(filepath), full_page=True)
            logger.info("截图已保存: {}", filepath)
        except Exception as exc:
            logger.warning("截图保存失败: {}", exc)

    def close(self) -> None:
        if self._context:
            try:
                self._persist_storage_state()
                self._context.close()
            except Exception as exc:
                logger.debug("关闭 Playwright 上下文失败，已忽略: {}", exc)
            self._context = None

        if self._browser and not self._uses_persistent_context:
            try:
                self._browser.close()
            except Exception as exc:
                logger.debug("关闭 Playwright 浏览器失败，已忽略: {}", exc)
            self._browser = None
        else:
            self._browser = None

        if self._playwright:
            try:
                self._playwright.stop()
            except Exception as exc:
                logger.debug("停止 Playwright 失败，已忽略: {}", exc)
            self._playwright = None

        self._initialized = False
        self._uses_persistent_context = False
        logger.debug("Playwright 资源已释放")
