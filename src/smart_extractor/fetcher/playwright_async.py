"""Async Playwright fetcher with proxy/session/profile pooling and fallback."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Optional

from loguru import logger
from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright

from smart_extractor.config import FetcherConfig
from smart_extractor.fetcher.base import FetchResult
from smart_extractor.fetcher.playwright import (
    _ANTI_DETECT_SCRIPT,
    _DEFAULT_HEADERS,
    _SHELL_TEXT_MAX_LENGTH,
)
from smart_extractor.fetcher.static import StaticFetcher
from smart_extractor.utils.anti_detect import (
    AccessAttempt,
    ChallengeAssessment,
    assess_challenge,
    build_access_attempts,
    get_random_user_agent,
    headers_indicate_challenge,
    looks_like_challenge_text,
    looks_like_loading_text,
    mask_proxy_url,
)


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

    async def _ensure_playwright(self) -> Playwright:
        if self._playwright is None:
            self._playwright = await async_playwright().start()
        return self._playwright

    def _resolve_persistent_context_dir(self, override_dir: str = "") -> Optional[Path]:
        target = str(override_dir or self._config.persistent_context_dir or "").strip()
        return Path(target) if target else None

    def _resolve_storage_state_path(self, override_path: str = "") -> Optional[Path]:
        target = str(override_path or self._config.storage_state_path or "").strip()
        return Path(target) if target else None

    def _build_context_options(
        self,
        user_agent: str,
        *,
        include_storage_state: bool = True,
        storage_state_path: str = "",
    ) -> dict:
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
        resolved_path = self._resolve_storage_state_path(storage_state_path)
        if include_storage_state and resolved_path and resolved_path.exists():
            options["storage_state"] = str(resolved_path)
        return options

    def _build_launch_args(self) -> list[str]:
        return [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-default-browser-check",
            "--disable-features=IsolateOrigins,site-per-process",
        ]

    def _build_proxy_options(self, proxy_url: str | None = None) -> dict[str, str] | None:
        from urllib.parse import urlsplit

        resolved_proxy = str(proxy_url or self._config.proxy_url or "").strip()
        if not resolved_proxy:
            return None
        parts = urlsplit(resolved_proxy)
        hostname = parts.hostname or ""
        if not hostname:
            return None
        scheme = parts.scheme or "http"
        server = f"{scheme}://{hostname}"
        if parts.port:
            server = f"{server}:{parts.port}"
        payload = {"server": server}
        if parts.username:
            payload["username"] = parts.username
        if parts.password:
            payload["password"] = parts.password
        logger.info("AsyncPlaywrightFetcher 使用代理: {}", mask_proxy_url(resolved_proxy))
        return payload

    async def _ensure_browser(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            playwright = await self._ensure_playwright()
            user_agent = self._config.user_agent or get_random_user_agent()
            persistent_dir = self._resolve_persistent_context_dir()
            proxy_options = self._build_proxy_options()
            if persistent_dir:
                persistent_dir.mkdir(parents=True, exist_ok=True)
                self._uses_persistent_context = True
                self._context = await playwright.chromium.launch_persistent_context(
                    user_data_dir=str(persistent_dir),
                    headless=self._config.headless,
                    args=self._build_launch_args(),
                    proxy=proxy_options,
                    **self._build_context_options(user_agent, include_storage_state=False),
                )
                await self._context.add_init_script(_ANTI_DETECT_SCRIPT)
                self._browser = self._context.browser
            else:
                self._browser = await playwright.chromium.launch(
                    headless=self._config.headless,
                    args=self._build_launch_args(),
                    proxy=proxy_options,
                )
            self._initialized = True

    async def _get_context(self) -> BrowserContext:
        if self._context is not None:
            return self._context
        await self._ensure_browser()
        if self._browser is None:
            raise RuntimeError("Async Playwright 浏览器初始化失败")
        user_agent = self._config.user_agent or get_random_user_agent()
        self._context = await self._browser.new_context(**self._build_context_options(user_agent))
        await self._context.add_init_script(_ANTI_DETECT_SCRIPT)
        return self._context

    async def _persist_storage_state(
        self,
        context: BrowserContext,
        *,
        storage_state_path: str = "",
    ) -> None:
        resolved_path = self._resolve_storage_state_path(storage_state_path)
        if resolved_path is None:
            return
        try:
            resolved_path.parent.mkdir(parents=True, exist_ok=True)
            await context.storage_state(path=str(resolved_path))
        except Exception as exc:
            logger.warning("保存异步 storage_state 失败: {}", exc)

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

    async def _looks_like_challenge_page(
        self,
        page: Page,
        *,
        status_code: int = 0,
        headers: dict[str, str] | None = None,
    ) -> bool:
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

    async def _stabilize_page(
        self,
        page: Page,
        url: str,
        *,
        status_code: int = 0,
        headers: dict[str, str] | None = None,
    ) -> int:
        max_attempts = max(1, int(self._config.challenge_retry_attempts or 1))
        reload_count = 0
        for attempt in range(1, max_attempts + 1):
            await self._wait_for_meaningful_content(page)
            if not await self._looks_like_shell_page(page) and not await self._looks_like_challenge_page(
                page,
                status_code=status_code,
                headers=headers,
            ):
                return reload_count
            if attempt >= max_attempts:
                return reload_count
            logger.info(
                "AsyncPlaywrightFetcher 检测到挑战页/壳页，执行站内恢复: url={} reload_attempt={}",
                url,
                attempt,
            )
            await self._warm_up_page(page)
            if self._config.challenge_retry_backoff_ms > 0:
                await page.wait_for_timeout(self._config.challenge_retry_backoff_ms)
            await page.reload(timeout=self._config.timeout, wait_until="domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=min(self._config.timeout, 5000))
            except Exception:
                pass
            reload_count += 1
        return reload_count

    async def _assess_page(
        self,
        page: Page,
        *,
        status_code: int,
        headers: dict[str, str] | None,
    ) -> ChallengeAssessment:
        title_text = await self._extract_title_text(page)
        body_text = await self._extract_body_text(page)
        combined_text = "\n".join(part for part in (title_text, body_text) if part)
        return assess_challenge(
            text=combined_text,
            headers=headers,
            status_code=status_code,
        )

    async def _launch_attempt_context(
        self,
        attempt: AccessAttempt,
        *,
        user_agent: str,
    ) -> tuple[BrowserContext, Browser | None, bool]:
        playwright = await self._ensure_playwright()
        proxy_options = self._build_proxy_options(attempt.proxy_url or None)
        profile_dir = self._resolve_persistent_context_dir(attempt.profile_dir)
        if profile_dir is not None:
            profile_dir.mkdir(parents=True, exist_ok=True)
            context = await playwright.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                headless=self._config.headless,
                args=self._build_launch_args(),
                proxy=proxy_options,
                **self._build_context_options(user_agent, include_storage_state=False),
            )
            await context.add_init_script(_ANTI_DETECT_SCRIPT)
            return context, context.browser, True

        browser = await playwright.chromium.launch(
            headless=self._config.headless,
            args=self._build_launch_args(),
            proxy=proxy_options,
        )
        context = await browser.new_context(
            **self._build_context_options(
                user_agent,
                include_storage_state=True,
                storage_state_path=attempt.storage_state_path,
            )
        )
        await context.add_init_script(_ANTI_DETECT_SCRIPT)
        return context, browser, False

    async def _fetch_dynamic_attempt(
        self,
        url: str,
        *,
        attempt: AccessAttempt,
        retry_count_offset: int,
        overall_start: float,
    ) -> tuple[FetchResult, ChallengeAssessment]:
        context: BrowserContext | None = None
        browser: Browser | None = None
        persistent_context = False
        page: Page | None = None
        try:
            user_agent = self._config.user_agent or get_random_user_agent()
            context, browser, persistent_context = await self._launch_attempt_context(
                attempt,
                user_agent=user_agent,
            )
            page = await context.new_page()
            response = await page.goto(
                url,
                timeout=self._config.timeout,
                wait_until="domcontentloaded",
            )
            status_code = response.status if response else 0
            headers = dict(response.headers) if response else {}
            try:
                await page.wait_for_load_state("networkidle", timeout=min(self._config.timeout, 5000))
            except Exception:
                pass
            reload_count = int(
                await self._stabilize_page(
                    page,
                    url,
                    status_code=status_code,
                    headers=headers,
                )
                or 0
            )
            if self._config.wait_after_load > 0:
                await page.wait_for_timeout(self._config.wait_after_load)
            try:
                await page.wait_for_selector("body", timeout=5000)
            except Exception:
                pass
            html = await page.content()
            assessment = await self._assess_page(
                page,
                status_code=status_code,
                headers=headers,
            )
            if context is not None:
                await self._persist_storage_state(
                    context,
                    storage_state_path=attempt.storage_state_path,
                )
            result = FetchResult(
                url=url,
                html=html,
                status_code=status_code,
                headers=headers,
                elapsed_ms=(time.perf_counter() - overall_start) * 1000,
                is_shell_page=assessment.shell_page or assessment.challenge,
                retry_count=retry_count_offset + reload_count,
            )
            return result, assessment
        except Exception as exc:
            error_message = f"{type(exc).__name__}: {exc}"
            assessment = assess_challenge(error=error_message)
            result = FetchResult(
                url=url,
                status_code=0,
                error=error_message,
                elapsed_ms=(time.perf_counter() - overall_start) * 1000,
                retry_count=retry_count_offset,
            )
            return result, assessment
        finally:
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass
            if context is not None and context is not self._context:
                try:
                    await self._persist_storage_state(
                        context,
                        storage_state_path=attempt.storage_state_path,
                    )
                    await context.close()
                except Exception:
                    pass
            if browser is not None and not persistent_context and browser is not self._browser:
                try:
                    await browser.close()
                except Exception:
                    pass

    async def _fetch_static_fallback(
        self,
        url: str,
        *,
        attempt: AccessAttempt,
        retry_count_offset: int,
    ) -> tuple[FetchResult, ChallengeAssessment]:
        static_config = self._config.model_copy(
            update={
                "proxy_url": attempt.proxy_url or None,
                "proxy_urls": [],
                "fetch_max_attempts": 1,
            }
        )
        result = await asyncio.to_thread(self._run_static_fetch, static_config, url)
        result.retry_count += retry_count_offset
        assessment = assess_challenge(
            text=result.html[:4000],
            headers=result.headers,
            status_code=result.status_code,
            error=result.error or "",
        )
        return result, assessment

    @staticmethod
    def _run_static_fetch(config: FetcherConfig, url: str) -> FetchResult:
        fetcher = StaticFetcher(config)
        try:
            return fetcher.fetch(url)
        finally:
            fetcher.close()

    async def fetch(self, url: str) -> FetchResult:
        overall_start = time.perf_counter()
        attempts = build_access_attempts(url, self._config, prefer_dynamic=True)
        last_result: FetchResult | None = None
        for attempt_index, attempt in enumerate(attempts, start=1):
            logger.info(
                "AsyncPlaywrightFetcher 抓取开始: url={} attempt={} mode={} proxy={} session_slot={} profile_slot={}",
                url,
                attempt_index,
                attempt.fetcher_mode,
                attempt.masked_proxy_url or "direct",
                attempt.session_slot,
                attempt.profile_slot,
            )
            if attempt.fetcher_mode == "static":
                result, assessment = await self._fetch_static_fallback(
                    url,
                    attempt=attempt,
                    retry_count_offset=attempt_index - 1,
                )
            else:
                result, assessment = await self._fetch_dynamic_attempt(
                    url,
                    attempt=attempt,
                    retry_count_offset=attempt_index - 1,
                    overall_start=overall_start,
                )
            last_result = result
            if not assessment.retryable or attempt_index >= len(attempts):
                return result
            logger.warning(
                "AsyncPlaywrightFetcher 将切换到下一个尝试: url={} attempt={} reason={}",
                url,
                attempt_index,
                assessment.reason or "retryable",
            )
        return last_result or FetchResult(
            url=url,
            status_code=0,
            error="AsyncPlaywrightFetcher 未生成任何结果",
            elapsed_ms=(time.perf_counter() - overall_start) * 1000,
        )

    async def close(self) -> None:
        if self._context is not None:
            try:
                await self._persist_storage_state(self._context)
                await self._context.close()
            except Exception:
                pass
            self._context = None
        if self._browser is not None and not self._uses_persistent_context:
            try:
                await self._browser.close()
            except Exception:
                pass
            self._browser = None
        else:
            self._browser = None
        if self._playwright is not None:
            try:
                await self._playwright.stop()
            except Exception:
                pass
            self._playwright = None
        self._initialized = False
        self._uses_persistent_context = False
