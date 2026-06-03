"""Playwright fetcher with proxy/session/profile pooling and multi-level fallback."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

from loguru import logger
from playwright.sync_api import Browser, BrowserContext, Page, Playwright, sync_playwright

from smart_extractor.config import FetcherConfig
from smart_extractor.fetcher.base import BaseFetcher, FetchResult
from smart_extractor.fetcher.static import StaticFetcher, _detect_shell_markers, _diagnostic_headers
from smart_extractor.fetcher.url_preflight import preflight_url
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


_DEFAULT_HEADERS = {
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
    "DNT": "1",
}

_SHELL_TEXT_MAX_LENGTH = 40

_CORE_CONTENT_SELECTORS = (
    "main",
    "article",
    "[role='main']",
    "[itemtype*='Product']",
    "[itemtype*='Article']",
    "[itemtype*='JobPosting']",
    "[data-testid*='product']",
    "[class*='product']",
    "[class*='price']",
    "[class*='pricing']",
    "[class*='plan']",
    "[class*='job']",
    "[class*='article']",
    "[class*='news']",
)

_ANTI_DETECT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
Object.defineProperty(navigator, 'language', { get: () => 'zh-CN' });
Object.defineProperty(navigator, 'languages', { get: () => ['zh-CN', 'zh', 'en-US', 'en'] });
window.chrome = window.chrome || { runtime: {} };
"""


def _classify_playwright_error(exc: Exception) -> str:
    message = str(exc or "").lower()
    if "timeout" in message and "networkidle" in message:
        return "network_idle_timeout"
    if "timeout" in message and ("selector" in message or "locator" in message):
        return "render_timeout"
    if "timeout" in message:
        return "render_timeout"
    if "net::err_name_not_resolved" in message or "dns" in message:
        return "dns_timeout"
    if "net::err_connection" in message or "connection" in message:
        return "network"
    if "ssl" in message or "tls" in message or "certificate" in message:
        return "tls_timeout"
    return type(exc).__name__


class PlaywrightFetcher(BaseFetcher):
    """Playwright-based fetcher with per-attempt browser isolation."""

    def __init__(self, config: Optional[FetcherConfig] = None):
        self._config = config or FetcherConfig()
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._initialized = False
        self._uses_persistent_context = False

    def _ensure_playwright(self) -> Playwright:
        if self._playwright is None:
            self._playwright = sync_playwright().start()
        return self._playwright

    def _resolve_storage_state_path(self, override_path: str = "") -> Optional[Path]:
        target = str(override_path or self._config.storage_state_path or "").strip()
        return Path(target) if target else None

    def _resolve_persistent_context_dir(self, override_dir: str = "") -> Optional[Path]:
        target = str(override_dir or self._config.persistent_context_dir or "").strip()
        return Path(target) if target else None

    def _build_context_options(
        self,
        user_agent: str,
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
        is_mobile = "Mobile/" in user_agent or "Android" in user_agent or "iPhone" in user_agent
        if is_mobile:
            options["viewport"] = {"width": 390, "height": 844}
            options["is_mobile"] = True
            options["has_touch"] = True
        if "Chrome/" in user_agent or "Edg/" in user_agent:
            options["extra_http_headers"].update(
                {
                    "sec-ch-ua": '"Chromium";v="131", "Google Chrome";v="131", "Not_A Brand";v="24"',
                    "sec-ch-ua-mobile": "?1" if is_mobile else "?0",
                    "sec-ch-ua-platform": '"Android"' if is_mobile else '"Windows"',
                }
            )
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
        logger.info("PlaywrightFetcher 使用代理: {}", mask_proxy_url(resolved_proxy))
        return payload

    # Compatibility wrappers retained for existing tests and call sites.
    def _ensure_browser(self) -> Optional[Browser]:
        if self._initialized:
            return self._browser
        playwright = self._ensure_playwright()
        user_agent = self._config.user_agent or get_random_user_agent()
        persistent_dir = self._resolve_persistent_context_dir()
        proxy_options = self._build_proxy_options()
        if persistent_dir:
            persistent_dir.mkdir(parents=True, exist_ok=True)
            self._uses_persistent_context = True
            self._context = playwright.chromium.launch_persistent_context(
                user_data_dir=str(persistent_dir),
                headless=self._config.headless,
                args=self._build_launch_args(),
                proxy=proxy_options,
                **self._build_context_options(
                    user_agent,
                    include_storage_state=False,
                ),
            )
            self._context.add_init_script(_ANTI_DETECT_SCRIPT)
            self._browser = self._context.browser
        else:
            self._browser = playwright.chromium.launch(
                headless=self._config.headless,
                args=self._build_launch_args(),
                proxy=proxy_options,
            )
        self._initialized = True
        return self._browser

    def _get_context(self) -> BrowserContext:
        if self._context is not None:
            return self._context
        browser = self._ensure_browser()
        if browser is None:
            raise RuntimeError("Playwright 浏览器初始化失败")
        user_agent = self._config.user_agent or get_random_user_agent()
        self._context = browser.new_context(**self._build_context_options(user_agent))
        self._context.add_init_script(_ANTI_DETECT_SCRIPT)
        return self._context

    def _create_page(self, context: BrowserContext | None = None) -> Page:
        return (context or self._get_context()).new_page()

    def _persist_storage_state(
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
            context.storage_state(path=str(resolved_path))
        except Exception as exc:
            logger.warning("保存 Playwright storage_state 失败: {}", exc)

    @staticmethod
    def _extract_body_text(page: Page) -> str:
        try:
            return page.locator("body").inner_text(timeout=1000).strip()
        except Exception:
            return ""

    @staticmethod
    def _extract_title_text(page: Page) -> str:
        try:
            return page.title().strip()
        except Exception:
            return ""

    def _looks_like_shell_page(self, page: Page) -> bool:
        body_text = self._extract_body_text(page)
        return not body_text or looks_like_loading_text(body_text, max_length=_SHELL_TEXT_MAX_LENGTH)

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
        except Exception:
            pass
        try:
            page.evaluate(
                """
                () => {
                    const steps = [0.35, 0.7, 1.0].map(v => Math.floor(document.body.scrollHeight * v));
                    for (const top of steps) window.scrollTo(0, top);
                    window.scrollTo(0, Math.min(window.innerHeight * 0.25, 240));
                }
                """
            )
        except Exception:
            pass
        try:
            page.wait_for_timeout(600)
        except Exception:
            pass

    def _wait_for_meaningful_content(self, page: Page) -> None:
        deadline = time.time() + min(max(self._config.wait_after_load / 1000, 2), 12)
        last_length = 0
        while time.time() < deadline:
            body_text = self._extract_body_text(page)
            if len(body_text) >= 80 and not looks_like_loading_text(body_text):
                return
            if len(body_text) <= last_length + 10:
                self._warm_up_page(page)
            last_length = max(last_length, len(body_text))
            page.wait_for_timeout(500)

    def _wait_for_core_selectors(self, page: Page) -> None:
        timeout_ms = max(0, int(getattr(self._config, "core_selector_wait_ms", 2500) or 0))
        if timeout_ms <= 0:
            return
        try:
            page.wait_for_selector(
                ", ".join(_CORE_CONTENT_SELECTORS),
                timeout=timeout_ms,
                state="attached",
            )
        except Exception:
            return

    @staticmethod
    def _measure_dom_stability(page: Page) -> dict[str, int]:
        try:
            first = len(PlaywrightFetcher._extract_body_text(page))
            page.wait_for_timeout(350)
            second = len(PlaywrightFetcher._extract_body_text(page))
            return {
                "dom_text_length_before": first,
                "dom_text_length_after": second,
                "dom_text_length_delta": second - first,
            }
        except Exception:
            return {}

    @staticmethod
    def _extract_reader_html(page: Page) -> str:
        try:
            blocks = page.evaluate(
                """
                () => {
                    const selectors = [
                        'main', 'article', '[role="main"]',
                        '[itemtype*="Product"]', '[itemtype*="Article"]', '[itemtype*="JobPosting"]',
                        '[class*="product"]', '[class*="price"]', '[class*="pricing"]', '[class*="plan"]',
                        '[class*="job"]', '[class*="article"]', '[class*="news"]',
                        '[data-testid*="product"]'
                    ];
                    const seen = new Set();
                    const out = [];
                    for (const selector of selectors) {
                        for (const node of document.querySelectorAll(selector)) {
                            const text = (node.innerText || node.textContent || '').trim();
                            if (text.length < 40 || seen.has(text)) continue;
                            seen.add(text);
                            out.push(`<section data-smart-reader-block="1">${node.innerHTML}</section>`);
                            if (out.length >= 8) return out;
                        }
                    }
                    return out;
                }
                """
            )
            if isinstance(blocks, list):
                return "\n".join(str(item) for item in blocks if str(item).strip())
        except Exception:
            return ""
        return ""

    def _stabilize_page(
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
            self._wait_for_meaningful_content(page)
            if not self._looks_like_shell_page(page) and not self._looks_like_challenge_page(
                page,
                status_code=status_code,
                headers=headers,
            ):
                return reload_count
            if attempt >= max_attempts:
                return reload_count
            logger.info(
                "PlaywrightFetcher 检测到挑战页/壳页，执行站内恢复: url={} reload_attempt={}",
                url,
                attempt,
            )
            self._warm_up_page(page)
            if self._config.challenge_retry_backoff_ms > 0:
                page.wait_for_timeout(self._config.challenge_retry_backoff_ms)
            page.reload(timeout=self._config.timeout, wait_until="domcontentloaded")
            try:
                network_idle_timeout = max(
                    0,
                    min(
                        int(getattr(self._config, "network_idle_timeout_ms", 2500) or 0),
                        self._config.timeout,
                    ),
                )
                if network_idle_timeout:
                    page.wait_for_load_state("networkidle", timeout=network_idle_timeout)
            except Exception:
                pass
            reload_count += 1
        return reload_count

    def _save_screenshot(self, page: Page, url: str, *, suffix: str = "") -> None:
        try:
            screenshot_dir = Path(self._config.screenshot_dir)
            screenshot_dir.mkdir(parents=True, exist_ok=True)
            url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{timestamp}_{url_hash}"
            if suffix:
                filename = f"{filename}_{suffix}"
            page.screenshot(path=str(screenshot_dir / f"{filename}.png"), full_page=True)
        except Exception as exc:
            logger.warning("截图保存失败: {}", exc)

    def _assess_page(
        self,
        page: Page,
        *,
        status_code: int,
        headers: dict[str, str] | None,
    ) -> ChallengeAssessment:
        title_text = self._extract_title_text(page)
        body_text = self._extract_body_text(page)
        combined_text = "\n".join(part for part in (title_text, body_text) if part)
        return assess_challenge(
            text=combined_text,
            headers=headers,
            status_code=status_code,
        )

    @staticmethod
    def _build_diagnostics(
        *,
        stage: str,
        reason: str,
        status_code: int = 0,
        original_url: str = "",
        final_url: str = "",
        headers: dict[str, str] | None = None,
        body_size: int = 0,
        is_shell_page: bool = False,
        retry_count: int = 0,
        redirect_chain: list[str] | None = None,
        raw_error: str = "",
        shell_markers: list[str] | None = None,
    ) -> dict[str, object]:
        normalized_headers = {str(key).lower(): str(value) for key, value in (headers or {}).items()}
        return {
            "failure_stage": stage,
            "failure_reason": reason,
            "http_status": int(status_code or 0),
            "original_url": original_url,
            "final_url": final_url,
            "redirect_chain": list(redirect_chain or []),
            "content_type": str(normalized_headers.get("content-type") or ""),
            "content_encoding": str(normalized_headers.get("content-encoding") or ""),
            "response_headers": _diagnostic_headers(headers or {}),
            "body_size": int(body_size or 0),
            "is_shell_page": bool(is_shell_page),
            "retry_count": int(retry_count or 0),
            "raw_error": str(raw_error or ""),
            "shell_markers": list(shell_markers or []),
            "preflight_type_mismatch": str(
                normalized_headers.get("x-smart-preflight-type-mismatch") or ""
            ),
            "dom_text_length_before": int(normalized_headers.get("x-smart-dom-text-length-before") or 0),
            "dom_text_length_after": int(normalized_headers.get("x-smart-dom-text-length-after") or 0),
            "dom_text_length_delta": int(normalized_headers.get("x-smart-dom-text-length-delta") or 0),
        }

    def _maybe_early_stop_challenge(
        self,
        page: Page,
        *,
        status_code: int,
        headers: dict[str, str] | None,
    ) -> ChallengeAssessment | None:
        if status_code not in {401, 403, 429} and not headers_indicate_challenge(headers):
            return None
        wait_ms = max(0, min(int(getattr(self._config, "challenge_early_stop_ms", 1200) or 0), 5000))
        if wait_ms:
            try:
                page.wait_for_timeout(wait_ms)
            except Exception:
                pass
        assessment = self._assess_page(
            page,
            status_code=status_code,
            headers=headers,
        )
        if assessment.blocked or assessment.challenge:
            return assessment
        return None

    def _launch_attempt_context(
        self,
        attempt: AccessAttempt,
        *,
        user_agent: str,
    ) -> tuple[BrowserContext, Browser | None, bool]:
        playwright = self._ensure_playwright()
        proxy_options = self._build_proxy_options(attempt.proxy_url or None)
        profile_dir = self._resolve_persistent_context_dir(attempt.profile_dir)
        if profile_dir is not None:
            profile_dir.mkdir(parents=True, exist_ok=True)
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                headless=self._config.headless,
                args=self._build_launch_args(),
                proxy=proxy_options,
                **self._build_context_options(
                    user_agent,
                    include_storage_state=False,
                ),
            )
            context.add_init_script(_ANTI_DETECT_SCRIPT)
            return context, context.browser, True

        browser = playwright.chromium.launch(
            headless=self._config.headless,
            args=self._build_launch_args(),
            proxy=proxy_options,
        )
        context = browser.new_context(
            **self._build_context_options(
                user_agent,
                include_storage_state=True,
                storage_state_path=attempt.storage_state_path,
            )
        )
        context.add_init_script(_ANTI_DETECT_SCRIPT)
        return context, browser, False

    @staticmethod
    def _attach_json_capture(page: Page, captured_json: list[dict[str, str]]) -> None:
        def handle_response(response) -> None:
            try:
                headers = response.headers or {}
                content_type = str(headers.get("content-type") or "").lower()
                request_url = str(getattr(response, "url", "") or "")
                if "json" not in content_type and not request_url.lower().endswith(".json"):
                    return
                text = response.text()
                if not text or len(text) > 120_000:
                    return
                parsed = json.loads(text)
                if not isinstance(parsed, (dict, list)):
                    return
                lowered_url = request_url.lower()
                priority = any(
                    marker in lowered_url
                    for marker in (
                        "api",
                        "graphql",
                        "product",
                        "price",
                        "pricing",
                        "plan",
                        "plans",
                        "job",
                        "jobs",
                        "career",
                        "careers",
                        "greenhouse",
                        "lever",
                        "ashby",
                        "workday",
                        "icims",
                        "smartrecruiters",
                        "posting",
                        "article",
                        "news",
                        "search",
                        "announcement",
                        "notice",
                    )
                )
                item = {
                    "url": request_url[:240],
                    "text": json.dumps(parsed, ensure_ascii=False)[:8000],
                    "priority": "1" if priority else "0",
                }
                if priority:
                    captured_json.insert(0, item)
                    del captured_json[8:]
                elif len(captured_json) < 8:
                    captured_json.append(item)
            except Exception:
                return

        try:
            page.on("response", handle_response)
        except Exception:
            return

    @staticmethod
    def _append_captured_json_hints(html: str, captured_json: list[dict[str, str]]) -> str:
        if not captured_json:
            return html
        scripts = []
        for index, item in enumerate(captured_json, start=1):
            payload = item.get("text", "")
            scripts.append(
                f'<script type="application/json" data-smart-captured-response="{index}">{payload}</script>'
            )
        marker = "\n".join(scripts)
        if "</body>" in html:
            return html.replace("</body>", marker + "\n</body>", 1)
        return f"{html}\n{marker}"

    def _fetch_dynamic_attempt(
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
            user_agent = self._config.user_agent or get_random_user_agent(
                mobile=bool(attempt.mobile_user_agent)
            )
            create_page_method = getattr(self, "_create_page")
            default_create_page = (
                getattr(create_page_method, "__self__", None) is self
                and getattr(create_page_method, "__func__", None) is PlaywrightFetcher._create_page
            )
            if default_create_page:
                context, browser, persistent_context = self._launch_attempt_context(
                    attempt,
                    user_agent=user_agent,
                )
                page = self._create_page(context)
            else:
                try:
                    page = create_page_method()
                except TypeError:
                    page = create_page_method(None)
            captured_json: list[dict[str, str]] = []
            self._attach_json_capture(page, captured_json)
            response = page.goto(
                url,
                timeout=self._config.timeout,
                wait_until="domcontentloaded",
            )
            status_code = response.status if response else 0
            headers = dict(response.headers) if response else {}
            self._wait_for_core_selectors(page)
            dom_stability = self._measure_dom_stability(page)
            if bool(getattr(self._config, "challenge_early_stop_enabled", True)):
                early_assessment = self._maybe_early_stop_challenge(
                    page,
                    status_code=status_code,
                    headers=headers,
                )
                if early_assessment is not None:
                    html = page.content()
                    shell_markers = _detect_shell_markers(html, headers=headers)
                    headers = {
                        **headers,
                        "x-smart-fetch-attempt-reason": attempt.reason,
                        "x-smart-fetch-mobile-ua": "1" if attempt.mobile_user_agent else "0",
                        "x-smart-fetch-json-responses": str(len(captured_json)),
                        "x-smart-fetch-html-length": str(len(html or "")),
                        "x-smart-fetch-early-stop": early_assessment.reason or "challenge",
                    }
                    return FetchResult(
                        url=url,
                        html=html,
                        status_code=status_code,
                        headers=headers,
                        elapsed_ms=(time.perf_counter() - overall_start) * 1000,
                        is_shell_page=True,
                        retry_count=retry_count_offset,
                        diagnostics=self._build_diagnostics(
                            stage="render",
                            reason=early_assessment.reason or "challenge_page",
                            status_code=status_code,
                            original_url=url,
                            final_url=url,
                            headers=headers,
                            body_size=len(html or ""),
                            is_shell_page=True,
                            retry_count=retry_count_offset,
                            shell_markers=shell_markers,
                        ),
                    ), early_assessment
            try:
                network_idle_timeout = max(
                    0,
                    min(
                        int(getattr(self._config, "network_idle_timeout_ms", 2500) or 0),
                        self._config.timeout,
                    ),
                )
                if network_idle_timeout:
                    page.wait_for_load_state("networkidle", timeout=network_idle_timeout)
            except Exception:
                pass

            reload_count = int(
                self._stabilize_page(
                    page,
                    url,
                    status_code=status_code,
                    headers=headers,
                )
                or 0
            )
            if self._config.wait_after_load > 0:
                page.wait_for_timeout(self._config.wait_after_load)
            try:
                page.wait_for_selector("body", timeout=5000)
            except Exception:
                pass

            if self._config.screenshot:
                self._save_screenshot(page, url, suffix=f"attempt{attempt.attempt_no}")

            html = page.content()
            reader_html = ""
            if bool(getattr(self._config, "reader_mode_rescue_enabled", True)):
                body_text = self._extract_body_text(page)
                if len(body_text) < 160 or looks_like_loading_text(body_text, max_length=160):
                    reader_html = self._extract_reader_html(page)
                    if reader_html:
                        html = (
                            html.replace("</body>", reader_html + "\n</body>", 1)
                            if "</body>" in html
                            else f"{html}\n{reader_html}"
                        )
            if captured_json:
                html = self._append_captured_json_hints(html, captured_json)
            shell_markers = _detect_shell_markers(html, headers=headers)
            headers = {
                **headers,
                "x-smart-fetch-attempt-reason": attempt.reason,
                "x-smart-fetch-mobile-ua": "1" if attempt.mobile_user_agent else "0",
                "x-smart-fetch-json-responses": str(len(captured_json)),
                "x-smart-fetch-html-length": str(len(html or "")),
                "x-smart-reader-mode-blocks": "1" if reader_html else "0",
                "x-smart-dom-text-length-before": str(dom_stability.get("dom_text_length_before", 0)),
                "x-smart-dom-text-length-after": str(dom_stability.get("dom_text_length_after", 0)),
                "x-smart-dom-text-length-delta": str(dom_stability.get("dom_text_length_delta", 0)),
            }
            assessment = self._assess_page(
                page,
                status_code=status_code,
                headers=headers,
            )
            if context is not None:
                self._persist_storage_state(
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
                diagnostics=self._build_diagnostics(
                    stage="render",
                    reason=assessment.reason or "",
                    status_code=status_code,
                    original_url=url,
                    final_url=url,
                    headers=headers,
                    body_size=len(html or ""),
                    is_shell_page=assessment.shell_page or assessment.challenge,
                    retry_count=retry_count_offset + reload_count,
                    shell_markers=shell_markers,
                ),
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
                diagnostics=self._build_diagnostics(
                    stage="render",
                    reason=_classify_playwright_error(exc),
                    retry_count=retry_count_offset,
                    raw_error=error_message,
                ),
            )
            return result, assessment
        finally:
            if page is not None:
                try:
                    page.close()
                except Exception:
                    pass
            if context is not None and context is not self._context:
                try:
                    self._persist_storage_state(
                        context,
                        storage_state_path=attempt.storage_state_path,
                    )
                    context.close()
                except Exception:
                    pass
            if browser is not None and not persistent_context and browser is not self._browser:
                try:
                    browser.close()
                except Exception:
                    pass

    def _fetch_static_fallback(
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
                "static_fallback_to_dynamic": False,
            }
        )
        fetcher = StaticFetcher(static_config)
        try:
            result = fetcher.fetch(url)
        finally:
            fetcher.close()
        result.retry_count += retry_count_offset
        assessment = assess_challenge(
            text=result.html[:4000],
            headers=result.headers,
            status_code=result.status_code,
            error=result.error or "",
        )
        return result, assessment

    def fetch(self, url: str) -> FetchResult:
        overall_start = time.perf_counter()
        preflight_headers: dict[str, str] = {}
        if bool(getattr(self._config, "url_preflight_enabled", True)):
            preflight = preflight_url(
                url,
                timeout_ms=int(getattr(self._config, "url_preflight_timeout_ms", 5000) or 5000),
                headers=dict(_DEFAULT_HEADERS),
                verify_ssl=bool(self._config.verify_ssl),
                sitemap_fallback_enabled=bool(getattr(self._config, "sitemap_fallback_enabled", True)),
            )
            preflight_headers = {
                "x-smart-url-preflight": "ok" if preflight.reachable else "unreachable",
                "x-smart-url-preflight-reason": preflight.reason,
                "x-smart-url-preflight-repair-reason": preflight.repair_reason,
                "x-smart-final-url": preflight.final_url,
                "x-smart-canonical-url": preflight.canonical_url,
                "x-smart-preflight-type-mismatch": preflight.headers.get(
                    "x-smart-preflight-type-mismatch",
                    "",
                ),
            }
            if (
                not preflight.reachable
                and bool(getattr(self._config, "url_preflight_abort_unreachable", True))
            ):
                return FetchResult(
                    url=url,
                    status_code=preflight.status_code,
                    headers={**preflight.headers, **preflight_headers},
                    error=f"unreachable_url: {preflight.reason}",
                    elapsed_ms=(time.perf_counter() - overall_start) * 1000,
                    diagnostics=self._build_diagnostics(
                        stage="preflight",
                        reason=preflight.reason,
                        status_code=preflight.status_code,
                        original_url=preflight.original_url,
                        final_url=preflight.final_url,
                        headers=preflight.headers,
                        redirect_chain=preflight.redirect_chain,
                    ),
                )
            if preflight.target_url and preflight.target_url != url:
                logger.info("PlaywrightFetcher URL preflight resolved: {} -> {}", url, preflight.target_url)
                url = preflight.target_url
        attempts = build_access_attempts(url, self._config, prefer_dynamic=True)
        last_result: FetchResult | None = None

        for attempt_index, attempt in enumerate(attempts, start=1):
            logger.info(
                "PlaywrightFetcher 抓取开始: url={} attempt={} mode={} proxy={} session_slot={} profile_slot={}",
                url,
                attempt_index,
                attempt.fetcher_mode,
                attempt.masked_proxy_url or "direct",
                attempt.session_slot,
                attempt.profile_slot,
            )
            if attempt.fetcher_mode == "static":
                result, assessment = self._fetch_static_fallback(
                    url,
                    attempt=attempt,
                    retry_count_offset=attempt_index - 1,
                )
            else:
                result, assessment = self._fetch_dynamic_attempt(
                    url,
                    attempt=attempt,
                    retry_count_offset=attempt_index - 1,
                    overall_start=overall_start,
                )
            last_result = result
            if preflight_headers:
                result.headers = {**(result.headers or {}), **preflight_headers}
            if not assessment.retryable or attempt_index >= len(attempts):
                return result
            logger.warning(
                "PlaywrightFetcher 将切换到下一个尝试: url={} attempt={} reason={}",
                url,
                attempt_index,
                assessment.reason or "retryable",
            )

        return last_result or FetchResult(
            url=url,
            status_code=0,
            error="PlaywrightFetcher 未生成任何结果",
            elapsed_ms=(time.perf_counter() - overall_start) * 1000,
        )

    def close(self) -> None:
        if self._context is not None:
            try:
                self._persist_storage_state(self._context)
                self._context.close()
            except Exception as exc:
                logger.debug("关闭 Playwright 上下文失败，已忽略: {}", exc)
            self._context = None
        if self._browser is not None and not self._uses_persistent_context:
            try:
                self._browser.close()
            except Exception as exc:
                logger.debug("关闭 Playwright 浏览器失败，已忽略: {}", exc)
            self._browser = None
        else:
            self._browser = None
        if self._playwright is not None:
            try:
                self._playwright.stop()
            except Exception as exc:
                logger.debug("停止 Playwright 失败，已忽略: {}", exc)
            self._playwright = None
        self._initialized = False
        self._uses_persistent_context = False
