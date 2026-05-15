"""Anti-blocking helpers: proxy rotation, challenge detection, and session/profile pooling."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
import random
import tempfile
import threading
import time
from typing import Mapping, Optional
from urllib.parse import urlsplit, urlunsplit

from loguru import logger


_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
]

ANTI_BOT_TEXT_MARKERS = (
    "安全验证",
    "人机验证",
    "验证码",
    "异常访问",
    "访问受限",
    "verify you are human",
    "captcha",
    "checking your browser",
    "enable javascript",
    "enable cookies",
    "press and hold",
    "just a moment",
    "access denied",
    "suspicious traffic",
    "unusual traffic",
    "blocked due to suspicious activity",
    "ddos protection",
    "ray id",
    "cloudflare",
    "cf-chl",
    "robot check",
    "security check",
    "attention required",
    "please wait while we verify",
)

ANTI_BOT_HEADER_MARKERS = (
    "cf-ray",
    "cf-mitigated",
    "x-sucuri-block",
    "x-distil-cs",
    "x-akamai-session-info",
)

LOADING_TEXT_MARKERS = (
    "加载中",
    "请稍候",
    "loading",
    "initializing",
)

RETRYABLE_ERROR_MARKERS = (
    "proxy",
    "timeout",
    "timed out",
    "connection reset",
    "connection refused",
    "network is unreachable",
    "temporarily unavailable",
    "too many requests",
)


@dataclass(slots=True, frozen=True)
class AccessAttempt:
    attempt_no: int
    fetcher_mode: str
    proxy_url: str = ""
    session_slot: int = 0
    profile_slot: int = 0
    profile_dir: str = ""
    storage_state_path: str = ""
    reason: str = ""

    @property
    def masked_proxy_url(self) -> str:
        return mask_proxy_url(self.proxy_url)


@dataclass(slots=True, frozen=True)
class ChallengeAssessment:
    challenge: bool = False
    shell_page: bool = False
    blocked: bool = False
    retryable: bool = False
    status_code: int = 0
    reason: str = ""


def get_random_user_agent() -> str:
    """Randomly select a desktop browser user agent."""
    return random.choice(_USER_AGENTS)  # nosec B311


def random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
    """Sleep for a small randomized delay to soften request rhythm."""
    delay = random.uniform(min_seconds, max_seconds)  # nosec B311
    logger.debug("随机延迟 {:.2f} 秒", delay)
    time.sleep(delay)


def mask_proxy_url(proxy_url: str) -> str:
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


def looks_like_challenge_text(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    return any(marker.lower() in normalized for marker in ANTI_BOT_TEXT_MARKERS)


def looks_like_loading_text(text: str, *, max_length: int = 80) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    return len(normalized) <= max_length and any(
        marker.lower() in normalized for marker in LOADING_TEXT_MARKERS
    )


def headers_indicate_challenge(headers: Mapping[str, str] | None) -> bool:
    if not headers:
        return False
    lowered = {str(key).lower(): str(value).lower() for key, value in headers.items()}
    if any(marker in lowered for marker in ANTI_BOT_HEADER_MARKERS):
        return True
    server = lowered.get("server", "")
    powered_by = lowered.get("x-powered-by", "")
    return "cloudflare" in server or "sucuri" in server or "perimeterx" in powered_by


def assess_challenge(
    *,
    text: str = "",
    headers: Mapping[str, str] | None = None,
    status_code: int = 0,
    error: str = "",
) -> ChallengeAssessment:
    normalized_error = str(error or "").strip().lower()
    challenge = looks_like_challenge_text(text) or headers_indicate_challenge(headers)
    shell_page = looks_like_loading_text(text)
    blocked = challenge or status_code in {401, 403, 429}
    retryable_error = any(marker in normalized_error for marker in RETRYABLE_ERROR_MARKERS)
    retryable = blocked or shell_page or retryable_error
    reason = ""
    if challenge:
        reason = "challenge_page"
    elif shell_page:
        reason = "shell_page"
    elif status_code in {401, 403, 429}:
        reason = f"http_{status_code}"
    elif retryable_error:
        reason = "transport_error"
    return ChallengeAssessment(
        challenge=challenge,
        shell_page=shell_page,
        blocked=blocked,
        retryable=retryable,
        status_code=int(status_code or 0),
        reason=reason,
    )


def build_proxy_candidates(
    primary_proxy_url: str = "",
    extra_proxy_urls: list[str] | None = None,
    *,
    allow_direct: bool = True,
) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in [primary_proxy_url, *(extra_proxy_urls or [])]:
        normalized = str(raw or "").strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        if normalized:
            ordered.append(normalized)
    if allow_direct and "" not in seen:
        ordered.append("")
    return ordered or [""]


def _domain_bucket(url: str) -> str:
    domain = (urlsplit(str(url or "").strip()).hostname or "").strip().lower()
    return domain or "unknown-domain"


def _profile_pool_root(root_dir: str = "") -> Path:
    if str(root_dir or "").strip():
        return Path(root_dir)
    return Path(tempfile.gettempdir()) / "smart-extractor-profile-pool"


def build_profile_artifacts(
    *,
    url: str,
    proxy_url: str = "",
    session_slot: int = 0,
    profile_slot: int = 0,
    root_dir: str = "",
) -> tuple[str, str]:
    domain = _domain_bucket(url)
    proxy_key = hashlib.sha256(str(proxy_url or "").encode("utf-8")).hexdigest()[:10]
    slot_key = f"s{max(int(session_slot or 0), 0)}-p{max(int(profile_slot or 0), 0)}"
    profile_root = _profile_pool_root(root_dir) / domain
    profile_dir = profile_root / "profiles" / slot_key / proxy_key
    storage_state_path = profile_root / "storage_state" / f"{slot_key}-{proxy_key}.json"
    return str(profile_dir), str(storage_state_path)


def build_access_attempts(
    url: str,
    config,
    *,
    prefer_dynamic: bool = True,
) -> list[AccessAttempt]:
    max_attempts = max(int(getattr(config, "fetch_max_attempts", 1) or 1), 1)
    proxy_candidates = build_proxy_candidates(
        getattr(config, "proxy_url", "") or "",
        list(getattr(config, "proxy_urls", []) or []),
        allow_direct=True,
    )
    rotation_enabled = bool(getattr(config, "proxy_rotation_enabled", True))
    session_pool_size = max(int(getattr(config, "browser_session_pool_size", 1) or 1), 1)
    profile_pool_size = max(
        int(getattr(config, "persistent_profile_pool_size", 1) or 1),
        1,
    )
    persistent_root = str(getattr(config, "persistent_context_dir", "") or "").strip()
    attempts: list[AccessAttempt] = []
    seen: set[tuple[object, ...]] = set()

    def add_attempt(attempt_no: int, fetcher_mode: str, proxy_url: str, reason: str) -> None:
        normalized_mode = str(fetcher_mode or "dynamic").strip().lower() or "dynamic"
        session_slot = (attempt_no - 1) % session_pool_size
        profile_slot = (attempt_no - 1) % profile_pool_size
        profile_dir = ""
        storage_state_path = ""
        if normalized_mode == "dynamic":
            profile_dir, storage_state_path = build_profile_artifacts(
                url=url,
                proxy_url=proxy_url,
                session_slot=session_slot,
                profile_slot=profile_slot,
                root_dir=persistent_root,
            )
        key = (
            normalized_mode,
            str(proxy_url or "").strip(),
            session_slot if normalized_mode == "dynamic" else 0,
            profile_slot if normalized_mode == "dynamic" else 0,
        )
        if key in seen:
            return
        seen.add(key)
        attempts.append(
            AccessAttempt(
                attempt_no=attempt_no,
                fetcher_mode=normalized_mode,
                proxy_url=str(proxy_url or "").strip(),
                session_slot=session_slot,
                profile_slot=profile_slot,
                profile_dir=profile_dir,
                storage_state_path=storage_state_path,
                reason=reason,
            )
        )

    primary_mode = "dynamic" if prefer_dynamic else "static"
    for attempt_no in range(1, max_attempts + 1):
        if rotation_enabled:
            proxy_url = proxy_candidates[(attempt_no - 1) % len(proxy_candidates)]
        else:
            proxy_url = proxy_candidates[0]
        add_attempt(attempt_no, primary_mode, proxy_url, "primary")

    if prefer_dynamic and bool(getattr(config, "challenge_fallback_to_static", True)):
        fallback_no = len(attempts) + 1
        for proxy_url in proxy_candidates:
            add_attempt(fallback_no, "static", proxy_url, "challenge_fallback")
            fallback_no += 1
            if fallback_no > max_attempts + max(len(proxy_candidates), 2):
                break

    return attempts or [AccessAttempt(attempt_no=1, fetcher_mode=primary_mode, reason="default")]


class URLDeduplicator:
    """Simple URL deduplication with optional file-backed cache."""

    def __init__(self, cache_file: Optional[str | Path] = None):
        self._visited: set[str] = set()
        self._cache_file = Path(cache_file) if cache_file else None
        self._lock = threading.RLock()

        if self._cache_file and self._cache_file.exists():
            self._load_cache()

    def _url_hash(self, url: str) -> str:
        return hashlib.sha256(url.strip().encode("utf-8")).hexdigest()

    def _load_cache(self) -> None:
        try:
            with open(self._cache_file, "r", encoding="utf-8") as handle:
                with self._lock:
                    for line in handle:
                        line = line.strip()
                        if line:
                            self._visited.add(line)
            logger.info("从缓存文件加载了 {} 条已访问记录", len(self._visited))
        except Exception as exc:
            logger.warning("加载 URL 缓存文件失败: {}", exc)

    def _save_hash(self, url_hash: str) -> None:
        if self._cache_file:
            try:
                self._cache_file.parent.mkdir(parents=True, exist_ok=True)
                with open(self._cache_file, "a", encoding="utf-8") as handle:
                    handle.write(url_hash + "\n")
            except Exception as exc:
                logger.warning("保存 URL 缓存失败: {}", exc)

    def is_visited(self, url: str) -> bool:
        with self._lock:
            return self._url_hash(url) in self._visited

    def mark_visited(self, url: str) -> None:
        url_hash = self._url_hash(url)
        with self._lock:
            if url_hash in self._visited:
                return
            self._visited.add(url_hash)
            self._save_hash(url_hash)
        logger.debug("标记 URL 为已访问: {}", url[:80])

    def count(self) -> int:
        with self._lock:
            return len(self._visited)

    def clear(self) -> None:
        with self._lock:
            self._visited.clear()
            if self._cache_file and self._cache_file.exists():
                self._cache_file.unlink()
        logger.info("已清空所有 URL 去重缓存")
