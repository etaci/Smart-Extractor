"""
反爬与访问特征识别工具。
"""

from __future__ import annotations

import hashlib
from pathlib import Path
import random
import threading
import time
from typing import Mapping, Optional

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


def get_random_user_agent() -> str:
    """随机选择一个桌面浏览器 User-Agent。"""
    return random.choice(_USER_AGENTS)  # nosec B311


def random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
    """随机延迟，模拟更自然的访问节奏。"""
    delay = random.uniform(min_seconds, max_seconds)  # nosec B311
    logger.debug("随机延迟 {:.2f} 秒", delay)
    time.sleep(delay)


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


class URLDeduplicator:
    """URL 去重器。"""

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
