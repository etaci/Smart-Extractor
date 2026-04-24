"""
反爬策略模块

提供 User-Agent 轮换、请求间隔随机化、URL 去重等反检测功能。
"""

import random
import hashlib
import time
from pathlib import Path
from typing import Optional

from loguru import logger


# 常用桌面浏览器 User-Agent 池
_USER_AGENTS = [
    # Chrome (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    # Chrome (Mac)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Firefox (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    # Firefox (Mac)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
]


def get_random_user_agent() -> str:
    """随机选择一个 User-Agent"""
    return random.choice(_USER_AGENTS)  # nosec B311


def random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
    """
    随机延迟，模拟人类浏览行为。

    Args:
        min_seconds: 最小延迟秒数
        max_seconds: 最大延迟秒数
    """
    delay = random.uniform(min_seconds, max_seconds)  # nosec B311
    logger.debug("随机延迟 {:.2f} 秒", delay)
    time.sleep(delay)


class URLDeduplicator:
    """
    URL 去重器

    使用 URL 的 MD5 哈希记录已访问的 URL，支持持久化到文件。
    """

    def __init__(self, cache_file: Optional[str | Path] = None):
        """
        Args:
            cache_file: 已访问 URL 缓存文件路径。如果为 None，仅使用内存缓存。
        """
        self._visited: set[str] = set()
        self._cache_file = Path(cache_file) if cache_file else None

        # 从缓存文件加载已访问记录
        if self._cache_file and self._cache_file.exists():
            self._load_cache()

    def _url_hash(self, url: str) -> str:
        """计算 URL 的稳定哈希"""
        return hashlib.sha256(url.strip().encode("utf-8")).hexdigest()

    def _load_cache(self) -> None:
        """从文件加载已访问 URL 缓存"""
        try:
            with open(self._cache_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self._visited.add(line)
            logger.info("从缓存文件加载了 {} 条已访问记录", len(self._visited))
        except Exception as e:
            logger.warning("加载 URL 缓存文件失败: {}", e)

    def _save_hash(self, url_hash: str) -> None:
        """追加保存一条哈希记录到文件"""
        if self._cache_file:
            try:
                self._cache_file.parent.mkdir(parents=True, exist_ok=True)
                with open(self._cache_file, "a", encoding="utf-8") as f:
                    f.write(url_hash + "\n")
            except Exception as e:
                logger.warning("保存 URL 缓存失败: {}", e)

    def is_visited(self, url: str) -> bool:
        """检查 URL 是否已被访问过"""
        return self._url_hash(url) in self._visited

    def mark_visited(self, url: str) -> None:
        """标记 URL 为已访问"""
        url_hash = self._url_hash(url)
        if url_hash not in self._visited:
            self._visited.add(url_hash)
            self._save_hash(url_hash)
            logger.debug("标记 URL 为已访问: {}", url[:80])

    def count(self) -> int:
        """返回已访问 URL 数量"""
        return len(self._visited)

    def clear(self) -> None:
        """清空所有已访问记录"""
        self._visited.clear()
        if self._cache_file and self._cache_file.exists():
            self._cache_file.unlink()
        logger.info("已清空所有 URL 去重缓存")
