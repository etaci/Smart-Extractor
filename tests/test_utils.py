"""
工具模块单元测试

测试 anti_detect、retry、logger 等工具函数。
"""

import time
import pytest

from smart_extractor.utils.anti_detect import (
    assess_challenge,
    build_access_attempts,
    build_proxy_candidates,
    get_random_user_agent,
    mask_proxy_url,
    random_delay,
    URLDeduplicator,
)
from smart_extractor.utils.retry import (
    create_api_retry,
    create_fetch_retry,
    retry_with_fallback,
)


# ===== User-Agent 测试 =====

class TestUserAgent:
    """User-Agent 工具测试"""

    def test_returns_string(self):
        """测试返回字符串"""
        ua = get_random_user_agent()
        assert isinstance(ua, str)
        assert len(ua) > 0

    def test_randomness(self):
        """测试随机性（多次调用应有不同结果的可能）"""
        agents = set()
        for _ in range(50):
            agents.add(get_random_user_agent())
        # 至少应该有多个不同的 UA
        assert len(agents) > 1

    def test_contains_browser_info(self):
        """测试包含浏览器信息"""
        ua = get_random_user_agent()
        assert "Mozilla" in ua or "Chrome" in ua or "Safari" in ua or "Firefox" in ua


# ===== 随机延迟测试 =====

class TestRandomDelay:
    """random_delay 功能测试"""

    def test_delay_executes(self):
        """测试延迟执行"""
        start = time.time()
        random_delay(0.01, 0.02)
        elapsed = time.time() - start
        assert elapsed >= 0.01

    def test_delay_within_range(self):
        """测试延迟在指定范围内"""
        start = time.time()
        random_delay(0.01, 0.05)
        elapsed = time.time() - start
        assert 0.01 <= elapsed < 0.2  # 宽裕的上界


# ===== URL 去重测试 =====

class TestURLDeduplicator:
    """URLDeduplicator 功能测试"""

    def test_first_not_visited(self):
        """测试首次出现的 URL 未被访问"""
        dedup = URLDeduplicator()
        assert dedup.is_visited("https://example.com") is False

    def test_visited_detected(self):
        """测试标记后可检测到已访问"""
        dedup = URLDeduplicator()
        dedup.mark_visited("https://example.com")
        assert dedup.is_visited("https://example.com") is True

    def test_different_urls(self):
        """测试不同 URL 互不影响"""
        dedup = URLDeduplicator()
        dedup.mark_visited("https://a.com")
        assert dedup.is_visited("https://b.com") is False

    def test_count(self):
        """测试已见 URL 计数"""
        dedup = URLDeduplicator()
        dedup.mark_visited("https://a.com")
        dedup.mark_visited("https://b.com")
        dedup.mark_visited("https://a.com")  # 重复标记
        assert dedup.count() == 2  # 只有 2 个唯一 URL


class TestAntiDetectHelpers:
    def test_mask_proxy_url(self):
        assert mask_proxy_url("http://user:pass@proxy.example.com:9000") == (
            "http://user:***@proxy.example.com:9000"
        )

    def test_build_proxy_candidates_keeps_order_and_direct_fallback(self):
        candidates = build_proxy_candidates(
            "http://proxy-a.example.com:8000",
            ["http://proxy-b.example.com:8001", "http://proxy-a.example.com:8000"],
        )
        assert candidates == [
            "http://proxy-a.example.com:8000",
            "http://proxy-b.example.com:8001",
            "",
        ]

    def test_build_access_attempts_generates_dynamic_and_static_fallback(self):
        class _Config:
            proxy_url = "http://proxy-a.example.com:8000"
            proxy_urls = ["http://proxy-b.example.com:8001"]
            proxy_rotation_enabled = True
            fetch_max_attempts = 2
            challenge_fallback_to_static = True
            browser_session_pool_size = 2
            persistent_profile_pool_size = 2
            persistent_context_dir = ""

        attempts = build_access_attempts(
            "https://example.com/product/1",
            _Config(),
            prefer_dynamic=True,
        )

        assert attempts[0].fetcher_mode == "dynamic"
        assert attempts[0].proxy_url == "http://proxy-a.example.com:8000"
        assert attempts[1].proxy_url == "http://proxy-b.example.com:8001"
        assert any(item.fetcher_mode == "static" for item in attempts)
        assert any(item.profile_dir for item in attempts if item.fetcher_mode == "dynamic")

    def test_assess_challenge_detects_block_and_transport_error(self):
        blocked = assess_challenge(
            text="Please verify you are human",
            headers={"cf-ray": "1"},
            status_code=403,
        )
        assert blocked.challenge is True
        assert blocked.retryable is True

        transport = assess_challenge(error="Proxy timeout while connecting upstream")
        assert transport.retryable is True
        assert transport.reason == "transport_error"


# ===== 重试策略测试 =====

class TestRetryStrategies:
    """重试策略测试"""

    def test_api_retry_decorator(self):
        """测试 API 重试装饰器创建"""
        decorator = create_api_retry(max_retries=2)
        assert callable(decorator)

    def test_fetch_retry_decorator(self):
        """测试 Fetch 重试装饰器创建"""
        decorator = create_fetch_retry(max_retries=2)
        assert callable(decorator)

    def test_retry_with_fallback_success(self):
        """测试主函数成功时直接返回"""
        def main_fn():
            return "main_result"

        def fallback_fn():
            return "fallback_result"

        result = retry_with_fallback(main_fn, fallback_fn)
        assert result == "main_result"

    def test_retry_with_fallback_uses_fallback(self):
        """测试主函数失败时使用降级"""
        def main_fn():
            raise RuntimeError("主函数失败")

        def fallback_fn():
            return "fallback_result"

        result = retry_with_fallback(main_fn, fallback_fn)
        assert result == "fallback_result"

    def test_retry_with_fallback_both_fail(self):
        """测试主函数和降级函数都失败"""
        def main_fn():
            raise RuntimeError("主函数失败")

        def fallback_fn():
            raise RuntimeError("降级也失败")

        with pytest.raises(RuntimeError, match="降级也失败"):
            retry_with_fallback(main_fn, fallback_fn)
