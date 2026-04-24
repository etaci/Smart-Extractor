"""Async LLM client and async Playwright fetcher — lightweight smoke/unit tests.

These don't hit external services; they exercise helper functions and the stats
reporting path to ensure the async modules load and behave consistently with
the sync counterparts.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest

from smart_extractor.config import FetcherConfig, LLMConfig
from smart_extractor.extractor.llm_client import UsageSample
from smart_extractor.extractor.llm_client_async import (
    AsyncLLMClient,
    _estimate_token_count,
    _estimate_usage,
    _pricing_for_model,
    _stringify_messages,
    _usage_from_object,
)


def test_stringify_messages_preserves_roles():
    messages = [
        {"role": "system", "content": "hello"},
        {"role": "user", "content": "world"},
    ]
    rendered = _stringify_messages(messages)
    assert "system: hello" in rendered
    assert "user: world" in rendered


def test_estimate_token_count_handles_empty():
    assert _estimate_token_count("") == 0
    assert _estimate_token_count("   ") == 0


def test_estimate_token_count_returns_positive_for_text():
    count = _estimate_token_count("hello 世界")
    assert count > 0


def test_estimate_usage_sets_source_estimate():
    usage = _estimate_usage(prompt_text="abc", completion_text="def")
    assert isinstance(usage, UsageSample)
    assert usage.source == "estimate"


def test_usage_from_object_reads_attrs():
    @dataclass
    class _U:
        prompt_tokens: int = 12
        completion_tokens: int = 34

    usage = _usage_from_object(_U())
    assert usage is not None
    assert usage.prompt_tokens == 12
    assert usage.completion_tokens == 34
    assert usage.source == "api"


def test_usage_from_object_returns_none_for_missing_usage():
    assert _usage_from_object(None) is None
    assert _usage_from_object({"prompt_tokens": 0, "completion_tokens": 0}) is None


def test_pricing_for_known_model_is_nonzero():
    input_price, output_price = _pricing_for_model("gpt-4o-mini")
    assert input_price > 0
    assert output_price > 0


def test_pricing_for_unknown_model_returns_zero():
    assert _pricing_for_model("totally-unknown-model") == (0.0, 0.0)


def test_async_llm_client_records_stats_on_direct_call():
    """直接驱动 _record_call 验证 usage 统计口径。"""
    client = AsyncLLMClient(
        LLMConfig(
            api_key="test-key",
            base_url="https://example.com/v1",
            model="gpt-4o-mini",
            timeout=5,
        )
    )

    client._record_call(
        0.123,
        usage=UsageSample(prompt_tokens=100, completion_tokens=50, source="api"),
    )
    stats = client.get_stats()
    assert stats["total_calls"] == 1
    assert stats["prompt_tokens"] == 100
    assert stats["completion_tokens"] == 50
    assert stats["total_tokens"] == 150
    assert stats["api_usage_calls"] == 1
    assert stats["estimated_usage_calls"] == 0
    assert stats["api_usage_ratio"] == 1.0
    # gpt-4o-mini 定价为 (0.15, 0.60) per 1M tokens
    expected_cost = (100 * 0.15 + 50 * 0.60) / 1_000_000
    assert abs(stats["estimated_cost_usd"] - round(expected_cost, 6)) < 1e-9


def test_async_llm_client_records_estimate_and_api_separately():
    client = AsyncLLMClient(
        LLMConfig(
            api_key="test-key",
            base_url="https://example.com/v1",
            model="test-model",
            timeout=5,
        )
    )
    client._record_call(
        0.1,
        usage=UsageSample(prompt_tokens=10, completion_tokens=5, source="api"),
    )
    client._record_call(
        0.1,
        usage=UsageSample(prompt_tokens=20, completion_tokens=0, source="estimate"),
    )
    stats = client.get_stats()
    assert stats["api_usage_calls"] == 1
    assert stats["estimated_usage_calls"] == 1
    assert stats["api_usage_ratio"] == 0.5
    assert stats["prompt_tokens"] == 30


@pytest.mark.asyncio
async def test_async_stream_chat_collects_content_and_usage():
    """模拟 OpenAI 流式响应，验证能同时收集内容片段和 usage。"""
    client = AsyncLLMClient(
        LLMConfig(
            api_key="test-key",
            base_url="https://example.com/v1",
            model="gpt-4o-mini",
            timeout=5,
        )
    )

    chunks = [
        MagicMock(
            choices=[MagicMock(delta=MagicMock(content='{"a":'))],
            usage=None,
        ),
        MagicMock(
            choices=[MagicMock(delta=MagicMock(content='1}'))],
            usage=None,
        ),
        # usage-only chunk (OpenAI 流式最终块)
        MagicMock(
            choices=[],
            usage=MagicMock(prompt_tokens=7, completion_tokens=3),
        ),
    ]

    class _AsyncIter:
        def __init__(self, items):
            self._items = list(items)

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self._items:
                raise StopAsyncIteration
            return self._items.pop(0)

    client._openai_client.chat.completions.create = AsyncMock(
        return_value=_AsyncIter(chunks)
    )

    text, usage = await client._stream_chat(
        [{"role": "user", "content": "x"}], use_json_format=True
    )
    assert text == '{"a":1}'
    assert usage is not None
    assert usage.prompt_tokens == 7
    assert usage.completion_tokens == 3
    assert usage.source == "api"


def test_async_playwright_fetcher_construction_uses_config():
    """冒烟验证 AsyncPlaywrightFetcher 可被构造且懒加载不触发真实浏览器。"""
    from smart_extractor.fetcher.playwright_async import AsyncPlaywrightFetcher

    fetcher = AsyncPlaywrightFetcher(
        FetcherConfig(headless=True, timeout=1000, wait_after_load=10)
    )
    assert fetcher._initialized is False
    assert fetcher._browser is None
    # launch args 应包含反自动化指纹的关键 flag
    args = fetcher._build_launch_args()
    assert any("AutomationControlled" in a for a in args)
    # context options 传入 user_agent
    options = fetcher._build_context_options("MyUA/1.0")
    assert options["user_agent"] == "MyUA/1.0"
    assert options["viewport"]["width"] > 0
