"""LLMClient (sync) 辅助函数与 usage 统计的单元测试。"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock

from smart_extractor.config import LLMConfig
from smart_extractor.extractor.llm_client import LLMClient, UsageSample


def _make_client() -> LLMClient:
    return LLMClient(
        LLMConfig(
            api_key="test-key",
            base_url="https://example.com/v1",
            model="gpt-4o-mini",
            timeout=5,
        )
    )


def test_stringify_messages_is_stable():
    text = LLMClient._stringify_messages(
        [
            {"role": "system", "content": "s"},
            {"role": "user", "content": "u"},
        ]
    )
    assert "system: s" in text
    assert "user: u" in text


def test_estimate_token_count_is_zero_for_empty_string():
    assert LLMClient._estimate_token_count("") == 0
    assert LLMClient._estimate_token_count("   ") == 0


def test_estimate_token_count_positive_for_non_empty():
    assert LLMClient._estimate_token_count("hello world") > 0


def test_pricing_for_model_known_vs_unknown():
    assert LLMClient._pricing_for_model("gpt-4o-mini") == (0.15, 0.60)
    assert LLMClient._pricing_for_model("gpt-4o") == (5.00, 15.00)
    assert LLMClient._pricing_for_model("totally-unknown") == (0.0, 0.0)


def test_usage_from_object_reads_attributes():
    @dataclass
    class _U:
        prompt_tokens: int = 42
        completion_tokens: int = 8

    usage = LLMClient._usage_from_object(_U())
    assert usage is not None
    assert usage.prompt_tokens == 42
    assert usage.completion_tokens == 8
    assert usage.source == "api"


def test_usage_from_object_returns_none_for_zero_totals():
    assert (
        LLMClient._usage_from_object(
            {"prompt_tokens": 0, "completion_tokens": 0}
        )
        is None
    )
    assert LLMClient._usage_from_object(None) is None


def test_usage_from_chunk_picks_up_usage_attr():
    usage = LLMClient._usage_from_chunk(
        MagicMock(usage=MagicMock(prompt_tokens=4, completion_tokens=2))
    )
    assert usage is not None
    assert usage.prompt_tokens == 4
    assert usage.source == "api"


def test_usage_from_response_falls_back_to_estimate_when_missing():
    client = _make_client()
    # response 没有 usage，走 tiktoken 估算
    usage = client._usage_from_response(
        MagicMock(usage=None),
        fallback_prompt="some prompt text for estimation",
        fallback_completion="short",
    )
    assert usage.source == "estimate"
    assert usage.prompt_tokens > 0


def test_record_call_aggregates_counts_and_separates_source():
    client = _make_client()
    client._record_call(
        0.1,
        usage=UsageSample(prompt_tokens=10, completion_tokens=5, source="api"),
    )
    client._record_call(
        0.2,
        usage=UsageSample(prompt_tokens=30, completion_tokens=0, source="estimate"),
    )

    stats = client.get_stats()
    assert stats["total_calls"] == 2
    assert stats["prompt_tokens"] == 40
    assert stats["completion_tokens"] == 5
    assert stats["total_tokens"] == 45
    assert stats["api_usage_calls"] == 1
    assert stats["estimated_usage_calls"] == 1
    assert stats["api_usage_ratio"] == 0.5


def test_record_call_estimated_cost_matches_pricing_table():
    client = _make_client()
    client._record_call(
        0.0,
        usage=UsageSample(
            prompt_tokens=1_000_000, completion_tokens=1_000_000, source="api"
        ),
    )
    stats = client.get_stats()
    # gpt-4o-mini 价格为 (0.15 input, 0.60 output) per 1M tokens
    assert abs(stats["estimated_cost_usd"] - (0.15 + 0.60)) < 1e-6


def test_stream_chat_collects_chunks_and_usage():
    client = _make_client()

    chunks = [
        MagicMock(
            choices=[MagicMock(delta=MagicMock(content='{"a":'))],
            usage=None,
        ),
        MagicMock(
            choices=[MagicMock(delta=MagicMock(content='"b"}'))],
            usage=None,
        ),
        # OpenAI 流式最终 usage-only chunk
        MagicMock(
            choices=[],
            usage=MagicMock(prompt_tokens=11, completion_tokens=7),
        ),
    ]
    client._openai_client.chat.completions.create = MagicMock(return_value=iter(chunks))

    text, usage = client._stream_chat(
        [{"role": "user", "content": "x"}], use_json_format=True
    )
    assert text == '{"a":"b"}'
    assert usage is not None
    assert usage.prompt_tokens == 11
    assert usage.completion_tokens == 7
    assert usage.source == "api"


def test_stream_chat_returns_empty_on_exception():
    client = _make_client()
    client._openai_client.chat.completions.create = MagicMock(
        side_effect=RuntimeError("network down")
    )
    text, usage = client._stream_chat(
        [{"role": "user", "content": "x"}], use_json_format=False
    )
    assert text == ""
    assert usage is None
