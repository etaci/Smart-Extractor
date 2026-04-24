"""AsyncOpenAI + instructor 的异步 LLM 客户端。

与 `LLMClient` 对齐接口：`call_structured` / `call_json` / `get_stats`，但均为协程。
用于 `pipeline.run_batch_async`，可将多次 LLM 调用通过 `asyncio.gather` 真正并发执行。
"""

from __future__ import annotations

import os
import time
from typing import Any, Type

import instructor
from loguru import logger
from openai import AsyncOpenAI

from smart_extractor.config import LLMConfig
from smart_extractor.extractor.llm_client import UsageSample
from smart_extractor.extractor.llm_response import (
    _extract_chat_message_content,
    _safe_json_loads,
)
from smart_extractor.models.base import BaseExtractModel


class AsyncLLMClient:
    """AsyncOpenAI 版客户端，与同步 LLMClient 的 usage 统计口径一致。"""

    def __init__(self, config: LLMConfig):
        self._config = config

        os.environ.setdefault("NO_PROXY", "*")

        self._openai_client = AsyncOpenAI(
            api_key=self._config.api_key,
            base_url=self._config.base_url,
            timeout=self._config.timeout,
        )
        self._instructor_client = instructor.from_openai(
            self._openai_client,
            mode=instructor.Mode.JSON,
        )

        self._total_calls = 0
        self._total_time_ms = 0.0
        self._prompt_tokens = 0
        self._completion_tokens = 0
        self._total_tokens = 0
        self._estimated_cost_usd = 0.0
        self._api_usage_calls = 0
        self._estimated_usage_calls = 0

        logger.info(
            "AsyncLLMClient 初始化完成 (model={}, base_url={})",
            self._config.model,
            self._config.base_url,
        )

    async def call_structured(
        self,
        prompt: str,
        schema: Type[BaseExtractModel],
    ) -> BaseExtractModel:
        start_time = time.time()
        try:
            result, raw_completion = (
                await self._instructor_client.chat.completions.create_with_completion(
                    model=self._config.model,
                    response_model=schema,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=self._config.temperature,
                    max_tokens=self._config.max_tokens,
                )
            )
            completion_preview = ""
            if hasattr(result, "model_dump_json"):
                completion_preview = result.model_dump_json(
                    ensure_ascii=False,
                    indent=None,
                )
            usage = _usage_from_response(
                raw_completion,
                fallback_prompt=prompt,
                fallback_completion=completion_preview,
            )
            self._record_call(time.time() - start_time, usage=usage)
            return result
        except Exception:
            usage = _estimate_usage(prompt_text=prompt, completion_text="")
            self._record_call(time.time() - start_time, usage=usage)
            raise

    async def call_json(
        self, system_prompt: str, user_prompt: str
    ) -> dict[str, Any]:
        start_time = time.time()
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        message, usage = await self._stream_chat(messages, use_json_format=True)
        if message:
            usage = usage or _estimate_usage(
                prompt_text=_stringify_messages(messages),
                completion_text=message,
            )
            self._record_call(time.time() - start_time, usage=usage)
            return _safe_json_loads(message)

        logger.warning("async json_object 流式模式返回为空，降级纯流式")
        message, usage = await self._stream_chat(messages, use_json_format=False)
        if message:
            usage = usage or _estimate_usage(
                prompt_text=_stringify_messages(messages),
                completion_text=message,
            )
            self._record_call(time.time() - start_time, usage=usage)
            return _safe_json_loads(message)

        logger.warning("async 流式均为空，尝试非流式")
        try:
            response = await self._openai_client.chat.completions.create(
                model=self._config.model,
                messages=messages,
                temperature=self._config.temperature,
                max_tokens=self._config.max_tokens,
            )
            message = _extract_chat_message_content(response)
            usage = _usage_from_response(
                response,
                fallback_prompt=_stringify_messages(messages),
                fallback_completion=message,
            )
            self._record_call(time.time() - start_time, usage=usage)
            if message and message.strip():
                return _safe_json_loads(message)
        except Exception as exc:
            logger.warning("async 非流式调用失败: {}", exc)

        usage = _estimate_usage(
            prompt_text=_stringify_messages(messages),
            completion_text="",
        )
        self._record_call(time.time() - start_time, usage=usage)
        return {}

    async def _stream_chat(
        self,
        messages: list[dict[str, str]],
        use_json_format: bool = True,
    ) -> tuple[str, UsageSample | None]:
        try:
            kwargs: dict[str, Any] = {
                "model": self._config.model,
                "messages": messages,
                "temperature": self._config.temperature,
                "max_tokens": self._config.max_tokens,
                "stream": True,
                "stream_options": {"include_usage": True},
            }
            if use_json_format:
                kwargs["response_format"] = {"type": "json_object"}

            stream = await self._openai_client.chat.completions.create(**kwargs)

            collected: list[str] = []
            api_usage: UsageSample | None = None
            async for chunk in stream:
                chunk_usage = _usage_from_object(getattr(chunk, "usage", None))
                if chunk_usage is not None:
                    api_usage = chunk_usage
                choices = getattr(chunk, "choices", None)
                if not choices:
                    continue
                delta = getattr(choices[0], "delta", None)
                content = getattr(delta, "content", None)
                if content:
                    collected.append(content)

            return "".join(collected).strip(), api_usage
        except Exception as exc:
            logger.warning("async 流式调用异常: {}", exc)
            return "", None

    def _record_call(
        self,
        elapsed_seconds: float,
        *,
        usage: UsageSample,
    ) -> None:
        elapsed_ms = elapsed_seconds * 1000
        input_price_per_million, output_price_per_million = _pricing_for_model(
            self._config.model
        )
        estimated_cost = (
            usage.prompt_tokens * input_price_per_million
            + usage.completion_tokens * output_price_per_million
        ) / 1_000_000
        self._total_calls += 1
        self._total_time_ms += elapsed_ms
        self._prompt_tokens += usage.prompt_tokens
        self._completion_tokens += usage.completion_tokens
        self._total_tokens += usage.prompt_tokens + usage.completion_tokens
        self._estimated_cost_usd += estimated_cost
        if usage.source == "api":
            self._api_usage_calls += 1
        else:
            self._estimated_usage_calls += 1

    def get_stats(self) -> dict[str, float]:
        total = max(self._total_calls, 1)
        return {
            "total_calls": self._total_calls,
            "total_time_ms": self._total_time_ms,
            "avg_time_ms": self._total_time_ms / total,
            "prompt_tokens": self._prompt_tokens,
            "completion_tokens": self._completion_tokens,
            "total_tokens": self._total_tokens,
            "estimated_cost_usd": round(self._estimated_cost_usd, 6),
            "api_usage_calls": self._api_usage_calls,
            "estimated_usage_calls": self._estimated_usage_calls,
            "api_usage_ratio": round(self._api_usage_calls / total, 4),
        }

    async def aclose(self) -> None:
        try:
            await self._openai_client.close()
        except Exception as exc:
            logger.debug("关闭 AsyncOpenAI 客户端失败: {}", exc)


# ---- 共用辅助函数，与同步版 LLMClient 行为对齐 ---- #


def _stringify_messages(messages: list[dict[str, str]]) -> str:
    return "\n".join(
        f"{item.get('role', 'user')}: {item.get('content', '')}"
        for item in messages
    )


def _usage_from_object(usage_obj: Any) -> UsageSample | None:
    if usage_obj is None:
        return None
    prompt = getattr(usage_obj, "prompt_tokens", None)
    completion = getattr(usage_obj, "completion_tokens", None)
    if prompt is None and isinstance(usage_obj, dict):
        prompt = usage_obj.get("prompt_tokens")
    if completion is None and isinstance(usage_obj, dict):
        completion = usage_obj.get("completion_tokens")
    try:
        prompt_int = int(prompt) if prompt is not None else 0
        completion_int = int(completion) if completion is not None else 0
    except (TypeError, ValueError):
        return None
    if prompt_int <= 0 and completion_int <= 0:
        return None
    return UsageSample(
        prompt_tokens=prompt_int,
        completion_tokens=completion_int,
        source="api",
    )


def _usage_from_response(
    response: Any,
    *,
    fallback_prompt: str,
    fallback_completion: str,
) -> UsageSample:
    usage_obj = getattr(response, "usage", None)
    if usage_obj is None and isinstance(response, dict):
        usage_obj = response.get("usage")
    sample = _usage_from_object(usage_obj)
    if sample is not None:
        return sample
    return _estimate_usage(
        prompt_text=fallback_prompt,
        completion_text=fallback_completion,
    )


def _estimate_usage(*, prompt_text: str, completion_text: str) -> UsageSample:
    return UsageSample(
        prompt_tokens=_estimate_token_count(prompt_text),
        completion_tokens=_estimate_token_count(completion_text),
        source="estimate",
    )


def _estimate_token_count(text: str) -> int:
    normalized = str(text or "").strip()
    if not normalized:
        return 0
    try:
        import tiktoken

        encoding = tiktoken.get_encoding("cl100k_base")
        return len(encoding.encode(normalized))
    except Exception:
        return max(len(normalized) // 4, 1)


def _pricing_for_model(model_name: str) -> tuple[float, float]:
    normalized = str(model_name or "").strip().lower()
    pricing_table = {
        "gpt-4o-mini": (0.15, 0.60),
        "gpt-4.1-mini": (0.40, 1.60),
        "gpt-4o": (5.00, 15.00),
        "gpt-4.1": (2.00, 8.00),
    }
    for prefix, pricing in pricing_table.items():
        if normalized.startswith(prefix):
            return pricing
    return 0.0, 0.0
