"""LLM 调用客户端。"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Type

import instructor
from loguru import logger
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, wait_random

from smart_extractor.config import LLMConfig
from smart_extractor.extractor.llm_response import (
    _extract_chat_message_content,
    _safe_json_loads,
)
from smart_extractor.models.base import BaseExtractModel


@dataclass
class UsageSample:
    """单次调用的 token 使用快照。source='api' 表示来自 OpenAI response.usage，
    'estimate' 表示兜底到 tiktoken 估算，这两个口径在统计面板需要区分展示。"""

    prompt_tokens: int
    completion_tokens: int
    source: str  # 'api' or 'estimate'


class LLMClient:
    """封装结构化和 JSON 两类 LLM 调用。"""

    def __init__(self, config: LLMConfig):
        self._config = config

        os.environ.setdefault("NO_PROXY", "*")

        self._openai_client = OpenAI(
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
            "LLM 客户端初始化完成 (model={}, base_url={})",
            self._config.model,
            self._config.base_url,
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30) + wait_random(0, 2),
        reraise=True,
    )
    def call_structured(
        self,
        prompt: str,
        schema: Type[BaseExtractModel],
    ) -> BaseExtractModel:
        start_time = time.time()
        try:
            result, raw_completion = (
                self._instructor_client.chat.completions.create_with_completion(
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
            usage = self._usage_from_response(
                raw_completion,
                fallback_prompt=prompt,
                fallback_completion=completion_preview,
            )
            self._record_call(time.time() - start_time, usage=usage)
            return result
        except Exception:
            # 失败路径无法拿到真实 usage，用 tiktoken 兜底估算 prompt，
            # 仍计入统计以便排查失败成本占比
            usage = self._estimate_usage(prompt_text=prompt, completion_text="")
            self._record_call(time.time() - start_time, usage=usage)
            raise

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=2, max=10) + wait_random(0, 2),
        reraise=True,
    )
    def call_json(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        start_time = time.time()
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        message, usage = self._stream_chat(messages, use_json_format=True)
        if message:
            usage = usage or self._estimate_usage(
                prompt_text=self._stringify_messages(messages),
                completion_text=message,
            )
            self._record_call(time.time() - start_time, usage=usage)
            return _safe_json_loads(message)

        logger.warning("json_object 流式模式返回为空，降级为纯流式模式重试")
        message, usage = self._stream_chat(messages, use_json_format=False)
        if message:
            usage = usage or self._estimate_usage(
                prompt_text=self._stringify_messages(messages),
                completion_text=message,
            )
            self._record_call(time.time() - start_time, usage=usage)
            return _safe_json_loads(message)

        logger.warning("流式模式均返回为空，尝试非流式调用")
        try:
            response = self._openai_client.chat.completions.create(
                model=self._config.model,
                messages=messages,
                temperature=self._config.temperature,
                max_tokens=self._config.max_tokens,
            )
            message = _extract_chat_message_content(response)
            usage = self._usage_from_response(
                response,
                fallback_prompt=self._stringify_messages(messages),
                fallback_completion=message,
            )
            self._record_call(time.time() - start_time, usage=usage)
            if message and message.strip():
                return _safe_json_loads(message)
        except Exception as exc:
            logger.warning("非流式调用也失败: {}", exc)

        usage = self._estimate_usage(
            prompt_text=self._stringify_messages(messages),
            completion_text="",
        )
        self._record_call(time.time() - start_time, usage=usage)
        logger.warning("LLM 三级调用均返回为空，返回空字典由上层兜底")
        return {}

    def _stream_chat(
        self,
        messages: list[dict[str, str]],
        use_json_format: bool = True,
    ) -> tuple[str, UsageSample | None]:
        """流式调用；同时通过 stream_options.include_usage 收集真实 usage。

        若上游代理未回传 usage，返回 (text, None)，由调用方兜底估算。"""
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

            stream = self._openai_client.chat.completions.create(**kwargs)

            collected: list[str] = []
            api_usage: UsageSample | None = None
            for chunk in stream:
                # usage chunk 通常在流结束时独立下发
                chunk_usage = self._usage_from_chunk(chunk)
                if chunk_usage is not None:
                    api_usage = chunk_usage

                choices = getattr(chunk, "choices", None)
                if not choices:
                    continue
                delta = getattr(choices[0], "delta", None)
                content = getattr(delta, "content", None)
                if content:
                    collected.append(content)

            result = "".join(collected).strip()
            if result:
                logger.debug(
                    "流式调用成功收集 {} 个片段，总长度 {} 字符",
                    len(collected),
                    len(result),
                )
            return result, api_usage
        except Exception as exc:
            logger.warning("流式调用异常: {}", exc)
            return "", None

    @staticmethod
    def _stringify_messages(messages: list[dict[str, str]]) -> str:
        return "\n".join(
            f"{item.get('role', 'user')}: {item.get('content', '')}"
            for item in messages
        )

    def _usage_from_response(
        self,
        response: Any,
        *,
        fallback_prompt: str,
        fallback_completion: str,
    ) -> UsageSample:
        usage_obj = getattr(response, "usage", None)
        if usage_obj is None and isinstance(response, dict):
            usage_obj = response.get("usage")
        sample = self._usage_from_object(usage_obj)
        if sample is not None:
            return sample
        return self._estimate_usage(
            prompt_text=fallback_prompt,
            completion_text=fallback_completion,
        )

    @staticmethod
    def _usage_from_chunk(chunk: Any) -> UsageSample | None:
        usage_obj = getattr(chunk, "usage", None)
        if usage_obj is None and isinstance(chunk, dict):
            usage_obj = chunk.get("usage")
        return LLMClient._usage_from_object(usage_obj)

    @staticmethod
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

    def _estimate_usage(
        self,
        *,
        prompt_text: str,
        completion_text: str,
    ) -> UsageSample:
        return UsageSample(
            prompt_tokens=self._estimate_token_count(prompt_text),
            completion_tokens=self._estimate_token_count(completion_text),
            source="estimate",
        )

    @staticmethod
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

    @staticmethod
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

    def _record_call(
        self,
        elapsed_seconds: float,
        *,
        usage: UsageSample,
    ) -> None:
        elapsed_ms = elapsed_seconds * 1000
        input_price_per_million, output_price_per_million = self._pricing_for_model(
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
        logger.info(
            "LLM 调用完成: elapsed_ms={:.0f}, total_calls={}, "
            "prompt_tokens={}, completion_tokens={}, "
            "usage_source={}, estimated_cost_usd={:.6f}",
            elapsed_ms,
            self._total_calls,
            usage.prompt_tokens,
            usage.completion_tokens,
            usage.source,
            estimated_cost,
        )

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
