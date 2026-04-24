"""LLM 智能抽取器。"""

from __future__ import annotations

import json
from typing import Any, Optional, Type

from loguru import logger

from smart_extractor.config import LLMConfig
from smart_extractor.extractor.base import BaseExtractor
from smart_extractor.extractor.llm_analysis import (
    build_compare_prompt_payloads,
    build_context_prompt_payload,
    build_page_analysis_summary,
    normalize_compare_analysis,
    normalize_context_analysis,
)
from smart_extractor.extractor.llm_client import LLMClient
from smart_extractor.extractor.llm_fallbacks import (
    build_compare_fallback,
    build_context_fallback,
    build_dynamic_fallback_result,
    build_task_plan_fallback,
    normalize_compare_report,
    resolve_fallback_profile,
)
from smart_extractor.extractor.llm_prompts import (
    AUTO_ANALYZE_PROMPT_TEMPLATE,
    COMPARE_ANALYZE_PROMPT_TEMPLATE,
    COMPARE_SYSTEM_PROMPT,
    DEFAULT_PROMPT_TEMPLATE,
    DYNAMIC_SYSTEM_PROMPT,
    INSIGHT_ANALYZE_PROMPT_TEMPLATE,
    INSIGHT_SYSTEM_PROMPT,
    TASK_PLAN_PROMPT_TEMPLATE,
    TASK_PLAN_SYSTEM_PROMPT,
)
from smart_extractor.extractor.llm_response import (
    _extract_chat_message_content,
    _format_dynamic_text,
    _normalize_field_list,
    _normalize_url_list,
    _safe_json_loads,
)
from smart_extractor.extractor.llm_task_plan import normalize_task_plan_payload
from smart_extractor.models.base import BaseExtractModel, DynamicExtractResult
from smart_extractor.utils.display import get_field_label

_TIKTOKEN_ENCODING = None
_TIKTOKEN_LOADED = False


def _get_tiktoken_encoding():
    global _TIKTOKEN_ENCODING, _TIKTOKEN_LOADED
    if not _TIKTOKEN_LOADED:
        _TIKTOKEN_LOADED = True
        try:
            import tiktoken

            _TIKTOKEN_ENCODING = tiktoken.get_encoding("cl100k_base")
        except ImportError:
            pass
    return _TIKTOKEN_ENCODING


class LLMExtractor(BaseExtractor):
    """基于 OpenAI 兼容接口的抽取器。"""

    def __init__(self, config: Optional[LLMConfig] = None):
        self._config = config or LLMConfig()

        self._client = LLMClient(self._config)

    def extract(
        self,
        text: str,
        schema: Type[BaseExtractModel],
        prompt_template: str | None = None,
    ) -> BaseExtractModel:
        template = prompt_template or DEFAULT_PROMPT_TEMPLATE
        prompt = template.format(text=self._truncate_text(text))
        return self._call_structured_llm(prompt, schema)

    def extract_dynamic(
        self,
        text: str,
        source_url: str,
        selected_fields: list[str] | None = None,
    ) -> DynamicExtractResult:
        normalized_fields = [
            field.strip()
            for field in (selected_fields or [])
            if field and field.strip()
        ]
        fields_hint = (
            ", ".join(normalized_fields)
            if normalized_fields
            else "未指定，由你自动决定"
        )
        prompt = AUTO_ANALYZE_PROMPT_TEMPLATE.format(
            selected_fields_hint=fields_hint,
            source_url=source_url,
            text=self._truncate_text(text),
        )

        payload: dict[str, Any] | None = None
        try:
            payload = self._call_json_llm(
                system_prompt=DYNAMIC_SYSTEM_PROMPT,
                user_prompt=prompt,
            )
        except Exception as exc:
            logger.warning("LLM 调用失败或被拦截，启用文本直出兜底: {}", exc)

        need_fallback = (
            payload is None
            or not payload
            or (
                "usage" in payload
                and isinstance(payload.get("usage"), dict)
                and payload["usage"].get("completion_tokens") == 0
            )
            or (
                payload.get("object") in ("chat.completion.chunk", "chat.completion")
                and not payload.get("choices")
            )
        )
        if need_fallback:
            logger.info("模型输出无效或被拦截，启用规则抽取兜底")
            return self._build_fallback_result(
                text,
                source_url=source_url,
                selected_fields=normalized_fields,
            )

        is_flat_structure = not any(
            key in payload
            for key in ("page_type", "data", "candidate_fields", "selected_fields")
        )

        if is_flat_structure and payload:
            data = payload
            candidate_fields = list(data.keys())
            actual_fields = normalized_fields or candidate_fields
            page_type = "article"
            raw_labels: dict[str, Any] = {}
        else:
            page_type = str(payload.get("page_type") or "article").strip()
            if page_type == "unknown":
                page_type = "article"

            candidate_fields = _normalize_field_list(payload.get("candidate_fields"))
            actual_fields = (
                _normalize_field_list(payload.get("selected_fields"))
                or normalized_fields
                or candidate_fields
            )

            raw_data = payload.get("data")
            data = raw_data if isinstance(raw_data, dict) else {}

            if not actual_fields and data:
                actual_fields = list(data.keys())
                if not candidate_fields:
                    candidate_fields = list(data.keys())

            raw_labels = payload.get("field_labels", {})

        filtered_data = {
            field: data.get(field, "") for field in actual_fields if field in data
        }

        if not filtered_data and data:
            filtered_data = data
            actual_fields = list(data.keys())
            if not candidate_fields:
                candidate_fields = list(data.keys())

        if not any(v for v in filtered_data.values() if v not in (None, "", [], {})):
            logger.info("LLM 返回了结构但数据全空，启用规则抽取兜底")
            return self._build_fallback_result(
                text,
                source_url=source_url,
                selected_fields=actual_fields or normalized_fields,
            )

        normalized_labels = {
            field: get_field_label(field, raw_labels) for field in actual_fields
        }
        formatted_text = _format_dynamic_text(normalized_labels, filtered_data)

        return DynamicExtractResult(
            page_type=page_type,
            candidate_fields=candidate_fields,
            selected_fields=actual_fields,
            field_labels=normalized_labels,
            data=filtered_data,
            formatted_text=formatted_text,
            extraction_strategy="llm",
            strategy_details={"mode": "llm_dynamic", "source_url": source_url},
        )

    def extract_batch(
        self,
        texts: list[str],
        schema: Type[BaseExtractModel],
        *,
        max_workers: int | None = None,
    ) -> list[BaseExtractModel]:
        """并发批量抽取，保持与输入顺序一致。

        LLM API 调用为 I/O 密集型，串行 for 循环会造成 N×T 阻塞；此处使用
        ThreadPoolExecutor 派发，总耗时趋近于单次耗时（受 API 侧并发与本地
        线程池上限约束）。单条失败不影响其它项，失败位置会填 None 并记录日志。
        """
        if not texts:
            return []

        from concurrent.futures import ThreadPoolExecutor

        worker_count = max(1, int(max_workers) if max_workers else min(len(texts), 8))
        results: list[BaseExtractModel | None] = [None] * len(texts)

        def _run(index: int, payload: str):
            return index, self.extract(payload, schema)

        logger.info(
            "批量抽取启动: total={} workers={}", len(texts), worker_count
        )
        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = [
                pool.submit(_run, idx, text) for idx, text in enumerate(texts)
            ]
            completed = 0
            for future in futures:
                completed += 1
                try:
                    idx, value = future.result()
                    results[idx] = value
                    logger.info("批量抽取进度: {}/{}", completed, len(texts))
                except Exception as exc:
                    logger.error("批量抽取第 {} 项失败: {}", completed, exc)

        return [item for item in results if item is not None]

    def analyze_page(
        self,
        text: str,
        source_url: str,
    ) -> dict[str, Any]:
        return build_page_analysis_summary(
            self.extract_dynamic(
                text=text,
                source_url=source_url,
                selected_fields=None,
            )
        )

    def analyze_with_context(
        self,
        text: str,
        source_url: str,
        user_context: dict[str, Any],
    ) -> dict[str, Any]:
        page_result = self.extract_dynamic(
            text=text,
            source_url=source_url,
            selected_fields=None,
        )
        page_payload, result = build_context_prompt_payload(
            page_result,
            source_url=source_url,
        )
        payload = {"page": page_payload, "user_context": user_context}

        try:
            analysis = self._call_json_llm(
                system_prompt=INSIGHT_SYSTEM_PROMPT,
                user_prompt=INSIGHT_ANALYZE_PROMPT_TEMPLATE.format(
                    payload=json.dumps(payload, ensure_ascii=False)
                ),
            )
        except Exception as exc:
            logger.warning("上下文分析失败，启用本地兜底摘要: {}", exc)
            analysis = {}

        if not isinstance(analysis, dict) or not analysis:
            analysis = self._build_context_fallback(page_result, user_context)

        result["analysis"] = normalize_context_analysis(analysis)
        return result

    def analyze_many_with_context(
        self,
        pages: list[dict[str, Any]],
        user_context: dict[str, Any],
    ) -> dict[str, Any]:
        page_results: list[tuple[str, DynamicExtractResult]] = []
        for page in pages:
            text = str(page.get("text") or "")
            url = str(page.get("url") or "")
            page_results.append(
                (
                    url,
                    self.extract_dynamic(
                        text=text,
                        source_url=url,
                        selected_fields=None,
                    ),
                )
            )
        page_payloads, preview_items = build_compare_prompt_payloads(page_results)
        payload = {"pages": page_payloads, "user_context": user_context}
        try:
            analysis = self._call_json_llm(
                system_prompt=COMPARE_SYSTEM_PROMPT,
                user_prompt=COMPARE_ANALYZE_PROMPT_TEMPLATE.format(
                    payload=json.dumps(payload, ensure_ascii=False)
                ),
            )
        except Exception as exc:
            logger.warning("多页面比较分析失败，启用本地兜底: {}", exc)
            analysis = {}

        if not isinstance(analysis, dict) or not analysis:
            analysis = self._build_compare_fallback(preview_items, user_context)

        normalized_analysis = normalize_compare_analysis(analysis)
        return {
            "page_type": "comparison",
            "page_type_label": "多页对比",
            "page_preview": "已针对多个 URL 生成横向比较结果。",
            "items": preview_items,
            "comparison_matrix": normalized_analysis["comparison_matrix"],
            "analysis": normalized_analysis["analysis"],
            "report": self._normalize_compare_report(
                analysis.get("report"),
                preview_items=preview_items,
                user_context=user_context,
            ),
        }

    def parse_task_request(self, request_text: str) -> dict[str, Any]:
        prompt = TASK_PLAN_PROMPT_TEMPLATE.format(
            request_text=self._truncate_text(request_text)
        )

        try:
            payload = self._call_json_llm(
                system_prompt=TASK_PLAN_SYSTEM_PROMPT,
                user_prompt=prompt,
            )
        except Exception as exc:
            logger.warning("自然语言任务解析失败，启用本地兜底: {}", exc)
            payload = {}

        if not isinstance(payload, dict) or not payload:
            return self._build_task_plan_fallback(request_text)
        return normalize_task_plan_payload(payload)

    @classmethod
    def _build_fallback_result(
        cls,
        text: str,
        *,
        source_url: str = "",
        selected_fields: list[str] | None = None,
    ) -> DynamicExtractResult:
        return build_dynamic_fallback_result(
            text,
            source_url=source_url,
            selected_fields=selected_fields,
        )

    @staticmethod
    def _resolve_fallback_profile(
        text: str,
        selected_fields: list[str],
    ) -> tuple[str, list[str]]:
        return resolve_fallback_profile(text, selected_fields)

    @staticmethod
    def _build_context_fallback(
        page_result: DynamicExtractResult,
        user_context: dict[str, Any],
    ) -> dict[str, Any]:
        return build_context_fallback(page_result, user_context)

    @staticmethod
    def _build_compare_fallback(
        preview_items: list[dict[str, Any]],
        user_context: dict[str, Any],
    ) -> dict[str, Any]:
        return build_compare_fallback(preview_items, user_context)

    @staticmethod
    def _normalize_compare_report(
        report: Any,
        *,
        preview_items: list[dict[str, Any]],
        user_context: dict[str, Any],
    ) -> dict[str, Any]:
        return normalize_compare_report(
            report,
            preview_items=preview_items,
            user_context=user_context,
        )

    @staticmethod
    def _build_task_plan_fallback(request_text: str) -> dict[str, Any]:
        return build_task_plan_fallback(request_text)

    def _truncate_text(self, text: str) -> str:
        try:
            encoding = _get_tiktoken_encoding()
            if encoding:
                tokens = encoding.encode(text)
                max_input_tokens = 30000
                if len(tokens) > max_input_tokens:
                    logger.warning(
                        "文本过长 ({} tokens)，自动截断到 {} tokens",
                        len(tokens),
                        max_input_tokens,
                    )
                    return encoding.decode(tokens[:max_input_tokens])
        except Exception as exc:
            logger.debug("文本 token 估算失败: {}", exc)
        return text

    def _call_structured_llm(
        self,
        prompt: str,
        schema: Type[BaseExtractModel],
    ) -> BaseExtractModel:
        return self._client.call_structured(prompt, schema)

    def _call_json_llm(self, system_prompt: str, user_prompt: str) -> dict[str, Any]:
        return self._client.call_json(system_prompt, user_prompt)

    def get_stats(self) -> dict[str, float]:
        return self._client.get_stats()
