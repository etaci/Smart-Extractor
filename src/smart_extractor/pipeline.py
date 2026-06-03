from __future__ import annotations

import inspect
import re
import time
from pathlib import Path
from typing import Callable, Optional, Type
from urllib.parse import urlsplit

from loguru import logger

from smart_extractor.cleaner.html_cleaner import HTMLCleaner
from smart_extractor.cleaner.structured_hints import build_structured_hints
from smart_extractor.config import AppConfig, load_config
from smart_extractor.extractor.learned_profile_store import LearnedProfileStore
from smart_extractor.extractor.llm_extractor import LLMExtractor
from smart_extractor.extractor.rule_extractor import RuleBasedDynamicExtractor
from smart_extractor.fetcher.base import BaseFetcher, FetchResult
from smart_extractor.fetcher.playwright import PlaywrightFetcher
from smart_extractor.fetcher.playwright_async import AsyncPlaywrightFetcher
from smart_extractor.fetcher.static import StaticFetcher
from smart_extractor.models.base import BaseExtractModel, DynamicExtractResult, ExtractionMeta
from smart_extractor.models.custom import SchemaRegistry
from smart_extractor.storage.base import BaseStorage
from smart_extractor.storage.csv_storage import CSVStorage
from smart_extractor.storage.json_storage import JSONStorage
from smart_extractor.storage.sqlite_storage import SQLiteStorage
from smart_extractor.utils.anti_detect import URLDeduplicator, looks_like_challenge_text, looks_like_loading_text
from smart_extractor.validator.data_validator import DataValidator, ValidationResult


class PipelineResult:
    def __init__(self):
        self.url: str = ""
        self.success: bool = False
        self.data: Optional[BaseExtractModel] = None
        self.meta: Optional[ExtractionMeta] = None
        self.validation: Optional[ValidationResult] = None
        self.fetch_result: Optional[FetchResult] = None
        self.cleaned_text: str = ""
        self.storage_path: str = ""
        self.error: Optional[str] = None
        self.elapsed_ms: float = 0.0
        self.extractor_stats: dict[str, float] = {}
        self.validation_status: str = "failed"

    @property
    def summary(self) -> str:
        status = "PASS" if self.success else "FAIL"
        parts = [f"[{status}] {self.url}"]
        if self.elapsed_ms:
            parts.append(f"elapsed={self.elapsed_ms:.0f}ms")
        if self.validation:
            parts.append(f"quality={self.validation.quality_score:.1%}")
        if self.error:
            parts.append(f"error={self.error}")
        return " | ".join(parts)


class ExtractionPipeline:
    def __init__(
        self,
        config: Optional[AppConfig] = None,
        fetcher: Optional[BaseFetcher] = None,
        use_dynamic_fetcher: bool = True,
        fetcher_factory: Optional[Callable[[], object]] = None,
        async_fetcher_factory: Optional[Callable[[], object]] = None,
    ):
        self._config = config or load_config()
        self._use_dynamic_fetcher = bool(use_dynamic_fetcher)
        self._fetcher_factory = fetcher_factory or self._infer_fetcher_factory(fetcher, use_dynamic_fetcher)
        self._async_fetcher_factory = async_fetcher_factory or self._infer_async_fetcher_factory(fetcher, use_dynamic_fetcher)
        if fetcher is not None:
            self._fetcher = fetcher
        elif use_dynamic_fetcher:
            self._fetcher = PlaywrightFetcher(self._config.fetcher)
        else:
            self._fetcher = StaticFetcher(self._config.fetcher)
        self._cleaner = HTMLCleaner(self._config.cleaner)
        self._extractor = LLMExtractor(self._config.llm)
        self._rule_extractor = RuleBasedDynamicExtractor()
        self._validator = DataValidator()
        self._schema_registry = SchemaRegistry()
        self._schema_registry.load_from_directory()
        dedup_cache = Path(self._config.storage.output_dir) / ".url_dedup_cache"
        self._deduplicator = URLDeduplicator(cache_file=dedup_cache)
        self._learned_profile_store = LearnedProfileStore(Path(self._config.storage.output_dir) / "learned_profiles.json")
        self._storages: dict[str, BaseStorage] = {
            "json": JSONStorage(self._config.storage),
            "csv": CSVStorage(self._config.storage),
            "sqlite": SQLiteStorage(self._config.storage),
        }
        self._hooks: dict[str, list[Callable]] = {
            "before_fetch": [], "after_fetch": [], "after_clean": [],
            "after_extract": [], "after_validate": [], "after_store": [],
        }

    def _infer_fetcher_factory(self, fetcher: Optional[object], use_dynamic_fetcher: bool) -> Optional[Callable[[], object]]:
        if fetcher is None:
            return (lambda: PlaywrightFetcher(self._config.fetcher)) if use_dynamic_fetcher else (lambda: StaticFetcher(self._config.fetcher))
        if isinstance(fetcher, PlaywrightFetcher):
            return lambda: PlaywrightFetcher(self._config.fetcher)
        if isinstance(fetcher, StaticFetcher):
            return lambda: StaticFetcher(self._config.fetcher)
        return None

    def _infer_async_fetcher_factory(self, fetcher: Optional[object], use_dynamic_fetcher: bool) -> Optional[Callable[[], object]]:
        if fetcher is None and use_dynamic_fetcher:
            return lambda: AsyncPlaywrightFetcher(self._config.fetcher)
        if isinstance(fetcher, PlaywrightFetcher):
            return lambda: AsyncPlaywrightFetcher(self._config.fetcher)
        return None

    def _clone_schema_registry(self) -> SchemaRegistry:
        registry = SchemaRegistry()
        registry._schemas = dict(self._schema_registry._schemas)
        return registry

    def _build_worker_pipeline(self, *, async_mode: bool) -> "ExtractionPipeline":
        if async_mode and self._async_fetcher_factory is not None:
            worker_fetcher = self._async_fetcher_factory()
        elif self._fetcher_factory is not None:
            worker_fetcher = self._fetcher_factory()
        else:
            worker_fetcher = self._fetcher
        worker = ExtractionPipeline(
            config=self._config,
            fetcher=worker_fetcher,  # type: ignore[arg-type]
            use_dynamic_fetcher=self._use_dynamic_fetcher,
            fetcher_factory=self._fetcher_factory,
            async_fetcher_factory=self._async_fetcher_factory,
        )
        worker._schema_registry = self._clone_schema_registry()
        worker._hooks = {event: list(callbacks) for event, callbacks in self._hooks.items()}
        worker._extractor = LLMExtractor(self._config.llm) if type(self._extractor) is LLMExtractor else self._extractor
        worker._rule_extractor = RuleBasedDynamicExtractor() if type(self._rule_extractor) is RuleBasedDynamicExtractor else self._rule_extractor
        worker._validator = DataValidator() if type(self._validator) is DataValidator else self._validator
        return worker

    def add_hook(self, event: str, callback: Callable) -> None:
        if event in self._hooks:
            self._hooks[event].append(callback)
        else:
            logger.warning("未知 hook 事件: {}", event)

    def _fire_hooks(self, event: str, **kwargs) -> None:
        for callback in self._hooks.get(event, []):
            try:
                callback(**kwargs)
            except Exception as exc:
                logger.warning("hook '{}' 执行失败: {}", event, exc)

    def _resolve_schema(self, schema_name: str, schema: Optional[Type[BaseExtractModel]]) -> tuple[Optional[Type[BaseExtractModel]], bool, Optional[str]]:
        target_schema = schema
        use_dynamic_mode = target_schema is None and str(schema_name or "auto").lower() == "auto"
        if not use_dynamic_mode and target_schema is None:
            target_schema = self._schema_registry.get(schema_name)
            if target_schema is None:
                return None, False, f"未找到 Schema: {schema_name}"
        return target_schema, use_dynamic_mode, None

    def _finalize_result(self, result: PipelineResult, *, fetch_result: FetchResult, url: str, use_dynamic_mode: bool, target_schema: Optional[Type[BaseExtractModel]], storage_format: Optional[str], collection_name: str, css_selector: Optional[str], prompt_template: Optional[str], skip_storage: bool, selected_fields: Optional[list[str]], force_strategy: str) -> PipelineResult:
        if not fetch_result.is_success:
            result.error = self.normalize_user_facing_error(f"网页抓取失败: {fetch_result.error or fetch_result.status_code}")
            return result
        if fetch_result.is_shell_page:
            result.error = self.normalize_user_facing_error(self._shell_page_error_message())
            return result
        self._deduplicator.mark_visited(url)
        self._fire_hooks("after_fetch", url=url, fetch_result=fetch_result)
        base_cleaned_text = self._cleaner.clean(fetch_result.html, selector=css_selector)
        structured_hints = build_structured_hints(
            fetch_result.html,
            selected_fields=selected_fields,
        )
        self._annotate_page_type_consistency(
            fetch_result,
            requested_url=url,
            structured_hints=structured_hints,
            selected_fields=selected_fields or [],
        )
        if self._looks_like_verification_page(base_cleaned_text):
            result.error = self.normalize_user_facing_error("目标站点返回安全验证页面，当前无法提取真实内容")
            return result
        if self._looks_like_loading_page(base_cleaned_text) and not structured_hints:
            result.error = self.normalize_user_facing_error("页面仍停留在加载状态，当前无法提取真实内容")
            return result
        if (
            not base_cleaned_text.strip()
            and structured_hints
            and bool(getattr(self._config.fetcher, "empty_clean_rescue_enabled", True))
        ):
            base_cleaned_text = structured_hints
        if not base_cleaned_text.strip():
            result.error = self.normalize_user_facing_error("网页清洗后为空")
            return result
        cleaned_text = self._attach_structured_hints(
            fetch_result.html,
            base_cleaned_text,
            selected_fields=selected_fields,
            structured_hints=structured_hints,
        )
        result.cleaned_text = cleaned_text
        self._fire_hooks("after_clean", cleaned_text=cleaned_text)
        if use_dynamic_mode:
            extracted_data = self._run_dynamic_extraction(cleaned_text, source_url=url, selected_fields=selected_fields or [], force_strategy=force_strategy)
            extracted_data = self._maybe_complete_missing_fields(
                extracted_data,
                cleaned_text=cleaned_text,
                source_url=url,
                selected_fields=selected_fields or [],
            )
            self._attach_field_evidence(
                extracted_data,
                structured_hints=structured_hints,
                selected_fields=selected_fields or [],
            )
            self._attach_fetch_diagnostics_to_result(extracted_data, fetch_result)
        else:
            if target_schema is None:
                result.error = self.normalize_user_facing_error("Schema 未正确解析")
                return result
            extracted_data = self._extractor.extract(cleaned_text, target_schema, prompt_template=prompt_template)
        result.data = extracted_data
        result.extractor_stats = self.get_extractor_stats()
        meta = ExtractionMeta(source_url=url, extractor_model=self._config.llm.model, raw_text_length=len(cleaned_text))
        result.meta = meta
        self._fire_hooks("after_extract", data=extracted_data, meta=meta)
        validation = self._validator.validate(extracted_data)
        self._apply_fetch_validation_warnings(validation, fetch_result)
        result.validation = validation
        result.validation_status = getattr(validation, "status", "failed") or "failed"
        meta.confidence_score = validation.quality_score
        self._fire_hooks("after_validate", validation=validation)
        if not skip_storage:
            fmt = storage_format or self._config.storage.default_format
            storage = self._storages.get(fmt)
            if storage:
                result.storage_path = storage.save(extracted_data, meta=meta, collection_name=collection_name)
            else:
                logger.warning("未知存储格式: {}", fmt)
        self._fire_hooks("after_store", result=result)
        if not validation.is_valid:
            result.error = self.normalize_user_facing_error("提取结果未通过质量校验")
            return result
        if result.validation_status == "partial_success":
            result.error = self.normalize_user_facing_error("提取结果为部分成功，部分字段缺失或格式需要人工确认")
        result.success = True
        return result

    def _maybe_complete_missing_fields(
        self,
        extracted_data: DynamicExtractResult,
        *,
        cleaned_text: str,
        source_url: str,
        selected_fields: list[str],
    ) -> DynamicExtractResult:
        fields = selected_fields or list(getattr(extracted_data, "selected_fields", []) or [])
        if not fields:
            return extracted_data
        missing = [
            field
            for field in fields
            if (getattr(extracted_data, "data", {}) or {}).get(field) in (None, "", [], {})
        ]
        if not missing:
            return extracted_data
        if len(cleaned_text.strip()) < 80:
            return extracted_data
        strategy = str(getattr(extracted_data, "extraction_strategy", "") or "").lower()
        if strategy == "llm" and extracted_data.completeness_score() >= 0.75:
            return extracted_data
        try:
            completion = self._extractor.extract_dynamic(
                cleaned_text,
                source_url=source_url,
                selected_fields=missing,
            )
        except Exception as exc:
            logger.debug("missing-field LLM completion failed: {}", exc)
            return extracted_data
        completion_data = getattr(completion, "data", {}) or {}
        patched = dict(getattr(extracted_data, "data", {}) or {})
        changed = False
        for field in missing:
            value = completion_data.get(field)
            if value not in (None, "", [], {}):
                patched[field] = value
                changed = True
        if not changed:
            return extracted_data
        labels = dict(getattr(extracted_data, "field_labels", {}) or {})
        for field in missing:
            labels.setdefault(field, field)
        details = getattr(extracted_data, "strategy_details", {}) or {}
        extracted_data.data = patched
        extracted_data.field_labels = labels
        extracted_data.strategy_details = {
            **(details if isinstance(details, dict) else {}),
            "llm_rescue_trigger": "missing_fields_completion",
            "llm_missing_fields": missing,
        }
        return extracted_data

    @staticmethod
    def _attach_structured_hints(
        html: str,
        cleaned_text: str,
        *,
        selected_fields: Optional[list[str]] = None,
        structured_hints: str = "",
    ) -> str:
        hints = structured_hints or build_structured_hints(html, selected_fields=selected_fields)
        if not hints:
            return cleaned_text
        if cleaned_text.startswith(hints):
            return cleaned_text
        return f"{hints}\n\n{cleaned_text}"

    @staticmethod
    def _attach_field_evidence(
        extracted_data: DynamicExtractResult,
        *,
        structured_hints: str,
        selected_fields: list[str],
    ) -> None:
        evidence = ExtractionPipeline._extract_field_evidence(
            structured_hints,
            selected_fields=selected_fields or list(extracted_data.selected_fields or []),
        )
        details = extracted_data.strategy_details if isinstance(extracted_data.strategy_details, dict) else {}
        existing = details.get("field_evidence") if isinstance(details.get("field_evidence"), dict) else {}
        merged = {**existing}
        for field_name, values in evidence.items():
            bucket = list(merged.get(field_name) or [])
            for value in values:
                if value not in bucket:
                    bucket.append(value)
            merged[field_name] = bucket[:5]
        if merged:
            extracted_data.strategy_details = {**details, "field_evidence": merged}

    @staticmethod
    def _extract_field_evidence(
        structured_hints: str,
        *,
        selected_fields: list[str],
    ) -> dict[str, list[str]]:
        fields = {str(field).strip().lower() for field in selected_fields if str(field).strip()}
        evidence: dict[str, list[str]] = {field: [] for field in fields}
        if not structured_hints:
            return evidence
        for raw_line in structured_hints.splitlines():
            line = raw_line.strip()
            if not line or ":" not in line:
                continue
            field_name, value = line.split(":", 1)
            normalized_field = field_name.strip().lower().removesuffix("_candidates")
            if fields and normalized_field not in fields:
                continue
            values = [item.strip()[:240] for item in value.split("|") if item.strip()]
            if values:
                evidence.setdefault(normalized_field, [])
                for item in values:
                    if item not in evidence[normalized_field]:
                        evidence[normalized_field].append(item)
        return evidence

    @staticmethod
    def _attach_fetch_diagnostics_to_result(
        extracted_data: DynamicExtractResult,
        fetch_result: FetchResult,
    ) -> None:
        diagnostics = fetch_result.diagnostics if isinstance(fetch_result.diagnostics, dict) else {}
        if not diagnostics:
            return
        keys = (
            "page_type_mismatch",
            "page_type_mismatch_reason",
            "original_url",
            "final_url",
            "repair_reason",
            "preflight_type_mismatch",
        )
        payload = {key: diagnostics.get(key) for key in keys if diagnostics.get(key) not in (None, "", [], {})}
        if not payload:
            return
        details = extracted_data.strategy_details if isinstance(extracted_data.strategy_details, dict) else {}
        extracted_data.strategy_details = {**details, "fetch_diagnostics": payload}

    @staticmethod
    def _apply_fetch_validation_warnings(
        validation: ValidationResult,
        fetch_result: FetchResult,
    ) -> None:
        diagnostics = fetch_result.diagnostics if isinstance(fetch_result.diagnostics, dict) else {}
        if not diagnostics.get("page_type_mismatch"):
            return
        reason = str(diagnostics.get("page_type_mismatch_reason") or "page_type_mismatch")
        validation.add_warning(f"fetch_page_type_mismatch: {reason}")
        validation.quality_score = max(0.0, validation.quality_score - 0.05)

    @staticmethod
    def _annotate_page_type_consistency(
        fetch_result: FetchResult,
        *,
        requested_url: str,
        structured_hints: str,
        selected_fields: list[str],
    ) -> None:
        diagnostics = fetch_result.diagnostics if isinstance(fetch_result.diagnostics, dict) else {}
        if not diagnostics:
            fetch_result.diagnostics = {}
            diagnostics = fetch_result.diagnostics
        original_url = str(diagnostics.get("original_url") or requested_url or fetch_result.url or "")
        final_url = str(
            diagnostics.get("final_url")
            or (
                fetch_result.headers.get("x-smart-final-url", "")
                if isinstance(fetch_result.headers, dict)
                else ""
            )
        )
        final_url = final_url or str(fetch_result.url or requested_url or "")
        expected_type = ExtractionPipeline._infer_expected_page_type(original_url, selected_fields)
        if not expected_type:
            return
        hints_lower = structured_hints.lower()
        evidence_fields = ExtractionPipeline._page_type_evidence_fields(expected_type)
        has_type_evidence = any(re.search(rf"^{re.escape(field)}:", hints_lower, re.M) for field in evidence_fields)
        final_parts = urlsplit(final_url)
        path = (final_parts.path or "/").strip().lower()
        suspicious_path = path in {"", "/"} or any(
            marker in path
            for marker in (
                "/login",
                "/signin",
                "/account",
                "/region",
                "/locale",
                "/country",
                "/collections",
                "/category",
                "/categories",
                "/search",
            )
        )
        repair_reason = ""
        if isinstance(fetch_result.headers, dict):
            repair_reason = str(fetch_result.headers.get("x-smart-url-preflight-repair-reason") or "")
        if not repair_reason:
            repair_reason = str(diagnostics.get("repair_reason") or "")
        preflight_type_mismatch = ""
        if isinstance(fetch_result.headers, dict):
            preflight_type_mismatch = str(
                fetch_result.headers.get("x-smart-preflight-type-mismatch") or ""
            )
        original_host = (urlsplit(original_url).hostname or "").removeprefix("www.")
        final_host = (final_parts.hostname or "").removeprefix("www.")
        host_changed = bool(original_host and final_host and original_host != final_host)
        if (suspicious_path or host_changed or repair_reason or preflight_type_mismatch) and not has_type_evidence:
            diagnostics.update(
                {
                    "page_type_mismatch": True,
                    "page_type_mismatch_reason": (
                        preflight_type_mismatch
                        or f"expected_{expected_type}_but_final_url_or_content_lacks_type_evidence"
                    ),
                    "expected_page_type": expected_type,
                    "repair_reason": repair_reason,
                    "preflight_type_mismatch": preflight_type_mismatch,
                    "original_url": original_url,
                    "final_url": final_url,
                }
            )

    @staticmethod
    def _infer_expected_page_type(url: str, selected_fields: list[str]) -> str:
        field_set = {str(field).strip().lower() for field in selected_fields if str(field).strip()}
        path = urlsplit(str(url or "")).path.lower()
        if field_set & {"price", "sku", "gtin", "availability", "brand", "stock"}:
            return "product"
        if field_set & {"plan", "billing_period", "seat_price", "monthly_price", "annual_price"}:
            return "pricing"
        if field_set & {"company", "location", "employment_type", "salary_range", "job_id"}:
            return "job"
        if field_set & {"agency", "policy_number", "document_date", "effective_date"}:
            return "policy"
        if any(marker in path for marker in ("/product", "/products", "/p/")):
            return "product"
        if any(marker in path for marker in ("/pricing", "/plans", "/price")):
            return "pricing"
        if any(marker in path for marker in ("/job", "/jobs", "/careers", "/positions")):
            return "job"
        if any(marker in path for marker in ("/policy", "/notice", "/announcement", "/press")):
            return "policy"
        return ""

    @staticmethod
    def _page_type_evidence_fields(page_type: str) -> tuple[str, ...]:
        mapping = {
            "product": ("name", "product", "price", "sku", "brand", "availability"),
            "pricing": ("plan", "price", "billing_period"),
            "job": ("title", "company", "location", "employment_type", "job_id"),
            "policy": ("title", "agency", "policy_number", "publish_date", "content"),
        }
        return mapping.get(page_type, ())

    async def _fetch_async(self, url: str) -> FetchResult:
        import asyncio

        fetch_method = getattr(self._fetcher, "fetch")
        if inspect.iscoroutinefunction(fetch_method):
            return await fetch_method(url)
        return await asyncio.to_thread(fetch_method, url)

    async def aclose(self) -> None:
        import asyncio

        fetcher_close = getattr(self._fetcher, "close", None)
        if callable(fetcher_close):
            if inspect.iscoroutinefunction(fetcher_close):
                await fetcher_close()
            else:
                await asyncio.to_thread(fetcher_close)
        for storage in self._storages.values():
            close = getattr(storage, "close", None)
            if callable(close):
                if inspect.iscoroutinefunction(close):
                    await close()
                else:
                    await asyncio.to_thread(close)

    def run(self, url: str, schema_name: str = "auto", schema: Optional[Type[BaseExtractModel]] = None, storage_format: Optional[str] = None, collection_name: str = "default", css_selector: Optional[str] = None, prompt_template: Optional[str] = None, skip_storage: bool = False, selected_fields: Optional[list[str]] = None, force_strategy: str = "") -> PipelineResult:
        result = PipelineResult()
        result.url = url
        start_time = time.time()
        if self._deduplicator.is_visited(url):
            logger.info("[去重] URL 已在缓存中，但本次仍继续执行: {}", url)
        target_schema, use_dynamic_mode, schema_error = self._resolve_schema(schema_name, schema)
        if schema_error:
            result.error = schema_error
            return result
        try:
            self._fire_hooks("before_fetch", url=url)
            fetch_result = self._fetcher.fetch(url)
            result.fetch_result = fetch_result
            return self._finalize_result(result, fetch_result=fetch_result, url=url, use_dynamic_mode=use_dynamic_mode, target_schema=target_schema, storage_format=storage_format, collection_name=collection_name, css_selector=css_selector, prompt_template=prompt_template, skip_storage=skip_storage, selected_fields=selected_fields, force_strategy=force_strategy)
        except Exception as exc:
            result.error = self.normalize_user_facing_error(f"{type(exc).__name__}: {exc}")
            logger.exception("Pipeline 执行异常")
            return result
        finally:
            result.elapsed_ms = (time.time() - start_time) * 1000
            logger.info(result.summary)

    async def run_async(self, url: str, schema_name: str = "auto", schema: Optional[Type[BaseExtractModel]] = None, storage_format: Optional[str] = None, collection_name: str = "default", css_selector: Optional[str] = None, prompt_template: Optional[str] = None, skip_storage: bool = False, selected_fields: Optional[list[str]] = None, force_strategy: str = "") -> PipelineResult:
        import asyncio

        result = PipelineResult()
        result.url = url
        start_time = time.time()
        if self._deduplicator.is_visited(url):
            logger.info("[去重] URL 已在缓存中，但本次仍继续执行: {}", url)
        target_schema, use_dynamic_mode, schema_error = self._resolve_schema(schema_name, schema)
        if schema_error:
            result.error = schema_error
            return result
        try:
            self._fire_hooks("before_fetch", url=url)
            fetch_result = await self._fetch_async(url)
            result.fetch_result = fetch_result
            return await asyncio.to_thread(self._finalize_result, result, fetch_result=fetch_result, url=url, use_dynamic_mode=use_dynamic_mode, target_schema=target_schema, storage_format=storage_format, collection_name=collection_name, css_selector=css_selector, prompt_template=prompt_template, skip_storage=skip_storage, selected_fields=selected_fields, force_strategy=force_strategy)
        except Exception as exc:
            result.error = self.normalize_user_facing_error(f"{type(exc).__name__}: {exc}")
            logger.exception("Pipeline async 执行异常")
            return result
        finally:
            result.elapsed_ms = (time.time() - start_time) * 1000
            logger.info(result.summary)

    def _run_dynamic_extraction(self, cleaned_text: str, *, source_url: str, selected_fields: list[str], force_strategy: str = "") -> DynamicExtractResult:
        normalized_force_strategy = str(force_strategy or "").strip().lower()
        matched_profile = None
        llm_rescue_trigger = ""
        if normalized_force_strategy != "llm":
            matched_profile = self._learned_profile_store.find_best_match(source_url, selected_fields)
        if matched_profile is not None:
            rule_result = self._rule_extractor.extract(cleaned_text, source_url=source_url, profile=matched_profile, selected_fields=selected_fields or matched_profile.selected_fields)
            rule_score = rule_result.completeness_score()
            if self._is_usable_rule_result(rule_result):
                self._learned_profile_store.record_rule_attempt(matched_profile.profile_id, success=True, completeness=rule_score, source_url=source_url)
                return rule_result
            self._learned_profile_store.record_rule_attempt(matched_profile.profile_id, success=False, completeness=rule_score, source_url=source_url)
            llm_rescue_trigger = "learned_rule_low_completeness"
        extracted_data = self._extractor.extract_dynamic(cleaned_text, source_url=source_url, selected_fields=selected_fields)
        page_type = str(getattr(extracted_data, "page_type", "unknown") or "unknown")
        extracted_fields = list(getattr(extracted_data, "selected_fields", []) or [])
        field_labels = getattr(extracted_data, "field_labels", {}) or {}
        strategy = str(getattr(extracted_data, "extraction_strategy", "llm") or "llm")
        strategy_details = getattr(extracted_data, "strategy_details", {}) or {}
        if llm_rescue_trigger:
            extracted_data.strategy_details = {
                **(strategy_details if isinstance(strategy_details, dict) else {}),
                "llm_rescue_trigger": llm_rescue_trigger,
            }
            strategy_details = extracted_data.strategy_details
        if self._should_persist_learned_profile(extracted_data):
            learned_profile = self._learned_profile_store.upsert_from_result(source_url, page_type=page_type, selected_fields=extracted_fields, field_labels=field_labels if isinstance(field_labels, dict) else {}, strategy=strategy, completeness=extracted_data.completeness_score())
            extracted_data.learned_profile_id = learned_profile.profile_id
            extracted_data.strategy_details = {**(strategy_details if isinstance(strategy_details, dict) else {}), "profile_id": learned_profile.profile_id, "path_prefix": learned_profile.path_prefix, "domain": learned_profile.domain}
        return extracted_data

    @staticmethod
    def _is_usable_rule_result(result: DynamicExtractResult) -> bool:
        fields = result.selected_fields or result.candidate_fields
        if not fields:
            return False
        filled_count = sum(1 for field in fields if result.data.get(field) not in (None, "", [], {}))
        completeness = result.completeness_score()
        if len(fields) <= 2:
            return filled_count >= 1 and completeness >= 0.5
        return completeness >= 0.55 or filled_count >= 2

    @staticmethod
    def _should_persist_learned_profile(result: DynamicExtractResult) -> bool:
        strategy = str(getattr(result, "extraction_strategy", "") or "").strip().lower()
        if not strategy or strategy == "fallback":
            return False
        fields = result.selected_fields or result.candidate_fields
        if not fields:
            return False
        filled_count = sum(1 for field in fields if result.data.get(field) not in (None, "", [], {}))
        return filled_count >= 2 and result.completeness_score() >= 0.6

    def analyze_page(self, url: str, css_selector: Optional[str] = None) -> dict:
        fetch_result = self._fetcher.fetch(url)
        if not fetch_result.is_success:
            raise RuntimeError(self.normalize_user_facing_error(f"网页抓取失败: {fetch_result.error or fetch_result.status_code}"))
        if fetch_result.is_shell_page:
            raise RuntimeError(self.normalize_user_facing_error(self._shell_page_error_message()))
        cleaned_text = self._cleaner.clean(
            fetch_result.html,
            selector=css_selector,
        )
        if self._looks_like_verification_page(cleaned_text):
            raise RuntimeError(self.normalize_user_facing_error("目标站点返回安全验证页面，当前无法分析真实页面"))
        if self._looks_like_loading_page(cleaned_text):
            raise RuntimeError(self.normalize_user_facing_error("页面仍停留在加载状态，当前无法分析真实页面"))
        if not cleaned_text.strip():
            raise RuntimeError(self.normalize_user_facing_error("网页清洗后为空"))
        analysis = self._extractor.analyze_page(cleaned_text, source_url=url)
        matched_profile = self._learned_profile_store.find_best_match(url)
        analysis["learned_profile"] = None if matched_profile is None else {
            "profile_id": matched_profile.profile_id,
            "page_type": matched_profile.page_type,
            "selected_fields": matched_profile.selected_fields,
            "path_prefix": matched_profile.path_prefix,
            "last_strategy": matched_profile.last_strategy,
        }
        return analysis

    def analyze_with_context(self, url: str, user_context: dict, css_selector: Optional[str] = None) -> dict:
        fetch_result = self._fetcher.fetch(url)
        if not fetch_result.is_success:
            raise RuntimeError(self.normalize_user_facing_error(f"网页抓取失败: {fetch_result.error or fetch_result.status_code}"))
        if fetch_result.is_shell_page:
            raise RuntimeError(self.normalize_user_facing_error(self._shell_page_error_message()))
        cleaned_text = self._cleaner.clean(fetch_result.html, selector=css_selector)
        if self._looks_like_verification_page(cleaned_text):
            raise RuntimeError(self.normalize_user_facing_error("目标站点返回安全验证页面，当前无法分析真实页面"))
        if self._looks_like_loading_page(cleaned_text):
            raise RuntimeError(self.normalize_user_facing_error("页面仍停留在加载状态，当前无法分析真实页面"))
        if not cleaned_text.strip():
            raise RuntimeError(self.normalize_user_facing_error("网页清洗后为空"))
        return self._extractor.analyze_with_context(text=cleaned_text, source_url=url, user_context=user_context)

    def analyze_many_with_context(self, urls: list[str], user_context: dict, css_selector: Optional[str] = None) -> dict:
        pages: list[dict[str, str]] = []
        for url in urls:
            fetch_result = self._fetcher.fetch(url)
            if not fetch_result.is_success:
                raise RuntimeError(self.normalize_user_facing_error(f"网页抓取失败: {fetch_result.error or fetch_result.status_code}"))
            if fetch_result.is_shell_page:
                raise RuntimeError(self.normalize_user_facing_error(self._shell_page_error_message()))
            cleaned_text = self._cleaner.clean(fetch_result.html, selector=css_selector)
            if self._looks_like_verification_page(cleaned_text):
                raise RuntimeError(self.normalize_user_facing_error("目标站点返回安全验证页面，当前无法分析真实页面"))
            if self._looks_like_loading_page(cleaned_text):
                raise RuntimeError(self.normalize_user_facing_error("页面仍停留在加载状态，当前无法分析真实页面"))
            if not cleaned_text.strip():
                raise RuntimeError(self.normalize_user_facing_error("网页清洗后为空"))
            pages.append({"url": url, "text": cleaned_text})
        return self._extractor.analyze_many_with_context(pages=pages, user_context=user_context)

    def run_batch(self, urls: list[str], schema_name: str = "auto", storage_format: Optional[str] = None, collection_name: str = "default", max_workers: Optional[int] = None, skip_storage: bool = False, selected_fields: Optional[list[str]] = None) -> list[PipelineResult]:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading

        if not urls:
            return []
        workers = max_workers or self._config.scheduler.max_concurrency
        results: list[Optional[PipelineResult]] = [None] * len(urls)
        worker_local = threading.local()
        worker_pipelines: list[ExtractionPipeline] = []
        worker_lock = threading.Lock()

        def _get_worker_pipeline() -> "ExtractionPipeline":
            pipeline = getattr(worker_local, "pipeline", None)
            if pipeline is None:
                pipeline = self._build_worker_pipeline(async_mode=False)
                worker_local.pipeline = pipeline
                with worker_lock:
                    worker_pipelines.append(pipeline)
            return pipeline

        def _run_one(index: int, item_url: str) -> tuple[int, PipelineResult]:
            pipeline = _get_worker_pipeline()
            return index, pipeline.run(url=item_url, schema_name=schema_name, storage_format=storage_format, collection_name=collection_name, skip_storage=skip_storage, selected_fields=selected_fields)

        try:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_run_one, i, item_url): i for i, item_url in enumerate(urls)}
                for future in as_completed(futures):
                    index = futures[future]
                    try:
                        result_index, pipeline_result = future.result()
                        results[result_index] = pipeline_result
                    except Exception as exc:
                        err_result = PipelineResult()
                        err_result.url = urls[index]
                        err_result.error = str(exc)
                        results[index] = err_result
        finally:
            for pipeline in worker_pipelines:
                pipeline.close()
        return [item for item in results if item is not None]

    async def run_batch_async(self, urls: list[str], schema_name: str = "auto", storage_format: Optional[str] = None, collection_name: str = "default", max_concurrency: Optional[int] = None, skip_storage: bool = False, selected_fields: Optional[list[str]] = None) -> list[PipelineResult]:
        import asyncio

        if not urls:
            return []
        worker_count = min(len(urls), max(1, int(max_concurrency or self._config.scheduler.max_concurrency)))
        ordered: list[Optional[PipelineResult]] = [None] * len(urls)
        queue: asyncio.Queue[tuple[int, str]] = asyncio.Queue()
        for index, item_url in enumerate(urls):
            queue.put_nowait((index, item_url))

        async def _worker() -> None:
            pipeline = self._build_worker_pipeline(async_mode=True)
            try:
                while True:
                    try:
                        index, item_url = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    try:
                        ordered[index] = await pipeline.run_async(url=item_url, schema_name=schema_name, storage_format=storage_format, collection_name=collection_name, skip_storage=skip_storage, selected_fields=selected_fields)
                    except Exception as exc:
                        err_result = PipelineResult()
                        err_result.url = item_url
                        err_result.error = str(exc)
                        ordered[index] = err_result
                    finally:
                        queue.task_done()
            finally:
                await pipeline.aclose()

        tasks = [asyncio.create_task(_worker()) for _ in range(worker_count)]
        await queue.join()
        await asyncio.gather(*tasks, return_exceptions=False)
        return [item for item in ordered if item is not None]

    def get_schema_registry(self) -> SchemaRegistry:
        return self._schema_registry

    @staticmethod
    def _shell_page_error_message() -> str:
        return "页面疑似被站点风控，或只返回前端壳页/搜索壳页，当前未拿到可抽取的真实内容"

    @staticmethod
    def normalize_user_facing_error(error: str) -> str:
        message = str(error or "").strip()
        normalized = message.lower()
        if 'cannot read "image.png"' in normalized or "does not support image input" in normalized:
            return "当前模型不支持图片输入，无法读取 `image.png`。请改用文本内容、网页 URL，或切换到支持图片输入的模型。"
        return message

    @staticmethod
    def _looks_like_verification_page(text: str) -> bool:
        return looks_like_challenge_text(text)

    @staticmethod
    def _looks_like_loading_page(text: str) -> bool:
        return looks_like_loading_text(text, max_length=32)

    def get_extractor_stats(self) -> dict:
        return self._extractor.get_stats()

    def close(self) -> None:
        fetcher_close = getattr(self._fetcher, "close", None)
        if callable(fetcher_close):
            fetcher_close()
        for storage in self._storages.values():
            close = getattr(storage, "close", None)
            if callable(close):
                close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
