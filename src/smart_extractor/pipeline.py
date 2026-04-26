from __future__ import annotations

import inspect
import time
from pathlib import Path
from typing import Callable, Optional, Type

from loguru import logger

from smart_extractor.cleaner.html_cleaner import HTMLCleaner
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
        cleaned_text = self._cleaner.clean(fetch_result.html, selector=css_selector)
        result.cleaned_text = cleaned_text
        if self._looks_like_verification_page(cleaned_text):
            result.error = self.normalize_user_facing_error("目标站点返回安全验证页面，当前无法提取真实内容")
            return result
        if self._looks_like_loading_page(cleaned_text):
            result.error = self.normalize_user_facing_error("页面仍停留在加载状态，当前无法提取真实内容")
            return result
        if not cleaned_text.strip():
            result.error = self.normalize_user_facing_error("网页清洗后为空")
            return result
        self._fire_hooks("after_clean", cleaned_text=cleaned_text)
        if use_dynamic_mode:
            extracted_data = self._run_dynamic_extraction(cleaned_text, source_url=url, selected_fields=selected_fields or [], force_strategy=force_strategy)
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
        result.validation = validation
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
        result.success = True
        return result

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
        if normalized_force_strategy != "llm":
            matched_profile = self._learned_profile_store.find_best_match(source_url, selected_fields)
        if matched_profile is not None:
            rule_result = self._rule_extractor.extract(cleaned_text, source_url=source_url, profile=matched_profile, selected_fields=selected_fields or matched_profile.selected_fields)
            rule_score = rule_result.completeness_score()
            if self._is_usable_rule_result(rule_result):
                self._learned_profile_store.record_rule_attempt(matched_profile.profile_id, success=True, completeness=rule_score, source_url=source_url)
                return rule_result
            self._learned_profile_store.record_rule_attempt(matched_profile.profile_id, success=False, completeness=rule_score, source_url=source_url)
        extracted_data = self._extractor.extract_dynamic(cleaned_text, source_url=source_url, selected_fields=selected_fields)
        page_type = str(getattr(extracted_data, "page_type", "unknown") or "unknown")
        extracted_fields = list(getattr(extracted_data, "selected_fields", []) or [])
        field_labels = getattr(extracted_data, "field_labels", {}) or {}
        strategy = str(getattr(extracted_data, "extraction_strategy", "llm") or "llm")
        strategy_details = getattr(extracted_data, "strategy_details", {}) or {}
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
        cleaned_text = self._cleaner.clean(fetch_result.html, selector=css_selector)
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
