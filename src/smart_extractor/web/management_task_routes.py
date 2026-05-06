"""任务详情、导出与任务到模板/监控的增长路由。"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response

from smart_extractor.web.api_models import (
    NaturalLanguageTaskRequest,
    PromoteTaskTemplateRequest,
)
from smart_extractor.web.management_helpers import (
    apply_monitor_notification_defaults,
    normalize_field_labels,
    normalize_profile_payload,
    normalize_selected_fields,
    serialize_monitor,
    serialize_template,
)
from smart_extractor.web.task_insights import normalize_task_data
from smart_extractor.web.template_market import get_market_template

_PACKAGE_IDS = {
    "product": "market-product-monitor",
    "job": "market-job-compare",
    "news": "market-policy-watch",
}

_PACKAGE_FIELD_HINTS = {
    "product": {
        "page_types": {"product", "sku", "commodity", "goods"},
        "fields": {
            "name",
            "title",
            "price",
            "brand",
            "availability",
            "stock",
            "promotion",
            "description",
        },
        "labels": {"商品", "价格", "库存", "品牌", "促销", "售价", "规格"},
    },
    "job": {
        "page_types": {"job", "jobs", "career", "recruitment", "position"},
        "fields": {
            "title",
            "job_title",
            "company",
            "salary",
            "salary_range",
            "location",
            "requirements",
            "department",
            "benefits",
        },
        "labels": {"岗位", "招聘", "薪资", "地点", "要求", "职责", "公司", "福利"},
    },
    "news": {
        "page_types": {"news", "article", "announcement", "notice", "policy"},
        "fields": {
            "title",
            "publish_date",
            "published_at",
            "content",
            "summary",
            "source",
            "article",
        },
        "labels": {"新闻", "公告", "政策", "标题", "发布日期", "发布时间", "正文", "摘要"},
    },
}


def register_task_routes(
    router: APIRouter,
    *,
    api_guard: Callable[[Request], None],
    request_logger: Callable[[Request, str], Any],
    get_request_id: Callable[[Request], str],
    task_store: Any,
    learned_profile_store: Any,
    build_task_docx: Callable[[dict[str, Any]], bytes],
    build_task_xlsx: Callable[[dict[str, Any]], bytes],
    build_task_markdown: Callable[[dict[str, Any]], str],
    load_config: Callable[..., Any],
) -> None:
    def _tenant_id(request: Request) -> str:
        identity = getattr(request.state, "identity", None)
        return str(getattr(identity, "tenant_id", "default") or "default")

    def _require(request: Request, permission: str) -> None:
        identity = getattr(request.state, "identity", None)
        require = getattr(identity, "require", None)
        if callable(require):
            require(permission)

    def _record_funnel_event(
        request: Request,
        *,
        stage: str,
        channel: str,
        package_type: str,
        package_id: str = "",
        package_name: str = "",
        task_id: str = "",
        template_id: str = "",
        monitor_id: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        task_store.create_funnel_event(
            stage=stage,
            channel=channel,
            package_type=package_type,
            package_id=package_id,
            package_name=package_name,
            task_id=task_id,
            template_id=template_id,
            monitor_id=monitor_id,
            metadata=metadata,
            tenant_id=str(
                getattr(getattr(request.state, "identity", None), "tenant_id", "default")
                or "default"
            ),
        )

    def _domain_label(url: str) -> str:
        domain = urlparse(str(url or "")).netloc.strip().lower()
        return domain or "未命名站点"

    def _task_selected_fields(task: Any) -> list[str]:
        task_data = task.data if isinstance(task.data, dict) else {}
        selected_fields = normalize_selected_fields(task_data.get("selected_fields", []))
        if selected_fields:
            return selected_fields
        return normalize_selected_fields(list(normalize_task_data(task_data).keys()))

    def _task_field_labels(task: Any, selected_fields: list[str]) -> dict[str, str]:
        task_data = task.data if isinstance(task.data, dict) else {}
        field_labels = normalize_field_labels(task_data.get("field_labels", {}))
        for field_name in selected_fields:
            field_labels.setdefault(field_name, field_name)
        return field_labels

    def _infer_use_static(task: Any, market_template: dict[str, Any]) -> bool:
        task_data = task.data if isinstance(task.data, dict) else {}
        runtime_metrics = (
            task_data.get("_runtime_metrics")
            if isinstance(task_data.get("_runtime_metrics"), dict)
            else {}
        )
        fetcher_type = str(runtime_metrics.get("fetcher_type") or "").strip().lower()
        if fetcher_type == "static":
            return True
        if fetcher_type in {"browser", "playwright", "dynamic"}:
            return False
        return bool(market_template.get("use_static", False))

    def _infer_growth_package(task: Any, detail: dict[str, Any] | None = None) -> dict[str, Any]:
        task_data = task.data if isinstance(task.data, dict) else {}
        detail = detail or {}
        page_type = str(task_data.get("page_type") or "").strip().lower()
        schema_name = str(task.schema_name or "").strip().lower()
        selected_fields = _task_selected_fields(task)
        field_labels = _task_field_labels(task, selected_fields)
        normalized_payload = normalize_task_data(task_data)
        combined_terms = {
            str(item).strip().lower()
            for item in (
                selected_fields
                + list(normalized_payload.keys())
                + list(field_labels.values())
                + [task.url, page_type, schema_name]
            )
            if str(item).strip()
        }

        best_scene = "news"
        best_score = -1
        for scene, hints in _PACKAGE_FIELD_HINTS.items():
            score = 0
            if page_type in hints["page_types"]:
                score += 5
            if schema_name in hints["page_types"]:
                score += 3
            score += sum(1 for field in selected_fields if field.strip().lower() in hints["fields"])
            score += sum(
                1
                for field in normalized_payload.keys()
                if str(field).strip().lower() in hints["fields"]
            )
            score += sum(
                1
                for label in field_labels.values()
                if any(keyword in str(label).strip().lower() for keyword in hints["labels"])
            )
            score += sum(
                1
                for term in combined_terms
                if any(keyword in term for keyword in hints["labels"])
            )
            if detail.get("comparison", {}).get("changed", False):
                score += 1
            if score > best_score:
                best_scene = scene
                best_score = score

        package_id = _PACKAGE_IDS[best_scene]
        market_template = get_market_template(package_id)
        if market_template is None:
            raise HTTPException(status_code=500, detail="核心模板包未正确加载")
        return market_template

    def _merge_profile(
        base_profile: dict[str, Any],
        overlay_profile: dict[str, Any],
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        normalized = normalize_profile_payload(deepcopy(base_profile))
        normalized.update(normalize_profile_payload(overlay_profile))
        normalized.update(metadata)
        return normalized

    def _build_template_payload_from_task(
        task: Any,
        detail: dict[str, Any],
        *,
        market_template: dict[str, Any],
        template_name: str = "",
        profile_overlay: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        selected_fields = _task_selected_fields(task) or list(
            market_template.get("selected_fields", [])
        )
        field_labels = _task_field_labels(task, selected_fields)
        domain_label = _domain_label(task.url)
        profile = _merge_profile(
            market_template.get("profile", {}),
            profile_overlay or {},
            {
                "source_task_id": task.task_id,
                "source_task_quality_score": float(task.quality_score or 0.0),
                "growth_stage": "task_to_template",
                "growth_entry_package_id": market_template["template_id"],
                "history_signal": {
                    "repeat_url": bool(
                        detail.get("history_summary", {}).get("repeat_url", False)
                    ),
                    "has_detected_changes": bool(
                        detail.get("comparison", {}).get("changed", False)
                    ),
                },
            },
        )
        return {
            "name": str(template_name or "").strip()
            or f"{domain_label} {market_template['name']}模板",
            "url": str(task.url or "").strip(),
            "page_type": str(task.data.get("page_type") or market_template.get("page_type") or "unknown").strip()
            if isinstance(task.data, dict)
            else str(market_template.get("page_type") or "unknown").strip(),
            "schema_name": str(
                (task.data or {}).get("schema_name") if isinstance(task.data, dict) else ""
            ).strip()
            or str(market_template.get("schema_name") or task.schema_name or "auto").strip(),
            "storage_format": str(task.storage_format or market_template.get("storage_format") or "json").strip(),
            "use_static": _infer_use_static(task, market_template),
            "selected_fields": selected_fields,
            "field_labels": field_labels,
            "profile": profile,
        }

    def _build_monitor_payload_from_task(
        task: Any,
        detail: dict[str, Any],
        *,
        market_template: dict[str, Any],
        template_payload: dict[str, Any],
        monitor_name: str = "",
        schedule_enabled: bool | None = None,
        schedule_interval_minutes: int | None = None,
    ) -> dict[str, Any]:
        recommended_schedule = (
            market_template.get("recommended_schedule")
            if isinstance(market_template.get("recommended_schedule"), dict)
            else {}
        )
        recommended_enabled = bool(recommended_schedule.get("enabled", True))
        recommended_interval = int(recommended_schedule.get("interval_minutes", 180) or 180)
        interval = (
            int(schedule_interval_minutes)
            if schedule_interval_minutes is not None
            else recommended_interval
        )
        enabled = (
            bool(schedule_enabled)
            if schedule_enabled is not None
            else recommended_enabled
        )
        domain_label = _domain_label(task.url)
        profile = apply_monitor_notification_defaults(
            {
                **dict(template_payload["profile"]),
                "growth_stage": "task_to_monitor",
                "linked_template_name": template_payload["name"],
                "recommended_schedule": {
                    "enabled": recommended_enabled,
                    "interval_minutes": recommended_interval,
                },
            }
        )
        return {
            "name": str(monitor_name or "").strip() or f"{domain_label} {market_template['name']}",
            "url": template_payload["url"],
            "schema_name": template_payload["schema_name"],
            "storage_format": template_payload["storage_format"],
            "use_static": template_payload["use_static"],
            "selected_fields": list(template_payload["selected_fields"]),
            "field_labels": dict(template_payload["field_labels"]),
            "profile": profile,
            "schedule_enabled": enabled,
            "schedule_interval_minutes": interval,
        }

    def _build_growth_entry(
        task: Any,
        detail: dict[str, Any],
        tenant_id: str,
    ) -> dict[str, Any]:
        if task.status != "success":
            return {
                "eligible": False,
                "reason": "仅成功任务可沉淀为模板或升级为持续监控",
                "recommended_template_package_id": "",
                "recommended_template_package_name": "",
                "template_draft": None,
                "monitor_draft": None,
                "conversion_path": [],
                "recommended_actions": [],
                "existing_template_count": 0,
                "existing_monitor_count": 0,
                "history_signal": {
                    "repeat_url": bool(
                        detail.get("history_summary", {}).get("repeat_url", False)
                    ),
                    "has_detected_changes": bool(
                        detail.get("comparison", {}).get("changed", False)
                    ),
                },
            }

        selected_fields = _task_selected_fields(task)
        if not selected_fields:
            return {
                "eligible": False,
                "reason": "当前成功任务缺少可复用字段，暂不建议直接生成模板",
                "recommended_template_package_id": "",
                "recommended_template_package_name": "",
                "template_draft": None,
                "monitor_draft": None,
                "conversion_path": [],
                "recommended_actions": [],
                "existing_template_count": 0,
                "existing_monitor_count": 0,
                "history_signal": {
                    "repeat_url": bool(
                        detail.get("history_summary", {}).get("repeat_url", False)
                    ),
                    "has_detected_changes": bool(
                        detail.get("comparison", {}).get("changed", False)
                    ),
                },
            }

        market_template = _infer_growth_package(task, detail)
        template_draft = _build_template_payload_from_task(
            task,
            detail,
            market_template=market_template,
        )
        monitor_draft = _build_monitor_payload_from_task(
            task,
            detail,
            market_template=market_template,
            template_payload=template_draft,
        )
        templates = task_store.list_templates(limit=200, tenant_id=tenant_id)
        monitors = task_store.list_monitors(limit=200, tenant_id=tenant_id)
        existing_template_count = sum(1 for item in templates if item.url == task.url)
        existing_monitor_count = sum(1 for item in monitors if item.url == task.url)
        history_signal = {
            "repeat_url": bool(detail.get("history_summary", {}).get("repeat_url", False)),
            "total_runs": int(detail.get("history_summary", {}).get("total_runs", 0) or 0),
            "has_previous_success": bool(
                detail.get("comparison", {}).get("has_previous", False)
            ),
            "has_detected_changes": bool(
                detail.get("comparison", {}).get("changed", False)
            ),
            "changed_fields_count": int(
                detail.get("comparison", {}).get("changed_fields_count", 0) or 0
            ),
        }
        recommended_actions = [
            "先把这次成功任务保存成模板，沉淀可复用字段集和场景画像",
            f"再升级为持续监控，按建议频率 {monitor_draft['schedule_interval_minutes']} 分钟巡检一次",
            "最后补齐通知通道，让后续变化直接进入业务处理链路",
        ]
        conversion_path = [
            {
                "step": "save_template",
                "label": "保存成功任务为模板",
                "ready": True,
            },
            {
                "step": "create_monitor",
                "label": "基于模板开启持续监控",
                "ready": True,
            },
            {
                "step": "configure_notification",
                "label": "为监控补齐结构化通知",
                "ready": True,
            },
        ]
        return {
            "eligible": True,
            "reason": "该任务已成功完成，适合沉淀为模板并升级为持续监控",
            "recommended_template_package_id": market_template["template_id"],
            "recommended_template_package_name": market_template["name"],
            "template_draft": template_draft,
            "monitor_draft": monitor_draft,
            "conversion_path": conversion_path,
            "recommended_actions": recommended_actions,
            "existing_template_count": existing_template_count,
            "existing_monitor_count": existing_monitor_count,
            "history_signal": history_signal,
        }

    @router.post("/api/nl_task")
    async def api_natural_language_task(
        payload: NaturalLanguageTaskRequest,
        request: Request,
        _: None = Depends(api_guard),
    ):
        from smart_extractor.extractor.llm_extractor import LLMExtractor

        extractor = LLMExtractor(load_config().llm)
        plan = extractor.parse_task_request(payload.request_text.strip())
        request_logger(request, "-").info(
            "Parse natural language task: task_type={} urls={} fields={}",
            plan.get("task_type"),
            len(plan.get("urls", [])),
            plan.get("selected_fields", []),
        )
        return {
            "message": "已生成任务草案",
            "plan": plan,
            "request_id": get_request_id(request),
        }

    @router.get("/api/task/{task_id}")
    async def api_task_detail(
        task_id: str,
        request: Request,
        _: None = Depends(api_guard),
    ):
        _require(request, "task:read")
        tenant_id = _tenant_id(request)
        detail = task_store.get_task_detail_payload(task_id, tenant_id=tenant_id)
        task = task_store.get(task_id, tenant_id=tenant_id)
        if not detail or task is None:
            raise HTTPException(status_code=404, detail="任务不存在")
        detail["growth_entry"] = _build_growth_entry(task, detail, tenant_id)
        growth_entry = detail["growth_entry"]
        if growth_entry.get("eligible", False):
            _record_funnel_event(
                request,
                stage="growth_entry_exposed",
                channel="task_detail",
                package_type="template",
                package_id=str(growth_entry.get("recommended_template_package_id") or ""),
                package_name=str(growth_entry.get("recommended_template_package_name") or ""),
                task_id=task.task_id,
                metadata={
                    "existing_template_count": int(
                        growth_entry.get("existing_template_count", 0) or 0
                    ),
                    "existing_monitor_count": int(
                        growth_entry.get("existing_monitor_count", 0) or 0
                    ),
                },
            )
        request_logger(request, task_id).info("Get task detail")
        return detail

    @router.post("/api/task/{task_id}/template")
    async def api_promote_task_to_template(
        task_id: str,
        payload: PromoteTaskTemplateRequest,
        request: Request,
        _: None = Depends(api_guard),
    ):
        _require(request, "template:manage")
        if payload.create_monitor:
            _require(request, "monitor:manage")

        tenant_id = _tenant_id(request)
        task = task_store.get(task_id, tenant_id=tenant_id)
        if task is None:
            raise HTTPException(status_code=404, detail="任务不存在")
        if task.status != "success":
            raise HTTPException(status_code=400, detail="仅成功任务可生成模板")

        detail = task_store.get_task_detail_payload(task_id, tenant_id=tenant_id)
        if not detail:
            raise HTTPException(status_code=404, detail="任务不存在")
        growth_entry = _build_growth_entry(task, detail, tenant_id)
        if not growth_entry.get("eligible", False):
            raise HTTPException(
                status_code=400,
                detail=str(growth_entry.get("reason") or "当前任务暂不适合生成模板"),
            )

        market_template = get_market_template(
            str(growth_entry.get("recommended_template_package_id") or "").strip()
        )
        if market_template is None:
            raise HTTPException(status_code=500, detail="核心模板包未正确加载")

        template_payload = _build_template_payload_from_task(
            task,
            detail,
            market_template=market_template,
            template_name=payload.name,
            profile_overlay=payload.profile,
        )
        template = task_store.create_or_update_template(
            name=template_payload["name"],
            url=template_payload["url"],
            page_type=template_payload["page_type"],
            schema_name=template_payload["schema_name"],
            storage_format=template_payload["storage_format"],
            use_static=template_payload["use_static"],
            selected_fields=template_payload["selected_fields"],
            field_labels=template_payload["field_labels"],
            profile=template_payload["profile"],
            tenant_id=tenant_id,
        )

        monitor_payload = None
        monitor = None
        if payload.create_monitor:
            recommended_monitor = growth_entry.get("monitor_draft") or {}
            default_interval = (
                int(recommended_monitor.get("schedule_interval_minutes", 180) or 180)
                if isinstance(recommended_monitor, dict)
                else 180
            )
            interval = (
                default_interval
                if int(payload.schedule_interval_minutes or 180) == 180
                else int(payload.schedule_interval_minutes)
            )
            monitor_payload = _build_monitor_payload_from_task(
                task,
                detail,
                market_template=market_template,
                template_payload=template_payload,
                monitor_name=payload.monitor_name,
                schedule_enabled=payload.schedule_enabled,
                schedule_interval_minutes=interval,
            )
            monitor = task_store.create_or_update_monitor(
                name=monitor_payload["name"],
                url=monitor_payload["url"],
                schema_name=monitor_payload["schema_name"],
                storage_format=monitor_payload["storage_format"],
                use_static=monitor_payload["use_static"],
                selected_fields=monitor_payload["selected_fields"],
                field_labels=monitor_payload["field_labels"],
                profile=monitor_payload["profile"],
                schedule_enabled=monitor_payload["schedule_enabled"],
                schedule_interval_minutes=monitor_payload["schedule_interval_minutes"],
                tenant_id=tenant_id,
            )

        request_logger(request, task_id).info(
            "Promote task to template: template_id={} create_monitor={} monitor_id={}",
            template.template_id,
            payload.create_monitor,
            monitor.monitor_id if monitor is not None else "-",
        )
        _record_funnel_event(
            request,
            stage="task_promote_template",
            channel="task_growth",
            package_type="template",
            package_id=market_template["template_id"],
            package_name=market_template["name"],
            task_id=task.task_id,
            template_id=template.template_id,
            metadata={
                "selected_field_count": len(template_payload["selected_fields"]),
                "create_monitor": bool(payload.create_monitor),
            },
        )
        if monitor is not None:
            _record_funnel_event(
                request,
                stage="task_promote_monitor",
                channel="task_growth",
                package_type="template",
                package_id=market_template["template_id"],
                package_name=market_template["name"],
                task_id=task.task_id,
                template_id=template.template_id,
                monitor_id=monitor.monitor_id,
                metadata={
                    "schedule_enabled": bool(monitor_payload["schedule_enabled"])
                    if isinstance(monitor_payload, dict)
                    else False,
                    "schedule_interval_minutes": int(
                        monitor_payload["schedule_interval_minutes"]
                    )
                    if isinstance(monitor_payload, dict)
                    else 0,
                },
            )
        response_payload = {
            "message": "已从成功任务生成模板",
            "recommended_template_package_id": market_template["template_id"],
            "recommended_template_package_name": market_template["name"],
            "template": serialize_template(template),
            "growth_entry": growth_entry,
        }
        if monitor is not None:
            response_payload["message"] = "已从成功任务生成模板并开启持续监控"
            response_payload["monitor"] = serialize_monitor(monitor, learned_profile_store)
        return response_payload

    @router.get("/api/task/{task_id}/export")
    async def api_task_export(
        task_id: str,
        request: Request,
        format: str = "docx",
        _: None = Depends(api_guard),
    ):
        _require(request, "task:export")
        detail = task_store.get_task_detail_payload(task_id, tenant_id=_tenant_id(request))
        if not detail:
            raise HTTPException(status_code=404, detail="任务不存在")

        normalized_format = str(format or "docx").strip().lower()
        if normalized_format == "docx":
            content = build_task_docx(detail)
            media_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            file_name = f"{task_id}.docx"
        elif normalized_format == "xlsx":
            content = build_task_xlsx(detail)
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            file_name = f"{task_id}.xlsx"
        elif normalized_format == "md":
            content = build_task_markdown(detail).encode("utf-8")
            media_type = "text/markdown; charset=utf-8"
            file_name = f"{task_id}.md"
        elif normalized_format == "json":
            import json

            content = json.dumps(detail, ensure_ascii=False, indent=2).encode("utf-8")
            media_type = "application/json; charset=utf-8"
            file_name = f"{task_id}.json"
        else:
            raise HTTPException(status_code=400, detail="仅支持 docx、xlsx、md 或 json")

        request_logger(request, task_id).info("Export task: format={}", normalized_format)
        return Response(
            content=content,
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{file_name}"'},
        )
