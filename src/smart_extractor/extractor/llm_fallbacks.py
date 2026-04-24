"""LLM 失败后的兜底与启发式逻辑。"""

from __future__ import annotations

import json
import re
from typing import Any

from smart_extractor.extractor.learned_profile_store import LearnedProfile
from smart_extractor.extractor.llm_response import (
    _format_dynamic_text,
    _normalize_field_list,
    _normalize_url_list,
)
from smart_extractor.extractor.rule_extractor import RuleBasedDynamicExtractor
from smart_extractor.models.base import DynamicExtractResult
from smart_extractor.utils.display import build_field_labels

_FALLBACK_NOISE_PATTERNS = [
    r"^\s*\|.*$",
    r"^.*本条目存在以下问题.*$",
    r"^.*请协助改善本条目.*$",
    r"^.*请按照校对指引.*$",
    r"^.*帮助、讨论.*$",
    r"^.*此条目.*(应避免|含有|需要).*$",
    r"^.*请协助将有关资料.*$",
    r"^.*编辑这个条目.*$",
    r"^.*\(\d{4}年\d{1,2}月\d{1,2}日\).*$",
    r"^\s*展开.*$",
    r"^\s*折叠.*$",
    r"^\s*\[编辑\].*$",
    r"^\s*参见[:：]?\s*$",
    r"^\s*参考文献\s*$",
    r"^\s*外部链接\s*$",
    r"^\s*注释\s*$",
    r"^\s*#+ ?(参见|参考文献|外部链接|注释|延伸阅读)\s*$",
]
_FALLBACK_NOISE_RE = re.compile("|".join(_FALLBACK_NOISE_PATTERNS), re.MULTILINE)

_FALLBACK_PRODUCT_MARKERS = ("价格", "售价", "商品", "品牌", "库存", "规格")
_FALLBACK_JOB_MARKERS = ("任职要求", "岗位职责", "职位描述", "薪资", "工作地点")
_FALLBACK_PRODUCT_FIELDS = ["name", "price", "brand", "description", "summary"]
_FALLBACK_JOB_FIELDS = [
    "title",
    "company",
    "salary_range",
    "location",
    "requirements",
    "summary",
]
_FALLBACK_ARTICLE_FIELDS = ["title", "author", "publish_date", "summary", "content"]


def resolve_fallback_profile(
    text: str,
    selected_fields: list[str],
) -> tuple[str, list[str]]:
    if selected_fields:
        normalized = [field for field in selected_fields if str(field or "").strip()]
        if {"price", "brand", "name", "stock"} & set(normalized):
            return "product", normalized
        if {"company", "salary_range", "location", "requirements"} & set(normalized):
            return "job", normalized
        return "article", normalized

    normalized_text = str(text or "")
    if any(marker in normalized_text for marker in _FALLBACK_PRODUCT_MARKERS):
        return "product", list(_FALLBACK_PRODUCT_FIELDS)
    if any(marker in normalized_text for marker in _FALLBACK_JOB_MARKERS):
        return "job", list(_FALLBACK_JOB_FIELDS)
    return "article", list(_FALLBACK_ARTICLE_FIELDS)


def build_dynamic_fallback_result(
    text: str,
    *,
    source_url: str = "",
    selected_fields: list[str] | None = None,
) -> DynamicExtractResult:
    lines = text.splitlines()
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped or len(stripped) < 10:
            continue
        if _FALLBACK_NOISE_RE.match(stripped):
            continue
        clean_lines.append(stripped)

    fallback_text = "\n\n".join(clean_lines) if clean_lines else text
    normalized_fields = _normalize_field_list(selected_fields)
    page_type, fallback_fields = resolve_fallback_profile(text, normalized_fields)
    field_labels = build_field_labels(fallback_fields)

    rule_result = RuleBasedDynamicExtractor().extract(
        text,
        source_url=source_url,
        profile=LearnedProfile(
            profile_id="fallback-rule",
            domain="",
            path_prefix="/",
            page_type=page_type,
            selected_fields=fallback_fields,
            field_labels=field_labels,
            sample_url=source_url,
        ),
        selected_fields=fallback_fields,
    )
    rule_data = dict(rule_result.data or {})

    if fallback_text and (
        "content" in fallback_fields
        or not rule_data
        or page_type in {"article", "blog", "news"}
    ):
        rule_data.setdefault("content", fallback_text)

    actual_fields = [
        field
        for field in fallback_fields
        if rule_data.get(field) not in (None, "", [], {})
    ]
    if not actual_fields and fallback_text:
        actual_fields = ["content"]
        field_labels = {"content": "正文内容"}
        rule_data = {"content": fallback_text}
    else:
        if "content" in rule_data and "content" not in actual_fields:
            actual_fields.append("content")
        field_labels = build_field_labels(actual_fields, rule_result.field_labels)

    return DynamicExtractResult(
        page_type=page_type,
        candidate_fields=fallback_fields,
        selected_fields=actual_fields,
        field_labels=field_labels,
        data=rule_data,
        formatted_text=_format_dynamic_text(field_labels, rule_data),
        extraction_strategy="fallback",
        strategy_details={
            "mode": "fallback_rule",
            "source_url": source_url,
            "fallback_fields": fallback_fields,
        },
    )


def build_context_fallback(
    page_result: DynamicExtractResult,
    user_context: dict[str, Any],
) -> dict[str, Any]:
    goal = str(user_context.get("goal") or "summary")
    role = str(user_context.get("role") or "user")
    priority = str(user_context.get("priority") or "未说明")
    constraints = str(user_context.get("constraints") or "未说明")
    notes = str(user_context.get("notes") or "").strip()

    evidence_spans: list[dict[str, str]] = []
    for field in page_result.selected_fields or page_result.candidate_fields:
        value = page_result.data.get(field)
        if value in (None, "", [], {}):
            continue
        label = page_result.field_labels.get(field) or field
        rendered = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        evidence_spans.append({"label": label, "snippet": str(rendered)[:180]})
        if len(evidence_spans) >= 3:
            break

    if not evidence_spans and page_result.formatted_text:
        evidence_spans.append({"label": "页面预览", "snippet": page_result.formatted_text[:180]})

    title_like = ""
    for key in ("title", "name", "summary", "content"):
        value = page_result.data.get(key)
        if isinstance(value, str) and value.strip():
            title_like = value.strip()[:40]
            break

    headline = title_like or f"{page_result.page_type} 页面分析"
    summary = (
        f"系统已结合页面内容与用户目标（{goal}）生成初步判断，"
        f"当前更适合从 {priority} 这个角度继续阅读与决策。"
    )

    risks: list[str] = []
    if constraints == "未说明":
        risks.append("尚未提供明确限制条件，结论会偏通用。")
    if not notes:
        risks.append("缺少你的背景或已有判断，建议补充后再生成更细的建议。")
    if not page_result.data:
        risks.append("网页可抽取结构较少，部分结论只能作为初步参考。")
    if not risks:
        risks.append("建议核对页面中的关键承诺、价格、时间或适用范围，避免遗漏条件。")

    missing_information: list[str] = []
    if constraints == "未说明":
        missing_information.append("你的硬性限制条件，例如预算、地区、时间范围或必需项。")
    if priority == "未说明":
        missing_information.append("你最看重的决策维度，例如价格、稳定性、薪资或品牌。")
    if not notes:
        missing_information.append("你的背景信息或当前正在比较的备选对象。")

    return {
        "headline": headline,
        "summary": summary,
        "confidence": "medium" if page_result.data else "low",
        "key_points": [
            f"页面类型识别为 {page_result.page_type}，适合按 {goal} 目标来组织分析。",
            f"当前用户身份为 {role}，分析会优先围绕这个视角给出建议。",
            f"系统优先关注 {priority}，并会参考限制条件：{constraints}。",
        ],
        "risks": risks[:3],
        "recommended_actions": [
            "先核对页面中最关键的事实字段，再决定是否提交正式抽取任务。",
            "补充你的预算、必需项或淘汰项，可显著提升分析针对性。",
            "如果有备选 URL，下一步可做并排对比分析。",
        ],
        "missing_information": missing_information[:3],
        "evidence_spans": evidence_spans,
    }


def build_compare_fallback(
    preview_items: list[dict[str, Any]],
    user_context: dict[str, Any],
) -> dict[str, Any]:
    focus = str(user_context.get("focus") or "未说明")
    must_have = str(user_context.get("must_have") or "未说明")
    elimination = str(user_context.get("elimination") or "未说明")
    role = str(user_context.get("role") or "user")
    goal = str(user_context.get("goal") or "comparison")

    evidence_spans: list[dict[str, str]] = []
    comparison_matrix: list[dict[str, str]] = []
    for index, item in enumerate(preview_items[:4], start=1):
        title = ""
        data = item.get("data") or {}
        for key in ("title", "name", "summary", "content"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()[:60]
                break
        page_name = title or item.get("url") or f"对象 {index}"
        comparison_matrix.append(
            {
                "label": f"对象 {index}",
                "summary": (
                    f"{page_name} 的页面类型为 "
                    f"{item.get('page_type_label') or item.get('page_type') or '未知'}，"
                    f"当前适合从 {focus} 维度继续比较。"
                ),
            }
        )
        preview = str(item.get("preview") or "").strip()
        if preview:
            evidence_spans.append({"label": f"对象 {index}", "snippet": preview[:180]})

    missing_information: list[str] = []
    if focus == "未说明":
        missing_information.append("你最希望比较的维度，例如价格、功能、岗位匹配度或售后。")
    if must_have == "未说明":
        missing_information.append("你的必须满足条件，例如预算范围、远程、私有化部署或品牌要求。")
    if elimination == "未说明":
        missing_information.append("你的淘汰条件，例如价格过高、功能缺失、资历差距过大。")

    return {
        "headline": "多 URL 对比初步结论",
        "summary": (
            f"系统已基于 {len(preview_items)} 个页面生成初步比较，"
            f"当前更适合从 {focus} 维度继续收敛选择。"
        ),
        "confidence": "medium" if preview_items else "low",
        "key_points": [
            f"本次比较目标为 {goal}，系统会优先输出适合 {role} 视角的结论。",
            f"已读取 {len(preview_items)} 个 URL，可继续结合 must-have 与 elimination 条件做筛选。",
            f"当前重点比较维度：{focus}；必须满足：{must_have}；淘汰条件：{elimination}。",
        ],
        "risks": [
            "不同页面的信息完整度可能并不一致，某些结论只能作为初步判断。",
            "如果缺少明确的 must-have 或淘汰条件，系统难以给出更强的胜出结论。",
        ],
        "recommended_actions": [
            "先确认哪些条件是硬性门槛，再按这些门槛淘汰一轮。",
            "将当前看中的两个对象单独再做一轮精细对比。",
            "如果需要汇报，可直接基于当前比较结果整理成简报。",
        ],
        "missing_information": missing_information[:3],
        "evidence_spans": evidence_spans[:4],
        "comparison_matrix": comparison_matrix[:4],
        "report": {
            "title": "差异对比报告",
            "executive_summary": (
                f"本次共比较 {len(preview_items)} 个对象，当前建议优先围绕 {focus} 做下一轮筛选。"
            ),
            "common_points": [
                f"当前对象数量为 {len(preview_items)}，都已生成可供横向比较的页面预览。",
                f"本轮比较会优先参考你的 must-have（{must_have}）与淘汰条件（{elimination}）。",
            ],
            "difference_points": [item["summary"] for item in comparison_matrix[:4]],
            "recommendation": "先按硬性条件做一轮淘汰，再对剩余对象继续精细比较。",
            "next_steps": [
                "补充最关键的决策维度，例如价格、功能或售后。",
                "对最接近的两个对象再做一轮精细对比。",
                "如果需要汇报，可直接导出当前差异对比结果。",
            ],
        },
    }


def normalize_compare_report(
    report: Any,
    *,
    preview_items: list[dict[str, Any]],
    user_context: dict[str, Any],
) -> dict[str, Any]:
    fallback = build_compare_fallback(preview_items, user_context).get("report", {})
    if not isinstance(report, dict):
        return fallback
    return {
        "title": str(report.get("title") or fallback.get("title") or "差异对比报告"),
        "executive_summary": str(
            report.get("executive_summary")
            or fallback.get("executive_summary")
            or "系统已生成差异对比摘要。"
        ),
        "common_points": [
            str(item).strip()
            for item in report.get("common_points", [])
            if str(item).strip()
        ]
        or fallback.get("common_points", []),
        "difference_points": [
            str(item).strip()
            for item in report.get("difference_points", [])
            if str(item).strip()
        ]
        or fallback.get("difference_points", []),
        "recommendation": str(
            report.get("recommendation")
            or fallback.get("recommendation")
            or "建议先按关键条件筛选，再继续深挖差异。"
        ),
        "next_steps": [
            str(item).strip()
            for item in report.get("next_steps", [])
            if str(item).strip()
        ]
        or fallback.get("next_steps", []),
    }


def build_task_plan_fallback(request_text: str) -> dict[str, Any]:
    text = str(request_text or "").strip()
    lowered = text.lower()
    urls = _normalize_url_list(re.findall(r"https?://[^\s,，；]+", text))

    task_type = "single_extract"
    if "对比" in text or "比较" in text:
        task_type = "compare_analysis"
    elif "监控" in text or "告警" in text or "变化" in text:
        task_type = "monitor"
    elif len(urls) > 1 or "批量" in text:
        task_type = "batch_extract"

    field_keywords = {
        "title": ["标题", "title"],
        "content": ["正文", "内容", "全文", "content"],
        "summary": ["摘要", "总结", "summary"],
        "price": ["价格", "价钱", "售价", "price"],
        "author": ["作者", "发布者", "author"],
        "publish_date": ["发布时间", "日期", "时间", "publish_date"],
        "company": ["公司", "企业", "company"],
        "salary_range": ["薪资", "工资", "salary"],
        "brand": ["品牌", "brand"],
        "description": ["描述", "简介", "说明", "description"],
    }
    selected_fields = [
        field
        for field, keywords in field_keywords.items()
        if any(keyword.lower() in lowered for keyword in keywords)
    ]
    if not selected_fields and task_type == "compare_analysis":
        selected_fields = ["title", "price", "summary"]
    if not selected_fields:
        selected_fields = ["title", "content"]

    schema_name = "auto"
    if "商品" in text:
        schema_name = "product"
    elif "新闻" in text or "文章" in text:
        schema_name = "news"
    elif "招聘" in text or "岗位" in text:
        schema_name = "job"

    warnings: list[str] = []
    if not urls:
        warnings.append("当前还没有识别到 URL，回填后请手动补充。")
    if task_type == "compare_analysis" and len(urls) < 2:
        warnings.append("对比分析至少需要两个 URL。")
    if task_type == "monitor":
        warnings.append("当前为第一版自然语言创建，监控会先生成监控草案，不会自动定时执行。")

    return {
        "task_type": task_type,
        "summary": "已根据自然语言需求生成任务草案。",
        "urls": urls,
        "selected_fields": selected_fields,
        "use_static": "静态" in text,
        "storage_format": "csv" if "csv" in lowered or "excel" in lowered else "json",
        "schema_name": schema_name,
        "name": text[:20] or "自然语言任务",
        "confidence": "medium",
        "warnings": warnings,
    }
