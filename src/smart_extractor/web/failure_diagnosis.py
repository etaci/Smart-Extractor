"""Failure diagnosis helpers for task list and task detail payloads."""

from __future__ import annotations

from typing import Any


def build_task_failure_diagnosis(
    payload: dict[str, Any],
    data: dict[str, Any] | None = None,
) -> dict[str, object]:
    status = str(payload.get("status") or "").strip().lower()
    error = str(payload.get("error") or "").strip()
    normalized = error.lower()
    task_data = data or {}
    category = ""
    title = ""
    suggestion = ""
    severity = "info"

    if "captcha" in normalized or "challenge" in normalized or "验证" in error:
        category = "captcha"
        title = "疑似验证码或挑战页"
        suggestion = "启用代理池和会话/profile 池化，降低站点并发；如仍失败，将该站点加入挑战页恢复策略并重试。"
        severity = "warning"
    elif "403" in normalized or "forbidden" in normalized:
        category = "403"
        title = "目标站返回 403"
        suggestion = "切换代理或代理分组，降低站点级并发和访问频率；检查请求头、登录态和 robots/条款限制。"
        severity = "danger"
    elif "timeout" in normalized or "timed out" in normalized or "超时" in error:
        category = "timeout"
        title = "页面加载或执行超时"
        suggestion = "重试并提高 fetch/Playwright 超时；若页面静态可读，切换静态抓取，否则延长等待关键选择器。"
        severity = "warning"
    elif (
        "model" in normalized
        or "llm" in normalized
        or "jsondecode" in normalized
        or "invalid json" in normalized
        or "rate limit" in normalized
        or "api error" in normalized
        or "模型" in error
    ):
        category = "model_error"
        title = "模型调用或输出异常"
        suggestion = "检查模型 API Key、base_url、限流和响应格式；建议降低并发、启用重试，并保留原始响应用于排查。"
        severity = "danger"
    elif "field" in normalized or "schema" in normalized or "字段" in error or "缺失" in error:
        category = "missing_fields"
        title = "字段缺失"
        suggestion = "发起字段级人工反馈，标记正确/错误/缺失；系统会沉淀到模板评分和站点记忆。"
        severity = "warning"

    if not category:
        extracted = (
            task_data.get("data")
            if isinstance(task_data.get("data"), dict)
            else task_data
        )
        selected_fields = (
            [
                str(item).strip()
                for item in task_data.get("selected_fields", [])
                if str(item).strip()
            ]
            if isinstance(task_data.get("selected_fields"), list)
            else []
        )
        missing_fields = [
            field
            for field in selected_fields
            if not isinstance(extracted, dict) or extracted.get(field) in (None, "", [], {})
        ]
        if missing_fields:
            category = "missing_fields"
            title = "字段缺失"
            suggestion = "发起字段级人工反馈，标记正确/错误/缺失；系统会沉淀到模板评分和站点记忆。"
            severity = "warning"

    if not category and status == "failed":
        category = "unknown"
        title = "任务执行失败"
        suggestion = "查看任务详情和运行日志；优先确认网络、代理、站点限速和模型配置，再重试。"
        severity = "danger"

    return {
        "category": category,
        "title": title,
        "message": error,
        "suggestion": suggestion,
        "severity": severity,
        "actionable": bool(category),
    }
