from __future__ import annotations

import json
import re
import socket
import threading
import time
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

import pytest
import uvicorn
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from playwright.sync_api import sync_playwright


WEB_DIR = Path(__file__).resolve().parents[1] / "src" / "smart_extractor" / "web"
TEMPLATES = Jinja2Templates(directory=str(WEB_DIR / "templates"))


def _port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _runtime():
    return {
        "ready": True,
        "issues": [],
        "warnings": [],
        "api_token_required": True,
        "startup_check_enabled": True,
        "services": {
            "monitor_scheduler": {"enabled": True, "alive": True, "poll_interval_seconds": 2, "total_runs": 1, "last_claimed_count": 0, "last_triggered_count": 0, "last_failed_count": 0, "last_reclaimed_count": 0, "last_error": ""},
            "task_worker": {"enabled": True, "alive": True, "task_dispatch_mode": "inline", "worker_poll_interval_seconds": 1, "worker_stale_after_seconds": 5},
            "notification_retry": {"enabled": True, "alive": True, "poll_interval_seconds": 30, "total_runs": 1},
            "notification_digest": {"enabled": True, "alive": True, "poll_interval_seconds": 60, "total_runs": 1, "last_error": ""},
        },
    }


def _dashboard():
    return {
        "stats": {"total": 2, "success": 1, "failed": 0, "running": 1, "pending": 0, "success_rate": "50%"},
        "tasks": [
            {"task_id": "task-001", "url": "https://example.com/a", "domain": "example.com", "schema_name": "news", "storage_format": "json", "status": "success", "quality_score": 0.9, "elapsed_ms": 100, "created_at": "2026-04-23", "task_kind": "single", "batch_group_id": "", "total_items": 0, "completed_items": 0, "progress": {"percent": 100, "stage": "done"}},
            {"task_id": "task-batch", "url": "batch://demo", "domain": "batch", "schema_name": "auto", "storage_format": "json", "status": "running", "quality_score": 0.0, "elapsed_ms": 0, "created_at": "2026-04-23", "task_kind": "batch", "batch_group_id": "batch-demo", "total_items": 2, "completed_items": 1, "progress": {"percent": 50, "stage": "running"}},
        ],
        "insights": {
            "summary": {"repeat_urls": 1, "changed_tasks": 1, "active_monitors": 2, "notification_ready_monitors": 1, "notification_success_monitors": 1, "rule_based_tasks": 1, "fallback_tasks": 0, "learned_profile_hits": 1, "site_memory_saved_runs": 1, "memory_ready_pages": 1, "high_priority_alerts": 1, "avg_quality": "90%"},
            "recent_changes": [], "scenario_summary": [], "domain_leaderboard": [],
            "watchlist": [{"domain": "example.com", "url": "https://example.com/watch", "monitor_readiness": "ready", "total_runs": 2, "latest_quality": 0.9, "latest_status": "success"}],
            "monitor_alerts": [{"monitor_id": "mon-1", "name": "Watch", "business_summary": "Changed", "recommended_actions": ["check"], "notification_status_label": "sent", "last_notification_status": "sent", "alert_label": "Changed", "last_alert_level": "changed", "severity": "high", "severity_label": "High"}],
            "monitors": [],
        },
        "templates": [{"template_id": "tpl-1", "name": "Tpl", "url": "https://example.com/tpl", "page_type": "article", "storage_format": "json", "use_static": False, "selected_fields": ["title"], "field_labels": {"title": "Title"}, "profile": {"scenario_label": "Scenario", "business_goal": "Goal", "summary_style": "report"}}],
        "market_templates": [
            {"template_id": "market-product-monitor", "name": "Product", "description": "desc", "category": "monitor", "page_type": "product", "sample_url": "https://example.com/product", "storage_format": "json", "use_static": False, "selected_fields": ["price"], "field_labels": {"price": "Price"}, "tags": ["product"], "target_users": ["ops"], "default_outputs": ["brief"], "profile": {"scenario_label": "Scenario", "business_goal": "Goal", "summary_style": "report", "alert_focus": "price", "notify_on": ["changed"]}},
            {"template_id": "market-policy-watch", "name": "Policy", "description": "desc", "category": "monitor", "page_type": "article", "sample_url": "https://example.com/policy", "storage_format": "json", "use_static": False, "selected_fields": ["title"], "field_labels": {"title": "Title"}, "tags": ["policy"], "target_users": ["ops"], "default_outputs": ["brief"], "profile": {"scenario_label": "Scenario", "business_goal": "Goal", "summary_style": "report", "alert_focus": "title", "notify_on": ["changed"]}},
            {"template_id": "market-job-compare", "name": "Compare", "description": "desc", "category": "compare", "page_type": "job", "sample_url": "https://example.com/job", "storage_format": "json", "use_static": True, "selected_fields": ["salary"], "field_labels": {"salary": "Salary"}, "tags": ["job"], "target_users": ["ops"], "default_outputs": ["brief"], "profile": {"scenario_label": "Scenario", "business_goal": "Goal", "summary_style": "report", "alert_focus": "salary", "notify_on": ["changed"]}},
            {"template_id": "market-batch-article", "name": "Batch", "description": "desc", "category": "batch", "page_type": "article", "sample_url": "https://example.com/article", "storage_format": "csv", "use_static": True, "selected_fields": ["title"], "field_labels": {"title": "Title"}, "tags": ["batch"], "target_users": ["ops"], "default_outputs": ["brief"], "profile": {"scenario_label": "Scenario", "business_goal": "Goal", "summary_style": "report", "alert_focus": "title", "notify_on": ["changed"]}},
            {"template_id": "market-competitor-watch", "name": "Competitor", "description": "desc", "category": "monitor", "page_type": "product", "sample_url": "https://example.com/competitor", "storage_format": "json", "use_static": False, "selected_fields": ["title", "summary"], "field_labels": {"title": "Title", "summary": "Summary"}, "tags": ["competitor"], "target_users": ["ops"], "default_outputs": ["brief"], "profile": {"scenario_label": "Scenario", "business_goal": "Goal", "summary_style": "report", "alert_focus": "title", "notify_on": ["changed"]}},
        ],
        "monitors": [
            {"monitor_id": "mon-1", "name": "Monitor 1", "url": "https://example.com/p1", "profile": {"scenario_label": "S1", "business_goal": "G1", "alert_focus": "price"}, "business_summary": "Summary", "last_extraction_strategy": "rule", "last_notification_status": "sent", "notification_status_label": "Sent", "alert_label": "Changed", "last_alert_level": "changed", "severity": "high", "severity_label": "High", "schedule_enabled": True, "schedule_status": "active", "schedule_status_label": "Active", "schedule_interval_label": "60m", "last_trigger_source_label": "Manual"},
            {"monitor_id": "mon-2", "name": "Monitor 2", "url": "https://example.com/p2", "profile": {"scenario_label": "S2", "business_goal": "G2", "alert_focus": "title"}, "business_summary": "Summary", "last_extraction_strategy": "llm", "last_notification_status": "failed", "notification_status_label": "Failed", "alert_label": "Stable", "last_alert_level": "stable", "severity": "low", "severity_label": "Low", "schedule_enabled": True, "schedule_status": "paused", "schedule_status_label": "Paused", "schedule_interval_label": "120m", "last_trigger_source_label": "Scheduler"},
        ],
        "notifications": [{"notification_id": "notif-1", "monitor_id": "mon-1", "status": "failed", "status_label": "Failed", "status_message": "Webhook failed", "error_message": "500", "triggered_by": "manual", "triggered_by_label": "Manual", "channel_type": "webhook", "target": "https://example.com/hook", "created_at": "2026-04-23", "can_resend": True}],
        "learned_profiles": [{"profile_id": "lp-1", "domain": "example.com", "is_active": True, "status_label": "Active", "fields": ["title"], "path_prefixes": ["/p"], "memory_strength_label": "High", "stability_rate": 0.9, "hit_count": 2, "risk_level": "medium"}],
        "runtime_status": _runtime(),
    }


def _task(task_id: str = "task-001"):
    return {"task_id": task_id, "url": "https://example.com/a", "domain": "example.com", "task_kind": "single", "status": "success", "storage_format": "json", "quality_score": 0.95, "elapsed_ms": 128.0, "created_at": "2026-04-23", "history_summary": {"total_runs": 2, "success_runs": 2}, "progress": {"percent": 100.0, "stage": "completed"}, "recent_history": [{"task_id": "task-000", "created_at": "2026-04-22", "status": "success", "quality_score": 0.9}], "comparison": {"has_previous": True, "changed": True, "impact_summary": "Changed", "changed_fields": [{"label": "Price", "summary": "100 -> 90"}], "suggested_actions": ["review"]}, "error": "", "data": {"formatted_text": "Formatted result", "page_type": "article", "data": {"title": "Example"}, "extraction_strategy": "rule", "learned_profile_id": "lp-1"}, "batch_children": []}


def _app():
    app = FastAPI()
    app.mount("/static", StaticFiles(directory=str(WEB_DIR / "static")), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        payload = _dashboard()
        return TEMPLATES.TemplateResponse(request, "dashboard.html", {"stats": payload["stats"], "tasks": payload["tasks"], "insights": payload["insights"], "app_version": "1.2.0", "api_token_required": True, "runtime_status": payload["runtime_status"]})

    @app.get("/task/{task_id}", response_class=HTMLResponse)
    async def task_detail(request: Request, task_id: str):
        return TEMPLATES.TemplateResponse(request, "task_detail.html", {"task": _task(task_id), "app_version": "1.2.0"})

    return app


@pytest.fixture(scope="module")
def web_ui_base_url():
    port = _port()
    server = uvicorn.Server(uvicorn.Config(_app(), host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 15
    url = f"http://127.0.0.1:{port}"
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=1):
                break
        except Exception:
            time.sleep(0.1)
    yield url
    server.should_exit = True
    thread.join(timeout=5)


def _fulfill(route, payload, status=200, ctype="application/json"):
    body = payload if isinstance(payload, (bytes, str)) else json.dumps(payload, ensure_ascii=False)
    route.fulfill(status=status, content_type=ctype, body=body)


def _mock_api(page, calls):
    def handle(route):
        request = route.request
        parsed = urlparse(request.url)
        path = parsed.path
        calls.append(f"{request.method} {path}")
        if path == "/api/config/basic" and request.method == "GET":
            return _fulfill(route, {"api_key": "k", "base_url": "https://api.openai.com/v1", "model": "gpt-4o-mini", "temperature": 0.2})
        if path == "/api/auth/me":
            return _fulfill(route, {"user_id": "token-admin", "username": "token-admin", "role": "admin", "tenant_id": "default", "display_name": "Token Admin", "auth_mode": "api_token"})
        if path == "/api/auth/login":
            return _fulfill(route, {"access_token": "session-token", "token_type": "bearer", "user": {"user_id": "usr-1", "username": "admin", "role": "admin", "tenant_id": "default", "display_name": "Admin"}})
        if path == "/api/auth/register":
            data = json.loads(request.post_data or "{}")
            return _fulfill(route, {"access_token": "session-token", "token_type": "bearer", "user": {"user_id": "usr-2", "username": data.get("username", "new-user"), "role": "admin", "tenant_id": "default", "display_name": data.get("display_name") or data.get("username", "new-user")}})
        if path == "/api/config/basic":
            return _fulfill(route, {"message": "saved", "config": json.loads(request.post_data or "{}")})
        if path == "/api/runtime":
            return _fulfill(route, _runtime())
        if path == "/api/dashboard":
            return _fulfill(route, _dashboard())
        if path == "/api/templates" and request.method == "GET":
            return _fulfill(route, {"templates": _dashboard()["templates"]})
        if path == "/api/template_market":
            return _fulfill(route, {"templates": _dashboard()["market_templates"]})
        if path == "/api/actor_market":
            return _fulfill(route, {"actors": [{"actor_id": "actor-commerce", "name": "Commerce Actor", "description": "Install commerce capability", "category": "commerce", "tags": ["price"], "version": "1.0.0"}]})
        if path == "/api/actors":
            return _fulfill(route, {"actors": [{"actor_id": "actor-installed", "name": "Installed Actor", "description": "Ready", "category": "ops", "status": "active"}]})
        if path == "/api/actor_market/install" and request.method == "POST":
            return _fulfill(route, {"ok": True, "actor": {"actor_id": "actor-commerce"}})
        if path == "/api/quality":
            return _fulfill(route, {"summary": {"avg_quality": 0.9}, "recent": []})
        if path == "/api/cost":
            return _fulfill(route, {"summary": {"total_cost": 1.2}, "recent": []})
        if path == "/api/audit":
            return _fulfill(route, {"logs": [{"action": "auth.login", "resource_type": "session"}]})
        if path == "/api/annotations":
            return _fulfill(route, {"annotations": [{"task_id": "task-001", "status": "pending"}]})
        if path == "/api/workers":
            return _fulfill(route, {"workers": [{"worker_id": "worker-1", "status": "alive"}]})
        if path == "/api/proxies":
            return _fulfill(route, {"proxies": [{"proxy_id": "proxy-1", "status": "healthy"}]})
        if path == "/api/site_policies":
            return _fulfill(route, {"policies": [{"domain": "example.com", "strategy": "static"}]})
        if path == "/api/monitors" and request.method == "GET":
            return _fulfill(route, {"monitors": _dashboard()["monitors"]})
        if path == "/api/notifications":
            return _fulfill(route, {"notifications": _dashboard()["notifications"]})
        if path == "/api/notifications/notif-1/resend" and request.method == "POST":
            return _fulfill(route, {"ok": True, "notification_id": "notif-1"})
        if path == "/api/learned_profiles":
            return _fulfill(route, {"profiles": _dashboard()["learned_profiles"]})
        if path == "/api/learned_profiles/lp-1" and request.method == "GET":
            return _fulfill(route, {"profile": _dashboard()["learned_profiles"][0], "recent_hits": [{"task_id": "task-001", "status": "success"}], "monitors": [{"monitor_id": "mon-1", "name": "Monitor 1"}], "recommended_actions": ["review"]})
        if path.startswith("/api/learned_profiles/") or path in {"/api/learned_profiles/bulk/disable_risky", "/api/learned_profiles/bulk/relearn_risky"}:
            return _fulfill(route, {"ok": True, "count": 1})
        if path == "/api/nl_task":
            text = json.loads(request.post_data or "{}").get("request_text", "")
            urls = ["https://example.com/a", "https://example.com/b"] if "compare" in text else ["https://example.com/monitor"]
            task_type = "compare_analysis" if "compare" in text else "monitor"
            return _fulfill(route, {"plan": {"task_type": task_type, "name": "Plan", "summary": "Plan", "urls": urls, "selected_fields": ["title", "price"], "storage_format": "json", "use_static": True, "warnings": []}})
        if path == "/api/analyze_page":
            return _fulfill(route, {"page_type": "article", "page_type_label": "Article", "candidate_fields": ["title", "summary"], "field_labels": {"title": "Title", "summary": "Summary"}, "preview": "Preview"})
        if path in {"/api/extract", "/api/templates", "/api/monitors", "/api/template_market/install"}:
            return _fulfill(route, {"task_id": "task-created", "template": {"template_id": "tpl-new"}, "monitor": {"monitor_id": "mon-new"}})
        if path == "/api/monitors/mon-1/run" and request.method == "POST":
            return _fulfill(route, {"task_id": "task-mon-1", "reused_existing_task": False})
        if path == "/api/monitors/mon-1/pause" and request.method == "POST":
            return _fulfill(route, {"ok": True, "monitor_id": "mon-1"})
        if path == "/api/monitors/mon-2/resume" and request.method == "POST":
            return _fulfill(route, {"ok": True, "monitor_id": "mon-2"})
        if path == "/api/batch":
            return _fulfill(route, {"task_id": "task-batch-created", "count": 2, "task_ids": ["child-1", "child-2"], "batch_group_id": "batch-demo"})
        if path == "/api/analyze_insight":
            return _fulfill(route, {"page_type": "article", "page_type_label": "Article", "candidate_fields": ["title"], "field_labels": {"title": "Title"}, "page_preview": "Insight preview", "analysis": {"headline": "Insight", "confidence": "high", "summary": "Summary", "key_points": ["P"], "risks": ["R"], "recommended_actions": ["A"], "missing_information": ["M"], "evidence_spans": [{"label": "E", "snippet": "S"}]}})
        if path == "/api/analyze_compare_preview":
            return _fulfill(route, {"items": [{"url": "https://example.com/a", "page_type": "product", "page_type_label": "Product", "candidate_fields": ["price"], "field_labels": {"price": "Price"}, "preview": "A"}, {"url": "https://example.com/b", "page_type": "product", "page_type_label": "Product", "candidate_fields": ["price"], "field_labels": {"price": "Price"}, "preview": "B"}]})
        if path == "/api/analyze_compare":
            return _fulfill(route, {"page_type": "product", "page_type_label": "Product", "items": [{"url": "https://example.com/a"}, {"url": "https://example.com/b"}], "comparison_matrix": [{"label": "Price", "summary": "A cheaper"}], "report": {"title": "Report", "executive_summary": "Pick A", "common_points": ["C"], "difference_points": ["D"], "recommendation": "Pick A", "next_steps": ["N"]}, "analysis": {"headline": "Compare", "confidence": "high", "summary": "Summary", "key_points": ["P"], "risks": ["R"], "recommended_actions": ["A"], "missing_information": [], "evidence_spans": [{"label": "E", "snippet": "S"}]}})
        if path.startswith("/api/task/") and path.endswith("/export"):
            if "format=docx" in parsed.query or "format=xlsx" in parsed.query:
                return _fulfill(route, b"binary", ctype="application/octet-stream")
            return _fulfill(route, _task())
        if path == "/api/task/task-001":
            return _fulfill(route, _task())
        return _fulfill(route, {"detail": path}, status=404)

    page.route("**/api/**", handle)


def test_frontend_scripts_only_reference_rendered_element_ids():
    pages = {"dashboard.html": ["app.js", "dashboard_analysis.js", "dashboard_templates_monitors.js", "dashboard_learned_profiles.js", "dashboard_task_runtime.js"], "task_detail.html": ["task_detail.js", "task_detail_render.js"]}
    for template_name, script_names in pages.items():
        ids = {tag.get("id") for tag in BeautifulSoup((WEB_DIR / "templates" / template_name).read_text(encoding="utf-8"), "html.parser").find_all(attrs={"id": True})}
        missing = {}
        for script_name in script_names:
            content = (WEB_DIR / "static" / script_name).read_text(encoding="utf-8")
            for element_id in set(re.findall(r'getElementById\\(\"([^\"]+)\"\\)', content)):
                if element_id not in ids and element_id != "toast-container":
                    missing.setdefault(element_id, []).append(script_name)
        assert missing == {}


def test_dashboard_and_detail_controls_smoke(web_ui_base_url):
    calls, page_errors, console_errors = [], [], []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(accept_downloads=True)
        page.on("pageerror", lambda exc: page_errors.append(str(exc)))
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        page.add_init_script(
            """
            localStorage.setItem('smart_extractor_session_token', 'session-token');
            window.confirm = () => true;
            window.prompt = () => 'Auto Name';
            Object.defineProperty(navigator, 'clipboard', {
              value: { writeText: async (text) => { window.__copied = text; } },
              configurable: true
            });
            """
        )
        _mock_api(page, calls)

        page.goto(web_ui_base_url, wait_until="networkidle")
        page.click(".hero-actions [data-open-section='extract']")
        page.wait_for_function(
            "() => !document.getElementById('section-extract').classList.contains('section-hidden')"
        )
        page.click(".nav-item[data-section='overview']")
        page.click(".hero-actions [data-open-section='analyzer']")
        page.wait_for_function(
            "() => !document.getElementById('section-analyzer').classList.contains('section-hidden')"
        )
        page.click(".nav-item[data-section='overview']")
        page.click(".hero-actions [data-open-section='ops-assets']")
        page.wait_for_function(
            "() => !document.getElementById('section-ops-assets').classList.contains('section-hidden')"
        )
        page.click(".nav-item[data-section='tasks']")
        page.wait_for_function(
            "() => !document.getElementById('section-tasks').classList.contains('section-hidden')"
        )
        page.click("#load-quality-btn")
        page.click("#load-cost-btn")
        page.click("#load-audit-btn")
        page.click("#load-review-btn")
        page.click(".nav-item[data-section='assets']")
        page.wait_for_function(
            "() => !document.getElementById('section-assets').classList.contains('section-hidden')"
        )
        page.click("#load-runtime-ops-btn")
        page.click("#load-annotations-btn")
        page.select_option("#learned-profile-filter", "risky")
        page.select_option("#notification-filter", "failed")
        page.click(".nav-item[data-section='ops-assets']")
        page.click("#actor-market-refresh-btn")
        page.click(".nav-item[data-section='backoffice']")
        page.click("#backoffice-refresh-btn")
        page.click(".nav-item[data-section='overview']")
        page.evaluate("document.querySelector(\"#section-overview .quick-entry-card [data-quick-template='market-policy-watch']\").click()")
        page.click(".nav-item[data-section='overview']")
        page.evaluate("document.querySelector(\"#section-overview .quick-entry-card [data-quick-template='market-job-compare']\").click()")
        page.click(".nav-item[data-section='overview']")
        page.evaluate("document.querySelector(\"#section-overview .quick-entry-card [data-quick-template='market-batch-article']\").click()")
        page.click(".nav-item[data-section='overview']")
        page.evaluate("document.querySelector(\"#section-overview .scenario-card [data-quick-template='market-competitor-watch']\").click()")
        page.click(".nav-item[data-section='overview']")
        page.evaluate("document.querySelector(\"#section-overview .scenario-card [data-quick-template='market-product-monitor']\").click()")
        page.click(".nav-item[data-section='overview']")
        page.evaluate("document.querySelector(\"#section-overview .scenario-card [data-quick-template='market-policy-watch']\").click()")
        page.click(".nav-item[data-section='extract']")
        page.wait_for_selector("#nl-task-request", state="visible")
        page.fill("#nl-task-request", "monitor this page")
        page.click("#parse-nl-task-btn")
        page.wait_for_selector("#nl-save-monitor-btn:not([hidden])")
        page.click("#nl-apply-btn")
        page.click("#nl-run-task-btn")
        page.wait_for_timeout(300)
        page.click("#nl-save-monitor-btn")
        page.wait_for_timeout(300)
        page.goto(web_ui_base_url, wait_until="networkidle")
        page.click(".nav-item[data-section='extract']")
        page.wait_for_selector("#nl-task-request", state="visible")
        page.fill("#nl-task-request", "compare these pages")
        page.click("#parse-nl-task-btn")
        page.wait_for_function(
            "() => (document.getElementById('nl-task-result')?.textContent || '').includes('compare_analysis')"
        )
        page.evaluate("document.getElementById('nl-open-compare-btn').click()")

        page.click("[data-section='extract']")
        page.fill("#url", "https://example.com/input")
        page.click("#analyze-page-btn")
        page.click("#clear-fields-btn")
        page.click("#analyze-page-btn")
        page.fill("#monitor-scenario-label", "Scenario")
        page.fill("#monitor-alert-focus", "price")
        page.fill("#monitor-business-goal", "goal")
        page.fill("#monitor-webhook-url", "https://example.com/hook")
        page.fill("#monitor-notification-channels", "webhook|main|https://example.com/hook|secret")
        page.check("#monitor-digest-enabled")
        page.select_option("#monitor-digest-hour", "10")
        page.check("#monitor-schedule-enabled")
        page.select_option("#monitor-schedule-interval", "180")
        page.click("#save-template-btn")
        page.click("#save-monitor-btn")
        page.click("#submit-btn")
        page.click("#batch-append-current-btn")
        page.click("#batch-load-watchlist-btn")
        page.fill("#batch-urls", "https://example.com/a\nhttps://example.com/b")
        page.click("#batch-normalize-btn")
        page.select_option("#batch-domain-filter", "all")
        page.click("#batch-expand-all-btn")
        page.click("#batch-collapse-all-btn")
        page.check("input[name='batch-submit-mode'][value='continue']")
        page.select_option("#batch-group-select", "batch-demo")
        page.check("input[name='batch-submit-mode'][value='new']")
        page.click("#batch-btn")

        page.goto(web_ui_base_url, wait_until="networkidle")
        page.click("[data-section='analyzer']")
        page.fill("#insight-url", "https://example.com/insight")
        page.select_option("#insight-static-mode", "true")
        page.select_option("#insight-goal", "risk")
        page.select_option("#insight-role", "researcher")
        page.fill("#insight-priority", "high")
        page.select_option("#insight-output-format", "actions")
        page.fill("#insight-constraints", "keep it short")
        page.fill("#insight-notes", "Focus on evidence.")
        page.click("#insight-analyze-page-btn")
        page.click("#insight-submit-btn")
        page.wait_for_selector("#insight-results-panel", state="visible")
        for selector in [
            "#insight-export-brief-btn",
            "#insight-export-markdown-btn",
            "#insight-export-csv-btn",
            "#insight-export-json-btn",
        ]:
            with page.expect_download():
                page.evaluate(f"document.querySelector({selector!r}).click()")
        page.evaluate("document.getElementById('insight-mode-compare').click()")
        page.wait_for_function(
            "() => !document.getElementById('insight-compare-mode-panel').classList.contains('section-hidden')"
        )
        page.evaluate("document.getElementById('insight-mode-single').click()")
        page.wait_for_function(
            "() => document.getElementById('insight-compare-mode-panel').classList.contains('section-hidden')"
        )
        page.evaluate("document.getElementById('insight-mode-compare').click()")
        page.evaluate(
            "document.getElementById('compare-urls').value = 'https://example.com/a\\nhttps://example.com/b'"
        )
        page.select_option("#compare-static-mode", "true")
        page.fill("#compare-focus", "price")
        page.select_option("#compare-output-format", "brief")
        page.select_option("#compare-goal", "decision")
        page.select_option("#compare-role", "operator")
        page.fill("#compare-must-have", "stable price")
        page.fill("#compare-elimination", "missing data")
        page.fill("#compare-notes", "Prefer clear recommendation.")
        page.evaluate("document.getElementById('compare-analyze-btn').click()")
        page.evaluate("document.getElementById('insight-submit-btn').click()")

        page.click("[data-section='ops-assets']")
        page.select_option("#template-market-filter", "monitor")
        page.select_option("#template-market-filter", "all")
        page.evaluate("document.querySelector(\"[data-apply-market-template='market-policy-watch']\").click()")
        page.evaluate("document.querySelector(\"[data-install-market-template='market-policy-watch']\").click()")
        page.evaluate("document.querySelector(\"[data-install-actor='actor-commerce']\").click()")
        page.evaluate("document.querySelector(\"[data-apply-template='tpl-1']\").click()")
        page.click("[data-section='assets']")
        page.evaluate("document.getElementById('learned-profile-search').value = 'example'")
        page.select_option("#learned-profile-filter", "active")
        page.evaluate("document.getElementById('learned-profile-bulk-disable-btn').click()")
        page.evaluate("document.getElementById('learned-profile-bulk-relearn-btn').click()")
        page.evaluate("document.querySelector(\"[data-open-learned-profile='lp-1']\").click()")
        page.evaluate("document.getElementById('learned-profile-relearn-btn').click()")
        page.evaluate("document.getElementById('learned-profile-toggle-btn').click()")
        page.evaluate("document.getElementById('learned-profile-reset-btn').click()")
        page.evaluate("document.querySelector('[data-close-learned-profile-drawer]').click()")
        page.evaluate("document.querySelector(\"[data-disable-learned-profile='lp-1']\").click()")
        page.evaluate("document.querySelector(\"[data-reset-learned-profile='lp-1']\").click()")
        page.evaluate("document.querySelector(\"[data-delete-learned-profile='lp-1']\").click()")
        page.click("[data-section='assets']")
        page.select_option("#notification-filter", "failed")
        page.evaluate("document.getElementById('notification-refresh-btn').click()")
        page.evaluate("document.querySelector(\"[data-resend-notification='notif-1']\").click()")
        page.evaluate("document.querySelector(\"[data-run-monitor='mon-1']\").click()")
        page.evaluate("document.querySelector(\"[data-pause-monitor='mon-1']\").click()")
        page.evaluate("document.querySelector(\"[data-resume-monitor='mon-2']\").click()")

        page.goto(f"{web_ui_base_url}/task/task-001", wait_until="networkidle")
        with page.expect_download():
            page.click("#download-docx-btn")
        with page.expect_download():
            page.click("#download-xlsx-btn")
        with page.expect_download():
            page.click("#download-formatted-text-btn")
        page.click("#copy-formatted-text-btn")
        assert page.evaluate("window.__copied") == "Formatted result"
        with page.expect_download():
            page.click("#download-raw-json-btn")
        browser.close()

    assert any(call.startswith("POST /api/extract") for call in calls)
    assert any(call.startswith("POST /api/batch") for call in calls)
    assert any(call.startswith("POST /api/analyze_insight") for call in calls)
    assert any(call.startswith("POST /api/analyze_compare") for call in calls)
    assert page_errors == []
    assert console_errors == []


def test_dashboard_basic_config_controls_smoke(web_ui_base_url):
    calls, page_errors, console_errors = [], [], []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.on("pageerror", lambda exc: page_errors.append(str(exc)))
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        page.add_init_script(
            """
            localStorage.setItem('smart_extractor_session_token', 'session-token');
            window.confirm = () => true;
            window.prompt = () => 'Auto Name';
            """
        )
        _mock_api(page, calls)

        page.goto(web_ui_base_url, wait_until="networkidle")
        page.fill("#api-token", "updated-token")
        page.locator("#api-token").blur()
        page.locator("summary").filter(has_text="基础配置").click()
        page.click("#save-basic-config-btn")
        page.click("#refresh-basic-config-btn", force=True)
        browser.close()

    assert any(call.startswith("GET /api/config/basic") for call in calls)
    assert any(call.startswith("POST /api/config/basic") for call in calls)
    assert page_errors == []
    assert console_errors == []


def test_dashboard_login_screen_smoke(web_ui_base_url):
    calls, page_errors, console_errors = [], [], []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.on("pageerror", lambda exc: page_errors.append(str(exc)))
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        _mock_api(page, calls)

        page.goto(web_ui_base_url, wait_until="networkidle")
        page.click("#auth-login-tab")
        page.click("#auth-register-tab")
        page.wait_for_selector("#login-screen:not(.login-screen-hidden)")
        page.fill("#login-username", "admin")
        page.fill("#login-password", "secret")
        page.click("#login-submit-btn")
        page.wait_for_function(
            "() => document.getElementById('login-screen')?.classList.contains('login-screen-hidden')"
        )
        page.click(".nav-item[data-section='overview']")
        browser.close()

    assert any(call.startswith("POST /api/auth/register") for call in calls)
    assert any(call.startswith("GET /api/dashboard") for call in calls)
    assert page_errors == []
    assert console_errors == []
