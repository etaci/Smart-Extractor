"""Backend-first actor/plugin market packages."""

from __future__ import annotations

import json
from typing import Any


_ACTOR_PACKAGES: list[dict[str, Any]] = [
    {
        "actor_id": "actor-web-change-monitor",
        "name": "通用网页变化监控 Actor",
        "version": "1.0.0",
        "category": "monitoring",
        "package_strength": "core",
        "description": "对标 Apify Actor 的可安装监控包，适合把一次抽取升级为持续网页变化监控。",
        "capabilities": [
            "extract",
            "monitor",
            "notify",
            "history_compare",
            "site_rate_limit",
        ],
        "template_package_id": "market-policy-watch",
        "input_schema": {
            "url": "string",
            "selected_fields": "string[]",
            "schedule_interval_minutes": "number",
        },
        "output_fields": ["changed", "changed_fields", "summary", "quality_score"],
        "default_config": {
            "create_template": True,
            "create_monitor": True,
            "schedule_interval_minutes": 180,
        },
        "pricing_hint": "适合按监控页数或变化通知量收费",
    },
    {
        "actor_id": "actor-product-price-watch",
        "name": "商品价格监控 Actor",
        "version": "1.0.0",
        "category": "commerce",
        "package_strength": "core",
        "description": "围绕价格、库存、促销文案持续巡检，适合作为商品情报类商业包。",
        "capabilities": ["extract", "monitor", "notify", "proxy_pool"],
        "template_package_id": "market-product-monitor",
        "input_schema": {
            "url": "string",
            "schedule_interval_minutes": "number",
            "alert_fields": "string[]",
        },
        "output_fields": ["price", "availability", "brand", "description"],
        "default_config": {
            "create_template": True,
            "create_monitor": True,
            "schedule_interval_minutes": 120,
        },
        "pricing_hint": "适合按 SKU 数、监控频率和通知 SLA 分层定价",
    },
    {
        "actor_id": "actor-job-watch",
        "name": "招聘岗位监控 Actor",
        "version": "1.0.0",
        "category": "recruiting",
        "package_strength": "core",
        "description": "适合持续跟踪岗位、薪资、地点和要求变化，支撑招聘运营或求职情报。",
        "capabilities": ["extract", "monitor", "notify", "annotation_feedback"],
        "template_package_id": "market-job-compare",
        "input_schema": {
            "url": "string",
            "schedule_interval_minutes": "number",
        },
        "output_fields": ["title", "company", "salary_range", "location", "requirements"],
        "default_config": {
            "create_template": True,
            "create_monitor": True,
            "schedule_interval_minutes": 360,
        },
        "pricing_hint": "适合按岗位页数、监控频率与结构化字段质量收费",
    },
    {
        "actor_id": "actor-news-announcement-watch",
        "name": "新闻公告监控 Actor",
        "version": "1.0.0",
        "category": "intelligence",
        "package_strength": "core",
        "description": "持续跟踪新闻、公告、政策页面的标题、发布日期和正文变化。",
        "capabilities": ["extract", "monitor", "digest", "notify", "history_compare"],
        "template_package_id": "market-policy-watch",
        "input_schema": {
            "url": "string",
            "digest_hour": "number",
            "schedule_interval_minutes": "number",
        },
        "output_fields": ["title", "publish_date", "content", "summary"],
        "default_config": {
            "create_template": True,
            "create_monitor": True,
            "schedule_interval_minutes": 180,
        },
        "pricing_hint": "适合按监控源数、摘要产出频率和通知数收费",
    },
]


def list_actor_packages() -> list[dict[str, Any]]:
    return [json.loads(json.dumps(item, ensure_ascii=False)) for item in _ACTOR_PACKAGES]


def get_actor_package(actor_id: str) -> dict[str, Any] | None:
    normalized = str(actor_id or "").strip()
    for item in _ACTOR_PACKAGES:
        if item["actor_id"] == normalized:
            return json.loads(json.dumps(item, ensure_ascii=False))
    return None
