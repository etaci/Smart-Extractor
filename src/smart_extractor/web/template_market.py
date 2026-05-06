"""Curated backend-first template packages."""

from __future__ import annotations

import json
from typing import Any


_MARKET_TEMPLATES: list[dict[str, Any]] = [
    {
        "template_id": "market-product-monitor",
        "name": "商品价格监控",
        "description": "持续跟踪商品页的价格、库存、促销信息与卖点变化，适合电商运营、渠道和定价团队。",
        "category": "monitor",
        "package_strength": "core",
        "growth_goal": "一次抽取后，快速升级成价格与库存持续监控",
        "page_type": "product",
        "schema_name": "product",
        "storage_format": "json",
        "use_static": False,
        "selected_fields": ["name", "price", "brand", "availability", "description"],
        "field_labels": {
            "name": "商品名称",
            "price": "价格",
            "brand": "品牌",
            "availability": "库存状态",
            "description": "商品描述",
        },
        "tags": ["电商", "价格", "库存", "监控"],
        "target_users": ["电商运营", "渠道管理", "定价策略"],
        "default_outputs": ["价格变动摘要", "补货预警", "促销跟踪简报"],
        "notification_capable": True,
        "sample_url": "https://example.com/product",
        "recommended_schedule": {"enabled": True, "interval_minutes": 120},
        "activation_steps": [
            "先确认价格、库存、卖点字段能稳定抽取",
            "保存为模板后立即开启持续监控",
            "配置变更通知，把降价和缺货直接推送给业务方",
        ],
        "profile": {
            "scenario_label": "商品价格监控",
            "business_goal": "第一时间发现价格、库存、促销文案变化，支撑调价、促销复盘和竞价响应。",
            "alert_focus": "价格、库存、促销、商品描述",
            "notify_on": ["changed", "error"],
            "summary_style": "price_monitor_brief",
            "notification_cooldown_minutes": 30,
            "min_change_count": 1,
            "playbook": [
                "价格变化后先确认是否是活动价、会员价或临时券后价。",
                "库存异常时同步给运营和供应链，避免继续投流。",
                "卖点变更后复核竞品策略与站内素材是否要同步调整。",
            ],
        },
    },
    {
        "template_id": "market-job-compare",
        "name": "招聘岗位监控",
        "description": "跟踪岗位页的薪资、地点、部门、职责和任职要求变化，适合招聘运营、HRBP 和求职情报场景。",
        "category": "monitor",
        "package_strength": "core",
        "growth_goal": "从一次岗位抽取升级为持续监控招聘要求变化",
        "page_type": "job",
        "schema_name": "job",
        "storage_format": "json",
        "use_static": False,
        "selected_fields": ["title", "company", "salary_range", "location", "requirements"],
        "field_labels": {
            "title": "岗位名称",
            "company": "公司",
            "salary_range": "薪资范围",
            "location": "工作地点",
            "requirements": "任职要求",
        },
        "tags": ["招聘", "岗位", "薪资", "监控"],
        "target_users": ["招聘运营", "HRBP", "求职情报"],
        "default_outputs": ["岗位变更摘要", "薪资调整提醒", "招聘策略观察"],
        "notification_capable": True,
        "sample_url": "https://example.com/job",
        "recommended_schedule": {"enabled": True, "interval_minutes": 360},
        "activation_steps": [
            "确认岗位名称、薪资、地点、要求字段已抽取完整",
            "保存模板后开启岗位页持续监控",
            "把要求与薪资变化推送给招聘或用人团队",
        ],
        "profile": {
            "scenario_label": "招聘岗位监控",
            "business_goal": "跟踪招聘岗位的薪资和要求变化，及时发现 JD 调整、HC 变化与招聘策略收缩/放宽。",
            "alert_focus": "薪资、地点、任职要求、岗位名称",
            "notify_on": ["changed", "error"],
            "summary_style": "job_monitor_brief",
            "notification_cooldown_minutes": 60,
            "min_change_count": 1,
            "playbook": [
                "薪资范围变化后优先确认是否影响候选人搜寻策略。",
                "要求变更后同步给招聘和用人经理，避免旧版本 JD 继续流转。",
                "地点调整后复核是否需要切换投放渠道和候选人池。",
            ],
        },
    },
    {
        "template_id": "market-policy-watch",
        "name": "新闻/公告监控",
        "description": "跟踪新闻、公告、政策页的标题、发布日期、正文和摘要变化，适合资讯运营、合规、投研与品牌团队。",
        "category": "monitor",
        "package_strength": "core",
        "growth_goal": "把一次资讯抽取升级为持续的新闻/公告变化监控",
        "page_type": "news",
        "schema_name": "news",
        "storage_format": "json",
        "use_static": True,
        "selected_fields": ["title", "publish_date", "content", "summary"],
        "field_labels": {
            "title": "标题",
            "publish_date": "发布日期",
            "content": "正文",
            "summary": "摘要",
        },
        "tags": ["新闻", "公告", "政策", "监控"],
        "target_users": ["资讯运营", "合规团队", "投研分析", "品牌公关"],
        "default_outputs": ["更新摘要", "公告变更提醒", "每日重点简报"],
        "notification_capable": True,
        "sample_url": "https://example.com/news",
        "recommended_schedule": {"enabled": True, "interval_minutes": 180},
        "activation_steps": [
            "确认标题、发布时间、正文、摘要已抽取稳定",
            "保存模板后开启持续监控",
            "为 changed/error 配置通知，避免错过关键公告版本更新",
        ],
        "profile": {
            "scenario_label": "新闻/公告监控",
            "business_goal": "及时发现新闻、公告、政策页的标题、发布时间与正文变化，降低漏读、晚读和误读风险。",
            "alert_focus": "标题、发布日期、摘要、正文",
            "notify_on": ["changed", "error"],
            "summary_style": "news_monitor_brief",
            "digest_enabled": True,
            "digest_hour": 9,
            "playbook": [
                "标题或发布时间变化后先判断是新稿上线还是旧稿修订。",
                "正文变化后导出对比摘要，优先同步给合规、运营或投研负责人。",
                "连续多次更新的页面建议保留监控并开启日报汇总。",
            ],
        },
    },
]

_LEGACY_TEMPLATE_ALIASES: dict[str, str] = {
    "market-news-brief": "market-policy-watch",
    "market-job-monitor": "market-job-compare",
    "market-news-announcement-monitor": "market-policy-watch",
    "market-competitor-watch": "market-product-monitor",
    "market-batch-article": "market-policy-watch",
}


def list_market_templates() -> list[dict[str, Any]]:
    return [json.loads(json.dumps(item, ensure_ascii=False)) for item in _MARKET_TEMPLATES]


def get_market_template(template_id: str) -> dict[str, Any] | None:
    normalized = str(template_id or "").strip()
    normalized = _LEGACY_TEMPLATE_ALIASES.get(normalized, normalized)
    for item in _MARKET_TEMPLATES:
        if item["template_id"] == normalized:
            return json.loads(json.dumps(item, ensure_ascii=False))
    return None
