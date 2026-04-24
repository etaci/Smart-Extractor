"""
任务模板市场。
"""

from __future__ import annotations

import json
from typing import Any


_MARKET_TEMPLATES: list[dict[str, Any]] = [
    {
        "template_id": "market-product-monitor",
        "name": "商品价格监控",
        "description": "适合持续追踪商品页的价格、库存、品牌和描述变化，第一时间发现降价或缺货。",
        "category": "monitor",
        "page_type": "product",
        "schema_name": "product",
        "storage_format": "json",
        "use_static": False,
        "selected_fields": ["name", "price", "brand", "description"],
        "field_labels": {
            "name": "商品名",
            "price": "价格",
            "brand": "品牌",
            "description": "描述",
        },
        "tags": ["电商", "监控", "价格"],
        "target_users": ["电商运营", "价格策略", "渠道同学"],
        "default_outputs": ["价格异动摘要", "运营执行版", "活动复盘卡"],
        "notification_capable": True,
        "sample_url": "https://example.com/product",
        "profile": {
            "scenario_label": "电商价格监控",
            "business_goal": "快速发现价格、库存和卖点变化，支持运营调价与活动复盘。",
            "alert_focus": "价格、库存、品牌卖点",
            "notify_on": ["changed", "error"],
            "playbook": [
                "价格变化后，先确认是否命中活动价或临时折扣。",
                "库存异常时，尽快同步给运营或商品同学。",
            ],
        },
    },
    {
        "template_id": "market-news-brief",
        "name": "新闻摘要提取",
        "description": "提取新闻标题、正文、发布时间和摘要，适合资讯汇总与每日简报。",
        "category": "extract",
        "page_type": "news",
        "schema_name": "news",
        "storage_format": "json",
        "use_static": True,
        "selected_fields": ["title", "content", "publish_date", "summary"],
        "field_labels": {
            "title": "标题",
            "content": "正文",
            "publish_date": "发布时间",
            "summary": "摘要",
        },
        "tags": ["新闻", "摘要", "内容"],
        "target_users": ["内容运营", "投研分析", "品牌团队"],
        "default_outputs": ["晨会摘要", "日报简报", "内容归档版"],
        "notification_capable": True,
        "sample_url": "https://example.com/news",
        "profile": {
            "scenario_label": "资讯摘要简报",
            "business_goal": "把长内容压缩成可读摘要，方便日报、晨会或投研整理。",
            "alert_focus": "标题、摘要、发布时间",
            "notify_on": ["changed"],
            "playbook": [
                "优先复核发布时间和核心结论，避免旧闻重复进入简报。",
            ],
        },
    },
    {
        "template_id": "market-job-compare",
        "name": "岗位对比分析",
        "description": "适合拿多个岗位页做薪资、要求、地点和公司信息对比。",
        "category": "compare",
        "page_type": "job",
        "schema_name": "job",
        "storage_format": "json",
        "use_static": False,
        "selected_fields": ["title", "company", "salary_range", "location", "requirements"],
        "field_labels": {
            "title": "岗位名",
            "company": "公司",
            "salary_range": "薪资",
            "location": "地点",
            "requirements": "要求",
        },
        "tags": ["招聘", "对比", "岗位"],
        "target_users": ["求职者", "招聘运营", "团队负责人"],
        "default_outputs": ["岗位对比卡", "投递决策版", "面试复盘提纲"],
        "notification_capable": False,
        "sample_url": "https://example.com/job",
        "profile": {
            "scenario_label": "岗位对比决策",
            "business_goal": "比较多个岗位的薪资、要求和地点差异，快速沉淀面试投递判断。",
            "alert_focus": "薪资、地点、要求",
            "notify_on": ["changed"],
            "playbook": [
                "重点核对薪资区间和硬性要求变化，避免误判匹配度。",
            ],
        },
    },
    {
        "template_id": "market-batch-article",
        "name": "文章批量采集",
        "description": "适合批量抓取文章标题、作者、发布时间和正文内容。",
        "category": "batch",
        "page_type": "article",
        "schema_name": "auto",
        "storage_format": "csv",
        "use_static": True,
        "selected_fields": ["title", "author", "publish_date", "content"],
        "field_labels": {
            "title": "标题",
            "author": "作者",
            "publish_date": "发布时间",
            "content": "正文",
        },
        "tags": ["批量", "文章", "采集"],
        "target_users": ["内容团队", "知识库维护", "SEO 团队"],
        "default_outputs": ["内容归档", "CSV 批量结果", "来源清单"],
        "notification_capable": False,
        "sample_url": "https://example.com/article",
        "profile": {
            "scenario_label": "内容资产沉淀",
            "business_goal": "批量沉淀文章正文与作者信息，支持知识库、SEO 或内容分析。",
            "alert_focus": "标题、作者、发布时间",
            "notify_on": ["error"],
            "playbook": [
                "优先检查失败站点，保证批量采集覆盖率。",
            ],
        },
    },
    {
        "template_id": "market-policy-watch",
        "name": "政策页面变更追踪",
        "description": "追踪政策、公告、帮助中心页面的正文与发布时间变化，适合合规与运营团队。",
        "category": "monitor",
        "page_type": "article",
        "schema_name": "auto",
        "storage_format": "json",
        "use_static": True,
        "selected_fields": ["title", "publish_date", "content", "summary"],
        "field_labels": {
            "title": "标题",
            "publish_date": "发布时间",
            "content": "正文",
            "summary": "摘要",
        },
        "tags": ["政策", "公告", "监控"],
        "target_users": ["合规团队", "运营团队", "法务支持"],
        "default_outputs": ["更新摘要卡", "风险复核版", "领导汇报版"],
        "notification_capable": True,
        "sample_url": "https://example.com/policy",
        "profile": {
            "scenario_label": "政策更新监控",
            "business_goal": "发现政策或公告正文的版本变化，降低漏读风险。",
            "alert_focus": "发布时间、正文、摘要",
            "notify_on": ["changed", "error"],
            "playbook": [
                "政策正文发生变化后，先导出报告再同步合规负责人。",
                "如果发布时间更新但正文未变，建议人工确认是否只是页头刷新。",
            ],
        },
    },
    {
        "template_id": "market-competitor-watch",
        "name": "竞品卖点变化监控",
        "description": "持续跟踪竞品首页或产品页的标题、卖点和描述变化，适合市场与产品团队。",
        "category": "monitor",
        "page_type": "product",
        "schema_name": "auto",
        "storage_format": "json",
        "use_static": False,
        "selected_fields": ["title", "summary", "description", "content"],
        "field_labels": {
            "title": "标题",
            "summary": "总结",
            "description": "描述",
            "content": "正文",
        },
        "tags": ["竞品", "卖点", "监控"],
        "target_users": ["产品经理", "市场团队", "商业分析"],
        "default_outputs": ["卖点变化简报", "竞品同步版", "策略复盘卡"],
        "notification_capable": True,
        "sample_url": "https://example.com/competitor",
        "profile": {
            "scenario_label": "竞品变化监控",
            "business_goal": "识别竞品卖点和文案策略变化，为市场与产品策略提供输入。",
            "alert_focus": "标题、卖点、正文",
            "notify_on": ["changed", "error"],
            "playbook": [
                "卖点变化后，建议立即对比自家页面是否需要同步调整。",
            ],
        },
    },
]


def list_market_templates() -> list[dict[str, Any]]:
    return [json.loads(json.dumps(item, ensure_ascii=False)) for item in _MARKET_TEMPLATES]


def get_market_template(template_id: str) -> dict[str, Any] | None:
    normalized = str(template_id or "").strip()
    for item in _MARKET_TEMPLATES:
        if item["template_id"] == normalized:
            return json.loads(json.dumps(item, ensure_ascii=False))
    return None
