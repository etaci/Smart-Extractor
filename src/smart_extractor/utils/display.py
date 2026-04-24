"""展示层使用的中文标签映射。"""

from __future__ import annotations

from typing import Optional


FIELD_LABELS: dict[str, str] = {
    "title": "标题",
    "name": "名称",
    "content": "正文内容",
    "summary": "总结说明",
    "description": "描述",
    "sections": "章节",
    "related_terms": "相关词",
    "company": "公司",
    "salary_range": "薪资范围",
    "location": "地点",
    "requirements": "任职要求",
    "job_type": "工作类型",
    "experience_required": "经验要求",
    "education_required": "学历要求",
    "benefits": "福利待遇",
    "skills": "技能标签",
    "posted_date": "发布日期",
    "publish_date": "发布日期",
    "author": "作者",
    "data_stats": "数据统计",
    "price": "价格",
    "brand": "品牌",
    "specifications": "规格参数",
    "category": "分类",
    "tags": "标签",
    "url": "链接",
    "id": "编号",
    "featured_section": "主推荐区",
    "category_groups": "分类分组",
    "platform_features": "平台特色",
}

PAGE_TYPE_LABELS: dict[str, str] = {
    "job": "招聘页",
    "news": "新闻页",
    "product": "商品页",
    "article": "文章页",
    "blog": "博客页",
    "video": "视频页",
    "forum": "论坛页",
    "profile": "资料页",
    "listing": "列表页",
    "unknown": "未知",
}


def get_field_label(field_name: str, field_labels: Optional[dict[str, str]] = None) -> str:
    name = str(field_name or "").strip()
    if not name:
        return ""

    if field_labels:
        custom = str(field_labels.get(name) or "").strip()
        if custom:
            return custom

    mapped = FIELD_LABELS.get(name)
    if mapped:
        return mapped

    return name.replace("_", " ")


def get_page_type_label(page_type: str) -> str:
    normalized = str(page_type or "unknown").strip().lower() or "unknown"
    return PAGE_TYPE_LABELS.get(normalized, normalized)


def build_field_labels(fields: list[str], field_labels: Optional[dict[str, str]] = None) -> dict[str, str]:
    return {field: get_field_label(field, field_labels) for field in fields if str(field or "").strip()}
