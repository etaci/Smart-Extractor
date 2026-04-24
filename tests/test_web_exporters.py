from smart_extractor.web.exporters import (
    build_task_docx,
    build_task_markdown,
    build_task_xlsx,
)


def _detail_payload():
    return {
        "task_id": "task-000001",
        "url": "https://example.com/article",
        "status": "success",
        "storage_format": "json",
        "domain": "example.com",
        "created_at": "2026-04-20 12:00:00",
        "completed_at": "2026-04-20 12:00:02",
        "elapsed_ms": 2300.0,
        "quality_score": 0.95,
        "data": {
            "page_type": "article",
            "formatted_text": "这是一段润色结果",
            "data": {"title": "标题"},
            "_llm_usage": {
                "total_calls": 2,
                "prompt_tokens": 320,
                "completion_tokens": 80,
                "estimated_cost_usd": 0.00042,
            },
        },
        "comparison": {
            "has_previous": True,
            "changed": True,
            "changed_fields_count": 1,
            "summary_lines": ["标题从旧值变为新值"],
            "impact_summary": "主要变动集中在标题",
            "suggested_actions": ["先人工复核变化字段"],
            "changed_fields": [
                {
                    "field": "title",
                    "label": "标题",
                    "change_type": "updated",
                    "before": "旧值",
                    "after": "新值",
                    "summary": "标题从旧值变为新值",
                }
            ],
        },
        "recent_history": [
            {
                "task_id": "task-000000",
                "status": "success",
                "quality_score": 0.9,
                "created_at": "2026-04-20 11:00:00",
                "completed_at": "2026-04-20 11:00:02",
            }
        ],
    }


def test_build_task_markdown_contains_core_sections():
    content = build_task_markdown(_detail_payload())

    assert "# Smart Extractor 任务报告" in content
    assert "## 任务概览" in content
    assert "## 变化告警" in content
    assert "## 提取结果" in content


def test_build_task_docx_returns_binary_content():
    content = build_task_docx(_detail_payload())

    assert isinstance(content, bytes)
    assert len(content) > 100


def test_build_task_xlsx_returns_binary_content():
    content = build_task_xlsx(_detail_payload())

    assert isinstance(content, bytes)
    assert len(content) > 100
