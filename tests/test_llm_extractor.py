import json

from smart_extractor.config import LLMConfig
from smart_extractor.extractor.llm_extractor import (
    LLMExtractor,
    _extract_chat_message_content,
    _safe_json_loads,
)


def test_extract_chat_message_content_from_standard_dict_response():
    response = {
        "choices": [
            {
                "message": {
                    "content": '{"page_type":"job","data":{"title":"Python工程师"}}'
                }
            }
        ]
    }

    content = _extract_chat_message_content(response)

    assert _safe_json_loads(content)["data"]["title"] == "Python工程师"


def test_extract_chat_message_content_from_sse_text_response():
    sse_response = "\n".join(
        [
            'data: {"choices":[{"delta":{"content":"{\\"page_type\\":\\"job\\","}}]}',
            'data: {"choices":[{"delta":{"content":"\\"data\\":{\\"title\\":\\"Python工程师\\"}}"}}]}',
            "data: [DONE]",
        ]
    )

    content = _extract_chat_message_content(sse_response)

    assert _safe_json_loads(content) == {
        "page_type": "job",
        "data": {"title": "Python工程师"},
    }


def test_safe_json_loads_can_extract_embedded_json():
    wrapped = (
        "prefix "
        + json.dumps(
            {"page_type": "job", "data": {"title": "测试"}}, ensure_ascii=False
        )
        + " suffix"
    )

    payload = _safe_json_loads(wrapped)

    assert payload["data"]["title"] == "测试"


def test_build_context_fallback_includes_user_context():
    page_result = type(
        "PageResult",
        (),
        {
            "page_type": "product",
            "selected_fields": ["title", "price"],
            "candidate_fields": ["title", "price"],
            "field_labels": {"title": "标题", "price": "价格"},
            "data": {"title": "测试商品", "price": "1999"},
            "formatted_text": "“标题”：“测试商品”\n“价格”：“1999”",
        },
    )()

    payload = LLMExtractor._build_context_fallback(
        page_result,
        {
            "goal": "decision",
            "role": "consumer",
            "priority": "价格",
            "constraints": "预算 2000 内",
            "notes": "给家人买",
        },
    )

    assert payload["confidence"] == "medium"
    assert "decision" in payload["summary"]
    assert payload["evidence_spans"][0]["label"] == "标题"


def test_build_compare_fallback_includes_matrix_and_evidence():
    payload = LLMExtractor._build_compare_fallback(
        [
            {
                "url": "https://example.com/a",
                "page_type": "product",
                "page_type_label": "商品页",
                "preview": "对象 A 预览",
                "data": {"title": "对象 A"},
            },
            {
                "url": "https://example.com/b",
                "page_type": "product",
                "page_type_label": "商品页",
                "preview": "对象 B 预览",
                "data": {"title": "对象 B"},
            },
        ],
        {
            "goal": "comparison",
            "role": "consumer",
            "focus": "价格",
            "must_have": "预算内",
            "elimination": "无售后",
        },
    )

    assert payload["confidence"] == "medium"
    assert payload["comparison_matrix"][0]["label"] == "对象 1"
    assert payload["evidence_spans"][0]["label"] == "对象 1"
    assert payload["report"]["title"] == "差异对比报告"
    assert payload["report"]["difference_points"]


def test_build_task_plan_fallback_parses_compare_request():
    payload = LLMExtractor._build_task_plan_fallback(
        "帮我对比这两个商品页的价格和品牌 https://example.com/a https://example.com/b"
    )

    assert payload["task_type"] == "compare_analysis"
    assert len(payload["urls"]) == 2
    assert "price" in payload["selected_fields"]


def test_extract_dynamic_fallback_uses_rule_fields_when_llm_unavailable(monkeypatch):
    extractor = LLMExtractor(
        LLMConfig(
            api_key="test-key",
            base_url="https://example.com/v1",
            model="test-model",
            timeout=5,
        )
    )
    monkeypatch.setattr(
        extractor,
        "_call_json_llm",
        lambda **_: (_ for _ in ()).throw(RuntimeError("mock llm down")),
    )

    result = extractor.extract_dynamic(
        text=(
            "Python 工程师\n"
            "公司：OpenAI\n"
            "工作地点：上海\n"
            "薪资：20k-30k/月\n"
            "任职要求：熟悉 Python、FastAPI 和测试体系。"
        ),
        source_url="https://example.com/jobs/1",
    )

    assert result.extraction_strategy == "fallback"
    assert result.page_type == "job"
    assert result.data["company"] == "OpenAI"
    assert "20k-30k/月" in result.data["salary_range"]
    assert "requirements" in result.data
