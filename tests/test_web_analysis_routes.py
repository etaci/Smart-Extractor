from tests.web_route_testkit import _build_test_client


def test_api_analyze_page_returns_localized_labels(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)

    class DummyPipeline:
        def __init__(self, config=None, use_dynamic_fetcher=True):
            self.config = config
            self.use_dynamic_fetcher = use_dynamic_fetcher

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def analyze_page(self, url):
            return {
                "page_type": "listing",
                "candidate_fields": [
                    "title",
                    "content",
                    "summary",
                    "sections",
                    "related_terms",
                ],
                "field_labels": {
                    "title": "标题",
                    "content": "正文内容",
                    "summary": "总结说明",
                    "sections": "章节",
                    "related_terms": "相关词",
                },
                "preview": "“标题”：“示例页面”",
            }

    import smart_extractor.pipeline as pipeline_module

    monkeypatch.setattr(pipeline_module, "ExtractionPipeline", DummyPipeline)

    response = client.post(
        "/api/analyze_page",
        headers={"X-API-Token": "test-token"},
        json={
            "url": "https://example.com/list",
            "use_static": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["page_type"] == "listing"
    assert payload["page_type_label"] == "列表页"
    assert payload["field_labels"]["title"] == "标题"
    assert payload["field_labels"]["related_terms"] == "相关词"


def test_api_analyze_insight_returns_contextual_analysis(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)

    class DummyPipeline:
        def __init__(self, config=None, use_dynamic_fetcher=True):
            self.config = config
            self.use_dynamic_fetcher = use_dynamic_fetcher

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def analyze_with_context(self, url, user_context):
            assert user_context["goal"] == "decision"
            assert user_context["role"] == "consumer"
            return {
                "page_type": "product",
                "page_type_label": "商品页",
                "page_preview": "“标题”：“测试商品”",
                "candidate_fields": ["name", "price"],
                "field_labels": {"name": "商品名", "price": "价格"},
                "analysis": {
                    "headline": "测试商品购买判断",
                    "summary": "如果你更看重价格与基础功能，这个页面可以作为候选方案。",
                    "confidence": "high",
                    "key_points": ["价格信息明确", "页面已给出基础描述"],
                    "risks": ["缺少售后说明"],
                    "recommended_actions": ["继续对比售后与口碑"],
                    "missing_information": ["你的预算上限"],
                    "evidence_spans": [
                        {"label": "价格", "snippet": "价格：1999"},
                    ],
                },
            }

    import smart_extractor.pipeline as pipeline_module

    monkeypatch.setattr(pipeline_module, "ExtractionPipeline", DummyPipeline)

    response = client.post(
        "/api/analyze_insight",
        headers={"X-API-Token": "test-token"},
        json={
            "url": "https://example.com/product",
            "use_static": True,
            "goal": "decision",
            "role": "consumer",
            "priority": "价格",
            "constraints": "预算 2000 内",
            "notes": "给家人买",
            "output_format": "cards",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["page_type"] == "product"
    assert payload["analysis"]["headline"] == "测试商品购买判断"
    assert payload["analysis"]["confidence"] == "high"
    assert payload["analysis"]["evidence_spans"][0]["label"] == "价格"


def test_api_analyze_compare_preview_returns_multiple_items(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)

    class DummyPipeline:
        def __init__(self, config=None, use_dynamic_fetcher=True):
            self.config = config
            self.use_dynamic_fetcher = use_dynamic_fetcher

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def analyze_page(self, url):
            return {
                "page_type": "product",
                "candidate_fields": ["name", "price"],
                "field_labels": {"name": "商品名", "price": "价格"},
                "preview": f"预览: {url}",
            }

    import smart_extractor.pipeline as pipeline_module

    monkeypatch.setattr(pipeline_module, "ExtractionPipeline", DummyPipeline)

    response = client.post(
        "/api/analyze_compare_preview",
        headers={"X-API-Token": "test-token"},
        json={
            "urls": ["https://example.com/a", "https://example.com/b"],
            "use_static": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 2
    assert payload["items"][0]["page_type_label"] == "商品页"


def test_api_analyze_compare_returns_comparison_result(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)

    class DummyPipeline:
        def __init__(self, config=None, use_dynamic_fetcher=True):
            self.config = config
            self.use_dynamic_fetcher = use_dynamic_fetcher

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return None

        def analyze_many_with_context(self, urls, user_context):
            assert len(urls) == 2
            assert user_context["focus"] == "价格"
            return {
                "page_type": "comparison",
                "page_type_label": "多页对比",
                "page_preview": "已完成对比",
                "items": [
                    {"url": urls[0], "page_type": "product", "page_type_label": "商品页", "candidate_fields": ["price"], "field_labels": {"price": "价格"}, "preview": "A"},
                    {"url": urls[1], "page_type": "product", "page_type_label": "商品页", "candidate_fields": ["price"], "field_labels": {"price": "价格"}, "preview": "B"},
                ],
                "comparison_matrix": [
                    {"label": "价格", "summary": "A 更低，B 功能描述更完整"},
                ],
                "report": {
                    "title": "差异对比报告",
                    "executive_summary": "A 价格更低，但还需确认售后。",
                    "common_points": ["两者都属于商品页"],
                    "difference_points": ["A 价格更低", "B 描述更完整"],
                    "recommendation": "优先继续核对售后后再决定。",
                    "next_steps": ["补充售后信息"],
                },
                "analysis": {
                    "headline": "两款商品对比",
                    "summary": "如果只看价格，A 更有优势。",
                    "confidence": "high",
                    "key_points": ["A 价格更低"],
                    "risks": ["B 的价格说明不够明确"],
                    "recommended_actions": ["继续比较售后"],
                    "missing_information": ["你的预算上限"],
                    "evidence_spans": [{"label": "价格", "snippet": "A: 1999 / B: 2499"}],
                },
            }

    import smart_extractor.pipeline as pipeline_module

    monkeypatch.setattr(pipeline_module, "ExtractionPipeline", DummyPipeline)

    response = client.post(
        "/api/analyze_compare",
        headers={"X-API-Token": "test-token"},
        json={
            "urls": ["https://example.com/a", "https://example.com/b"],
            "use_static": True,
            "goal": "comparison",
            "role": "consumer",
            "focus": "价格",
            "must_have": "预算 2500 内",
            "elimination": "无售后",
            "notes": "给家人买",
            "output_format": "table",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["page_type"] == "comparison"
    assert payload["analysis"]["headline"] == "两款商品对比"
    assert payload["comparison_matrix"][0]["label"] == "价格"
    assert payload["report"]["title"] == "差异对比报告"


def test_api_natural_language_task_returns_plan(monkeypatch, tmp_path):
    client, _ = _build_test_client(monkeypatch, tmp_path)

    class DummyExtractor:
        def __init__(self, config=None):
            self.config = config

        def parse_task_request(self, request_text):
            assert "监控" in request_text
            return {
                "task_type": "monitor",
                "summary": "已生成监控草案",
                "urls": ["https://example.com/item"],
                "selected_fields": ["price", "stock"],
                "use_static": False,
                "storage_format": "json",
                "schema_name": "product",
                "name": "商品监控",
                "confidence": "high",
                "warnings": ["当前不会自动定时执行"],
            }

    import smart_extractor.extractor.llm_extractor as extractor_module

    monkeypatch.setattr(extractor_module, "LLMExtractor", DummyExtractor)

    response = client.post(
        "/api/nl_task",
        headers={"X-API-Token": "test-token"},
        json={
            "request_text": "帮我监控这个商品页的价格和库存变化 https://example.com/item",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["plan"]["task_type"] == "monitor"
    assert payload["plan"]["selected_fields"] == ["price", "stock"]
