from smart_extractor.extractor.specialized_extractors import SpecializedPageExtractor


def test_specialized_product_extractor_prefers_structured_hints():
    result = SpecializedPageExtractor().extract(
        text=(
            "Structured extraction hints:\n"
            "name: Smart Extractor Pro\n"
            "price: USD 29.99\n"
            "availability: InStock\n\n"
            "Cookie banner\nBuy now"
        ),
        source_url="https://example.com/product",
        page_type="product",
        selected_fields=["name", "price", "availability"],
    )

    assert result is not None
    assert result.extraction_strategy == "specialized_rule"
    assert result.data["name"] == "Smart Extractor Pro"
    assert result.data["price"] == "USD 29.99"
    assert result.data["availability"] == "in_stock"


def test_specialized_policy_extractor_returns_policy_number():
    result = SpecializedPageExtractor().extract(
        text=(
            "Structured extraction hints:\n"
            "title: Data Governance Notice\n"
            "publish_date: 20 May 26\n"
            "agency: Digital Office\n"
            "policy_number: No. ABC-2026-7\n\n"
            "No. ABC-2026-7\nContent paragraph with enough detail for policy extraction."
        ),
        source_url="https://example.gov/policy",
        page_type="policy",
    )

    assert result is not None
    assert result.page_type == "policy"
    assert result.data["publish_date"] == "2026-05-20"
    assert result.data["policy_number"] == "No. ABC-2026-7"
