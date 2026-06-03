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


def test_specialized_pricing_extractor_keeps_free_and_enterprise_tiers():
    result = SpecializedPageExtractor().extract(
        text=(
            "Structured extraction hints:\n"
            "plan: Pro\n"
            "price: USD 49 /month\n"
            "billing_period: monthly\n"
            "free_tier: Free\n"
            "enterprise_tier: Enterprise\n\n"
            "Free forever. Enterprise custom pricing. Pro starts at $49 per user/month."
        ),
        source_url="https://example.com/pricing",
        page_type="pricing",
        selected_fields=["plan", "price", "billing_period", "free_tier", "enterprise_tier"],
    )

    assert result is not None
    assert result.extraction_strategy == "specialized_rule"
    assert result.data["free_tier"] == "Free"
    assert result.data["enterprise_tier"] == "Enterprise"
    assert result.data["billing_period"] == "per month"


def test_specialized_job_extractor_keeps_ats_fields_from_hints():
    result = SpecializedPageExtractor().extract(
        text=(
            "Structured extraction hints:\n"
            "title: Staff Backend Engineer\n"
            "company: Acme AI\n"
            "location: Remote\n"
            "employment_type: Full-time\n"
            "job_id: WD-42\n"
            "requirements: Build reliable extraction systems\n"
            "ats_platform: workday\n"
            "job_page_kind: detail\n\n"
            "Careers page"
        ),
        source_url="https://example.com/jobs/wd-42",
        page_type="job",
        selected_fields=["title", "company", "location", "employment_type", "job_id", "requirements"],
    )

    assert result is not None
    assert result.data["employment_type"] == "Full-time"
    assert result.data["job_id"] == "WD-42"
    source_fields = result.strategy_details["source_fields"]
    assert source_fields["_ats_platform"] == "workday"
    assert source_fields["_job_page_kind"] == "detail"
