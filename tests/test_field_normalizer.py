from smart_extractor.extractor.field_normalizer import (
    normalize_billing_period,
    normalize_date,
    normalize_price,
)


def test_normalize_price_removes_marketing_prefix_and_keeps_period():
    assert normalize_price("From $499 per seat/month") == "USD 499 per seat/month"


def test_normalize_date_accepts_short_english_dates():
    assert normalize_date("20 May 26") == "2026-05-20"


def test_normalize_billing_period_accepts_common_variants():
    assert normalize_billing_period("$19 /mo") == "per month"
    assert normalize_billing_period("billed annually") == "per year"
