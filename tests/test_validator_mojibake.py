from smart_extractor.models.base import DynamicExtractResult
from smart_extractor.validator.data_validator import DataValidator


def test_dynamic_result_warns_on_mojibake_success_text():
    data = DynamicExtractResult(
        page_type="job",
        selected_fields=["title", "company", "location"],
        candidate_fields=["title", "company", "location"],
        data={
            "title": "\u935a\u5e9d\u7c21\u935f\u6a38\u7d1d\u941e?",
            "company": "Acme",
            "location": "Remote",
        },
    )

    result = DataValidator().validate(data)

    assert result.is_valid is True
    assert result.status == "partial_success"
    assert result.field_incomplete_reason == "decode_mojibake_suspected"
    assert any("decode_mojibake_suspected" in warning for warning in result.warnings)
