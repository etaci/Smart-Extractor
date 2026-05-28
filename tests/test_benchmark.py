from types import SimpleNamespace

from smart_extractor.benchmark import (
    BenchmarkRunner,
    BenchmarkSample,
    build_benchmark_report,
    load_benchmark_samples,
)
from smart_extractor.fetcher.base import FetchResult
from smart_extractor.models.base import DynamicExtractResult


def test_benchmark_report_splits_fetch_content_extraction_and_fields():
    records = [
        {
            "url": "https://example.com/a",
            "success": True,
            "fetch_success": True,
            "content_ready": True,
            "llm_used": True,
            "field_valid_rate": 0.75,
            "elapsed_ms": 100,
        },
        {
            "url": "https://example.com/b",
            "success": False,
            "fetch_success": False,
            "content_ready": False,
            "llm_used": False,
            "field_valid_rate": 0.0,
            "failure_reason": "connect_timeout",
            "elapsed_ms": 300,
        },
    ]

    report = build_benchmark_report(records, run_label="fixed")

    assert report["fetch_success_rate"] == 0.5
    assert report["content_ready_rate"] == 1.0
    assert report["extraction_success_on_fetched_pages"] == 1.0
    assert report["llm_success_rate"] == 1.0
    assert report["field_valid_rate_on_success"] == 0.75
    assert report["failure_breakdown"]["connect_timeout"] == 1
    assert report["slow_samples"][0]["url"] == "https://example.com/b"


def test_benchmark_runner_records_fetch_diagnostics(tmp_path):
    class DummyPipeline:
        def run(self, url, **kwargs):
            return SimpleNamespace(
                success=True,
                elapsed_ms=42.0,
                cleaned_text="title\nprice",
                error="",
                fetch_result=FetchResult(
                    url=url,
                    html="<html>ok</html>",
                    status_code=200,
                    diagnostics={
                        "failure_stage": "",
                        "failure_reason": "",
                        "http_status": 200,
                    },
                ),
                data=DynamicExtractResult(
                    page_type="product",
                    selected_fields=["name", "price"],
                    data={"name": "Widget", "price": "99"},
                    extraction_strategy="llm",
                ),
                validation=SimpleNamespace(quality_score=1.0),
            )

        def close(self):
            return None

    runner = BenchmarkRunner(lambda: DummyPipeline(), history_dir=tmp_path)
    report = runner.run(
        [BenchmarkSample(url="https://example.com/p", selected_fields=["name", "price"])],
        run_label="fixed",
    )

    assert report["total"] == 1
    assert report["fetch_success_rate"] == 1.0
    assert report["records"][0]["field_valid_rate"] == 1.0
    assert list(tmp_path.glob("benchmark_*.json"))
    assert list(tmp_path.glob("benchmark_*.csv"))


def test_load_benchmark_samples_from_json(tmp_path):
    path = tmp_path / "samples.json"
    path.write_text(
        '{"samples":[{"url":"https://example.com","selected_fields":["title"],"min_fields":1}]}',
        encoding="utf-8",
    )

    samples = load_benchmark_samples(path, split="fixed")

    assert len(samples) == 1
    assert samples[0].url == "https://example.com"
    assert samples[0].selected_fields == ["title"]
    assert samples[0].split == "fixed"
