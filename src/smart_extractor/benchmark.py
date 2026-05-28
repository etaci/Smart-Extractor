"""Backend benchmark runner for fixed and exploratory extraction datasets."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable


@dataclass(slots=True)
class BenchmarkSample:
    url: str
    page_type: str = "auto"
    selected_fields: list[str] = field(default_factory=list)
    expected_fields: dict[str, Any] = field(default_factory=dict)
    min_fields: int = 1
    split: str = "fixed"
    anti_bot_risk: bool = False
    dynamic: bool = False


def load_benchmark_samples(path: str | Path, *, split: str = "") -> list[BenchmarkSample]:
    source = Path(path)
    if not source.exists():
        return []
    if source.suffix.lower() == ".csv":
        with source.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        return [_sample_from_mapping(row, split=split) for row in rows]
    payload = json.loads(source.read_text(encoding="utf-8"))
    rows = payload.get("samples", payload) if isinstance(payload, dict) else payload
    return [_sample_from_mapping(row, split=split) for row in rows if isinstance(row, dict)]


class BenchmarkRunner:
    def __init__(
        self,
        pipeline_factory: Callable[[], Any],
        *,
        history_dir: str | Path = "benchmark_results",
    ):
        self._pipeline_factory = pipeline_factory
        self._history_dir = Path(history_dir)

    def run(
        self,
        samples: Iterable[BenchmarkSample],
        *,
        write_history: bool = True,
        run_label: str = "",
    ) -> dict[str, Any]:
        records: list[dict[str, Any]] = []
        pipeline = self._pipeline_factory()
        try:
            for sample in samples:
                records.append(self._run_one(pipeline, sample))
        finally:
            close = getattr(pipeline, "close", None)
            if callable(close):
                close()
        report = build_benchmark_report(records, run_label=run_label)
        if write_history:
            self.write_history(report)
        return report

    def _run_one(self, pipeline: Any, sample: BenchmarkSample) -> dict[str, Any]:
        result = pipeline.run(
            sample.url,
            schema_name="auto",
            selected_fields=sample.selected_fields,
            skip_storage=True,
        )
        fetch_result = getattr(result, "fetch_result", None)
        diagnostics = (
            getattr(fetch_result, "diagnostics", {})
            if fetch_result is not None and isinstance(getattr(fetch_result, "diagnostics", None), dict)
            else {}
        )
        data = getattr(result, "data", None)
        payload = getattr(data, "data", {}) if data is not None else {}
        selected = list(getattr(data, "selected_fields", []) or sample.selected_fields or [])
        filled = sum(1 for field in selected if payload.get(field) not in (None, "", [], {}))
        llm_used = str(getattr(data, "extraction_strategy", "") or "").lower() == "llm"
        validation = getattr(result, "validation", None)
        return {
            "url": sample.url,
            "split": sample.split,
            "page_type": sample.page_type,
            "success": bool(getattr(result, "success", False)),
            "fetch_success": bool(fetch_result is not None and getattr(fetch_result, "is_success", False)),
            "content_ready": bool(getattr(result, "cleaned_text", "") and not getattr(fetch_result, "is_shell_page", False)),
            "llm_used": llm_used,
            "field_valid_rate": filled / max(len(selected), 1) if selected else 0.0,
            "filled_fields": filled,
            "expected_min_fields_met": filled >= int(sample.min_fields or 1),
            "elapsed_ms": float(getattr(result, "elapsed_ms", 0.0) or 0.0),
            "error": str(getattr(result, "error", "") or ""),
            "failure_reason": str(diagnostics.get("failure_reason") or getattr(result, "error", "") or ""),
            "failure_stage": str(diagnostics.get("failure_stage") or ""),
            "http_status": int(diagnostics.get("http_status", 0) or 0),
            "quality_score": float(getattr(validation, "quality_score", 0.0) or 0.0) if validation else 0.0,
        }

    def write_history(self, report: dict[str, Any]) -> None:
        self._history_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = self._history_dir / f"benchmark_{timestamp}.json"
        csv_path = self._history_dir / f"benchmark_{timestamp}.csv"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        rows = list(report.get("records", []) or [])
        if rows:
            with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)


def build_benchmark_report(records: list[dict[str, Any]], *, run_label: str = "") -> dict[str, Any]:
    total = len(records)
    fetched = [item for item in records if item.get("fetch_success")]
    successful = [item for item in records if item.get("success")]
    llm_rows = [item for item in records if item.get("llm_used")]
    failure_breakdown: dict[str, int] = {}
    for item in records:
        if item.get("success"):
            continue
        key = str(item.get("failure_reason") or item.get("error") or "unknown")
        failure_breakdown[key] = failure_breakdown.get(key, 0) + 1
    slow_samples = sorted(records, key=lambda item: float(item.get("elapsed_ms") or 0), reverse=True)[:10]
    return {
        "run_label": run_label,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "total": total,
        "fetch_success_rate": _rate(len(fetched), total),
        "content_ready_rate": _rate(sum(1 for item in records if item.get("content_ready")), max(len(fetched), 1)),
        "extraction_success_on_fetched_pages": _rate(len(successful), max(len(fetched), 1)),
        "llm_success_rate": _rate(sum(1 for item in llm_rows if item.get("success")), max(len(llm_rows), 1)),
        "field_valid_rate_on_success": round(
            sum(float(item.get("field_valid_rate") or 0.0) for item in successful) / max(len(successful), 1),
            4,
        ),
        "avg_cost": 0.0,
        "avg_latency": round(sum(float(item.get("elapsed_ms") or 0.0) for item in records) / max(total, 1), 2),
        "failure_breakdown": failure_breakdown,
        "slow_samples": slow_samples,
        "records": records,
    }


def _sample_from_mapping(row: dict[str, Any], *, split: str = "") -> BenchmarkSample:
    selected = row.get("selected_fields") or []
    if isinstance(selected, str):
        selected = [item.strip() for item in selected.split(",") if item.strip()]
    expected = row.get("expected_fields") or {}
    if isinstance(expected, str) and expected.strip():
        try:
            expected = json.loads(expected)
        except Exception:
            expected = {}
    return BenchmarkSample(
        url=str(row.get("url") or "").strip(),
        page_type=str(row.get("page_type") or row.get("schema_name") or "auto"),
        selected_fields=list(selected or []),
        expected_fields=expected if isinstance(expected, dict) else {},
        min_fields=int(row.get("min_fields") or row.get("minimum_fields") or 1),
        split=split or str(row.get("split") or "fixed"),
        anti_bot_risk=_truthy(row.get("anti_bot_risk")),
        dynamic=_truthy(row.get("dynamic")),
    )


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / max(denominator, 1), 4)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}
