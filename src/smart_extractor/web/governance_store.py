"""Audit, review, quality, and cost governance helpers."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable
from uuid import uuid4

ConnectionFactory = Callable[[], object]


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def create_audit_log(
    *,
    lock: Any,
    connect: ConnectionFactory,
    tenant_id: str,
    actor_user_id: str,
    actor_role: str,
    action: str,
    resource_type: str,
    resource_id: str,
    request_id: str,
    http_method: str,
    path: str,
    remote_addr: str,
    auth_mode: str,
    payload: dict[str, Any] | None = None,
) -> str:
    audit_id = f"adt-{uuid4().hex[:12]}"
    with lock:
        with connect() as conn:
            conn.execute(
                """
                INSERT INTO audit_logs (
                    audit_id, tenant_id, actor_user_id, actor_role, action, resource_type,
                    resource_id, request_id, http_method, path, remote_addr, auth_mode,
                    payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    audit_id,
                    tenant_id,
                    actor_user_id,
                    actor_role,
                    action,
                    resource_type,
                    resource_id,
                    request_id,
                    http_method,
                    path,
                    remote_addr,
                    auth_mode,
                    json.dumps(payload or {}, ensure_ascii=False),
                    _now_text(),
                ),
            )
            conn.commit()
    return audit_id


def fetch_audit_logs(
    *,
    connect: ConnectionFactory,
    tenant_id: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM audit_logs
            WHERE tenant_id=?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (tenant_id, max(int(limit or 50), 1)),
        ).fetchall()
    payloads: list[dict[str, Any]] = []
    for row in rows:
        payloads.append(
            {
                "audit_id": row["audit_id"],
                "tenant_id": row["tenant_id"],
                "actor_user_id": row["actor_user_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "resource_type": row["resource_type"],
                "resource_id": row["resource_id"],
                "request_id": row["request_id"],
                "http_method": row["http_method"],
                "path": row["path"],
                "remote_addr": row["remote_addr"],
                "auth_mode": row["auth_mode"],
                "payload": json.loads(row["payload_json"] or "{}"),
                "created_at": row["created_at"],
            }
        )
    return payloads


def upsert_task_review(
    *,
    lock: Any,
    connect: ConnectionFactory,
    tenant_id: str,
    task_id: str,
    reviewer_user_id: str,
    confirmed: bool,
    accuracy_score: float,
    notes: str,
) -> dict[str, Any]:
    now = _now_text()
    normalized_score = max(0.0, min(float(accuracy_score or 0.0), 1.0))
    normalized_reviewer = str(reviewer_user_id or "").strip()
    with lock:
        with connect() as conn:
            existing = conn.execute(
                """
                SELECT review_id
                FROM task_reviews
                WHERE tenant_id=? AND task_id=? AND reviewer_user_id=?
                """,
                (tenant_id, task_id, normalized_reviewer),
            ).fetchone()
            if existing is None:
                review_id = f"rvw-{uuid4().hex[:12]}"
                conn.execute(
                    """
                    INSERT INTO task_reviews (
                        review_id, tenant_id, task_id, reviewer_user_id, confirmed,
                        accuracy_score, notes, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        review_id,
                        tenant_id,
                        task_id,
                        normalized_reviewer,
                        1 if confirmed else 0,
                        normalized_score,
                        str(notes or "").strip(),
                        now,
                        now,
                    ),
                )
            else:
                review_id = str(existing["review_id"] or "")
                conn.execute(
                    """
                    UPDATE task_reviews
                    SET confirmed=?, accuracy_score=?, notes=?, updated_at=?
                    WHERE tenant_id=? AND review_id=?
                    """,
                    (
                        1 if confirmed else 0,
                        normalized_score,
                        str(notes or "").strip(),
                        now,
                        tenant_id,
                        review_id,
                    ),
                )
            conn.commit()
    return {
        "review_id": review_id,
        "tenant_id": tenant_id,
        "task_id": task_id,
        "reviewer_user_id": normalized_reviewer,
        "confirmed": bool(confirmed),
        "accuracy_score": normalized_score,
        "notes": str(notes or "").strip(),
        "updated_at": now,
    }


def fetch_task_reviews(
    *,
    connect: ConnectionFactory,
    tenant_id: str,
    limit: int = 200,
) -> list[dict[str, Any]]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM task_reviews
            WHERE tenant_id=?
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (tenant_id, max(int(limit or 200), 1)),
        ).fetchall()
    return [
        {
            "review_id": row["review_id"],
            "task_id": row["task_id"],
            "reviewer_user_id": row["reviewer_user_id"],
            "confirmed": bool(row["confirmed"] or 0),
            "accuracy_score": float(row["accuracy_score"] or 0.0),
            "notes": row["notes"] or "",
            "created_at": row["created_at"] or "",
            "updated_at": row["updated_at"] or "",
        }
        for row in rows
    ]


def _classify_failure(error: str) -> str:
    normalized = str(error or "").strip().lower()
    if not normalized:
        return "unknown"
    if "timeout" in normalized:
        return "timeout"
    if "429" in normalized or "rate limit" in normalized:
        return "rate_limit"
    if "401" in normalized or "403" in normalized or "鉴权" in normalized:
        return "auth"
    if "challenge" in normalized or "风控" in normalized or "验证" in normalized:
        return "anti_bot"
    if "schema" in normalized or "字段" in normalized or "quality" in normalized:
        return "quality"
    if "network" in normalized or "connect" in normalized or "dns" in normalized:
        return "network"
    return "other"


@dataclass
class GovernanceService:
    task_store: Any
    connect: ConnectionFactory

    def build_quality_dashboard(
        self,
        *,
        tenant_id: str,
        recent_limit: int = 200,
    ) -> dict[str, Any]:
        tasks = self.task_store.list_all(limit=recent_limit, tenant_id=tenant_id)
        stats = self.task_store.stats(tenant_id=tenant_id)
        templates = self.task_store.list_templates(limit=200, tenant_id=tenant_id)
        reviews = fetch_task_reviews(connect=self.connect, tenant_id=tenant_id, limit=500)
        failure_counter = Counter(
            _classify_failure(task.error or "")
            for task in tasks
            if str(task.status or "").strip().lower() == "failed"
        )
        quality_values = [
            float(task.quality_score or 0.0)
            for task in tasks
            if float(task.quality_score or 0.0) > 0
        ]
        confirmed_reviews = [item for item in reviews if item["confirmed"]]
        accuracy_reviews = [
            item["accuracy_score"]
            for item in reviews
            if float(item.get("accuracy_score", 0.0) or 0.0) > 0
        ]
        reused_templates = [item for item in templates if int(item.use_count or 0) > 0]

        return {
            "scenario": {
                "primary": "网页变化监控 + 结构化通知",
                "reason": "现有后端已经具备监控、历史对比、通知、任务流水与告警数据，最适合先形成付费闭环。",
            },
            "summary": {
                "extraction_success_rate": stats.get("success_rate", "0.0%"),
                "avg_quality_score": round(
                    sum(quality_values) / max(len(quality_values), 1),
                    4,
                )
                if quality_values
                else 0.0,
                "field_accuracy_rate": round(
                    sum(accuracy_reviews) / max(len(accuracy_reviews), 1),
                    4,
                )
                if accuracy_reviews
                else None,
                "manual_confirmation_rate": round(
                    len(confirmed_reviews) / max(len(reviews), 1),
                    4,
                )
                if reviews
                else None,
                "manual_review_coverage": round(
                    len(reviews) / max(len(tasks), 1),
                    4,
                )
                if tasks
                else 0.0,
                "template_reuse_rate": round(
                    len(reused_templates) / max(len(templates), 1),
                    4,
                )
                if templates
                else None,
            },
            "failure_breakdown": [
                {"category": key, "count": value}
                for key, value in failure_counter.most_common()
            ],
            "recent_reviews": reviews[:20],
        }

    def build_cost_dashboard(
        self,
        *,
        tenant_id: str,
        recent_limit: int = 200,
    ) -> dict[str, Any]:
        tasks = self.task_store.list_all(limit=recent_limit, tenant_id=tenant_id)
        rows: list[dict[str, Any]] = []
        batch_costs: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"task_count": 0, "total_cost_usd": 0.0, "total_tokens": 0}
        )
        total_cost = 0.0
        total_tokens = 0
        total_playwright_elapsed_ms = 0.0
        total_retry_count = 0
        total_retry_cost_usd = 0.0
        for task in tasks:
            data = task.data if isinstance(task.data, dict) else {}
            usage = data.get("_llm_usage") if isinstance(data.get("_llm_usage"), dict) else {}
            runtime = (
                data.get("_runtime_metrics")
                if isinstance(data.get("_runtime_metrics"), dict)
                else {}
            )
            row = {
                "task_id": task.task_id,
                "status": task.status,
                "url": task.url,
                "batch_group_id": task.batch_group_id,
                "total_tokens": int(usage.get("total_tokens", 0) or 0),
                "prompt_tokens": int(usage.get("prompt_tokens", 0) or 0),
                "completion_tokens": int(usage.get("completion_tokens", 0) or 0),
                "estimated_cost_usd": float(usage.get("estimated_cost_usd", 0.0) or 0.0),
                "fetcher_type": str(runtime.get("fetcher_type") or ""),
                "playwright_elapsed_ms": float(runtime.get("playwright_elapsed_ms", 0.0) or 0.0),
                "retry_count": int(runtime.get("retry_count", 0) or 0),
                "retry_cost_usd": float(runtime.get("retry_cost_usd", 0.0) or 0.0),
            }
            rows.append(row)
            total_cost += row["estimated_cost_usd"]
            total_tokens += row["total_tokens"]
            total_playwright_elapsed_ms += row["playwright_elapsed_ms"]
            total_retry_count += row["retry_count"]
            total_retry_cost_usd += row["retry_cost_usd"]
            if row["batch_group_id"]:
                batch_entry = batch_costs[row["batch_group_id"]]
                batch_entry["task_count"] += 1
                batch_entry["total_cost_usd"] += row["estimated_cost_usd"]
                batch_entry["total_tokens"] += row["total_tokens"]

        return {
            "summary": {
                "total_tasks": len(rows),
                "total_tokens": total_tokens,
                "total_model_cost_usd": round(total_cost, 6),
                "avg_cost_per_task_usd": round(total_cost / max(len(rows), 1), 6),
                "total_playwright_elapsed_ms": round(total_playwright_elapsed_ms, 2),
                "total_retry_count": total_retry_count,
                "total_retry_cost_usd": round(total_retry_cost_usd, 6),
            },
            "recent_tasks": rows[:50],
            "batch_costs": [
                {
                    "batch_group_id": key,
                    "task_count": value["task_count"],
                    "total_cost_usd": round(value["total_cost_usd"], 6),
                    "total_tokens": value["total_tokens"],
                }
                for key, value in sorted(
                    batch_costs.items(),
                    key=lambda item: (-item[1]["total_cost_usd"], item[0]),
                )
            ][:20],
        }
