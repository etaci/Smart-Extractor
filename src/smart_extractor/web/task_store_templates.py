"""Template persistence helpers for SQLiteTaskStore."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Callable

from smart_extractor.web.task_models import TemplateRecord

ConnectionFactory = Callable[[], sqlite3.Connection]


def upsert_template(
    *,
    lock: Any,
    connect: ConnectionFactory,
    name: str,
    url: str,
    page_type: str,
    schema_name: str,
    storage_format: str,
    use_static: bool,
    selected_fields: list[str],
    field_labels: dict[str, str],
    profile: dict[str, Any] | None = None,
    template_id: str = "",
) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    normalized_template_id = str(template_id or "").strip()
    normalized_profile = dict(profile or {})

    with lock:
        with connect() as conn:
            if normalized_template_id:
                existing = conn.execute(
                    "SELECT id FROM extraction_templates WHERE template_id=?",
                    (normalized_template_id,),
                ).fetchone()
                if existing is not None:
                    conn.execute(
                        """
                        UPDATE extraction_templates
                        SET name=?, url=?, page_type=?, schema_name=?, storage_format=?,
                            use_static=?, selected_fields_json=?, field_labels_json=?, profile_json=?, updated_at=?
                        WHERE template_id=?
                        """,
                        (
                            name,
                            url,
                            page_type,
                            schema_name,
                            storage_format,
                            1 if use_static else 0,
                            json.dumps(selected_fields, ensure_ascii=False),
                            json.dumps(field_labels, ensure_ascii=False),
                            json.dumps(normalized_profile, ensure_ascii=False),
                            now,
                            normalized_template_id,
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO extraction_templates (
                            template_id, name, url, page_type, schema_name, storage_format,
                            use_static, selected_fields_json, field_labels_json, profile_json, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            normalized_template_id,
                            name,
                            url,
                            page_type,
                            schema_name,
                            storage_format,
                            1 if use_static else 0,
                            json.dumps(selected_fields, ensure_ascii=False),
                            json.dumps(field_labels, ensure_ascii=False),
                            json.dumps(normalized_profile, ensure_ascii=False),
                            now,
                            now,
                        ),
                    )
            else:
                row_id = conn.execute(
                    """
                    INSERT INTO extraction_templates (
                        template_id, name, url, page_type, schema_name, storage_format,
                        use_static, selected_fields_json, field_labels_json, profile_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "",
                        name,
                        url,
                        page_type,
                        schema_name,
                        storage_format,
                        1 if use_static else 0,
                        json.dumps(selected_fields, ensure_ascii=False),
                        json.dumps(field_labels, ensure_ascii=False),
                        json.dumps(normalized_profile, ensure_ascii=False),
                        now,
                        now,
                    ),
                ).lastrowid
                normalized_template_id = f"tpl-{int(row_id):06d}"
                conn.execute(
                    "UPDATE extraction_templates SET template_id=? WHERE id=?",
                    (normalized_template_id, row_id),
                )
            conn.commit()

    return normalized_template_id


def fetch_templates(
    *,
    connect: ConnectionFactory,
    limit: int = 20,
) -> list[TemplateRecord]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM extraction_templates ORDER BY updated_at DESC, id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    return [TemplateRecord.from_row(row) for row in rows]


def fetch_template(
    *,
    connect: ConnectionFactory,
    template_id: str,
) -> TemplateRecord | None:
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM extraction_templates WHERE template_id=?",
            (template_id,),
        ).fetchone()
    if row is None:
        return None
    return TemplateRecord.from_row(row)


def touch_template(
    *,
    lock: Any,
    connect: ConnectionFactory,
    template_id: str,
) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with lock:
        with connect() as conn:
            conn.execute(
                "UPDATE extraction_templates SET last_used_at=?, updated_at=? WHERE template_id=?",
                (now, now, template_id),
            )
            conn.commit()
