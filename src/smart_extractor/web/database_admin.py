"""Task-store database migration, backup, and restore helpers."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from smart_extractor.config import AppConfig, resolve_local_config_path
from smart_extractor.web.database import (
    build_connection_factory,
    resolve_sqlite_database_url,
)
from smart_extractor.web.task_store_schema import initialize_task_store_schema

TASK_STORE_TABLES: tuple[str, ...] = (
    "web_users",
    "web_sessions",
    "web_tasks",
    "extraction_templates",
    "monitor_profiles",
    "web_task_dispatch_queue",
    "monitor_notification_events",
    "audit_logs",
    "task_reviews",
)


def resolve_task_store_database_url(config: AppConfig) -> str:
    configured = str(
        config.storage.task_store_database_url or config.storage.database_url or ""
    ).strip()
    if configured:
        return configured
    return resolve_sqlite_database_url(Path(config.storage.output_dir) / "web_tasks.db")


def create_task_store_connection_factory(config: AppConfig):
    return build_connection_factory(
        database_url=resolve_task_store_database_url(config),
        sqlite_busy_timeout_ms=config.storage.sqlite_busy_timeout_ms,
        sqlite_enable_wal=config.storage.sqlite_enable_wal,
        sqlite_synchronous=config.storage.sqlite_synchronous,
    )


def migrate_task_store_database(config: AppConfig) -> dict[str, Any]:
    connect = create_task_store_connection_factory(config)
    initialize_task_store_schema(connect=connect)
    return {
        "database_url": resolve_task_store_database_url(config),
        "dialect": str(getattr(connect, "database_dialect", "sqlite")),
        "tables": list(TASK_STORE_TABLES),
    }


def _optional_backup_files(config: AppConfig) -> list[tuple[str, Path]]:
    files = [
        ("config/local.yaml", resolve_local_config_path()),
        ("output/learned_profiles.json", Path(config.storage.output_dir) / "learned_profiles.json"),
    ]
    return [(name, path) for name, path in files if path.exists()]


def backup_task_store_database(
    config: AppConfig,
    *,
    backup_path: str | Path = "",
) -> Path:
    target_dir = Path(backup_path) if str(backup_path or "").strip() else Path(config.storage.backup_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    output_path = target_dir / f"task-store-backup-{timestamp}.json"

    connect = create_task_store_connection_factory(config)
    backup_payload: dict[str, Any] = {
        "version": 1,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "database_url": resolve_task_store_database_url(config),
        "dialect": str(getattr(connect, "database_dialect", "sqlite")),
        "tables": {},
        "files": {},
    }
    with connect() as conn:
        for table_name in TASK_STORE_TABLES:
            rows = conn.execute(f"SELECT * FROM {table_name} ORDER BY id ASC").fetchall()  # nosec B608
            backup_payload["tables"][table_name] = [dict(row) for row in rows]

    for logical_name, path in _optional_backup_files(config):
        backup_payload["files"][logical_name] = path.read_text(encoding="utf-8")

    output_path.write_text(
        json.dumps(backup_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def restore_task_store_database(
    config: AppConfig,
    *,
    backup_file: str | Path,
) -> dict[str, Any]:
    backup_path = Path(backup_file)
    payload = json.loads(backup_path.read_text(encoding="utf-8"))
    tables = payload.get("tables", {}) if isinstance(payload, dict) else {}
    files = payload.get("files", {}) if isinstance(payload, dict) else {}

    connect = create_task_store_connection_factory(config)
    initialize_task_store_schema(connect=connect)
    restored_rows: dict[str, int] = {}
    with connect() as conn:
        conn.begin_immediate()
        for table_name in reversed(TASK_STORE_TABLES):
            conn.execute(f"DELETE FROM {table_name}")  # nosec B608

        for table_name in TASK_STORE_TABLES:
            rows = tables.get(table_name, []) if isinstance(tables, dict) else []
            restored_rows[table_name] = len(rows)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                columns = [column for column in row.keys() if column != "id"]
                if not columns:
                    continue
                placeholders = ", ".join("?" for _ in columns)
                column_list = ", ".join(columns)
                values = [row.get(column) for column in columns]
                conn.execute(
                    f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})",  # nosec B608
                    tuple(values),
                )
        conn.commit()

    if isinstance(files, dict):
        for logical_name, content in files.items():
            if logical_name == "config/local.yaml":
                resolve_local_config_path().write_text(str(content), encoding="utf-8")
            elif logical_name == "output/learned_profiles.json":
                learned_profile_path = Path(config.storage.output_dir) / "learned_profiles.json"
                learned_profile_path.parent.mkdir(parents=True, exist_ok=True)
                learned_profile_path.write_text(str(content), encoding="utf-8")

    return {
        "backup_file": str(backup_path),
        "restored_rows": restored_rows,
        "database_url": resolve_task_store_database_url(config),
    }
