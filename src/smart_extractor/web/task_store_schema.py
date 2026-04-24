"""SQLiteTaskStore 的建表与字段迁移辅助函数。"""

from __future__ import annotations

import sqlite3
from typing import Callable

ConnectionFactory = Callable[[], sqlite3.Connection]


def ensure_column(
    conn: sqlite3.Connection,
    table_name: str,
    existing_columns: set[str],
    column_name: str,
    column_sql: str,
) -> None:
    if column_name in existing_columns:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")  # nosec B608
    existing_columns.add(column_name)


def initialize_task_store_schema(*, connect: ConnectionFactory) -> None:
    with connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS web_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL UNIQUE,
                request_id TEXT NOT NULL DEFAULT '-',
                url TEXT NOT NULL,
                schema_name TEXT NOT NULL,
                storage_format TEXT NOT NULL,
                batch_group_id TEXT NOT NULL DEFAULT '',
                task_kind TEXT NOT NULL DEFAULT 'single',
                parent_task_id TEXT NOT NULL DEFAULT '',
                total_items INTEGER NOT NULL DEFAULT 0,
                completed_items INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                completed_at TEXT NOT NULL DEFAULT '',
                elapsed_ms REAL NOT NULL DEFAULT 0,
                quality_score REAL NOT NULL DEFAULT 0,
                progress_percent REAL NOT NULL DEFAULT 0,
                progress_stage TEXT NOT NULL DEFAULT '',
                data_json TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT ''
            )
            """
        )
        existing_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(web_tasks)").fetchall()
        }
        if "progress_percent" not in existing_columns:
            conn.execute(
                "ALTER TABLE web_tasks ADD COLUMN progress_percent REAL NOT NULL DEFAULT 0"
            )
        if "progress_stage" not in existing_columns:
            conn.execute(
                "ALTER TABLE web_tasks ADD COLUMN progress_stage TEXT NOT NULL DEFAULT ''"
            )
        if "batch_group_id" not in existing_columns:
            conn.execute(
                "ALTER TABLE web_tasks ADD COLUMN batch_group_id TEXT NOT NULL DEFAULT ''"
            )
        if "task_kind" not in existing_columns:
            conn.execute(
                "ALTER TABLE web_tasks ADD COLUMN task_kind TEXT NOT NULL DEFAULT 'single'"
            )
        if "parent_task_id" not in existing_columns:
            conn.execute(
                "ALTER TABLE web_tasks ADD COLUMN parent_task_id TEXT NOT NULL DEFAULT ''"
            )
        if "total_items" not in existing_columns:
            conn.execute(
                "ALTER TABLE web_tasks ADD COLUMN total_items INTEGER NOT NULL DEFAULT 0"
            )
        if "completed_items" not in existing_columns:
            conn.execute(
                "ALTER TABLE web_tasks ADD COLUMN completed_items INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_web_tasks_status ON web_tasks(status)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_web_tasks_created_at ON web_tasks(created_at)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_web_tasks_url ON web_tasks(url)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_web_tasks_batch_group_id ON web_tasks(batch_group_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_web_tasks_parent_task_id ON web_tasks(parent_task_id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS extraction_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_id TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                url TEXT NOT NULL DEFAULT '',
                page_type TEXT NOT NULL DEFAULT 'unknown',
                schema_name TEXT NOT NULL DEFAULT 'auto',
                storage_format TEXT NOT NULL DEFAULT 'json',
                use_static INTEGER NOT NULL DEFAULT 0,
                selected_fields_json TEXT NOT NULL DEFAULT '[]',
                field_labels_json TEXT NOT NULL DEFAULT '{}',
                profile_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_used_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_extraction_templates_updated_at ON extraction_templates(updated_at)"
        )
        template_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(extraction_templates)").fetchall()
        }
        ensure_column(
            conn,
            "extraction_templates",
            template_columns,
            "profile_json",
            "profile_json TEXT NOT NULL DEFAULT '{}'",
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monitor_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                monitor_id TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                schema_name TEXT NOT NULL DEFAULT 'auto',
                storage_format TEXT NOT NULL DEFAULT 'json',
                use_static INTEGER NOT NULL DEFAULT 0,
                selected_fields_json TEXT NOT NULL DEFAULT '[]',
                field_labels_json TEXT NOT NULL DEFAULT '{}',
                profile_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_task_id TEXT NOT NULL DEFAULT '',
                last_checked_at TEXT NOT NULL DEFAULT '',
                last_status TEXT NOT NULL DEFAULT '',
                last_alert_level TEXT NOT NULL DEFAULT '',
                last_alert_message TEXT NOT NULL DEFAULT '',
                last_changed_fields_json TEXT NOT NULL DEFAULT '[]',
                last_notification_status TEXT NOT NULL DEFAULT '',
                last_notification_message TEXT NOT NULL DEFAULT '',
                last_notification_at TEXT NOT NULL DEFAULT '',
                last_extraction_strategy TEXT NOT NULL DEFAULT '',
                last_learned_profile_id TEXT NOT NULL DEFAULT '',
                schedule_enabled INTEGER NOT NULL DEFAULT 0,
                schedule_interval_minutes INTEGER NOT NULL DEFAULT 60,
                schedule_next_run_at TEXT NOT NULL DEFAULT '',
                schedule_last_run_at TEXT NOT NULL DEFAULT '',
                schedule_paused_at TEXT NOT NULL DEFAULT '',
                schedule_claimed_by TEXT NOT NULL DEFAULT '',
                schedule_claimed_at TEXT NOT NULL DEFAULT '',
                schedule_lease_until TEXT NOT NULL DEFAULT '',
                schedule_last_error TEXT NOT NULL DEFAULT '',
                schedule_claim_count INTEGER NOT NULL DEFAULT 0,
                last_trigger_source TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_monitor_profiles_updated_at ON monitor_profiles(updated_at)"
        )
        monitor_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(monitor_profiles)").fetchall()
        }
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "profile_json",
            "profile_json TEXT NOT NULL DEFAULT '{}'",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "last_notification_status",
            "last_notification_status TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "last_notification_message",
            "last_notification_message TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "last_notification_at",
            "last_notification_at TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "last_extraction_strategy",
            "last_extraction_strategy TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "last_learned_profile_id",
            "last_learned_profile_id TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "schedule_enabled",
            "schedule_enabled INTEGER NOT NULL DEFAULT 0",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "schedule_interval_minutes",
            "schedule_interval_minutes INTEGER NOT NULL DEFAULT 60",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "schedule_next_run_at",
            "schedule_next_run_at TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "schedule_last_run_at",
            "schedule_last_run_at TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "schedule_paused_at",
            "schedule_paused_at TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "schedule_claimed_by",
            "schedule_claimed_by TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "schedule_claimed_at",
            "schedule_claimed_at TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "schedule_lease_until",
            "schedule_lease_until TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "schedule_last_error",
            "schedule_last_error TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "schedule_claim_count",
            "schedule_claim_count INTEGER NOT NULL DEFAULT 0",
        )
        ensure_column(
            conn,
            "monitor_profiles",
            monitor_columns,
            "last_trigger_source",
            "last_trigger_source TEXT NOT NULL DEFAULT ''",
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_monitor_profiles_schedule_next_run_at ON monitor_profiles(schedule_next_run_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_monitor_profiles_schedule_lease_until ON monitor_profiles(schedule_lease_until)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS web_task_dispatch_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'queued',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                claimed_at TEXT NOT NULL DEFAULT '',
                worker_id TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_web_task_dispatch_queue_status ON web_task_dispatch_queue(status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_web_task_dispatch_queue_updated_at ON web_task_dispatch_queue(updated_at)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monitor_notification_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                notification_id TEXT NOT NULL UNIQUE,
                monitor_id TEXT NOT NULL DEFAULT '',
                task_id TEXT NOT NULL DEFAULT '',
                channel_type TEXT NOT NULL DEFAULT 'webhook',
                target TEXT NOT NULL DEFAULT '',
                event_type TEXT NOT NULL DEFAULT 'monitor_alert',
                status TEXT NOT NULL DEFAULT 'pending',
                status_message TEXT NOT NULL DEFAULT '',
                attempt_no INTEGER NOT NULL DEFAULT 1,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                next_retry_at TEXT NOT NULL DEFAULT '',
                response_code INTEGER,
                error_type TEXT NOT NULL DEFAULT '',
                error_message TEXT NOT NULL DEFAULT '',
                payload_snapshot_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                sent_at TEXT NOT NULL DEFAULT '',
                retry_of_notification_id TEXT NOT NULL DEFAULT '',
                triggered_by TEXT NOT NULL DEFAULT 'system'
            )
            """
        )
        notification_columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(monitor_notification_events)"
            ).fetchall()
        }
        ensure_column(
            conn,
            "monitor_notification_events",
            notification_columns,
            "status_message",
            "status_message TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_notification_events",
            notification_columns,
            "retry_of_notification_id",
            "retry_of_notification_id TEXT NOT NULL DEFAULT ''",
        )
        ensure_column(
            conn,
            "monitor_notification_events",
            notification_columns,
            "triggered_by",
            "triggered_by TEXT NOT NULL DEFAULT 'system'",
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_monitor_notification_events_monitor_id ON monitor_notification_events(monitor_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_monitor_notification_events_task_id ON monitor_notification_events(task_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_monitor_notification_events_status ON monitor_notification_events(status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_monitor_notification_events_created_at ON monitor_notification_events(created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_monitor_notification_events_retry_of_notification_id ON monitor_notification_events(retry_of_notification_id)"
        )
        conn.commit()
