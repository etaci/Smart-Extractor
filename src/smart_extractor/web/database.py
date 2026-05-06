"""Database helpers for SQLite and PostgreSQL task-store backends."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from loguru import logger

try:  # pragma: no cover - optional dependency in local test env
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - optional dependency in local test env
    psycopg = None
    dict_row = None


def _normalize_database_url(database_url: str) -> str:
    value = str(database_url or "").strip()
    if not value:
        raise ValueError("database_url cannot be empty")
    return value


def _convert_placeholders(sql: str, dialect: str) -> str:
    if dialect != "postgres":
        return sql
    return sql.replace("?", "%s")


@dataclass(frozen=True)
class DatabaseTarget:
    dialect: str
    database_url: str


def parse_database_target(database_url: str) -> DatabaseTarget:
    normalized_url = _normalize_database_url(database_url)
    parsed = urlparse(normalized_url)
    scheme = parsed.scheme.lower()
    if scheme in {"postgres", "postgresql"}:
        return DatabaseTarget(dialect="postgres", database_url=normalized_url)
    if scheme == "sqlite":
        return DatabaseTarget(dialect="sqlite", database_url=normalized_url)
    raise ValueError(f"unsupported database scheme: {scheme}")


class DatabaseCursor:
    def __init__(self, raw_cursor: Any, *, dialect: str, rowcount: int | None = None):
        self._raw_cursor = raw_cursor
        self._dialect = dialect
        self._rowcount = int(rowcount or 0)

    @property
    def lastrowid(self) -> int | None:
        value = getattr(self._raw_cursor, "lastrowid", None)
        return int(value) if value not in {None, ""} else None

    @property
    def rowcount(self) -> int:
        raw_rowcount = getattr(self._raw_cursor, "rowcount", None)
        if raw_rowcount is None:
            return self._rowcount
        try:
            return int(raw_rowcount)
        except (TypeError, ValueError):
            return self._rowcount

    def fetchone(self) -> Any:
        return self._raw_cursor.fetchone()

    def fetchall(self) -> list[Any]:
        rows = self._raw_cursor.fetchall()
        return list(rows or [])


class DatabaseConnection:
    def __init__(
        self,
        raw_connection: Any,
        *,
        dialect: str,
        busy_timeout_ms: int = 5000,
        enable_wal: bool = True,
        synchronous: str = "NORMAL",
    ):
        self._raw_connection = raw_connection
        self.dialect = dialect
        self._last_rowcount = 0
        if dialect == "sqlite":
            self._configure_sqlite(
                busy_timeout_ms=busy_timeout_ms,
                enable_wal=enable_wal,
                synchronous=synchronous,
            )

    def _configure_sqlite(
        self,
        *,
        busy_timeout_ms: int,
        enable_wal: bool,
        synchronous: str,
    ) -> None:
        self._raw_connection.row_factory = sqlite3.Row
        self._raw_connection.execute("PRAGMA foreign_keys = ON")
        self._raw_connection.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
        if enable_wal:
            self._raw_connection.execute("PRAGMA journal_mode = WAL")
        self._raw_connection.execute(f"PRAGMA synchronous = {synchronous}")
        self._raw_connection.execute("PRAGMA temp_store = MEMORY")

    @property
    def total_changes(self) -> int:
        if self.dialect == "sqlite":
            return int(getattr(self._raw_connection, "total_changes", 0) or 0)
        return self._last_rowcount

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> DatabaseCursor:
        normalized_sql = _convert_placeholders(sql, self.dialect)
        raw_cursor = self._raw_connection.execute(normalized_sql, tuple(params or ()))
        rowcount = getattr(raw_cursor, "rowcount", 0)
        try:
            self._last_rowcount = int(rowcount or 0)
        except (TypeError, ValueError):
            self._last_rowcount = 0
        return DatabaseCursor(raw_cursor, dialect=self.dialect, rowcount=self._last_rowcount)

    def begin_immediate(self) -> None:
        if self.dialect == "sqlite":
            self.execute("BEGIN IMMEDIATE")
            return
        self.execute("BEGIN")

    def table_columns(self, table_name: str) -> set[str]:
        normalized_table = str(table_name or "").strip()
        if not normalized_table:
            return set()
        if self.dialect == "sqlite":
            rows = self.execute(f"PRAGMA table_info({normalized_table})").fetchall()
            return {str(row["name"]) for row in rows}
        rows = self.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=?
            """,
            (normalized_table,),
        ).fetchall()
        return {str(row["column_name"]) for row in rows}

    def commit(self) -> None:
        self._raw_connection.commit()

    def rollback(self) -> None:
        self._raw_connection.rollback()

    def close(self) -> None:
        self._raw_connection.close()

    def __enter__(self) -> "DatabaseConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is not None:
                self.rollback()
        finally:
            self.close()


def _sqlite_url_from_path(db_path: str | Path) -> str:
    path = Path(db_path).expanduser().resolve()
    return f"sqlite:///{path.as_posix()}"


def resolve_sqlite_database_url(db_path: str | Path) -> str:
    return _sqlite_url_from_path(db_path)


def build_connection_factory(
    *,
    database_url: str,
    sqlite_busy_timeout_ms: int = 5000,
    sqlite_enable_wal: bool = True,
    sqlite_synchronous: str = "NORMAL",
):
    target = parse_database_target(database_url)

    def _connect() -> DatabaseConnection:
        if target.dialect == "sqlite":
            parsed = urlparse(target.database_url)
            raw_path = parsed.path or ""
            if raw_path.startswith("/") and raw_path[2:3] == ":":
                raw_path = raw_path[1:]
            db_path = Path(raw_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            raw_connection = sqlite3.connect(
                db_path,
                check_same_thread=False,
                timeout=max(sqlite_busy_timeout_ms / 1000, 1.0),
            )
            return DatabaseConnection(
                raw_connection,
                dialect="sqlite",
                busy_timeout_ms=sqlite_busy_timeout_ms,
                enable_wal=sqlite_enable_wal,
                synchronous=sqlite_synchronous,
            )

        if psycopg is None:  # pragma: no cover - exercised only when postgres configured
            raise RuntimeError(
                "PostgreSQL backend requires psycopg. Please install smart-extractor with PostgreSQL dependencies."
            )
        raw_connection = psycopg.connect(
            target.database_url,
            row_factory=dict_row,
            autocommit=False,
        )
        logger.info("Connected task store to PostgreSQL")
        return DatabaseConnection(raw_connection, dialect="postgres")

    _connect.database_dialect = target.dialect  # type: ignore[attr-defined]
    _connect.database_url = target.database_url  # type: ignore[attr-defined]
    return _connect
