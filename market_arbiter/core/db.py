"""SQLite connection and migration helpers for LiquidSniper."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path


MIGRATIONS_DIR = Path(__file__).with_name("migrations")


def _utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def connect(db_path: str) -> sqlite3.Connection:
    """Create a SQLite connection configured for concurrent local access."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def _ensure_schema_migrations_table(conn: sqlite3.Connection) -> None:
    """Create migration tracking table if it does not yet exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_ts TEXT NOT NULL
        );
        """
    )


def apply_migrations(conn: sqlite3.Connection) -> None:
    """Apply pending SQL migrations in filename order."""
    _ensure_schema_migrations_table(conn)

    applied_versions = {
        row[0]
        for row in conn.execute("SELECT version FROM schema_migrations;").fetchall()
    }

    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    for migration_file in migration_files:
        version = migration_file.stem
        if version in applied_versions:
            continue

        sql = migration_file.read_text(encoding="utf-8")
        with conn:
            conn.executescript(sql)
            conn.execute(
                "INSERT INTO schema_migrations(version, applied_ts) VALUES (?, ?);",
                (version, _utc_now_iso()),
            )


def init_db(db_path: str) -> sqlite3.Connection:
    """Connect to a database and apply all pending migrations."""
    conn = connect(db_path)
    apply_migrations(conn)
    return conn
