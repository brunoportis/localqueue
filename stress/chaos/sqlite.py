from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

COUNTS_ZERO = {"ready": 0, "processing": 0, "acked": 0, "failed": 0}


def connect(path: Path, *, timeout: float = 5.0) -> sqlite3.Connection:
    return sqlite3.connect(path, timeout=timeout)


def create_queue_db(path: Path, *, synchronous: str = "NORMAL") -> sqlite3.Connection:
    connection = connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(f"PRAGMA synchronous={synchronous}")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, status INTEGER NOT NULL, payload BLOB NOT NULL)"
    )
    connection.commit()
    return connection


def counts(connection: sqlite3.Connection) -> dict[str, int]:
    names = ("ready", "processing", "acked", "failed")
    values = dict(
        connection.execute("SELECT status, COUNT(*) FROM messages GROUP BY status")
    )
    return {name: int(values.get(index, 0)) for index, name in enumerate(names)}


def pragmas(
    connection: sqlite3.Connection, busy_timeout_ms: int = 5000
) -> dict[str, Any]:
    return {
        "journal_mode": str(
            connection.execute("PRAGMA journal_mode").fetchone()[0]
        ).lower(),
        "synchronous": int(connection.execute("PRAGMA synchronous").fetchone()[0]),
        "busy_timeout_ms": busy_timeout_ms,
    }


def integrity(connection: sqlite3.Connection) -> str:
    return str(connection.execute("PRAGMA integrity_check").fetchone()[0]).lower()
