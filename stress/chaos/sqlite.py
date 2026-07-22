from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


def database_path(queue_dir: Path) -> Path:
    return queue_dir / "localqueue.db"


def inspect_persistent_pragmas(path: Path) -> dict[str, Any]:
    with sqlite3.connect(path) as connection:
        return {
            "journal_mode": str(
                connection.execute("PRAGMA journal_mode").fetchone()[0]
            ).lower(),
        }


def product_sqlite_settings(queue: Any) -> dict[str, Any]:
    journal_mode, synchronous = queue._native.pragma_settings()
    busy_timeout_ms = queue._native._test_busy_timeout()
    return {
        "journal_mode": str(journal_mode).lower(),
        "synchronous": int(synchronous),
        "busy_timeout_ms": int(busy_timeout_ms),
    }


def integrity_check(path: Path) -> str:
    with sqlite3.connect(path) as connection:
        return str(connection.execute("PRAGMA integrity_check").fetchone()[0])


def checkpoint(path: Path) -> tuple[int, int, int]:
    with sqlite3.connect(path) as connection:
        row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    return int(row[0]), int(row[1]), int(row[2])


def sqlite_error_fields(error: BaseException) -> dict[str, str]:
    code = getattr(error, "sqlite_errorname", None)
    return {
        "public_type": type(error).__name__,
        "sqlite_code": str(code or ""),
        "message": " ".join(str(error).split()),
    }
