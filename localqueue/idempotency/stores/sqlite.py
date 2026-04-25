from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import suppress
from dataclasses import asdict
from pathlib import Path

from ._shared import _SQLITE_IDEMPOTENCY_SCHEMA_VERSION
from .base import IdempotencyRecord


class SQLiteIdempotencyStore:
    path: Path
    _connection: sqlite3.Connection
    _lock: threading.Lock

    def __init__(self, path: str | Path, timeout: float = 15.0) -> None:
        self.path = Path(path)
        if self.path.parent != Path("."):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            self.path, timeout=timeout, check_same_thread=False
        )
        try:
            self._connection.execute("PRAGMA journal_mode=WAL;")
            self._connection.execute("PRAGMA synchronous=NORMAL;")
            current_schema_version = self._ensure_supported_schema_version()
            self._connection.execute(
                "CREATE TABLE IF NOT EXISTS idempotency_records ("
                "key TEXT PRIMARY KEY, "
                "record_json TEXT NOT NULL, "
                "status TEXT NOT NULL, "
                "completed_at REAL"
                ")"
            )
            self._migrate_sqlite_schema(current_schema_version)
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS idempotency_records_completed_idx "
                "ON idempotency_records(status, completed_at)"
            )
            self._connection.execute(
                f"PRAGMA user_version = {_SQLITE_IDEMPOTENCY_SCHEMA_VERSION}"
            )
            self._connection.commit()
        except Exception:
            self._connection.close()
            raise
        self._lock = threading.Lock()

    def load(self, key: str) -> IdempotencyRecord | None:
        with self._lock:
            cursor = self._connection.execute(
                "SELECT record_json FROM idempotency_records WHERE key = ?", (key,)
            )
            row = cursor.fetchone()
        if row is None:
            return None
        payload = json.loads(row[0])
        return IdempotencyRecord(**payload)

    def save(self, key: str, record: IdempotencyRecord) -> None:
        payload = json.dumps(asdict(record), separators=(",", ":"))
        with self._lock:
            self._connection.execute(
                "INSERT INTO idempotency_records("
                "key, record_json, status, completed_at"
                ") VALUES(?, ?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET "
                "record_json = excluded.record_json, "
                "status = excluded.status, "
                "completed_at = excluded.completed_at",
                (key, payload, record.status, record.completed_at),
            )
            self._connection.commit()

    def delete(self, key: str) -> None:
        with self._lock:
            self._connection.execute(
                "DELETE FROM idempotency_records WHERE key = ?", (key,)
            )
            self._connection.commit()

    def prune_completed(self, *, older_than: float, now: float) -> int:
        cutoff = now - older_than
        with self._lock:
            cursor = self._connection.execute(
                "DELETE FROM idempotency_records "
                "WHERE status = 'succeeded' AND completed_at IS NOT NULL "
                "AND completed_at <= ?",
                (cutoff,),
            )
            self._connection.commit()
            return cursor.rowcount

    def count_completed_older_than(self, *, older_than: float, now: float) -> int:
        cutoff = now - older_than
        with self._lock:
            cursor = self._connection.execute(
                "SELECT COUNT(*) FROM idempotency_records "
                "WHERE status = 'succeeded' AND completed_at IS NOT NULL "
                "AND completed_at <= ?",
                (cutoff,),
            )
            return int(cursor.fetchone()[0])

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __del__(self) -> None:  # pragma: no cover
        with suppress(Exception):
            self.close()

    def _migrate_sqlite_schema(self, current_version: int) -> None:
        columns = self._sqlite_columns("idempotency_records")
        if "status" not in columns:
            self._connection.execute(
                "ALTER TABLE idempotency_records ADD COLUMN status TEXT NOT NULL "
                "DEFAULT 'pending'"
            )
        if "completed_at" not in columns:
            self._connection.execute(
                "ALTER TABLE idempotency_records ADD COLUMN completed_at REAL"
            )

        if current_version < 1:
            cursor = self._connection.execute(
                "SELECT key, record_json FROM idempotency_records"
            )
            for key, raw in cursor.fetchall():
                record = IdempotencyRecord(**json.loads(raw))
                self._connection.execute(
                    "UPDATE idempotency_records SET status = ?, completed_at = ? "
                    "WHERE key = ?",
                    (record.status, record.completed_at, key),
                )

    def _sqlite_columns(self, table: str) -> set[str]:
        cursor = self._connection.execute(f"PRAGMA table_info({table})")
        return {str(row[1]) for row in cursor.fetchall()}

    def _ensure_supported_schema_version(self) -> int:
        cursor = self._connection.execute("PRAGMA user_version")
        row = cursor.fetchone()
        current_version = 0 if row is None else int(row[0])
        if current_version > _SQLITE_IDEMPOTENCY_SCHEMA_VERSION:
            raise ValueError(
                "unsupported SQLite idempotency schema version: "
                + f"{current_version}; expected at most "
                + f"{_SQLITE_IDEMPOTENCY_SCHEMA_VERSION}"
            )
        return current_version
