from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import suppress
from dataclasses import asdict
from pathlib import Path

from ._shared import _SQLITE_RETRY_SCHEMA_VERSION
from .base import RetryRecord


class SQLiteAttemptStore:
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
                "CREATE TABLE IF NOT EXISTS retry_records ("
                "key TEXT PRIMARY KEY, "
                "record_json TEXT NOT NULL, "
                "first_attempt_at REAL, "
                "exhausted INTEGER"
                ")"
            )
            self._migrate_sqlite_schema(current_schema_version)
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS retry_records_exhausted_idx "
                "ON retry_records(exhausted, first_attempt_at)"
            )
            self._connection.execute(
                f"PRAGMA user_version = {_SQLITE_RETRY_SCHEMA_VERSION}"
            )
            self._connection.commit()
        except Exception:
            self._connection.close()
            raise
        self._lock = threading.Lock()

    def load(self, key: str) -> RetryRecord | None:
        with self._lock:
            cursor = self._connection.execute(
                "SELECT record_json FROM retry_records WHERE key = ?", (key,)
            )
            row = cursor.fetchone()
        if row is None:
            return None
        payload = json.loads(row[0])
        return RetryRecord(**payload)

    def save(self, key: str, record: RetryRecord) -> None:
        payload = json.dumps(asdict(record), separators=(",", ":"))
        with self._lock:
            self._connection.execute(
                "INSERT INTO retry_records("
                "key, record_json, first_attempt_at, exhausted"
                ") VALUES(?, ?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET "
                "record_json = excluded.record_json, "
                "first_attempt_at = excluded.first_attempt_at, "
                "exhausted = excluded.exhausted",
                (key, payload, record.first_attempt_at, int(record.exhausted)),
            )
            self._connection.commit()

    def delete(self, key: str) -> None:
        with self._lock:
            self._connection.execute("DELETE FROM retry_records WHERE key = ?", (key,))
            self._connection.commit()

    def prune_exhausted(self, *, older_than: float, now: float) -> int:
        cutoff = now - older_than
        with self._lock:
            cursor = self._connection.execute(
                "DELETE FROM retry_records "
                "WHERE exhausted = 1 AND first_attempt_at <= ?",
                (cutoff,),
            )
            self._connection.commit()
            return cursor.rowcount

    def count_exhausted_older_than(self, *, older_than: float, now: float) -> int:
        cutoff = now - older_than
        with self._lock:
            cursor = self._connection.execute(
                "SELECT COUNT(*) FROM retry_records "
                "WHERE exhausted = 1 AND first_attempt_at <= ?",
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
        columns = self._sqlite_columns("retry_records")
        if "first_attempt_at" not in columns:
            self._connection.execute(
                "ALTER TABLE retry_records ADD COLUMN first_attempt_at REAL"
            )
        if "exhausted" not in columns:
            self._connection.execute(
                "ALTER TABLE retry_records ADD COLUMN exhausted INTEGER"
            )

        if current_version < 1:
            cursor = self._connection.execute(
                "SELECT key, record_json FROM retry_records"
            )
            for key, raw in cursor.fetchall():
                record = RetryRecord(**json.loads(raw))
                self._connection.execute(
                    "UPDATE retry_records SET first_attempt_at = ?, exhausted = ? "
                    "WHERE key = ?",
                    (record.first_attempt_at, int(record.exhausted), key),
                )

    def _sqlite_columns(self, table: str) -> set[str]:
        cursor = self._connection.execute(f"PRAGMA table_info({table})")
        return {str(row[1]) for row in cursor.fetchall()}

    def _ensure_supported_schema_version(self) -> int:
        cursor = self._connection.execute("PRAGMA user_version")
        row = cursor.fetchone()
        current_version = 0 if row is None else int(row[0])
        if current_version > _SQLITE_RETRY_SCHEMA_VERSION:
            raise ValueError(
                "unsupported SQLite retry schema version: "
                f"{current_version}; expected at most {_SQLITE_RETRY_SCHEMA_VERSION}"
            )
        return current_version
