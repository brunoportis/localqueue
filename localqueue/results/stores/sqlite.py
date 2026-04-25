from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import suppress
from pathlib import Path
from typing import Any

from ._shared import _SQLITE_RESULT_SCHEMA_VERSION


class SQLiteResultStore:
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
                "CREATE TABLE IF NOT EXISTS queue_results ("
                "key TEXT PRIMARY KEY, "
                "value_json TEXT NOT NULL"
                ")"
            )
            self._migrate_sqlite_schema(current_schema_version)
            self._connection.execute(
                f"PRAGMA user_version = {_SQLITE_RESULT_SCHEMA_VERSION}"
            )
            self._connection.commit()
        except Exception:
            self._connection.close()
            raise
        self._lock = threading.Lock()

    def load(self, key: str) -> Any | None:
        with self._lock:
            cursor = self._connection.execute(
                "SELECT value_json FROM queue_results WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def save(self, key: str, value: Any) -> None:
        payload = json.dumps(value, separators=(",", ":"))
        with self._lock:
            self._connection.execute(
                "INSERT INTO queue_results(key, value_json) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json",
                (key, payload),
            )
            self._connection.commit()

    def delete(self, key: str) -> None:
        with self._lock:
            self._connection.execute("DELETE FROM queue_results WHERE key = ?", (key,))
            self._connection.commit()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __del__(self) -> None:  # pragma: no cover
        with suppress(Exception):
            self.close()

    def _migrate_sqlite_schema(self, current_version: int) -> None:
        if current_version < 1:
            return

    def _ensure_supported_schema_version(self) -> int:
        cursor = self._connection.execute("PRAGMA user_version")
        row = cursor.fetchone()
        current_version = 0 if row is None else int(row[0])
        if current_version > _SQLITE_RESULT_SCHEMA_VERSION:
            raise ValueError(
                "unsupported SQLite result schema version: "
                + f"{current_version}; expected at most "
                + f"{_SQLITE_RESULT_SCHEMA_VERSION}"
            )
        return current_version
