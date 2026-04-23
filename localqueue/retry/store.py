from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import suppress
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from types import ModuleType
    import lmdb

_ENVS: dict[tuple[str, int], Any] = {}
_ENVS_LOCK = threading.Lock()
_SQLITE_RETRY_SCHEMA_VERSION = 1


def _import_lmdb() -> ModuleType:
    try:
        import lmdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "LMDB support requires the optional dependency; "
            'install with `pip install "localqueue[lmdb]"`'
        ) from exc
    return lmdb


@dataclass(slots=True)
class RetryRecord:
    attempts: int = 0
    first_attempt_at: float = 0.0
    exhausted: bool = False

    @classmethod
    def new(cls) -> "RetryRecord":
        return cls(attempts=0, first_attempt_at=time.time(), exhausted=False)


class AttemptStoreLockedError(RuntimeError):
    path: str

    def __init__(self, path: str | Path) -> None:
        resolved = str(Path(path).resolve())
        super().__init__(
            f"LMDB store at {resolved!r} is locked by another process; "
            + "use a different store_path/db path or stop the competing process"
        )
        self.path = resolved


class AttemptStore(Protocol):
    def load(self, key: str) -> RetryRecord | None: ...

    def save(self, key: str, record: RetryRecord) -> None: ...

    def delete(self, key: str) -> None: ...

    def prune_exhausted(self, *, older_than: float, now: float) -> int: ...

    def count_exhausted_older_than(self, *, older_than: float, now: float) -> int: ...


class MemoryAttemptStore:
    _records: dict[str, RetryRecord]
    _lock: threading.Lock

    def __init__(self) -> None:
        self._records = {}
        self._lock = threading.Lock()

    def load(self, key: str) -> RetryRecord | None:
        with self._lock:
            record = self._records.get(key)
            if record is None:
                return None
            return RetryRecord(**asdict(record))

    def save(self, key: str, record: RetryRecord) -> None:
        with self._lock:
            self._records[key] = RetryRecord(**asdict(record))

    def delete(self, key: str) -> None:
        with self._lock:
            _ = self._records.pop(key, None)

    def prune_exhausted(self, *, older_than: float, now: float) -> int:
        cutoff = now - older_than
        with self._lock:
            doomed = [
                key
                for key, record in self._records.items()
                if record.exhausted and record.first_attempt_at <= cutoff
            ]
            for key in doomed:
                _ = self._records.pop(key, None)
            return len(doomed)

    def count_exhausted_older_than(self, *, older_than: float, now: float) -> int:
        cutoff = now - older_than
        with self._lock:
            return sum(
                1
                for record in self._records.values()
                if record.exhausted and record.first_attempt_at <= cutoff
            )


class LMDBAttemptStore:
    path: Path
    _env: lmdb.Environment

    def __init__(self, path: str | Path, *, map_size: int = 10**7) -> None:
        lmdb = _import_lmdb()
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        key = (str(self.path.resolve()), map_size)
        with _ENVS_LOCK:
            env = _ENVS.get(key)
            if env is None:
                try:
                    env = lmdb.open(
                        str(self.path),
                        map_size=map_size,
                        subdir=True,
                        lock=True,
                    )
                except lmdb.LockError as exc:
                    raise AttemptStoreLockedError(self.path) from exc
                _ENVS[key] = env
            self._env = env

    def load(self, key: str) -> RetryRecord | None:
        with self._env.begin() as txn:
            raw = txn.get(key.encode("utf-8"))
        if raw is None:
            return None
        payload = json.loads(bytes(raw).decode("utf-8"))
        return RetryRecord(**payload)

    def save(self, key: str, record: RetryRecord) -> None:
        payload = json.dumps(asdict(record), separators=(",", ":")).encode("utf-8")
        with self._env.begin(write=True) as txn:
            _ = txn.put(key.encode("utf-8"), payload)

    def delete(self, key: str) -> None:
        with self._env.begin(write=True) as txn:
            _ = txn.delete(key.encode("utf-8"))

    def prune_exhausted(self, *, older_than: float, now: float) -> int:
        cutoff = now - older_than
        with self._env.begin(write=True) as txn:
            cursor = txn.cursor()
            doomed: list[bytes] = []
            if cursor.first():
                for key, raw in cursor:
                    record = RetryRecord(**json.loads(bytes(raw).decode("utf-8")))
                    if record.exhausted and record.first_attempt_at <= cutoff:
                        doomed.append(bytes(key))
            for key in doomed:
                _ = txn.delete(key)
            return len(doomed)

    def count_exhausted_older_than(self, *, older_than: float, now: float) -> int:
        cutoff = now - older_than
        with self._env.begin() as txn:
            cursor = txn.cursor()
            count = 0
            if cursor.first():
                for key, raw in cursor:
                    record = RetryRecord(**json.loads(bytes(raw).decode("utf-8")))
                    if record.exhausted and record.first_attempt_at <= cutoff:
                        count += 1
            return count


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
