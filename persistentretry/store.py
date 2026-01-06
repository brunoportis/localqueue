from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol

import lmdb

_ENVS: dict[tuple[str, int], lmdb.Environment] = {}
_ENVS_LOCK = threading.Lock()


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


class LMDBAttemptStore:
    path: Path
    _env: lmdb.Environment

    def __init__(self, path: str | Path, *, map_size: int = 10**7) -> None:
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


class SQLiteAttemptStore:
    path: Path
    _connection: sqlite3.Connection
    _lock: threading.Lock

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if self.path.parent != Path("."):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path, check_same_thread=False)
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS retry_records ("
            "key TEXT PRIMARY KEY, "
            "record_json TEXT NOT NULL"
            ")"
        )
        self._connection.commit()
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
                "INSERT INTO retry_records(key, record_json) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET record_json = excluded.record_json",
                (key, payload),
            )
            self._connection.commit()

    def delete(self, key: str) -> None:
        with self._lock:
            self._connection.execute("DELETE FROM retry_records WHERE key = ?", (key,))
            self._connection.commit()

    def close(self) -> None:
        with self._lock:
            self._connection.close()
