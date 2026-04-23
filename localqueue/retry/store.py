import sqlite3
import threading
from pathlib import Path

from .stores import (
    AttemptStore,
    AttemptStoreLockedError,
    LMDBAttemptStore as _LMDBAttemptStore,
    MemoryAttemptStore,
    RetryRecord,
    SQLiteAttemptStore as _SQLiteAttemptStore,
)
from .stores._shared import (
    _ENVS,
    _ENVS_LOCK,
    _SQLITE_RETRY_SCHEMA_VERSION,
    import_lmdb as _import_lmdb,
)

__all__ = [
    "AttemptStore",
    "AttemptStoreLockedError",
    "LMDBAttemptStore",
    "MemoryAttemptStore",
    "RetryRecord",
    "SQLiteAttemptStore",
    "_ENVS",
    "_ENVS_LOCK",
    "_SQLITE_RETRY_SCHEMA_VERSION",
    "_import_lmdb",
]


class LMDBAttemptStore(_LMDBAttemptStore):
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


class SQLiteAttemptStore(_SQLiteAttemptStore):
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
