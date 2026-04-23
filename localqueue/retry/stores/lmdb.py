from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING

from ._shared import _ENVS, _ENVS_LOCK, import_lmdb
from .base import AttemptStoreLockedError, RetryRecord

if TYPE_CHECKING:
    import lmdb


class LMDBAttemptStore:
    path: Path
    _env: lmdb.Environment

    def __init__(self, path: str | Path, *, map_size: int = 10**7) -> None:
        lmdb = import_lmdb()
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
                for _, raw in cursor:
                    record = RetryRecord(**json.loads(bytes(raw).decode("utf-8")))
                    if record.exhausted and record.first_attempt_at <= cutoff:
                        count += 1
            return count
