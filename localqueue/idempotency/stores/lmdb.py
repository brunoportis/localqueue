from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING

from ._shared import _ENVS, _ENVS_LOCK, import_lmdb
from .base import IdempotencyRecord, IdempotencyStoreLockedError

if TYPE_CHECKING:
    import lmdb


class LMDBIdempotencyStore:
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
                    raise IdempotencyStoreLockedError(self.path) from exc
                _ENVS[key] = env
            self._env = env

    def load(self, key: str) -> IdempotencyRecord | None:
        with self._env.begin() as txn:
            raw = txn.get(key.encode("utf-8"))
        if raw is None:
            return None
        payload = json.loads(bytes(raw).decode("utf-8"))
        return IdempotencyRecord(**payload)

    def save(self, key: str, record: IdempotencyRecord) -> None:
        payload = json.dumps(asdict(record), separators=(",", ":")).encode("utf-8")
        with self._env.begin(write=True) as txn:
            _ = txn.put(key.encode("utf-8"), payload)

    def delete(self, key: str) -> None:
        with self._env.begin(write=True) as txn:
            _ = txn.delete(key.encode("utf-8"))

    def prune_completed(self, *, older_than: float, now: float) -> int:
        cutoff = now - older_than
        with self._env.begin(write=True) as txn:
            cursor = txn.cursor()
            doomed: list[bytes] = []
            if cursor.first():
                for key, raw in cursor:
                    record = IdempotencyRecord(**json.loads(bytes(raw).decode("utf-8")))
                    if (
                        record.status == "succeeded"
                        and record.completed_at is not None
                        and record.completed_at <= cutoff
                    ):
                        doomed.append(bytes(key))
            for key in doomed:
                _ = txn.delete(key)
            return len(doomed)

    def count_completed_older_than(self, *, older_than: float, now: float) -> int:
        cutoff = now - older_than
        with self._env.begin() as txn:
            cursor = txn.cursor()
            count = 0
            if cursor.first():
                for _, raw in cursor:
                    record = IdempotencyRecord(**json.loads(bytes(raw).decode("utf-8")))
                    if (
                        record.status == "succeeded"
                        and record.completed_at is not None
                        and record.completed_at <= cutoff
                    ):
                        count += 1
            return count
