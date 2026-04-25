from __future__ import annotations

import threading
from dataclasses import asdict

from .base import IdempotencyRecord


class MemoryIdempotencyStore:
    _records: dict[str, IdempotencyRecord]
    _lock: threading.Lock

    def __init__(self) -> None:
        self._records = {}
        self._lock = threading.Lock()

    def load(self, key: str) -> IdempotencyRecord | None:
        with self._lock:
            record = self._records.get(key)
            if record is None:
                return None
            return IdempotencyRecord(**asdict(record))

    def save(self, key: str, record: IdempotencyRecord) -> None:
        with self._lock:
            self._records[key] = IdempotencyRecord(**asdict(record))

    def delete(self, key: str) -> None:
        with self._lock:
            _ = self._records.pop(key, None)

    def prune_completed(self, *, older_than: float, now: float) -> int:
        cutoff = now - older_than
        with self._lock:
            doomed = [
                key
                for key, record in self._records.items()
                if record.status == "succeeded"
                and record.completed_at is not None
                and record.completed_at <= cutoff
            ]
            for key in doomed:
                _ = self._records.pop(key, None)
            return len(doomed)

    def count_completed_older_than(self, *, older_than: float, now: float) -> int:
        cutoff = now - older_than
        with self._lock:
            return sum(
                1
                for record in self._records.values()
                if record.status == "succeeded"
                and record.completed_at is not None
                and record.completed_at <= cutoff
            )
