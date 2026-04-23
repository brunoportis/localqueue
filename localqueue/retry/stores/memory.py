from __future__ import annotations

import threading
from dataclasses import asdict

from .base import RetryRecord


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
