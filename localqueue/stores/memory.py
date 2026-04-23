from __future__ import annotations

import threading
import time
from collections import Counter

from ._shared import (
    _DEAD,
    _INFLIGHT,
    _READY,
    QueueRecord,
    attempt_event,
    dead_key,
    dead_record_age,
    inflight_key,
    ready_key,
    replace_record,
    stats_from_records,
    validate_limit,
)


class MemoryQueueStore:
    _records: dict[str, dict[str, QueueRecord]]
    _seq: dict[str, int]
    _worker_stats: dict[str, Counter[str]]
    _worker_heartbeats: dict[str, dict[str, float]]
    _dedupe_keys: dict[str, dict[str, str]]
    _lock: threading.Lock

    def __init__(self) -> None:
        self._records = {}
        self._seq = {}
        self._worker_stats = {}
        self._worker_heartbeats = {}
        self._dedupe_keys = {}
        self._lock = threading.Lock()

    def enqueue(
        self,
        queue: str,
        value: object,
        *,
        available_at: float,
        dedupe_key: str | None = None,
    ):
        with self._lock:
            if dedupe_key is not None:
                existing_id = self._dedupe_keys.get(queue, {}).get(dedupe_key)
                if existing_id is not None:
                    record = self._records.get(queue, {}).get(existing_id)
                    if record is not None:
                        return record.to_message()
                    _ = self._dedupe_keys.setdefault(queue, {}).pop(dedupe_key, None)
            record = QueueRecord.new(queue, value, available_at, dedupe_key=dedupe_key)
            seq = self._next_seq(queue)
            record = replace_record(
                record, index_key=ready_key(queue, available_at, seq, record.id)
            )
            self._records.setdefault(queue, {})[record.id] = record
            if dedupe_key is not None:
                self._dedupe_keys.setdefault(queue, {})[dedupe_key] = record.id
            return record.to_message()

    def dequeue(
        self,
        queue: str,
        *,
        lease_timeout: float,
        now: float,
        leased_by: str | None = None,
    ):
        with self._lock:
            self._reclaim_expired(queue, now)
            ready = [
                record
                for record in self._records.get(queue, {}).values()
                if record.state == _READY and record.available_at <= now
            ]
            if not ready:
                return None
            record = min(ready, key=lambda item: item.index_key or b"")
            leased_until = now + lease_timeout
            updated = replace_record(
                record,
                attempts=record.attempts + 1,
                leased_until=leased_until,
                leased_by=leased_by,
                state=_INFLIGHT,
                index_key=inflight_key(queue, leased_until, record.id),
                attempt_history=record.attempt_history
                + [
                    attempt_event(
                        "leased",
                        at=now,
                        attempt=record.attempts + 1,
                        leased_by=leased_by,
                    )
                ],
            )
            self._records[queue][record.id] = updated
            if leased_by is not None:
                self._worker_stats.setdefault(queue, Counter())[leased_by] += 1
            return updated.to_message()

    def get(self, queue: str, message_id: str):
        with self._lock:
            record = self._records.get(queue, {}).get(message_id)
            if record is None:
                return None
            return record.to_message()

    def ack(self, queue: str, message_id: str) -> bool:
        with self._lock:
            record = self._records.get(queue, {}).pop(message_id, None)
            if record is None:
                return False
            if record.dedupe_key is not None:
                self._dedupe_keys.get(queue, {}).pop(record.dedupe_key, None)
            return True

    def release(
        self,
        queue: str,
        message_id: str,
        *,
        available_at: float,
        last_error: dict[str, object] | None = None,
        failed_at: float | None = None,
    ) -> bool:
        with self._lock:
            record = self._records.get(queue, {}).get(message_id)
            if record is None:
                return False
            seq = self._next_seq(queue)
            self._records[queue][message_id] = replace_record(
                record,
                available_at=available_at,
                leased_until=None,
                leased_by=None,
                last_error=last_error if last_error is not None else record.last_error,
                failed_at=failed_at if failed_at is not None else record.failed_at,
                state=_READY,
                index_key=ready_key(queue, available_at, seq, message_id),
                attempt_history=record.attempt_history
                + [
                    attempt_event(
                        "released",
                        at=time.time(),
                        attempt=record.attempts,
                        last_error=last_error
                        if last_error is not None
                        else record.last_error,
                    )
                ],
            )
            return True

    def dead_letter(
        self,
        queue: str,
        message_id: str,
        *,
        last_error: dict[str, object] | None = None,
        failed_at: float | None = None,
    ) -> bool:
        with self._lock:
            record = self._records.get(queue, {}).get(message_id)
            if record is None:
                return False
            self._records[queue][message_id] = replace_record(
                record,
                leased_until=None,
                leased_by=None,
                last_error=last_error if last_error is not None else record.last_error,
                failed_at=failed_at if failed_at is not None else record.failed_at,
                state=_DEAD,
                index_key=dead_key(queue, message_id),
                attempt_history=record.attempt_history
                + [
                    attempt_event(
                        "dead_lettered",
                        at=time.time(),
                        attempt=record.attempts,
                        last_error=last_error
                        if last_error is not None
                        else record.last_error,
                    )
                ],
            )
            return True

    def qsize(self, queue: str, *, now: float) -> int:
        with self._lock:
            self._reclaim_expired(queue, now)
            return sum(
                1
                for record in self._records.get(queue, {}).values()
                if record.state == _READY and record.available_at <= now
            )

    def stats(self, queue: str, *, now: float):
        with self._lock:
            self._reclaim_expired(queue, now)
            return stats_from_records(
                self._records.get(queue, {}).values(),
                now=now,
                leases_by_worker_id=dict(
                    sorted(self._worker_stats.get(queue, {}).items())
                ),
                last_seen_by_worker_id=dict(
                    sorted(self._worker_heartbeats.get(queue, {}).items())
                ),
            )

    def record_worker_heartbeat(
        self, queue: str, worker_id: str, *, now: float
    ) -> None:
        with self._lock:
            self._worker_heartbeats.setdefault(queue, {})[worker_id] = now

    def dead_letters(self, queue: str, *, limit: int | None = None):
        validate_limit(limit)
        with self._lock:
            records = [
                record
                for record in self._records.get(queue, {}).values()
                if record.state == _DEAD
            ]
            records.sort(key=lambda record: record.index_key or b"")
            if limit is not None:
                records = records[:limit]
            return [record.to_message() for record in records]

    def requeue_dead(self, queue: str, message_id: str, *, available_at: float) -> bool:
        with self._lock:
            record = self._records.get(queue, {}).get(message_id)
            if record is None or record.state != _DEAD:
                return False
            seq = self._next_seq(queue)
            self._records[queue][message_id] = replace_record(
                record,
                available_at=available_at,
                leased_until=None,
                leased_by=None,
                state=_READY,
                index_key=ready_key(queue, available_at, seq, message_id),
            )
            return True

    def prune_dead_letters(self, queue: str, *, older_than: float, now: float) -> int:
        with self._lock:
            records = self._records.get(queue, {})
            doomed = [
                message_id
                for message_id, record in records.items()
                if record.state == _DEAD and dead_record_age(record, now=now) >= older_than
            ]
            for message_id in doomed:
                record = records.pop(message_id, None)
                if record is not None and record.dedupe_key is not None:
                    self._dedupe_keys.get(queue, {}).pop(record.dedupe_key, None)
            return len(doomed)

    def count_dead_letters_older_than(
        self, queue: str, *, older_than: float, now: float
    ) -> int:
        with self._lock:
            return sum(
                1
                for record in self._records.get(queue, {}).values()
                if record.state == _DEAD and dead_record_age(record, now=now) >= older_than
            )

    def empty(self, queue: str, *, now: float) -> bool:
        return self.qsize(queue, now=now) == 0

    def purge(self, queue: str) -> int:
        with self._lock:
            count = len(self._records.get(queue, {}))
            self._records[queue] = {}
            self._worker_stats.pop(queue, None)
            self._worker_heartbeats.pop(queue, None)
            self._dedupe_keys.pop(queue, None)
            return count

    def _next_seq(self, queue: str) -> int:
        value = self._seq.get(queue, 0) + 1
        self._seq[queue] = value
        return value

    def _reclaim_expired(self, queue: str, now: float) -> None:
        for record in self._records.get(queue, {}).values():
            if (
                record.state == _INFLIGHT
                and record.leased_until is not None
                and record.leased_until <= now
            ):
                seq = self._next_seq(queue)
                self._records[queue][record.id] = replace_record(
                    record,
                    available_at=now,
                    leased_until=None,
                    leased_by=None,
                    state=_READY,
                    index_key=ready_key(queue, now, seq, record.id),
                    attempt_history=record.attempt_history
                    + [
                        attempt_event(
                            "lease_expired",
                            at=now,
                            attempt=record.attempts,
                            leased_by=record.leased_by,
                        )
                    ],
                )
