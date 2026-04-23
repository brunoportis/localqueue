from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from collections import Counter
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import ModuleType
    import lmdb

_ENVS: dict[tuple[str, int], Any] = {}
_ENVS_LOCK = threading.Lock()
_READY = "ready"
_INFLIGHT = "inflight"
_DEAD = "dead"
_QUEUE_RECORD_VERSION = 4
_SQLITE_SCHEMA_VERSION = 2


def _import_lmdb() -> ModuleType:
    try:
        import lmdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "LMDB support requires the optional dependency; "
            'install with `pip install "localqueue[lmdb]"`'
        ) from exc
    return lmdb


def _attempt_event(
    event_type: str,
    *,
    at: float,
    attempt: int,
    leased_by: str | None = None,
    last_error: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": event_type,
        "at": at,
        "attempt": attempt,
    }
    if leased_by is not None:
        event["leased_by"] = leased_by
    if last_error is not None:
        event["last_error"] = last_error
    return event


@dataclass(frozen=True, slots=True)
class QueueMessage:
    id: str
    value: Any
    queue: str
    state: str = _READY
    attempts: int = 0
    created_at: float = 0.0
    available_at: float = 0.0
    leased_until: float | None = None
    leased_by: str | None = None
    last_error: dict[str, Any] | None = None
    failed_at: float | None = None
    attempt_history: list[dict[str, Any]] = field(default_factory=list)
    dedupe_key: str | None = None


@dataclass(frozen=True, slots=True)
class QueueStats:
    ready: int = 0
    delayed: int = 0
    inflight: int = 0
    dead: int = 0
    total: int = 0
    by_worker_id: dict[str, int] = field(default_factory=dict)
    leases_by_worker_id: dict[str, int] = field(default_factory=dict)
    last_seen_by_worker_id: dict[str, float] = field(default_factory=dict)
    oldest_ready_age_seconds: float | None = None
    oldest_inflight_age_seconds: float | None = None
    average_inflight_age_seconds: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "delayed": self.delayed,
            "inflight": self.inflight,
            "dead": self.dead,
            "total": self.total,
            "by_worker_id": self.by_worker_id,
            "leases_by_worker_id": self.leases_by_worker_id,
            "last_seen_by_worker_id": self.last_seen_by_worker_id,
            "oldest_ready_age_seconds": self.oldest_ready_age_seconds,
            "oldest_inflight_age_seconds": self.oldest_inflight_age_seconds,
            "average_inflight_age_seconds": self.average_inflight_age_seconds,
        }


@dataclass(frozen=True, slots=True)
class _QueueRecord:
    id: str
    value: Any
    queue: str
    attempts: int
    created_at: float
    available_at: float
    leased_until: float | None
    leased_by: str | None
    last_error: dict[str, Any] | None
    failed_at: float | None
    dedupe_key: str | None
    state: str
    index_key: bytes | None
    attempt_history: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def new(
        cls,
        queue: str,
        value: Any,
        available_at: float,
        *,
        dedupe_key: str | None = None,
    ) -> "_QueueRecord":
        return cls(
            id=uuid.uuid4().hex,
            value=value,
            queue=queue,
            attempts=0,
            created_at=time.time(),
            available_at=available_at,
            leased_until=None,
            leased_by=None,
            last_error=None,
            failed_at=None,
            dedupe_key=dedupe_key,
            state=_READY,
            index_key=None,
        )

    def to_message(self) -> QueueMessage:
        return QueueMessage(
            id=self.id,
            value=self.value,
            queue=self.queue,
            state=self.state,
            attempts=self.attempts,
            created_at=self.created_at,
            available_at=self.available_at,
            leased_until=self.leased_until,
            leased_by=self.leased_by,
            last_error=self.last_error,
            failed_at=self.failed_at,
            attempt_history=list(self.attempt_history),
            dedupe_key=self.dedupe_key,
        )


class QueueStore(Protocol):
    def enqueue(
        self,
        queue: str,
        value: Any,
        *,
        available_at: float,
        dedupe_key: str | None = None,
    ) -> QueueMessage: ...

    def dequeue(
        self,
        queue: str,
        *,
        lease_timeout: float,
        now: float,
        leased_by: str | None = None,
    ) -> QueueMessage | None: ...

    def get(self, queue: str, message_id: str) -> QueueMessage | None: ...

    def ack(self, queue: str, message_id: str) -> bool: ...

    def release(
        self,
        queue: str,
        message_id: str,
        *,
        available_at: float,
        last_error: dict[str, Any] | None = None,
        failed_at: float | None = None,
    ) -> bool: ...

    def dead_letter(
        self,
        queue: str,
        message_id: str,
        *,
        last_error: dict[str, Any] | None = None,
        failed_at: float | None = None,
    ) -> bool: ...

    def qsize(self, queue: str, *, now: float) -> int: ...

    def stats(self, queue: str, *, now: float) -> QueueStats: ...

    def record_worker_heartbeat(
        self, queue: str, worker_id: str, *, now: float
    ) -> None: ...

    def dead_letters(
        self, queue: str, *, limit: int | None = None
    ) -> list[QueueMessage]: ...

    def requeue_dead(
        self, queue: str, message_id: str, *, available_at: float
    ) -> bool: ...

    def prune_dead_letters(
        self, queue: str, *, older_than: float, now: float
    ) -> int: ...

    def count_dead_letters_older_than(
        self, queue: str, *, older_than: float, now: float
    ) -> int: ...

    def empty(self, queue: str, *, now: float) -> bool: ...

    def purge(self, queue: str) -> int: ...


class QueueStoreLockedError(RuntimeError):
    path: str

    def __init__(self, path: str | Path) -> None:
        resolved = str(Path(path).resolve())
        super().__init__(
            f"LMDB queue store at {resolved!r} is locked by another process; "
            + "use a different store_path/db path or stop the competing process"
        )
        self.path = resolved


class MemoryQueueStore:
    _records: dict[str, dict[str, _QueueRecord]]
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
        value: Any,
        *,
        available_at: float,
        dedupe_key: str | None = None,
    ) -> QueueMessage:
        with self._lock:
            if dedupe_key is not None:
                existing_id = self._dedupe_keys.get(queue, {}).get(dedupe_key)
                if existing_id is not None:
                    record = self._records.get(queue, {}).get(existing_id)
                    if record is not None:
                        return record.to_message()
                    _ = self._dedupe_keys.setdefault(queue, {}).pop(dedupe_key, None)
            record = _QueueRecord.new(queue, value, available_at, dedupe_key=dedupe_key)
            seq = self._next_seq(queue)
            record = replace(
                record, index_key=self._ready_key(queue, available_at, seq, record.id)
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
    ) -> QueueMessage | None:
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
            updated = replace(
                record,
                attempts=record.attempts + 1,
                leased_until=leased_until,
                leased_by=leased_by,
                state=_INFLIGHT,
                index_key=self._inflight_key(queue, leased_until, record.id),
                attempt_history=record.attempt_history
                + [
                    _attempt_event(
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

    def get(self, queue: str, message_id: str) -> QueueMessage | None:
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
        last_error: dict[str, Any] | None = None,
        failed_at: float | None = None,
    ) -> bool:
        with self._lock:
            record = self._records.get(queue, {}).get(message_id)
            if record is None:
                return False
            seq = self._next_seq(queue)
            self._records[queue][message_id] = replace(
                record,
                available_at=available_at,
                leased_until=None,
                leased_by=None,
                last_error=last_error if last_error is not None else record.last_error,
                failed_at=failed_at if failed_at is not None else record.failed_at,
                state=_READY,
                index_key=self._ready_key(queue, available_at, seq, message_id),
                attempt_history=record.attempt_history
                + [
                    _attempt_event(
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
        last_error: dict[str, Any] | None = None,
        failed_at: float | None = None,
    ) -> bool:
        with self._lock:
            record = self._records.get(queue, {}).get(message_id)
            if record is None:
                return False
            self._records[queue][message_id] = replace(
                record,
                leased_until=None,
                leased_by=None,
                last_error=last_error if last_error is not None else record.last_error,
                failed_at=failed_at if failed_at is not None else record.failed_at,
                state=_DEAD,
                index_key=self._dead_key(queue, message_id),
                attempt_history=record.attempt_history
                + [
                    _attempt_event(
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

    def stats(self, queue: str, *, now: float) -> QueueStats:
        with self._lock:
            self._reclaim_expired(queue, now)
            return _stats_from_records(
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

    def dead_letters(
        self, queue: str, *, limit: int | None = None
    ) -> list[QueueMessage]:
        _validate_limit(limit)
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
            self._records[queue][message_id] = replace(
                record,
                available_at=available_at,
                leased_until=None,
                leased_by=None,
                state=_READY,
                index_key=self._ready_key(queue, available_at, seq, message_id),
            )
            return True

    def prune_dead_letters(self, queue: str, *, older_than: float, now: float) -> int:
        with self._lock:
            records = self._records.get(queue, {})
            doomed = [
                message_id
                for message_id, record in records.items()
                if record.state == _DEAD
                and _dead_record_age(record, now=now) >= older_than
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
                if record.state == _DEAD
                and _dead_record_age(record, now=now) >= older_than
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
        for record in list(self._records.get(queue, {}).values()):
            if (
                record.state == _INFLIGHT
                and record.leased_until is not None
                and record.leased_until <= now
            ):
                seq = self._next_seq(queue)
                self._records[queue][record.id] = replace(
                    record,
                    available_at=now,
                    leased_until=None,
                    leased_by=None,
                    state=_READY,
                    index_key=self._ready_key(queue, now, seq, record.id),
                    attempt_history=record.attempt_history
                    + [
                        _attempt_event(
                            "lease_expired",
                            at=now,
                            attempt=record.attempts,
                            leased_by=record.leased_by,
                        )
                    ],
                )

    @staticmethod
    def _ready_key(queue: str, available_at: float, seq: int, message_id: str) -> bytes:
        return _ready_key(queue, available_at, seq, message_id)

    @staticmethod
    def _inflight_key(queue: str, leased_until: float, message_id: str) -> bytes:
        return _inflight_key(queue, leased_until, message_id)

    @staticmethod
    def _dead_key(queue: str, message_id: str) -> bytes:
        return _dead_key(queue, message_id)


class SQLiteQueueStore:
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
                "CREATE TABLE IF NOT EXISTS queue_messages ("
                "queue TEXT NOT NULL, "
                "id TEXT NOT NULL, "
                "record_json TEXT NOT NULL, "
                "state TEXT NOT NULL, "
                "created_at REAL, "
                "available_at REAL NOT NULL, "
                "leased_until REAL, "
                "leased_by TEXT, "
                "last_leased_at REAL, "
                "failed_at REAL, "
                "sequence INTEGER NOT NULL, "
                "PRIMARY KEY(queue, id)"
                ")"
            )
            self._connection.execute(
                "CREATE TABLE IF NOT EXISTS queue_sequences ("
                "queue TEXT PRIMARY KEY, "
                "value INTEGER NOT NULL"
                ")"
            )
            self._connection.execute(
                "CREATE TABLE IF NOT EXISTS queue_worker_stats ("
                "queue TEXT NOT NULL, "
                "worker_id TEXT NOT NULL, "
                "leased_count INTEGER NOT NULL, "
                "PRIMARY KEY(queue, worker_id)"
                ")"
            )
            self._connection.execute(
                "CREATE TABLE IF NOT EXISTS queue_worker_heartbeats ("
                "queue TEXT NOT NULL, "
                "worker_id TEXT NOT NULL, "
                "last_seen REAL NOT NULL, "
                "PRIMARY KEY(queue, worker_id)"
                ")"
            )
            self._connection.execute(
                "CREATE TABLE IF NOT EXISTS queue_dedupe_keys ("
                "queue TEXT NOT NULL, "
                "dedupe_key TEXT NOT NULL, "
                "message_id TEXT NOT NULL, "
                "PRIMARY KEY(queue, dedupe_key)"
                ")"
            )
            self._migrate_sqlite_schema(current_schema_version)
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS queue_messages_ready_idx "
                "ON queue_messages(queue, state, available_at, sequence)"
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS queue_messages_inflight_idx "
                "ON queue_messages(queue, state, leased_until)"
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS queue_messages_dead_idx "
                "ON queue_messages(queue, state, id)"
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS queue_messages_dead_age_idx "
                "ON queue_messages(queue, state, failed_at, created_at)"
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS queue_messages_inflight_worker_idx "
                "ON queue_messages(queue, state, leased_by)"
            )
            self._connection.execute(f"PRAGMA user_version = {_SQLITE_SCHEMA_VERSION}")
            self._connection.commit()
        except Exception:
            self._connection.close()
            raise
        self._lock = threading.Lock()

    def enqueue(
        self,
        queue: str,
        value: Any,
        *,
        available_at: float,
        dedupe_key: str | None = None,
    ) -> QueueMessage:
        _validate_json_serializable(value)
        with self._transaction() as connection:
            if dedupe_key is not None:
                existing_id = self._lookup_dedupe_key(connection, queue, dedupe_key)
                if existing_id is not None:
                    record = self._get_record(connection, queue, existing_id)
                    if record is not None:
                        return record.to_message()
                    self._delete_dedupe_key(connection, queue, dedupe_key)
            record = _QueueRecord.new(queue, value, available_at, dedupe_key=dedupe_key)
            seq = self._next_seq(connection, queue)
            record = replace(
                record, index_key=_ready_key(queue, available_at, seq, record.id)
            )
            self._upsert_record(connection, record, sequence=seq)
            if dedupe_key is not None:
                self._set_dedupe_key(connection, queue, dedupe_key, record.id)
            return record.to_message()

    def dequeue(
        self,
        queue: str,
        *,
        lease_timeout: float,
        now: float,
        leased_by: str | None = None,
    ) -> QueueMessage | None:
        with self._transaction() as connection:
            self._reclaim_expired(connection, queue, now)
            cursor = connection.execute(
                "SELECT id, record_json FROM queue_messages "
                "WHERE queue = ? AND state = ? AND available_at <= ? "
                "ORDER BY available_at, sequence LIMIT 1",
                (queue, _READY, now),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            record = _decode_record(row[1])
            leased_until = now + lease_timeout
            updated = replace(
                record,
                attempts=record.attempts + 1,
                leased_until=leased_until,
                leased_by=leased_by,
                state=_INFLIGHT,
                index_key=_inflight_key(queue, leased_until, record.id),
                attempt_history=record.attempt_history
                + [
                    _attempt_event(
                        "leased",
                        at=now,
                        attempt=record.attempts + 1,
                        leased_by=leased_by,
                    )
                ],
            )
            self._upsert_record(connection, updated, sequence=self._sequence(row[1]))
            if leased_by is not None:
                self._increment_worker_stats(connection, queue, leased_by)
            return updated.to_message()

    def get(self, queue: str, message_id: str) -> QueueMessage | None:
        with self._lock:
            record = self._get_record(self._connection, queue, message_id)
            if record is None:
                return None
            return record.to_message()

    def ack(self, queue: str, message_id: str) -> bool:
        with self._transaction() as connection:
            record = self._get_record(connection, queue, message_id)
            if record is None:
                return False
            self._delete_dedupe_key_for_record(connection, record)
            cursor = connection.execute(
                "DELETE FROM queue_messages WHERE queue = ? AND id = ?",
                (queue, message_id),
            )
            return cursor.rowcount > 0

    def release(
        self,
        queue: str,
        message_id: str,
        *,
        available_at: float,
        last_error: dict[str, Any] | None = None,
        failed_at: float | None = None,
    ) -> bool:
        with self._transaction() as connection:
            record = self._get_record(connection, queue, message_id)
            if record is None:
                return False
            seq = self._next_seq(connection, queue)
            updated = replace(
                record,
                available_at=available_at,
                leased_until=None,
                leased_by=None,
                last_error=last_error if last_error is not None else record.last_error,
                failed_at=failed_at if failed_at is not None else record.failed_at,
                state=_READY,
                index_key=_ready_key(queue, available_at, seq, message_id),
                attempt_history=record.attempt_history
                + [
                    _attempt_event(
                        "released",
                        at=time.time(),
                        attempt=record.attempts,
                        last_error=last_error
                        if last_error is not None
                        else record.last_error,
                    )
                ],
            )
            self._upsert_record(connection, updated, sequence=seq)
            return True

    def dead_letter(
        self,
        queue: str,
        message_id: str,
        *,
        last_error: dict[str, Any] | None = None,
        failed_at: float | None = None,
    ) -> bool:
        with self._transaction() as connection:
            record = self._get_record(connection, queue, message_id)
            if record is None:
                return False
            updated = replace(
                record,
                leased_until=None,
                leased_by=None,
                last_error=last_error if last_error is not None else record.last_error,
                failed_at=failed_at if failed_at is not None else record.failed_at,
                state=_DEAD,
                index_key=_dead_key(queue, message_id),
                attempt_history=record.attempt_history
                + [
                    _attempt_event(
                        "dead_lettered",
                        at=time.time(),
                        attempt=record.attempts,
                        last_error=last_error
                        if last_error is not None
                        else record.last_error,
                    )
                ],
            )
            self._upsert_record(
                connection,
                updated,
                sequence=self._sequence_for_id(connection, queue, message_id),
            )
            return True

    def qsize(self, queue: str, *, now: float) -> int:
        with self._lock:
            cursor = self._connection.execute(
                "SELECT COUNT(*) FROM queue_messages "
                "WHERE queue = ? AND state = ? AND available_at <= ?",
                (queue, _READY, now),
            )
            return int(cursor.fetchone()[0])

    def stats(self, queue: str, *, now: float) -> QueueStats:
        with self._transaction() as connection:
            self._reclaim_expired(connection, queue, now)
            row = connection.execute(
                "SELECT "
                "COUNT(*) AS total, "
                "COALESCE(SUM(CASE WHEN state = ? AND available_at <= ? "
                "THEN 1 ELSE 0 END), 0) AS ready, "
                "COALESCE(SUM(CASE WHEN state = ? AND available_at > ? "
                "THEN 1 ELSE 0 END), 0) AS delayed, "
                "COALESCE(SUM(CASE WHEN state = ? THEN 1 ELSE 0 END), 0) "
                "AS inflight, "
                "COALESCE(SUM(CASE WHEN state = ? THEN 1 ELSE 0 END), 0) "
                "AS dead, "
                "MAX(CASE WHEN state = ? AND available_at <= ? "
                "THEN ? - available_at ELSE NULL END) AS oldest_ready_age, "
                "MAX(CASE WHEN state = ? AND last_leased_at IS NOT NULL "
                "THEN ? - last_leased_at ELSE NULL END) "
                "AS oldest_inflight_age, "
                "AVG(CASE WHEN state = ? AND last_leased_at IS NOT NULL "
                "THEN ? - last_leased_at ELSE NULL END) "
                "AS average_inflight_age "
                "FROM queue_messages WHERE queue = ?",
                (
                    _READY,
                    now,
                    _READY,
                    now,
                    _INFLIGHT,
                    _DEAD,
                    _READY,
                    now,
                    now,
                    _INFLIGHT,
                    now,
                    _INFLIGHT,
                    now,
                    queue,
                ),
            ).fetchone()
            by_worker = connection.execute(
                "SELECT leased_by, COUNT(*) FROM queue_messages "
                "WHERE queue = ? AND state = ? AND leased_by IS NOT NULL "
                "GROUP BY leased_by ORDER BY leased_by",
                (queue, _INFLIGHT),
            )
            worker_stats = connection.execute(
                "SELECT worker_id, leased_count FROM queue_worker_stats "
                "WHERE queue = ? ORDER BY worker_id",
                (queue,),
            )
            heartbeat_stats = connection.execute(
                "SELECT worker_id, last_seen FROM queue_worker_heartbeats "
                "WHERE queue = ? ORDER BY worker_id",
                (queue,),
            )
            assert row is not None
            return QueueStats(
                ready=int(row[1]),
                delayed=int(row[2]),
                inflight=int(row[3]),
                dead=int(row[4]),
                total=int(row[0]),
                by_worker_id={worker: int(count) for worker, count in by_worker},
                leases_by_worker_id={
                    worker: int(count) for worker, count in worker_stats.fetchall()
                },
                last_seen_by_worker_id={
                    worker: float(last_seen)
                    for worker, last_seen in heartbeat_stats.fetchall()
                },
                oldest_ready_age_seconds=(
                    None if row[5] is None else max(float(row[5]), 0.0)
                ),
                oldest_inflight_age_seconds=(
                    None if row[6] is None else max(float(row[6]), 0.0)
                ),
                average_inflight_age_seconds=(
                    None if row[7] is None else max(float(row[7]), 0.0)
                ),
            )

    def record_worker_heartbeat(
        self, queue: str, worker_id: str, *, now: float
    ) -> None:
        with self._transaction() as connection:
            _ = connection.execute(
                "INSERT INTO queue_worker_heartbeats(queue, worker_id, last_seen) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(queue, worker_id) DO UPDATE SET "
                "last_seen = excluded.last_seen",
                (queue, worker_id, now),
            )

    def dead_letters(
        self, queue: str, *, limit: int | None = None
    ) -> list[QueueMessage]:
        _validate_limit(limit)
        query = (
            "SELECT record_json FROM queue_messages "
            "WHERE queue = ? AND state = ? ORDER BY id"
        )
        params: tuple[Any, ...]
        if limit is None:
            params = (queue, _DEAD)
        else:
            query += " LIMIT ?"
            params = (queue, _DEAD, limit)
        with self._lock:
            cursor = self._connection.execute(query, params)
            return [_decode_record(row[0]).to_message() for row in cursor.fetchall()]

    def requeue_dead(self, queue: str, message_id: str, *, available_at: float) -> bool:
        with self._transaction() as connection:
            record = self._get_record(connection, queue, message_id)
            if record is None or record.state != _DEAD:
                return False
            seq = self._next_seq(connection, queue)
            updated = replace(
                record,
                available_at=available_at,
                leased_until=None,
                leased_by=None,
                state=_READY,
                index_key=_ready_key(queue, available_at, seq, message_id),
            )
            self._upsert_record(connection, updated, sequence=seq)
            return True

    def prune_dead_letters(self, queue: str, *, older_than: float, now: float) -> int:
        cutoff = now - older_than
        with self._transaction() as connection:
            _ = connection.execute(
                "DELETE FROM queue_dedupe_keys "
                "WHERE queue = ? AND message_id IN ("
                "SELECT id FROM queue_messages "
                "WHERE queue = ? AND state = ? "
                "AND COALESCE(failed_at, created_at, 0) <= ?"
                ")",
                (queue, queue, _DEAD, cutoff),
            )
            cursor = connection.execute(
                "DELETE FROM queue_messages "
                "WHERE queue = ? AND state = ? "
                "AND COALESCE(failed_at, created_at, 0) <= ?",
                (queue, _DEAD, cutoff),
            )
            return cursor.rowcount

    def count_dead_letters_older_than(
        self, queue: str, *, older_than: float, now: float
    ) -> int:
        cutoff = now - older_than
        with self._lock:
            cursor = self._connection.execute(
                "SELECT COUNT(*) FROM queue_messages "
                "WHERE queue = ? AND state = ? "
                "AND COALESCE(failed_at, created_at, 0) <= ?",
                (queue, _DEAD, cutoff),
            )
            return int(cursor.fetchone()[0])

    def empty(self, queue: str, *, now: float) -> bool:
        return self.qsize(queue, now=now) == 0

    def purge(self, queue: str) -> int:
        with self._transaction() as connection:
            _ = connection.execute(
                "DELETE FROM queue_dedupe_keys WHERE queue = ?", (queue,)
            )
            cursor = connection.execute(
                "DELETE FROM queue_messages WHERE queue = ?", (queue,)
            )
            _ = connection.execute(
                "DELETE FROM queue_worker_stats WHERE queue = ?", (queue,)
            )
            _ = connection.execute(
                "DELETE FROM queue_worker_heartbeats WHERE queue = ?", (queue,)
            )
            return cursor.rowcount

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __del__(self) -> None:  # pragma: no cover
        with suppress(Exception):
            self.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                yield self._connection
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise

    def _next_seq(self, connection: sqlite3.Connection, queue: str) -> int:
        _ = _safe_queue(queue)
        cursor = connection.execute(
            "SELECT value FROM queue_sequences WHERE queue = ?", (queue,)
        )
        row = cursor.fetchone()
        value = 1 if row is None else int(row[0]) + 1
        connection.execute(
            "INSERT INTO queue_sequences(queue, value) VALUES(?, ?) "
            "ON CONFLICT(queue) DO UPDATE SET value = excluded.value",
            (queue, value),
        )
        return value

    def _increment_worker_stats(
        self, connection: sqlite3.Connection, queue: str, worker_id: str
    ) -> None:
        cursor = connection.execute(
            "SELECT leased_count FROM queue_worker_stats "
            "WHERE queue = ? AND worker_id = ?",
            (queue, worker_id),
        )
        row = cursor.fetchone()
        leased_count = 1 if row is None else int(row[0]) + 1
        connection.execute(
            "INSERT INTO queue_worker_stats(queue, worker_id, leased_count) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(queue, worker_id) DO UPDATE SET "
            "leased_count = excluded.leased_count",
            (queue, worker_id, leased_count),
        )

    def _lookup_dedupe_key(
        self, connection: sqlite3.Connection, queue: str, dedupe_key: str
    ) -> str | None:
        cursor = connection.execute(
            "SELECT message_id FROM queue_dedupe_keys "
            "WHERE queue = ? AND dedupe_key = ?",
            (queue, dedupe_key),
        )
        row = cursor.fetchone()
        return None if row is None else str(row[0])

    def _set_dedupe_key(
        self,
        connection: sqlite3.Connection,
        queue: str,
        dedupe_key: str,
        message_id: str,
    ) -> None:
        connection.execute(
            "INSERT INTO queue_dedupe_keys(queue, dedupe_key, message_id) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(queue, dedupe_key) DO UPDATE SET "
            "message_id = excluded.message_id",
            (queue, dedupe_key, message_id),
        )

    def _delete_dedupe_key(
        self, connection: sqlite3.Connection, queue: str, dedupe_key: str
    ) -> None:
        _ = connection.execute(
            "DELETE FROM queue_dedupe_keys WHERE queue = ? AND dedupe_key = ?",
            (queue, dedupe_key),
        )

    def _delete_dedupe_key_for_record(
        self, connection: sqlite3.Connection, record: _QueueRecord
    ) -> None:
        if record.dedupe_key is None:
            return
        self._delete_dedupe_key(connection, record.queue, record.dedupe_key)

    def _get_record(
        self, connection: sqlite3.Connection, queue: str, message_id: str
    ) -> _QueueRecord | None:
        _ = _safe_queue(queue)
        cursor = connection.execute(
            "SELECT record_json FROM queue_messages WHERE queue = ? AND id = ?",
            (queue, message_id),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return _decode_record(row[0])

    def _upsert_record(
        self, connection: sqlite3.Connection, record: _QueueRecord, *, sequence: int
    ) -> None:
        connection.execute(
            "INSERT INTO queue_messages("
            "queue, id, record_json, state, created_at, available_at, "
            "leased_until, leased_by, last_leased_at, failed_at, sequence"
            ") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(queue, id) DO UPDATE SET "
            "record_json = excluded.record_json, "
            "state = excluded.state, "
            "created_at = excluded.created_at, "
            "available_at = excluded.available_at, "
            "leased_until = excluded.leased_until, "
            "leased_by = excluded.leased_by, "
            "last_leased_at = excluded.last_leased_at, "
            "failed_at = excluded.failed_at, "
            "sequence = excluded.sequence",
            (
                record.queue,
                record.id,
                _encode_record(record).decode("utf-8"),
                record.state,
                record.created_at,
                record.available_at,
                record.leased_until,
                record.leased_by,
                _last_leased_at(record),
                record.failed_at,
                sequence,
            ),
        )

    def _sequence_for_id(
        self, connection: sqlite3.Connection, queue: str, message_id: str
    ) -> int:
        cursor = connection.execute(
            "SELECT sequence FROM queue_messages WHERE queue = ? AND id = ?",
            (queue, message_id),
        )
        row = cursor.fetchone()
        return 0 if row is None else int(row[0])

    def _sequence(self, raw_record: str) -> int:
        record = _decode_record(raw_record)
        if record.index_key is None:
            return 0
        return _sequence_from_index_key(record.index_key)

    def _reclaim_expired(
        self, connection: sqlite3.Connection, queue: str, now: float
    ) -> None:
        cursor = connection.execute(
            "SELECT id, record_json FROM queue_messages "
            "WHERE queue = ? AND state = ? AND leased_until <= ? "
            "ORDER BY leased_until",
            (queue, _INFLIGHT, now),
        )
        for message_id, raw in cursor.fetchall():
            record = _decode_record(raw)
            seq = self._next_seq(connection, queue)
            updated = replace(
                record,
                available_at=now,
                leased_until=None,
                leased_by=None,
                state=_READY,
                index_key=_ready_key(queue, now, seq, message_id),
                attempt_history=record.attempt_history
                + [
                    _attempt_event(
                        "lease_expired",
                        at=now,
                        attempt=record.attempts,
                        leased_by=record.leased_by,
                    )
                ],
            )
            self._upsert_record(connection, updated, sequence=seq)

    def _migrate_sqlite_schema(self, current_version: int) -> None:
        existing_columns = self._sqlite_columns("queue_messages")
        columns_to_add = {
            "created_at": "REAL",
            "leased_by": "TEXT",
            "last_leased_at": "REAL",
            "failed_at": "REAL",
        }
        for column, definition in columns_to_add.items():
            if column not in existing_columns:
                self._connection.execute(
                    f"ALTER TABLE queue_messages ADD COLUMN {column} {definition}"
                )

        if current_version < 2:
            cursor = self._connection.execute(
                "SELECT queue, id, record_json FROM queue_messages"
            )
            for queue, message_id, raw in cursor.fetchall():
                record = _decode_record(raw)
                self._connection.execute(
                    "UPDATE queue_messages SET "
                    "created_at = ?, leased_by = ?, last_leased_at = ?, failed_at = ? "
                    "WHERE queue = ? AND id = ?",
                    (
                        record.created_at,
                        record.leased_by,
                        _last_leased_at(record),
                        record.failed_at,
                        queue,
                        message_id,
                    ),
                )

    def _sqlite_columns(self, table: str) -> set[str]:
        cursor = self._connection.execute(f"PRAGMA table_info({table})")
        return {str(row[1]) for row in cursor.fetchall()}

    def _ensure_supported_schema_version(self) -> int:
        cursor = self._connection.execute("PRAGMA user_version")
        row = cursor.fetchone()
        current_version = 0 if row is None else int(row[0])
        if current_version > _SQLITE_SCHEMA_VERSION:
            raise ValueError(
                "unsupported SQLite queue schema version: "
                f"{current_version}; expected at most {_SQLITE_SCHEMA_VERSION}"
            )
        return current_version


class LMDBQueueStore:
    path: Path
    _env: lmdb.Environment

    def __init__(self, path: str | Path, *, map_size: int = 10**8) -> None:
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
                    raise QueueStoreLockedError(self.path) from exc
                _ENVS[key] = env
            self._env = env

    def enqueue(
        self,
        queue: str,
        value: Any,
        *,
        available_at: float,
        dedupe_key: str | None = None,
    ) -> QueueMessage:
        _validate_json_serializable(value)
        with self._env.begin(write=True) as txn:
            if dedupe_key is not None:
                existing_id = self._lookup_dedupe_key(txn, queue, dedupe_key)
                if existing_id is not None:
                    record = self._get_record(txn, queue, existing_id)
                    if record is not None:
                        return record.to_message()
                    _ = txn.delete(_dedupe_key_key(queue, dedupe_key))
            record = _QueueRecord.new(queue, value, available_at, dedupe_key=dedupe_key)
            seq = self._next_seq(txn, queue)
            record = replace(
                record, index_key=_ready_key(queue, available_at, seq, record.id)
            )
            self._put_record(txn, record)
            assert record.index_key is not None
            _ = txn.put(record.index_key, record.id.encode("utf-8"))
            if dedupe_key is not None:
                self._set_dedupe_key(txn, queue, dedupe_key, record.id)
            return record.to_message()

    def dequeue(
        self,
        queue: str,
        *,
        lease_timeout: float,
        now: float,
        leased_by: str | None = None,
    ) -> QueueMessage | None:
        with self._env.begin(write=True) as txn:
            self._reclaim_expired(txn, queue, now)
            cursor = txn.cursor()
            prefix = _ready_prefix(queue)
            if not cursor.set_range(prefix):
                return None
            item = cursor.item()
            if item is None:
                return None
            key, raw_id = item
            key = bytes(key)
            if not key.startswith(prefix):
                return None
            available_at = _timestamp_from_ready_key(key)
            if available_at > now:
                return None
            record = self._get_record(txn, queue, bytes(raw_id).decode("utf-8"))
            if record is None:
                _ = txn.delete(key)
                return None
            leased_until = now + lease_timeout
            updated = replace(
                record,
                attempts=record.attempts + 1,
                leased_until=leased_until,
                leased_by=leased_by,
                state=_INFLIGHT,
                index_key=_inflight_key(queue, leased_until, record.id),
                attempt_history=record.attempt_history
                + [
                    _attempt_event(
                        "leased",
                        at=now,
                        attempt=record.attempts + 1,
                        leased_by=leased_by,
                    )
                ],
            )
            _ = txn.delete(key)
            self._put_record(txn, updated)
            assert updated.index_key is not None
            _ = txn.put(updated.index_key, updated.id.encode("utf-8"))
            if leased_by is not None:
                self._increment_worker_stats(txn, queue, leased_by)
            return updated.to_message()

    def get(self, queue: str, message_id: str) -> QueueMessage | None:
        with self._env.begin() as txn:
            record = self._get_record(txn, queue, message_id)
            if record is None:
                return None
            return record.to_message()

    def ack(self, queue: str, message_id: str) -> bool:
        with self._env.begin(write=True) as txn:
            record = self._get_record(txn, queue, message_id)
            if record is None:
                return False
            self._delete_index(txn, record)
            self._delete_dedupe_key_for_record(txn, record)
            _ = txn.delete(_message_key(queue, message_id))
            return True

    def release(
        self,
        queue: str,
        message_id: str,
        *,
        available_at: float,
        last_error: dict[str, Any] | None = None,
        failed_at: float | None = None,
    ) -> bool:
        with self._env.begin(write=True) as txn:
            record = self._get_record(txn, queue, message_id)
            if record is None:
                return False
            self._delete_index(txn, record)
            seq = self._next_seq(txn, queue)
            updated = replace(
                record,
                available_at=available_at,
                leased_until=None,
                leased_by=None,
                last_error=last_error if last_error is not None else record.last_error,
                failed_at=failed_at if failed_at is not None else record.failed_at,
                state=_READY,
                index_key=_ready_key(queue, available_at, seq, message_id),
                attempt_history=record.attempt_history
                + [
                    _attempt_event(
                        "released",
                        at=time.time(),
                        attempt=record.attempts,
                        last_error=last_error
                        if last_error is not None
                        else record.last_error,
                    )
                ],
            )
            self._put_record(txn, updated)
            assert updated.index_key is not None
            _ = txn.put(updated.index_key, updated.id.encode("utf-8"))
            return True

    def dead_letter(
        self,
        queue: str,
        message_id: str,
        *,
        last_error: dict[str, Any] | None = None,
        failed_at: float | None = None,
    ) -> bool:
        with self._env.begin(write=True) as txn:
            record = self._get_record(txn, queue, message_id)
            if record is None:
                return False
            self._delete_index(txn, record)
            updated = replace(
                record,
                leased_until=None,
                leased_by=None,
                last_error=last_error if last_error is not None else record.last_error,
                failed_at=failed_at if failed_at is not None else record.failed_at,
                state=_DEAD,
                index_key=_dead_key(queue, message_id),
                attempt_history=record.attempt_history
                + [
                    _attempt_event(
                        "dead_lettered",
                        at=time.time(),
                        attempt=record.attempts,
                        last_error=last_error
                        if last_error is not None
                        else record.last_error,
                    )
                ],
            )
            self._put_record(txn, updated)
            assert updated.index_key is not None
            _ = txn.put(updated.index_key, updated.id.encode("utf-8"))
            return True

    def qsize(self, queue: str, *, now: float) -> int:
        with self._env.begin() as txn:
            count = 0
            cursor = txn.cursor()
            prefix = _ready_prefix(queue)
            if not cursor.set_range(prefix):
                return 0
            for key, _ in cursor:
                key = bytes(key)
                if not key.startswith(prefix):
                    break
                if _timestamp_from_ready_key(key) > now:
                    break
                count += 1
            return count

    def stats(self, queue: str, *, now: float) -> QueueStats:
        with self._env.begin(write=True) as txn:
            self._reclaim_expired(txn, queue, now)
            records = []
            cursor = txn.cursor()
            prefix = _message_prefix(queue)
            if cursor.set_range(prefix):
                for key, raw in cursor:
                    key = bytes(key)
                    if not key.startswith(prefix):
                        break
                    records.append(_decode_record(bytes(raw)))
            raw = txn.get(_worker_stats_key(queue))
            heartbeats_raw = txn.get(_worker_heartbeats_key(queue))
            return _stats_from_records(
                records,
                now=now,
                leases_by_worker_id=self._decode_worker_stats(
                    bytes(raw) if raw is not None else None
                ),
                last_seen_by_worker_id=self._decode_worker_heartbeats(
                    bytes(heartbeats_raw) if heartbeats_raw is not None else None
                ),
            )

    def dead_letters(
        self, queue: str, *, limit: int | None = None
    ) -> list[QueueMessage]:
        _validate_limit(limit)
        with self._env.begin() as txn:
            messages = []
            cursor = txn.cursor()
            prefix = _dead_prefix(queue)
            if not cursor.set_range(prefix):
                return []
            for key, raw_id in cursor:
                key = bytes(key)
                if not key.startswith(prefix):
                    break
                record = self._get_record(txn, queue, bytes(raw_id).decode("utf-8"))
                if record is not None:
                    messages.append(record.to_message())
                    if limit is not None and len(messages) >= limit:
                        break
            return messages

    def requeue_dead(self, queue: str, message_id: str, *, available_at: float) -> bool:
        with self._env.begin(write=True) as txn:
            record = self._get_record(txn, queue, message_id)
            if record is None or record.state != _DEAD:
                return False
            self._delete_index(txn, record)
            seq = self._next_seq(txn, queue)
            updated = replace(
                record,
                available_at=available_at,
                leased_until=None,
                leased_by=None,
                state=_READY,
                index_key=_ready_key(queue, available_at, seq, message_id),
                attempt_history=record.attempt_history
                + [
                    _attempt_event(
                        "requeued",
                        at=time.time(),
                        attempt=record.attempts,
                    )
                ],
            )
            self._put_record(txn, updated)
            assert updated.index_key is not None
            _ = txn.put(updated.index_key, updated.id.encode("utf-8"))
            return True

    def prune_dead_letters(self, queue: str, *, older_than: float, now: float) -> int:
        with self._env.begin(write=True) as txn:
            doomed: list[bytes] = []
            cursor = txn.cursor()
            prefix = _dead_prefix(queue)
            if not cursor.set_range(prefix):
                return 0
            for key, raw_id in cursor:
                key = bytes(key)
                if not key.startswith(prefix):
                    break
                record = self._get_record(txn, queue, bytes(raw_id).decode("utf-8"))
                if (
                    record is not None
                    and _dead_record_age(record, now=now) >= older_than
                ):
                    doomed.append(bytes(raw_id))
            for raw_id in doomed:
                message_id = raw_id.decode("utf-8")
                record = self._get_record(txn, queue, message_id)
                if record is None:
                    continue
                _ = txn.delete(_dead_key(queue, message_id))
                self._delete_dedupe_key_for_record(txn, record)
                _ = txn.delete(_message_key(queue, message_id))
            return len(doomed)

    def count_dead_letters_older_than(
        self, queue: str, *, older_than: float, now: float
    ) -> int:
        with self._env.begin() as txn:
            count = 0
            cursor = txn.cursor()
            prefix = _dead_prefix(queue)
            if not cursor.set_range(prefix):
                return 0
            for key, raw_id in cursor:
                key = bytes(key)
                if not key.startswith(prefix):
                    break
                record = self._get_record(txn, queue, bytes(raw_id).decode("utf-8"))
                if (
                    record is not None
                    and _dead_record_age(record, now=now) >= older_than
                ):
                    count += 1
            return count

    def empty(self, queue: str, *, now: float) -> bool:
        return self.qsize(queue, now=now) == 0

    def purge(self, queue: str) -> int:
        with self._env.begin(write=True) as txn:
            count = 0
            cursor = txn.cursor()
            prefix = _queue_prefix(queue)
            if not cursor.set_range(prefix):
                return 0
            keys = []
            for key, _ in cursor:
                key = bytes(key)
                if not key.startswith(prefix):
                    break
                keys.append(key)
            for key in keys:
                if key.startswith(_message_prefix(queue)):
                    count += 1
                _ = txn.delete(key)
            for key in list(self._iter_dedupe_keys(txn, queue)):
                _ = txn.delete(key)
            _ = txn.delete(_worker_stats_key(queue))
            _ = txn.delete(_worker_heartbeats_key(queue))
            return count

    def _reclaim_expired(self, txn: lmdb.Transaction, queue: str, now: float) -> None:
        cursor = txn.cursor()
        prefix = _inflight_prefix(queue)
        if not cursor.set_range(prefix):
            return
        expired: list[tuple[bytes, str]] = []
        for key, raw_id in cursor:
            key = bytes(key)
            if not key.startswith(prefix):
                break
            leased_until = _timestamp_from_inflight_key(key)
            if leased_until > now:
                break
            expired.append((key, bytes(raw_id).decode("utf-8")))

        for old_key, message_id in expired:
            record = self._get_record(txn, queue, message_id)
            _ = txn.delete(old_key)
            if record is None:
                continue
            seq = self._next_seq(txn, queue)
            updated = replace(
                record,
                available_at=now,
                leased_until=None,
                leased_by=None,
                state=_READY,
                index_key=_ready_key(queue, now, seq, message_id),
                attempt_history=record.attempt_history
                + [
                    _attempt_event(
                        "lease_expired",
                        at=now,
                        attempt=record.attempts,
                        leased_by=record.leased_by,
                    )
                ],
            )
            self._put_record(txn, updated)
            assert updated.index_key is not None
            _ = txn.put(updated.index_key, updated.id.encode("utf-8"))

    def _next_seq(self, txn: lmdb.Transaction, queue: str) -> int:
        key = _seq_key(queue)
        raw = txn.get(key)
        value = 1 if raw is None else int(bytes(raw).decode("ascii")) + 1
        _ = txn.put(key, str(value).encode("ascii"))
        return value

    def _get_record(
        self, txn: lmdb.Transaction, queue: str, message_id: str
    ) -> _QueueRecord | None:
        raw = txn.get(_message_key(queue, message_id))
        if raw is None:
            return None
        return _decode_record(bytes(raw))

    def _put_record(self, txn: lmdb.Transaction, record: _QueueRecord) -> None:
        _ = txn.put(_message_key(record.queue, record.id), _encode_record(record))

    def _delete_index(self, txn: lmdb.Transaction, record: _QueueRecord) -> None:
        if record.index_key is not None:
            _ = txn.delete(record.index_key)

    def _lookup_dedupe_key(
        self, txn: lmdb.Transaction, queue: str, dedupe_key: str
    ) -> str | None:
        raw = txn.get(_dedupe_key_key(queue, dedupe_key))
        return None if raw is None else bytes(raw).decode("utf-8")

    def _set_dedupe_key(
        self,
        txn: lmdb.Transaction,
        queue: str,
        dedupe_key: str,
        message_id: str,
    ) -> None:
        _ = txn.put(_dedupe_key_key(queue, dedupe_key), message_id.encode("utf-8"))

    def _delete_dedupe_key(
        self, txn: lmdb.Transaction, queue: str, dedupe_key: str
    ) -> None:
        _ = txn.delete(_dedupe_key_key(queue, dedupe_key))

    def _delete_dedupe_key_for_record(
        self, txn: lmdb.Transaction, record: _QueueRecord
    ) -> None:
        if record.dedupe_key is None:
            return
        self._delete_dedupe_key(txn, record.queue, record.dedupe_key)

    def _iter_dedupe_keys(self, txn: lmdb.Transaction, queue: str) -> Iterator[bytes]:
        cursor = txn.cursor()
        prefix = _dedupe_prefix(queue)
        if not cursor.set_range(prefix):
            return
        for key, _ in cursor:
            key = bytes(key)
            if not key.startswith(prefix):
                break
            yield key

    def _increment_worker_stats(
        self, txn: lmdb.Transaction, queue: str, worker_id: str
    ) -> None:
        raw = txn.get(_worker_stats_key(queue))
        stats = self._decode_worker_stats(bytes(raw) if raw is not None else None)
        stats[worker_id] = stats.get(worker_id, 0) + 1
        _ = txn.put(_worker_stats_key(queue), _encode_worker_stats(stats))

    def record_worker_heartbeat(
        self, queue: str, worker_id: str, *, now: float
    ) -> None:
        with self._env.begin(write=True) as txn:
            raw = txn.get(_worker_heartbeats_key(queue))
            heartbeats = self._decode_worker_heartbeats(
                bytes(raw) if raw is not None else None
            )
            heartbeats[worker_id] = now
            _ = txn.put(
                _worker_heartbeats_key(queue),
                _encode_worker_heartbeats(heartbeats),
            )

    @staticmethod
    def _decode_worker_stats(raw: bytes | None) -> dict[str, int]:
        if raw is None:
            return {}
        try:
            payload = json.loads(bytes(raw).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("queue worker stats are not valid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("queue worker stats payload must be a JSON object")
        return {str(key): int(value) for key, value in payload.items()}

    @staticmethod
    def _decode_worker_heartbeats(raw: bytes | None) -> dict[str, float]:
        if raw is None:
            return {}
        try:
            payload = json.loads(bytes(raw).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "queue worker heartbeat payload is not valid JSON"
            ) from exc
        if not isinstance(payload, dict):
            raise ValueError("queue worker heartbeat payload must be a JSON object")
        return {str(key): float(value) for key, value in payload.items()}


def _safe_queue(queue: str) -> str:
    if not queue:
        raise ValueError("queue name cannot be empty")
    if ":" in queue:
        raise ValueError("queue name cannot contain ':'")
    return queue


def _millis(value: float) -> int:
    return max(int(value * 1000), 0)


def _timestamp(value: str) -> float:
    return int(value) / 1000


def _queue_prefix(queue: str) -> bytes:
    return f"queue:{_safe_queue(queue)}:".encode("utf-8")


def _message_prefix(queue: str) -> bytes:
    return f"queue:{_safe_queue(queue)}:message:".encode("utf-8")


def _message_key(queue: str, message_id: str) -> bytes:
    return f"queue:{_safe_queue(queue)}:message:{message_id}".encode("utf-8")


def _seq_key(queue: str) -> bytes:
    return f"queue:{_safe_queue(queue)}:seq".encode("utf-8")


def _ready_prefix(queue: str) -> bytes:
    return f"queue:{_safe_queue(queue)}:ready:".encode("utf-8")


def _ready_key(queue: str, available_at: float, seq: int, message_id: str) -> bytes:
    return (
        f"queue:{_safe_queue(queue)}:ready:"
        + f"{_millis(available_at):020d}:{seq:020d}:{message_id}"
    ).encode("utf-8")


def _timestamp_from_ready_key(key: bytes) -> float:
    return _timestamp_from_index_key(key, expected_state=_READY)


def _inflight_prefix(queue: str) -> bytes:
    return f"queue:{_safe_queue(queue)}:inflight:".encode("utf-8")


def _inflight_key(queue: str, leased_until: float, message_id: str) -> bytes:
    return (
        f"queue:{_safe_queue(queue)}:inflight:{_millis(leased_until):020d}:{message_id}"
    ).encode("utf-8")


def _timestamp_from_inflight_key(key: bytes) -> float:
    return _timestamp_from_index_key(key, expected_state=_INFLIGHT)


def _dead_key(queue: str, message_id: str) -> bytes:
    return f"queue:{_safe_queue(queue)}:dead:{message_id}".encode("utf-8")


def _dead_prefix(queue: str) -> bytes:
    return f"queue:{_safe_queue(queue)}:dead:".encode("utf-8")


def _worker_stats_key(queue: str) -> bytes:
    return f"queue:{_safe_queue(queue)}:worker-stats".encode("utf-8")


def _worker_heartbeats_key(queue: str) -> bytes:
    return f"queue:{_safe_queue(queue)}:worker-heartbeats".encode("utf-8")


def _dedupe_token(dedupe_key: str) -> str:
    return dedupe_key.encode("utf-8").hex()


def _dedupe_prefix(queue: str) -> bytes:
    return f"queue:{_safe_queue(queue)}:dedupe:".encode("utf-8")


def _dedupe_key_key(queue: str, dedupe_key: str) -> bytes:
    return f"queue:{_safe_queue(queue)}:dedupe:{_dedupe_token(dedupe_key)}".encode(
        "utf-8"
    )


def _encode_record(record: _QueueRecord) -> bytes:
    payload = {
        "version": _QUEUE_RECORD_VERSION,
        "id": record.id,
        "value": record.value,
        "queue": record.queue,
        "attempts": record.attempts,
        "created_at": record.created_at,
        "available_at": record.available_at,
        "leased_until": record.leased_until,
        "leased_by": record.leased_by,
        "last_error": record.last_error,
        "failed_at": record.failed_at,
        "dedupe_key": record.dedupe_key,
        "attempt_history": record.attempt_history,
        "state": record.state,
        "index_key": record.index_key.decode("utf-8")
        if record.index_key is not None
        else None,
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _validate_json_serializable(value: Any) -> None:
    try:
        _ = json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("queue values must be JSON-serializable") from exc


def _decode_record(raw: bytes | str) -> _QueueRecord:
    try:
        payload = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("queue record is not valid JSON") from exc

    version = payload.get("version")
    if version not in {1, 2, 3, _QUEUE_RECORD_VERSION}:
        raise ValueError(f"unsupported queue record version: {version!r}")

    index_key = payload["index_key"]
    return _QueueRecord(
        id=payload["id"],
        value=payload["value"],
        queue=payload["queue"],
        attempts=payload["attempts"],
        created_at=payload["created_at"],
        available_at=payload["available_at"],
        leased_until=payload["leased_until"],
        leased_by=payload.get("leased_by"),
        last_error=payload.get("last_error"),
        failed_at=payload.get("failed_at"),
        dedupe_key=payload.get("dedupe_key"),
        attempt_history=payload.get("attempt_history", []),
        state=payload["state"],
        index_key=index_key.encode("utf-8") if index_key is not None else None,
    )


def _encode_worker_stats(stats: dict[str, int]) -> bytes:
    return json.dumps(dict(sorted(stats.items())), separators=(",", ":")).encode(
        "utf-8"
    )


def _encode_worker_heartbeats(stats: dict[str, float]) -> bytes:
    return json.dumps(dict(sorted(stats.items())), separators=(",", ":")).encode(
        "utf-8"
    )


def _stats_from_records(
    records: Iterable[_QueueRecord],
    *,
    now: float,
    leases_by_worker_id: dict[str, int] | None = None,
    last_seen_by_worker_id: dict[str, float] | None = None,
) -> QueueStats:
    ready = 0
    delayed = 0
    inflight = 0
    dead = 0
    total = 0
    by_worker_id: Counter[str] = Counter()
    ready_ages: list[float] = []
    inflight_ages: list[float] = []
    for record in records:
        total += 1
        if record.state == _READY:
            if record.available_at <= now:
                ready += 1
                ready_ages.append(max(now - record.available_at, 0.0))
            else:
                delayed += 1
        elif record.state == _INFLIGHT:
            inflight += 1
            if record.leased_by:
                by_worker_id[record.leased_by] += 1
            leased_at = _last_leased_at(record)
            if leased_at is not None:
                inflight_ages.append(max(now - leased_at, 0.0))
        elif record.state == _DEAD:
            dead += 1
    return QueueStats(
        ready=ready,
        delayed=delayed,
        inflight=inflight,
        dead=dead,
        total=total,
        by_worker_id=dict(sorted(by_worker_id.items())),
        leases_by_worker_id=dict(sorted((leases_by_worker_id or {}).items())),
        last_seen_by_worker_id=dict(sorted((last_seen_by_worker_id or {}).items())),
        oldest_ready_age_seconds=max(ready_ages) if ready_ages else None,
        oldest_inflight_age_seconds=max(inflight_ages) if inflight_ages else None,
        average_inflight_age_seconds=(
            sum(inflight_ages) / len(inflight_ages) if inflight_ages else None
        ),
    )


def _validate_limit(limit: int | None) -> None:
    if limit is not None and limit < 0:
        raise ValueError("limit cannot be negative")


def _dead_record_age(record: _QueueRecord, *, now: float) -> float:
    anchor = record.failed_at if record.failed_at is not None else record.created_at
    return max(now - anchor, 0.0)


def _last_leased_at(record: _QueueRecord) -> float | None:
    for event in reversed(record.attempt_history):
        if event.get("type") == "leased":
            leased_at = event.get("at")
            return float(leased_at) if leased_at is not None else None
    return None


def _timestamp_from_index_key(key: bytes, *, expected_state: str) -> float:
    try:
        decoded = key.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"malformed LMDB queue index key {key!r}: not UTF-8") from exc

    parts = decoded.split(":")
    if len(parts) < 5 or parts[0] != "queue" or parts[2] != expected_state:
        raise ValueError(
            f"malformed LMDB queue index key {decoded!r}: "
            + f"expected queue:<name>:{expected_state}:<timestamp>:..."
        )
    return _timestamp(parts[3])


def _sequence_from_index_key(key: bytes) -> int:
    try:
        decoded = key.decode("utf-8")
    except UnicodeDecodeError:
        return 0

    parts = decoded.split(":")
    if len(parts) >= 6 and parts[2] == _READY:
        return int(parts[4])
    return 0
