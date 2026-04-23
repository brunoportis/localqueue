from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any, Iterator

from ._shared import (
    _DEAD,
    _INFLIGHT,
    _READY,
    _SQLITE_SCHEMA_VERSION,
    attempt_event,
    decode_record,
    dead_key,
    encode_record,
    last_leased_at,
    ready_key,
    replace_record,
    safe_queue,
    sequence_from_index_key,
    validate_json_serializable,
    validate_limit,
    QueueRecord,
)


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
    ):
        validate_json_serializable(value)
        with self._transaction() as connection:
            if dedupe_key is not None:
                existing_id = self._lookup_dedupe_key(connection, queue, dedupe_key)
                if existing_id is not None:
                    record = self._get_record(connection, queue, existing_id)
                    if record is not None:
                        return record.to_message()
                    self._delete_dedupe_key(connection, queue, dedupe_key)
            record = QueueRecord.new(queue, value, available_at, dedupe_key=dedupe_key)
            seq = self._next_seq(connection, queue)
            record = replace_record(
                record, index_key=ready_key(queue, available_at, seq, record.id)
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
    ):
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
            record = decode_record(row[1])
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
            self._upsert_record(connection, updated, sequence=self._sequence(row[1]))
            if leased_by is not None:
                self._increment_worker_stats(connection, queue, leased_by)
            return updated.to_message()

    def get(self, queue: str, message_id: str):
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
            updated = replace_record(
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
            updated = replace_record(
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

    def stats(self, queue: str, *, now: float):
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
            from .base import QueueStats

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

    def dead_letters(self, queue: str, *, limit: int | None = None):
        validate_limit(limit)
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
            return [decode_record(row[0]).to_message() for row in cursor.fetchall()]

    def requeue_dead(self, queue: str, message_id: str, *, available_at: float) -> bool:
        with self._transaction() as connection:
            record = self._get_record(connection, queue, message_id)
            if record is None or record.state != _DEAD:
                return False
            seq = self._next_seq(connection, queue)
            updated = replace_record(
                record,
                available_at=available_at,
                leased_until=None,
                leased_by=None,
                state=_READY,
                index_key=ready_key(queue, available_at, seq, message_id),
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
        _ = safe_queue(queue)
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
        self, connection: sqlite3.Connection, record: QueueRecord
    ) -> None:
        if record.dedupe_key is None:
            return
        self._delete_dedupe_key(connection, record.queue, record.dedupe_key)

    def _get_record(
        self, connection: sqlite3.Connection, queue: str, message_id: str
    ) -> QueueRecord | None:
        _ = safe_queue(queue)
        cursor = connection.execute(
            "SELECT record_json FROM queue_messages WHERE queue = ? AND id = ?",
            (queue, message_id),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return decode_record(row[0])

    def _upsert_record(
        self, connection: sqlite3.Connection, record: QueueRecord, *, sequence: int
    ) -> None:
        record_json = encode_record(record).decode("utf-8")
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
                record_json,
                record.state,
                record.created_at,
                record.available_at,
                record.leased_until,
                record.leased_by,
                last_leased_at(record),
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
        record = decode_record(raw_record)
        if record.index_key is None:
            return 0
        return sequence_from_index_key(record.index_key)

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
            record = decode_record(raw)
            seq = self._next_seq(connection, queue)
            updated = replace_record(
                record,
                available_at=now,
                leased_until=None,
                leased_by=None,
                state=_READY,
                index_key=ready_key(queue, now, seq, message_id),
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
                record = decode_record(raw)
                self._connection.execute(
                    "UPDATE queue_messages SET "
                    "created_at = ?, leased_by = ?, last_leased_at = ?, failed_at = ? "
                    "WHERE queue = ? AND id = ?",
                    (
                        record.created_at,
                        record.leased_by,
                        last_leased_at(record),
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


def inflight_key(queue: str, leased_until: float, message_id: str) -> bytes:
    from ._shared import inflight_key as shared_inflight_key

    return shared_inflight_key(queue, leased_until, message_id)
