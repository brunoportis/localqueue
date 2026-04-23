from __future__ import annotations

import json
import time
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

from ._shared import (
    _DEAD,
    _ENVS,
    _ENVS_LOCK,
    dead_prefix,
    dead_record_age,
    decode_record,
    dedupe_key_key,
    dedupe_prefix,
    encode_record,
    encode_worker_heartbeats,
    encode_worker_stats,
    import_lmdb,
    inflight_key,
    inflight_prefix,
    message_key,
    message_prefix,
    queue_prefix,
    ready_key,
    ready_prefix,
    replace_record,
    seq_key,
    stats_from_records,
    timestamp_from_inflight_key,
    timestamp_from_ready_key,
    validate_json_serializable,
    validate_limit,
    worker_heartbeats_key,
    worker_stats_key,
    QueueRecord,
    attempt_event,
    dead_key,
)
from .base import QueueStoreLockedError

if TYPE_CHECKING:
    import lmdb


class LMDBQueueStore:
    path: Path
    _env: lmdb.Environment

    def __init__(self, path: str | Path, *, map_size: int = 10**8) -> None:
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
                    raise QueueStoreLockedError(self.path) from exc
                _ENVS[key] = env
            self._env = env

    def enqueue(
        self,
        queue: str,
        value: object,
        *,
        available_at: float,
        dedupe_key: str | None = None,
    ):
        validate_json_serializable(value)
        with self._env.begin(write=True) as txn:
            if dedupe_key is not None:
                existing_id = self._lookup_dedupe_key(txn, queue, dedupe_key)
                if existing_id is not None:
                    record = self._get_record(txn, queue, existing_id)
                    if record is not None:
                        return record.to_message()
                    _ = txn.delete(dedupe_key_key(queue, dedupe_key))
            record = QueueRecord.new(queue, value, available_at, dedupe_key=dedupe_key)
            seq = self._next_seq(txn, queue)
            record = replace_record(
                record, index_key=ready_key(queue, available_at, seq, record.id)
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
    ):
        with self._env.begin(write=True) as txn:
            self._reclaim_expired(txn, queue, now)
            cursor = txn.cursor()
            prefix = ready_prefix(queue)
            if not cursor.set_range(prefix):
                return None
            item = cursor.item()
            if item is None:
                return None
            key, raw_id = item
            key = bytes(key)
            if not key.startswith(prefix):
                return None
            available_at = timestamp_from_ready_key(key)
            if available_at > now:
                return None
            record = self._get_record(txn, queue, bytes(raw_id).decode("utf-8"))
            if record is None:
                _ = txn.delete(key)
                return None
            leased_until = now + lease_timeout
            updated = replace_record(
                record,
                attempts=record.attempts + 1,
                leased_until=leased_until,
                leased_by=leased_by,
                state="inflight",
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
            _ = txn.delete(key)
            self._put_record(txn, updated)
            assert updated.index_key is not None
            _ = txn.put(updated.index_key, updated.id.encode("utf-8"))
            if leased_by is not None:
                self._increment_worker_stats(txn, queue, leased_by)
            return updated.to_message()

    def get(self, queue: str, message_id: str):
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
            _ = txn.delete(message_key(queue, message_id))
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
        with self._env.begin(write=True) as txn:
            record = self._get_record(txn, queue, message_id)
            if record is None:
                return False
            self._delete_index(txn, record)
            seq = self._next_seq(txn, queue)
            updated = replace_record(
                record,
                available_at=available_at,
                leased_until=None,
                leased_by=None,
                last_error=last_error if last_error is not None else record.last_error,
                failed_at=failed_at if failed_at is not None else record.failed_at,
                state="ready",
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
            self._put_record(txn, updated)
            assert updated.index_key is not None
            _ = txn.put(updated.index_key, updated.id.encode("utf-8"))
            return True

    def dead_letter(
        self,
        queue: str,
        message_id: str,
        *,
        last_error: dict[str, object] | None = None,
        failed_at: float | None = None,
    ) -> bool:
        with self._env.begin(write=True) as txn:
            record = self._get_record(txn, queue, message_id)
            if record is None:
                return False
            self._delete_index(txn, record)
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
            self._put_record(txn, updated)
            assert updated.index_key is not None
            _ = txn.put(updated.index_key, updated.id.encode("utf-8"))
            return True

    def qsize(self, queue: str, *, now: float) -> int:
        with self._env.begin() as txn:
            count = 0
            cursor = txn.cursor()
            prefix = ready_prefix(queue)
            if not cursor.set_range(prefix):
                return 0
            for key, _ in cursor:
                key = bytes(key)
                if not key.startswith(prefix):
                    break
                if timestamp_from_ready_key(key) > now:
                    break
                count += 1
            return count

    def stats(self, queue: str, *, now: float):
        with self._env.begin(write=True) as txn:
            self._reclaim_expired(txn, queue, now)
            records = []
            cursor = txn.cursor()
            prefix = message_prefix(queue)
            if cursor.set_range(prefix):
                for key, raw in cursor:
                    key = bytes(key)
                    if not key.startswith(prefix):
                        break
                    records.append(decode_record(bytes(raw)))
            raw = txn.get(worker_stats_key(queue))
            heartbeats_raw = txn.get(worker_heartbeats_key(queue))
            return stats_from_records(
                records,
                now=now,
                leases_by_worker_id=self._decode_worker_stats(
                    bytes(raw) if raw is not None else None
                ),
                last_seen_by_worker_id=self._decode_worker_heartbeats(
                    bytes(heartbeats_raw) if heartbeats_raw is not None else None
                ),
            )

    def dead_letters(self, queue: str, *, limit: int | None = None):
        validate_limit(limit)
        with self._env.begin() as txn:
            messages = []
            cursor = txn.cursor()
            prefix = dead_prefix(queue)
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
            updated = replace_record(
                record,
                available_at=available_at,
                leased_until=None,
                leased_by=None,
                state="ready",
                index_key=ready_key(queue, available_at, seq, message_id),
                attempt_history=record.attempt_history
                + [attempt_event("requeued", at=time.time(), attempt=record.attempts)],
            )
            self._put_record(txn, updated)
            assert updated.index_key is not None
            _ = txn.put(updated.index_key, updated.id.encode("utf-8"))
            return True

    def prune_dead_letters(self, queue: str, *, older_than: float, now: float) -> int:
        with self._env.begin(write=True) as txn:
            doomed: list[bytes] = []
            cursor = txn.cursor()
            prefix = dead_prefix(queue)
            if not cursor.set_range(prefix):
                return 0
            for key, raw_id in cursor:
                key = bytes(key)
                if not key.startswith(prefix):
                    break
                record = self._get_record(txn, queue, bytes(raw_id).decode("utf-8"))
                if (
                    record is not None
                    and dead_record_age(record, now=now) >= older_than
                ):
                    doomed.append(bytes(raw_id))
            for raw_id in doomed:
                message_id = raw_id.decode("utf-8")
                record = self._get_record(txn, queue, message_id)
                if record is None:
                    continue
                _ = txn.delete(dead_key(queue, message_id))
                self._delete_dedupe_key_for_record(txn, record)
                _ = txn.delete(message_key(queue, message_id))
            return len(doomed)

    def count_dead_letters_older_than(
        self, queue: str, *, older_than: float, now: float
    ) -> int:
        with self._env.begin() as txn:
            count = 0
            cursor = txn.cursor()
            prefix = dead_prefix(queue)
            if not cursor.set_range(prefix):
                return 0
            for key, raw_id in cursor:
                key = bytes(key)
                if not key.startswith(prefix):
                    break
                record = self._get_record(txn, queue, bytes(raw_id).decode("utf-8"))
                if (
                    record is not None
                    and dead_record_age(record, now=now) >= older_than
                ):
                    count += 1
            return count

    def empty(self, queue: str, *, now: float) -> bool:
        return self.qsize(queue, now=now) == 0

    def purge(self, queue: str) -> int:
        with self._env.begin(write=True) as txn:
            count = 0
            cursor = txn.cursor()
            prefix = queue_prefix(queue)
            if not cursor.set_range(prefix):
                return 0
            keys = []
            for key, _ in cursor:
                key = bytes(key)
                if not key.startswith(prefix):
                    break
                keys.append(key)
            for key in keys:
                if key.startswith(message_prefix(queue)):
                    count += 1
                _ = txn.delete(key)
            for key in self._iter_dedupe_keys(txn, queue):
                _ = txn.delete(key)
            _ = txn.delete(worker_stats_key(queue))
            _ = txn.delete(worker_heartbeats_key(queue))
            return count

    def _reclaim_expired(self, txn: lmdb.Transaction, queue: str, now: float) -> None:
        cursor = txn.cursor()
        prefix = inflight_prefix(queue)
        if not cursor.set_range(prefix):
            return
        expired: list[tuple[bytes, str]] = []
        for key, raw_id in cursor:
            key = bytes(key)
            if not key.startswith(prefix):
                break
            leased_until = timestamp_from_inflight_key(key)
            if leased_until > now:
                break
            expired.append((key, bytes(raw_id).decode("utf-8")))

        for old_key, message_id in expired:
            record = self._get_record(txn, queue, message_id)
            _ = txn.delete(old_key)
            if record is None:
                continue
            seq = self._next_seq(txn, queue)
            updated = replace_record(
                record,
                available_at=now,
                leased_until=None,
                leased_by=None,
                state="ready",
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
            self._put_record(txn, updated)
            assert updated.index_key is not None
            _ = txn.put(updated.index_key, updated.id.encode("utf-8"))

    def _next_seq(self, txn: lmdb.Transaction, queue: str) -> int:
        key = seq_key(queue)
        raw = txn.get(key)
        value = 1 if raw is None else int(bytes(raw).decode("ascii")) + 1
        _ = txn.put(key, str(value).encode("ascii"))
        return value

    def _get_record(
        self, txn: lmdb.Transaction, queue: str, message_id: str
    ) -> QueueRecord | None:
        raw = txn.get(message_key(queue, message_id))
        if raw is None:
            return None
        return decode_record(bytes(raw))

    def _put_record(self, txn: lmdb.Transaction, record: QueueRecord) -> None:
        payload = encode_record(record)
        _ = txn.put(message_key(record.queue, record.id), payload)

    def _delete_index(self, txn: lmdb.Transaction, record: QueueRecord) -> None:
        if record.index_key is not None:
            _ = txn.delete(record.index_key)

    def _lookup_dedupe_key(
        self, txn: lmdb.Transaction, queue: str, dedupe_key: str
    ) -> str | None:
        raw = txn.get(dedupe_key_key(queue, dedupe_key))
        return None if raw is None else bytes(raw).decode("utf-8")

    def _set_dedupe_key(
        self,
        txn: lmdb.Transaction,
        queue: str,
        dedupe_key: str,
        message_id: str,
    ) -> None:
        _ = txn.put(dedupe_key_key(queue, dedupe_key), message_id.encode("utf-8"))

    def _delete_dedupe_key(
        self, txn: lmdb.Transaction, queue: str, dedupe_key: str
    ) -> None:
        _ = txn.delete(dedupe_key_key(queue, dedupe_key))

    def _delete_dedupe_key_for_record(
        self, txn: lmdb.Transaction, record: QueueRecord
    ) -> None:
        if record.dedupe_key is None:
            return
        self._delete_dedupe_key(txn, record.queue, record.dedupe_key)

    def _iter_dedupe_keys(self, txn: lmdb.Transaction, queue: str) -> Iterator[bytes]:
        cursor = txn.cursor()
        prefix = dedupe_prefix(queue)
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
        raw = txn.get(worker_stats_key(queue))
        stats = self._decode_worker_stats(bytes(raw) if raw is not None else None)
        stats[worker_id] = stats.get(worker_id, 0) + 1
        _ = txn.put(worker_stats_key(queue), encode_worker_stats(stats))

    def record_worker_heartbeat(
        self, queue: str, worker_id: str, *, now: float
    ) -> None:
        with self._env.begin(write=True) as txn:
            raw = txn.get(worker_heartbeats_key(queue))
            heartbeats = self._decode_worker_heartbeats(
                bytes(raw) if raw is not None else None
            )
            heartbeats[worker_id] = now
            _ = txn.put(
                worker_heartbeats_key(queue),
                encode_worker_heartbeats(heartbeats),
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
