from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from collections.abc import Iterable
from typing import Any, Protocol

import lmdb

_ENVS: dict[tuple[str, int], lmdb.Environment] = {}
_ENVS_LOCK = threading.Lock()
_READY = "ready"
_INFLIGHT = "inflight"
_DEAD = "dead"
_QUEUE_RECORD_VERSION = 1


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


@dataclass(frozen=True, slots=True)
class QueueStats:
    ready: int = 0
    delayed: int = 0
    inflight: int = 0
    dead: int = 0
    total: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "ready": self.ready,
            "delayed": self.delayed,
            "inflight": self.inflight,
            "dead": self.dead,
            "total": self.total,
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
    state: str
    index_key: bytes | None

    @classmethod
    def new(cls, queue: str, value: Any, available_at: float) -> "_QueueRecord":
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
        )


class QueueStore(Protocol):
    def enqueue(
        self, queue: str, value: Any, *, available_at: float
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

    def dead_letters(
        self, queue: str, *, limit: int | None = None
    ) -> list[QueueMessage]: ...

    def requeue_dead(
        self, queue: str, message_id: str, *, available_at: float
    ) -> bool: ...

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
    _lock: threading.Lock

    def __init__(self) -> None:
        self._records = {}
        self._seq = {}
        self._lock = threading.Lock()

    def enqueue(self, queue: str, value: Any, *, available_at: float) -> QueueMessage:
        with self._lock:
            record = _QueueRecord.new(queue, value, available_at)
            seq = self._next_seq(queue)
            record = replace(
                record, index_key=self._ready_key(queue, available_at, seq, record.id)
            )
            self._records.setdefault(queue, {})[record.id] = record
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
            )
            self._records[queue][record.id] = updated
            return updated.to_message()

    def get(self, queue: str, message_id: str) -> QueueMessage | None:
        with self._lock:
            record = self._records.get(queue, {}).get(message_id)
            if record is None:
                return None
            return record.to_message()

    def ack(self, queue: str, message_id: str) -> bool:
        with self._lock:
            return self._records.get(queue, {}).pop(message_id, None) is not None

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
            return _stats_from_records(self._records.get(queue, {}).values(), now=now)

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

    def empty(self, queue: str, *, now: float) -> bool:
        return self.qsize(queue, now=now) == 0

    def purge(self, queue: str) -> int:
        with self._lock:
            count = len(self._records.get(queue, {}))
            self._records[queue] = {}
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


class LMDBQueueStore:
    path: Path
    _env: lmdb.Environment

    def __init__(self, path: str | Path, *, map_size: int = 10**8) -> None:
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

    def enqueue(self, queue: str, value: Any, *, available_at: float) -> QueueMessage:
        _validate_json_serializable(value)
        with self._env.begin(write=True) as txn:
            record = _QueueRecord.new(queue, value, available_at)
            seq = self._next_seq(txn, queue)
            record = replace(
                record, index_key=_ready_key(queue, available_at, seq, record.id)
            )
            self._put_record(txn, record)
            assert record.index_key is not None
            _ = txn.put(record.index_key, record.id.encode("utf-8"))
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
            )
            _ = txn.delete(key)
            self._put_record(txn, updated)
            assert updated.index_key is not None
            _ = txn.put(updated.index_key, updated.id.encode("utf-8"))
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
            return _stats_from_records(records, now=now)

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
            )
            self._put_record(txn, updated)
            assert updated.index_key is not None
            _ = txn.put(updated.index_key, updated.id.encode("utf-8"))
            return True

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


def _decode_record(raw: bytes) -> _QueueRecord:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("queue record is not valid JSON") from exc

    version = payload.get("version")
    if version != _QUEUE_RECORD_VERSION:
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
        state=payload["state"],
        index_key=index_key.encode("utf-8") if index_key is not None else None,
    )


def _stats_from_records(records: Iterable[_QueueRecord], *, now: float) -> QueueStats:
    ready = 0
    delayed = 0
    inflight = 0
    dead = 0
    total = 0
    for record in records:
        total += 1
        if record.state == _READY:
            if record.available_at <= now:
                ready += 1
            else:
                delayed += 1
        elif record.state == _INFLIGHT:
            inflight += 1
        elif record.state == _DEAD:
            dead += 1
    return QueueStats(
        ready=ready,
        delayed=delayed,
        inflight=inflight,
        dead=dead,
        total=total,
    )


def _validate_limit(limit: int | None) -> None:
    if limit is not None and limit < 0:
        raise ValueError("limit cannot be negative")


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
