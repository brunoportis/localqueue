from __future__ import annotations

import json
import threading
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Iterable, cast

from .base import QueueMessage, QueueStats

if TYPE_CHECKING:
    from types import ModuleType

_ENVS: dict[tuple[str, int], Any] = {}
_ENVS_LOCK = threading.Lock()
_READY = "ready"
_INFLIGHT = "inflight"
_DEAD = "dead"
_QUEUE_RECORD_VERSION = 4
_SQLITE_SCHEMA_VERSION = 2


def import_lmdb() -> ModuleType:
    try:
        import lmdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "LMDB support requires the optional dependency; "
            'install with `pip install "localqueue[lmdb]"`'
        ) from exc
    return lmdb


def attempt_event(
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
class QueueRecord:
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
    ) -> "QueueRecord":
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


def replace_record(record: QueueRecord, **changes: Any) -> QueueRecord:
    return cast("QueueRecord", replace(record, **changes))


def safe_queue(queue: str) -> str:
    if not queue:
        raise ValueError("queue name cannot be empty")
    if ":" in queue:
        raise ValueError("queue name cannot contain ':'")
    return queue


def millis(value: float) -> int:
    return max(int(value * 1000), 0)


def timestamp(value: str) -> float:
    return int(value) / 1000


def queue_prefix(queue: str) -> bytes:
    return f"queue:{safe_queue(queue)}:".encode("utf-8")


def message_prefix(queue: str) -> bytes:
    return f"queue:{safe_queue(queue)}:message:".encode("utf-8")


def message_key(queue: str, message_id: str) -> bytes:
    return f"queue:{safe_queue(queue)}:message:{message_id}".encode("utf-8")


def seq_key(queue: str) -> bytes:
    return f"queue:{safe_queue(queue)}:seq".encode("utf-8")


def ready_prefix(queue: str) -> bytes:
    return f"queue:{safe_queue(queue)}:ready:".encode("utf-8")


def ready_key(queue: str, available_at: float, seq: int, message_id: str) -> bytes:
    return (
        f"queue:{safe_queue(queue)}:ready:"
        + f"{millis(available_at):020d}:{seq:020d}:{message_id}"
    ).encode("utf-8")


def timestamp_from_ready_key(key: bytes) -> float:
    return timestamp_from_index_key(key, expected_state=_READY)


def inflight_prefix(queue: str) -> bytes:
    return f"queue:{safe_queue(queue)}:inflight:".encode("utf-8")


def inflight_key(queue: str, leased_until: float, message_id: str) -> bytes:
    return (
        f"queue:{safe_queue(queue)}:inflight:{millis(leased_until):020d}:{message_id}"
    ).encode("utf-8")


def timestamp_from_inflight_key(key: bytes) -> float:
    return timestamp_from_index_key(key, expected_state=_INFLIGHT)


def dead_key(queue: str, message_id: str) -> bytes:
    return f"queue:{safe_queue(queue)}:dead:{message_id}".encode("utf-8")


def dead_prefix(queue: str) -> bytes:
    return f"queue:{safe_queue(queue)}:dead:".encode("utf-8")


def worker_stats_key(queue: str) -> bytes:
    return f"queue:{safe_queue(queue)}:worker-stats".encode("utf-8")


def worker_heartbeats_key(queue: str) -> bytes:
    return f"queue:{safe_queue(queue)}:worker-heartbeats".encode("utf-8")


def dedupe_token(dedupe_key: str) -> str:
    return dedupe_key.encode("utf-8").hex()


def dedupe_prefix(queue: str) -> bytes:
    return f"queue:{safe_queue(queue)}:dedupe:".encode("utf-8")


def dedupe_key_key(queue: str, dedupe_key: str) -> bytes:
    return f"queue:{safe_queue(queue)}:dedupe:{dedupe_token(dedupe_key)}".encode(
        "utf-8"
    )


def encode_record(record: QueueRecord) -> bytes:
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


def validate_json_serializable(value: Any) -> None:
    try:
        _ = json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("queue values must be JSON-serializable") from exc


def decode_record(raw: bytes | str) -> QueueRecord:
    try:
        payload = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("queue record is not valid JSON") from exc

    version = payload.get("version")
    if version not in {1, 2, 3, _QUEUE_RECORD_VERSION}:
        raise ValueError(f"unsupported queue record version: {version!r}")

    index_key = payload["index_key"]
    return QueueRecord(
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


def encode_worker_stats(stats: dict[str, int]) -> bytes:
    return json.dumps(dict(sorted(stats.items())), separators=(",", ":")).encode(
        "utf-8"
    )


def encode_worker_heartbeats(stats: dict[str, float]) -> bytes:
    return json.dumps(dict(sorted(stats.items())), separators=(",", ":")).encode(
        "utf-8"
    )


@dataclass(slots=True)
class QueueStatsAccumulator:
    ready: int = 0
    delayed: int = 0
    inflight: int = 0
    dead: int = 0
    total: int = 0
    by_worker_id: Counter[str] = field(default_factory=Counter)
    ready_ages: list[float] = field(default_factory=list)
    inflight_ages: list[float] = field(default_factory=list)


def accumulate_record_stats(
    accumulator: QueueStatsAccumulator, record: QueueRecord, *, now: float
) -> None:
    accumulator.total += 1
    if record.state == _READY:
        accumulate_ready_stats(accumulator, record, now=now)
        return
    if record.state == _INFLIGHT:
        accumulate_inflight_stats(accumulator, record, now=now)
        return
    if record.state == _DEAD:
        accumulator.dead += 1


def accumulate_ready_stats(
    accumulator: QueueStatsAccumulator, record: QueueRecord, *, now: float
) -> None:
    if record.available_at <= now:
        accumulator.ready += 1
        accumulator.ready_ages.append(max(now - record.available_at, 0.0))
        return
    accumulator.delayed += 1


def accumulate_inflight_stats(
    accumulator: QueueStatsAccumulator, record: QueueRecord, *, now: float
) -> None:
    accumulator.inflight += 1
    if record.leased_by:
        accumulator.by_worker_id[record.leased_by] += 1
    leased_at = last_leased_at(record)
    if leased_at is not None:
        accumulator.inflight_ages.append(max(now - leased_at, 0.0))


def stats_from_records(
    records: Iterable[QueueRecord],
    *,
    now: float,
    leases_by_worker_id: dict[str, int] | None = None,
    last_seen_by_worker_id: dict[str, float] | None = None,
) -> QueueStats:
    accumulator = QueueStatsAccumulator()
    for record in records:
        accumulate_record_stats(accumulator, record, now=now)
    return QueueStats(
        ready=accumulator.ready,
        delayed=accumulator.delayed,
        inflight=accumulator.inflight,
        dead=accumulator.dead,
        total=accumulator.total,
        by_worker_id=dict(sorted(accumulator.by_worker_id.items())),
        leases_by_worker_id=dict(sorted((leases_by_worker_id or {}).items())),
        last_seen_by_worker_id=dict(sorted((last_seen_by_worker_id or {}).items())),
        oldest_ready_age_seconds=max(accumulator.ready_ages)
        if accumulator.ready_ages
        else None,
        oldest_inflight_age_seconds=max(accumulator.inflight_ages)
        if accumulator.inflight_ages
        else None,
        average_inflight_age_seconds=(
            sum(accumulator.inflight_ages) / len(accumulator.inflight_ages)
            if accumulator.inflight_ages
            else None
        ),
    )


def validate_limit(limit: int | None) -> None:
    if limit is not None and limit < 0:
        raise ValueError("limit cannot be negative")


def dead_record_age(record: QueueRecord, *, now: float) -> float:
    anchor = record.failed_at if record.failed_at is not None else record.created_at
    return max(now - anchor, 0.0)


def last_leased_at(record: QueueRecord) -> float | None:
    for event in reversed(record.attempt_history):
        if event.get("type") == "leased":
            leased_at = event.get("at")
            return float(leased_at) if leased_at is not None else None
    return None


def timestamp_from_index_key(key: bytes, *, expected_state: str) -> float:
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
    return timestamp(parts[3])


def sequence_from_index_key(key: bytes) -> int:
    try:
        decoded = key.decode("utf-8")
    except UnicodeDecodeError:
        return 0

    parts = decoded.split(":")
    if len(parts) >= 6 and parts[2] == _READY:
        return int(parts[4])
    return 0
