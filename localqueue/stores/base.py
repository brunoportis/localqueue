from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

_READY = "ready"


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
