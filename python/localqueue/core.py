"""Python facade for the persistent localqueue queue."""

from __future__ import annotations

import json
import logging
import os
import time as _time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional, Protocol, Union

from localqueue import localqueue as _native
from localqueue.diagnostics import QueueDiagnostics, build_diagnostics
from localqueue.exceptions import Empty, LocalQueueError
from localqueue.job import Job
from localqueue.maintenance import (
    BackupResult,
    IntegrityCheckResult,
    build_backup_result,
    build_integrity_result,
)

log = logging.getLogger(__name__)


class Serializer(Protocol):
    """Protocol implemented by compatible serializers."""

    def dumps(self, obj: Any) -> bytes: ...

    def loads(self, data: bytes) -> Any: ...


class JsonSerializer:
    """Default JSON serializer."""

    def dumps(self, obj: Any) -> bytes:
        return json.dumps(obj).encode("utf-8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))


@dataclass
class EnqueueItem:
    """A :meth:`SimpleQueue.put_many` item with an optional ``job_id``."""

    data: Any
    job_id: Optional[str] = None


class SimpleQueue:
    """Persistent SQLite queue with ACK/NACK, leases, and retries.

    A native Rust extension implements the transactional engine and preserves
    atomicity across multiple processes and threads.
    """

    def __init__(
        self,
        path: str,
        name: str = "default",
        *,
        lease_seconds: float = 60.0,
        max_retries: int = 3,
        fsync: bool = False,
        serializer: Optional[Serializer] = None,
    ) -> None:
        """Initialize the queue.

        :param path: directory where the SQLite database is stored.
        :param name: logical queue name.
        :param lease_seconds: lease duration for each claimed job.
        :param max_retries: retries allowed before moving to dead-letter.
        :param fsync: use ``PRAGMA synchronous=FULL`` when ``True``.
        :param serializer: object providing ``dumps`` and ``loads`` methods.
        """
        if not lease_seconds > 0:
            raise ValueError("'lease_seconds' must be positive")
        if max_retries < 0:
            raise ValueError("'max_retries' must be non-negative")

        self.path = Path(path)
        self.name = name
        self.lease_seconds = lease_seconds
        self.max_retries = max_retries
        self.serializer = serializer or JsonSerializer()

        self.path.mkdir(parents=True, exist_ok=True)
        db_path = self.path / "localqueue.db"

        self._native: Optional[_native.NativeQueue] = _native.NativeQueue(
            str(db_path),
            name,
            max_attempts=max_retries + 1,
            fsync=fsync,
        )

    def _get_native(self) -> "_native.NativeQueue":
        native = self._native
        if native is None:
            raise LocalQueueError("queue is closed")
        return native

    def put(self, data: Any, job_id: Optional[str] = None) -> int:
        """Add an item to the queue.

        :param data: payload to enqueue.
        :param job_id: optional unique identifier used for deduplication.
        :return: internal queue item ID.
        """
        payload = self.serializer.dumps(data)
        return self._get_native().put(payload, job_id)

    def put_many(self, items: list[Union[Any, EnqueueItem]]) -> list[int]:
        """Add multiple items to the queue in one transaction.

        :param items: plain payloads or :class:`EnqueueItem` instances for
            per-item ``job_id`` deduplication.
        :return: internal item IDs in input order.
        """
        payloads: list[bytes] = []
        job_ids: list[Optional[str]] = []
        has_job_id = False
        for item in items:
            if isinstance(item, EnqueueItem):
                payloads.append(self.serializer.dumps(item.data))
                job_ids.append(item.job_id)
                has_job_id = has_job_id or item.job_id is not None
            else:
                payloads.append(self.serializer.dumps(item))
                job_ids.append(None)
        return self._get_native().put_many(payloads, job_ids if has_job_id else None)

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Job:
        """Claim an item from the queue with a lease.

        :param block: wait for an available item when ``True``.
        :param timeout: maximum wait in seconds.
        :return: a :class:`Job` instance.
        :raises Empty: if ``block=False`` and the queue is empty, or if the
            timeout expires while ``block=True``.
        """
        lease_ms = int(self.lease_seconds * 1000)

        if not block:
            lease = self._get_native().get(lease_ms)
            if lease is None:
                raise Empty("queue is empty")
            return self._to_job(lease)

        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' must be non-negative")

        start = _time.monotonic()
        sleep = 0.01
        max_sleep = 0.25
        while True:
            lease = self._get_native().get(lease_ms)
            if lease is not None:
                return self._to_job(lease)

            if timeout is not None:
                elapsed = _time.monotonic() - start
                if elapsed >= timeout:
                    raise Empty("queue is empty")

            _time.sleep(sleep)
            sleep = min(sleep * 1.5, max_sleep)

    def get_nowait(self) -> Job:
        """Non-blocking variant of :meth:`get`."""
        return self.get(block=False)

    def ack(self, job: Job) -> None:
        """Acknowledge successful processing of a job."""
        self._get_native().ack(job.id, job.receipt)

    def nack(
        self,
        job: Job,
        *,
        delay: float = 0.0,
        last_error: Optional[str] = None,
    ) -> None:
        """Return a job to the queue after a transient failure.

        A job that has exhausted ``max_retries`` moves to dead-letter.
        """
        if not delay >= 0:
            raise ValueError("'delay' must be non-negative")
        delay_ms = int(delay * 1000)
        self._get_native().nack(job.id, job.receipt, delay_ms, last_error)

    def fail(self, job: Job, last_error: Optional[str] = None) -> None:
        """Mark a job as permanently failed and move it to dead-letter."""
        self._get_native().fail(job.id, job.receipt, last_error)

    def extend_lease(self, job: Job, seconds: float) -> None:
        """Extend a job's lease.

        Raises :class:`LeaseExpired` if the lease has already expired.
        """
        if not seconds > 0:
            raise ValueError("'seconds' must be positive")
        extend_ms = int(seconds * 1000)
        new_expiration = self._get_native().extend_lease(job.id, job.receipt, extend_ms)
        job.lease_expires_at = new_expiration / 1000.0

    def reclaim_expired_leases(self) -> int:
        """Reclaim jobs whose leases have expired.

        :return: number of reclaimed leases.
        """
        return self._get_native().reclaim_expired(None)

    def stats(self) -> dict[str, int]:
        """Return queue statistics."""
        stats = self._get_native().stats()
        return {
            "ready": stats.ready,
            "processing": stats.processing,
            "acked": stats.acked,
            "failed": stats.failed,
        }

    def diagnostics(self) -> QueueDiagnostics:
        """Return an immutable, read-only operational snapshot."""
        snapshot = self._get_native().diagnostics()
        serializer_type = type(self.serializer)
        serializer_identity = (
            "localqueue.JsonSerializer"
            if serializer_type is JsonSerializer
            else f"{serializer_type.__module__}.{serializer_type.__qualname__}"
        )
        return build_diagnostics(
            snapshot,
            queue_name=self.name,
            serializer_identity=serializer_identity,
            lease_seconds=self.lease_seconds,
            max_retries=self.max_retries,
        )

    def check_integrity(
        self, mode: Literal["full", "quick"] = "full"
    ) -> IntegrityCheckResult:
        """Run a read-only SQLite integrity check on the shared database."""
        if mode not in ("full", "quick"):
            raise ValueError("'mode' must be 'full' or 'quick'")
        return build_integrity_result(
            self._get_native().check_integrity(quick=mode == "quick")
        )

    def backup(
        self,
        destination: Union[str, os.PathLike[str]],
        *,
        overwrite: bool = False,
    ) -> BackupResult:
        """Create a consistent online backup in a destination SQLite file.

        The destination parent must already exist. Existing files are rejected
        unless ``overwrite=True`` is explicit.
        """
        destination_string = os.fspath(destination)
        snapshot = self._get_native().backup(destination_string, overwrite=overwrite)
        return build_backup_result(snapshot, destination=destination_string)

    def _to_job(self, lease: "_native.Lease") -> Job:
        data = self.serializer.loads(lease.payload)
        return Job(
            id=lease.id,
            data=data,
            attempts=max(0, lease.attempts - 1),
            receipt=lease.receipt,
            lease_expires_at=lease.lease_until / 1000.0,
            queue=self,
        )

    def purge(self, older_than: float, *, include_failed: bool = False) -> int:
        """Remove old messages from the queue.

        :param older_than: maximum age in seconds; older messages are removed.
        :param include_failed: also remove ``failed`` messages when ``True``;
            by default only ``acked`` messages are removed.
        :return: number of removed messages.
        """
        if older_than < 0:
            raise ValueError("'older_than' must be non-negative")

        older_than_ms = int(older_than * 1000)
        removed = 0
        removed += self._get_native().purge(older_than_ms, 2)  # acked
        if include_failed:
            removed += self._get_native().purge(older_than_ms, 3)  # failed
        return removed

    def list_failed(self, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        """List messages in dead-letter.

        :param limit: maximum number of messages.
        :param offset: pagination offset.
        :return: dictionaries containing message information.
        """
        if limit < 0:
            raise ValueError("'limit' must be non-negative")
        if offset < 0:
            raise ValueError("'offset' must be non-negative")

        failed = self._get_native().list_failed(limit, offset)
        return [
            {
                "id": msg.id,
                "data": self.serializer.loads(msg.payload),
                "attempts": msg.attempts,
                "last_error": msg.last_error,
                "created_at": msg.created_at / 1000.0,
                "updated_at": msg.updated_at / 1000.0,
            }
            for msg in failed
        ]

    def retry_failed(self, message_id: int) -> None:
        """Move a dead-letter message back to the queue.

        :param message_id: ID of the message to retry.
        """
        self._get_native().retry_failed(message_id)

    def vacuum(self) -> None:
        """Compact the ``localqueue.db`` shared by all queues.

        This operation can contend for the SQLite lock with active workers.
        """
        self._get_native().vacuum()

    def close(self) -> None:
        """Close the database connection."""
        if self._native is not None:
            self._native.close()
            self._native = None

    def __enter__(self) -> SimpleQueue:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
