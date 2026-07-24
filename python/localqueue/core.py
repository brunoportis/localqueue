"""Python facade for the persistent localqueue queue."""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time as _time
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import (
    Callable,
    Generic,
    Literal,
    Optional,
    Protocol,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

from localqueue import localqueue as _native
from localqueue.deadletter import (
    FailedMessage,
    FailureReason,
    _exception_message,
)
from localqueue.diagnostics import QueueDiagnostics, build_diagnostics
from localqueue.exceptions import Empty, Full, LocalQueueError
from localqueue.job import Job
from localqueue.maintenance import (
    BackupResult,
    IntegrityCheckResult,
    build_backup_result,
    build_integrity_result,
)
from localqueue.policies import DeliveryPolicy, DurabilityMode, _durability_fsync

log = logging.getLogger(__name__)
_ResultT = TypeVar("_ResultT")
_PayloadT = TypeVar("_PayloadT")

_BACKPRESSURE_MIN_SLEEP_SECONDS = 0.01
_BACKPRESSURE_MAX_SLEEP_SECONDS = 0.25


class Serializer(Protocol[_PayloadT]):
    """Protocol implemented by compatible serializers."""

    def dumps(self, obj: _PayloadT, /) -> bytes: ...

    def loads(self, data: bytes, /) -> _PayloadT: ...


class JsonSerializer(Generic[_PayloadT]):
    """Default JSON serializer with a caller-selected static payload type."""

    def dumps(self, obj: _PayloadT, /) -> bytes:
        return json.dumps(obj).encode("utf-8")

    def loads(self, data: bytes, /) -> _PayloadT:
        # json.loads cannot prove a caller-selected PayloadT. This cast is the
        # serializer's trust boundary; it does not add runtime validation.
        return cast(_PayloadT, json.loads(data.decode("utf-8")))


@dataclass
class EnqueueItem(Generic[_PayloadT]):
    """A :meth:`SimpleQueue.put_many` item with an optional ``job_id``."""

    data: _PayloadT
    job_id: Optional[str] = None


class QueueStats(TypedDict):
    """Counts for each persistent queue state."""

    ready: int
    processing: int
    acked: int
    failed: int


class SimpleQueue(Generic[_PayloadT]):
    """Persistent SQLite queue with ACK/NACK, leases, and retries.

    A native Rust extension implements the transactional engine and preserves
    atomicity across multiple processes and threads.
    """

    def __init__(
        self,
        path: str,
        name: str = "default",
        *,
        delivery: DeliveryPolicy = DeliveryPolicy(),
        durability: DurabilityMode = DurabilityMode.RELAXED,
        serializer: Optional[Serializer[_PayloadT]] = None,
        max_pending_jobs: Optional[int] = None,
    ) -> None:
        """Initialize the queue.

        :param path: directory where the SQLite database is stored.
        :param name: logical queue name.
        :param delivery: lease duration and retry policy for claimed jobs.
        :param durability: durability intent for committed operations.
        :param serializer: object providing ``dumps`` and ``loads`` methods.
        :param max_pending_jobs: optional positive logical backlog limit.
        """
        if not isinstance(delivery, DeliveryPolicy):
            raise TypeError("'delivery' must be a DeliveryPolicy")
        fsync = _durability_fsync(durability)
        if max_pending_jobs is not None:
            if not isinstance(max_pending_jobs, int) or isinstance(
                max_pending_jobs, bool
            ):
                raise TypeError("'max_pending_jobs' must be an integer or None")
            if max_pending_jobs <= 0:
                raise ValueError("'max_pending_jobs' must be positive")

        self.path = Path(path)
        self.name = name
        self.delivery = delivery
        self.durability = durability
        self.max_pending_jobs = max_pending_jobs
        self.serializer: Serializer[_PayloadT] = serializer or JsonSerializer()
        self._closed = threading.Event()
        self._enqueue_close_lock = threading.Lock()

        self.path.mkdir(parents=True, exist_ok=True)
        db_path = self.path / "localqueue.db"

        self._native: Optional[_native.NativeQueue] = _native.NativeQueue(
            str(db_path),
            name,
            max_attempts=delivery.max_retries + 1,
            fsync=fsync,
            max_pending_jobs=max_pending_jobs,
        )

    def _get_native(self) -> "_native.NativeQueue":
        native = self._native
        if native is None:
            raise LocalQueueError("queue is closed")
        return native

    def put(
        self,
        data: _PayloadT,
        job_id: Optional[str] = None,
        *,
        block: bool = True,
        timeout: Optional[float] = None,
    ) -> int:
        """Add an item to the queue.

        :param data: payload to enqueue.
        :param job_id: optional unique identifier used for deduplication.
        :param block: wait for logical capacity when ``True``.
        :param timeout: maximum total capacity wait in seconds.
        :return: internal queue item ID.
        :raises Full: when capacity is unavailable without further waiting.
        """
        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' must be non-negative")
        payload = self.serializer.dumps(data)
        return self._wait_for_capacity(
            lambda native, busy_timeout_ms: native.put(
                payload, job_id, busy_timeout_ms
            ),
            block=block,
            timeout=timeout,
        )

    def put_many(
        self,
        items: list[Union[_PayloadT, EnqueueItem[_PayloadT]]],
        *,
        block: bool = True,
        timeout: Optional[float] = None,
    ) -> list[int]:
        """Add multiple items to the queue in one transaction.

        :param items: plain payloads or :class:`EnqueueItem` instances for
            per-item ``job_id`` deduplication.
        :return: internal item IDs in input order.
        :raises Full: when the complete batch cannot fit without further waiting.
        """
        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' must be non-negative")
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
        if not payloads:
            self._get_native()
            return []
        native_job_ids = job_ids if has_job_id else None
        return self._wait_for_capacity(
            lambda native, busy_timeout_ms: native.put_many(
                payloads, native_job_ids, busy_timeout_ms
            ),
            block=block,
            timeout=timeout,
        )

    def _wait_for_capacity(
        self,
        operation: Callable[["_native.NativeQueue", Optional[int]], _ResultT],
        *,
        block: bool,
        timeout: Optional[float],
    ) -> _ResultT:
        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' must be non-negative")

        if not block:
            try:
                return self._run_enqueue_attempt(operation, None)
            except _native._FullImpossible:
                raise Full("queue is full") from None

        deadline = None if timeout is None else _time.monotonic() + timeout
        sleep_seconds = _BACKPRESSURE_MIN_SLEEP_SECONDS
        while True:
            if self._closed.is_set():
                raise LocalQueueError("queue is closed")
            remaining = (
                None if deadline is None else max(0.0, deadline - _time.monotonic())
            )
            busy_timeout_ms = (
                None
                if remaining is None
                else math.ceil(min(remaining, _BACKPRESSURE_MAX_SLEEP_SECONDS) * 1000)
            )
            try:
                return self._run_enqueue_attempt(operation, busy_timeout_ms)
            except _native._FullImpossible:
                raise Full("queue is full") from None
            except Full:
                if deadline is not None:
                    remaining = deadline - _time.monotonic()
                    if remaining <= 0:
                        raise
                    wait_seconds = min(sleep_seconds, remaining)
                else:
                    wait_seconds = sleep_seconds
                if self._closed.wait(wait_seconds):
                    raise LocalQueueError("queue is closed") from None
                sleep_seconds = min(
                    sleep_seconds * 1.5, _BACKPRESSURE_MAX_SLEEP_SECONDS
                )

    def _run_enqueue_attempt(
        self,
        operation: Callable[["_native.NativeQueue", Optional[int]], _ResultT],
        busy_timeout_ms: Optional[int],
    ) -> _ResultT:
        with self._enqueue_close_lock:
            if self._closed.is_set():
                raise LocalQueueError("queue is closed")
            return operation(self._get_native(), busy_timeout_ms)

    def get(
        self, block: bool = True, timeout: Optional[float] = None
    ) -> Job[_PayloadT]:
        """Claim an item from the queue with a lease.

        :param block: wait for an available item when ``True``.
        :param timeout: maximum wait in seconds.
        :return: a :class:`Job` instance.
        :raises Empty: if ``block=False`` and the queue is empty, or if the
            timeout expires while ``block=True``.
        """
        lease_ms = int(self.delivery.lease_seconds * 1000)

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

    def get_nowait(self) -> Job[_PayloadT]:
        """Non-blocking variant of :meth:`get`."""
        return self.get(block=False)

    def ack(self, job: Job[_PayloadT]) -> None:
        """Acknowledge successful processing of a job."""
        self._get_native().ack(job.id, job.receipt)

    def nack(
        self,
        job: Job[_PayloadT],
        *,
        delay: float = 0.0,
        last_error: Optional[str] = None,
    ) -> None:
        """Return a job to the queue after a transient failure.

        A job that has exhausted ``max_retries`` moves to dead-letter.
        """
        self._nack_with_reason(job, delay=delay, last_error=last_error)

    def _nack_with_reason(
        self,
        job: Job[_PayloadT],
        *,
        delay: float = 0.0,
        last_error: str | None = None,
        reason: FailureReason | None = None,
    ) -> None:
        if not delay >= 0:
            raise ValueError("'delay' must be non-negative")
        delay_ms = int(delay * 1000)
        stored_reason = None if reason is None else reason.value
        self._get_native().nack(
            job.id, job.receipt, delay_ms, last_error, stored_reason
        )

    def fail(self, job: Job[_PayloadT], last_error: Optional[str] = None) -> None:
        """Mark a job as permanently failed and move it to dead-letter."""
        self._fail_with_reason(
            job,
            last_error=last_error,
            reason=FailureReason.EXPLICIT_PERMANENT_FAILURE,
        )

    def _fail_with_reason(
        self,
        job: Job[_PayloadT],
        *,
        last_error: str | None = None,
        reason: FailureReason,
    ) -> None:
        self._get_native().fail(job.id, job.receipt, last_error, reason.value)

    def extend_lease(self, job: Job[_PayloadT], seconds: float) -> None:
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

    def stats(self) -> QueueStats:
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
            lease_seconds=self.delivery.lease_seconds,
            max_retries=self.delivery.max_retries,
        )

    def check_integrity(
        self,
        *,
        mode: Literal["full", "quick"] = "full",
        max_errors: int = 100,
    ) -> IntegrityCheckResult:
        """Run a read-only SQLite integrity check on the shared database."""
        if mode not in ("full", "quick"):
            raise ValueError("'mode' must be 'full' or 'quick'")
        if not isinstance(max_errors, int) or isinstance(max_errors, bool):
            raise TypeError("'max_errors' must be an integer")
        if not 1 <= max_errors <= 1000:
            raise ValueError("'max_errors' must be between 1 and 1000")
        return build_integrity_result(
            self._get_native().check_integrity(
                quick=mode == "quick", max_errors=max_errors
            )
        )

    def backup(
        self,
        destination_directory: Union[str, os.PathLike[str]],
    ) -> BackupResult:
        """Create a consistent online backup in a new destination directory.

        The destination parent must already exist. The destination itself must
        not exist and is reserved atomically by this operation.
        """
        destination_string = os.fspath(destination_directory)
        database_path = os.path.join(destination_string, "localqueue.db")
        stable_destination = os.path.abspath(destination_string)
        snapshot = self._get_native().backup(stable_destination)
        return build_backup_result(
            snapshot,
            destination=destination_string,
            database_path=database_path,
        )

    def _to_job(self, lease: "_native.Lease") -> Job[_PayloadT]:
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

    def list_failed(
        self, limit: int = 100, offset: int = 0
    ) -> list[FailedMessage[_PayloadT]]:
        """List messages in dead-letter.

        :param limit: maximum number of messages.
        :param offset: pagination offset.
        Results are ordered by increasing message ID. Offset pagination is
        deterministic while the failed set remains stable.
        """
        if not isinstance(limit, int) or isinstance(limit, bool):
            raise TypeError("'limit' must be an integer")
        if not isinstance(offset, int) or isinstance(offset, bool):
            raise TypeError("'offset' must be an integer")
        if limit < 0:
            raise ValueError("'limit' must be non-negative")
        if offset < 0:
            raise ValueError("'offset' must be non-negative")

        records: list[FailedMessage[_PayloadT]] = []
        for message in self._get_native().list_failed(limit, offset):
            decode_error = None
            data: _PayloadT | None = None
            try:
                data = self.serializer.loads(message.payload)
            except Exception as error:
                decode_error = _exception_message(error)
            records.append(
                FailedMessage(
                    id=message.id,
                    data=data,
                    raw_payload=message.payload,
                    attempts=message.attempts,
                    reason=FailureReason._from_stored(message.failure_reason),
                    last_error=message.last_error,
                    created_at=message.created_at / 1000.0,
                    updated_at=message.updated_at / 1000.0,
                    decode_error=decode_error,
                )
            )
        return records

    def retry_failed(self, message_id: int) -> None:
        """Move a dead-letter message back to the queue.

        :param message_id: ID of the message to retry.
        """
        if not isinstance(message_id, int) or isinstance(message_id, bool):
            raise TypeError("'message_id' must be an integer")
        self._get_native().retry_failed(message_id)

    def vacuum(self) -> None:
        """Compact the ``localqueue.db`` shared by all queues.

        This operation can contend for the SQLite lock with active workers.
        """
        self._get_native().vacuum()

    def close(self) -> None:
        """Close the database connection."""
        self._closed.set()
        with self._enqueue_close_lock:
            if self._native is not None:
                self._native.close()
                self._native = None

    def __enter__(self) -> SimpleQueue[_PayloadT]:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
