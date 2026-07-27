"""Type stubs for the native ``localqueue.localqueue`` module."""

from __future__ import annotations

from typing import Optional

__version__: str

class Lease:
    id: int
    payload: bytes
    attempts: int
    receipt: str
    lease_until: int

class Stats:
    ready: int
    processing: int
    acked: int
    failed: int

class FailedMessage:
    id: int
    payload: bytes
    attempts: int
    last_error: Optional[str]
    failure_reason: Optional[str]
    failure_category: Optional[str]
    created_at: int
    updated_at: int

class DiagnosticsSnapshot:
    schema_version: int
    sqlite_version: str
    observed_at_ms: int
    journal_mode: str
    synchronous: int
    durability_mode: str
    busy_timeout_ms: int
    database_size_bytes: Optional[int]
    wal_size_bytes: Optional[int]
    shm_size_bytes: Optional[int]
    page_count: int
    page_size: int
    freelist_count: int
    ready: int
    processing: int
    acked: int
    failed: int
    max_pending_jobs: Optional[int]
    pending_jobs: int
    available_slots: Optional[int]
    oldest_available_age_ms: Optional[int]
    oldest_processing_updated_age_ms: Optional[int]
    active_leases: int
    expired_leases: int
    oldest_expired_lease_age_ms: Optional[int]

class IntegrityCheckSnapshot:
    schema_version: int
    mode: str
    max_errors: int
    ok: bool
    messages: list[str]
    elapsed_ms: int

class BackupSnapshot:
    schema_version: int
    elapsed_ms: int
    pages_copied: int
    page_count: int
    database_size_bytes: int
    verified: bool
    verification_mode: str
    verification_messages: list[str]

class NativeQueue:
    def __init__(
        self,
        path: str,
        queue: str,
        max_attempts: int = 3,
        fsync: bool = False,
        max_pending_jobs: Optional[int] = None,
    ) -> None: ...
    def put(
        self,
        payload: bytes,
        job_id: Optional[str] = None,
        busy_timeout_ms: Optional[int] = None,
    ) -> int: ...
    def put_many(
        self,
        payloads: list[bytes],
        job_ids: Optional[list[Optional[str]]] = None,
        busy_timeout_ms: Optional[int] = None,
    ) -> list[int]: ...
    def fanout(
        self,
        payload: bytes,
        targets: list[tuple[str, Optional[str]]],
    ) -> list[int]: ...
    def _fanout_with_identity(
        self,
        payload: bytes,
        targets: list[tuple[str, Optional[str], Optional[str], Optional[str]]],
    ) -> list[tuple[int, bool]]: ...
    def _enqueue_batch_with_identity(
        self,
        entries: list[tuple[str, bytes, Optional[str], Optional[str], Optional[str]]],
        capacity: Optional[list[tuple[str, int]]] = None,
    ) -> list[tuple[int, bool]]: ...
    def _enqueue_batch_with_identity_and_checkpoint(
        self,
        entries: list[tuple[str, bytes, Optional[str], Optional[str], Optional[str]]],
        capacity: Optional[list[tuple[str, int]]],
        checkpoint: Optional[
            tuple[str, str, Optional[str], Optional[int], str, Optional[str], int]
        ],
    ) -> tuple[list[tuple[int, bool]], Optional[str], Optional[int]]: ...
    def _enqueue_batch_with_identity_and_checkpoint_and_execution(
        self,
        entries: list[tuple[str, bytes, Optional[str], Optional[str], Optional[str]]],
        capacity: Optional[list[tuple[str, int]]],
        checkpoint: Optional[
            tuple[str, str, Optional[str], Optional[int], str, Optional[str], int]
        ],
        execution_id: Optional[str],
    ) -> tuple[list[tuple[int, bool]], Optional[str], Optional[int]]: ...
    def _execution_create(
        self,
        execution_id: str,
        bus_name: str,
        source_name: str,
        checkpoint_name: Optional[str] = None,
    ) -> None: ...
    def _execution_inspect(
        self, execution_id: str
    ) -> Optional[tuple[str, str, str, Optional[str], bool, int, int]]: ...
    def _execution_mark_source_completed(self, execution_id: str) -> bool: ...
    def _execution_delivery_states(
        self, execution_id: str
    ) -> tuple[int, int, int, int, int]: ...
    def _execution_open(
        self,
        candidate: str,
        bus: str,
        source: str,
        checkpoint: str,
        fingerprint: str,
        generation: Optional[str],
    ) -> tuple[str, bool]: ...
    def _execution_claim_source(self, id: str, receipt: str, lease_ms: int) -> bool: ...
    def _execution_extend_source_lease(
        self, id: str, receipt: str, lease_ms: int
    ) -> int: ...
    def _execution_release_source_lease(self, id: str, receipt: str) -> bool: ...
    def _execution_mark_source_completed_claimed(
        self, id: str, receipt: str
    ) -> bool: ...
    def _execution_finalize_if_complete(self, id: str) -> bool: ...
    def _execution_snapshot(self, id: str) -> tuple[object, ...]: ...
    def _enqueue_batch_with_claimed_execution(
        self,
        entries: list[tuple[str, bytes, Optional[str], Optional[str], Optional[str]]],
        capacity: Optional[list[tuple[str, int]]],
        checkpoint: tuple[
            str, str, Optional[str], Optional[int], str, Optional[str], int
        ],
        execution_id: str,
        receipt: str,
        dispatched: int,
        unrouted: int,
    ) -> tuple[list[tuple[int, bool]], str, int]: ...
    def _checkpoint_inspect(
        self,
        bus_name: str,
        checkpoint_name: str,
    ) -> Optional[tuple[str, Optional[str], str, int, int, int, int, int]]: ...
    def _checkpoint_reset(self, bus_name: str, checkpoint_name: str) -> bool: ...
    def ack_and_fanout(
        self,
        id: int,
        receipt: str,
        payload: bytes,
        targets: list[tuple[str, Optional[str]]],
    ) -> list[int]: ...
    def _ack_and_fanout_with_identity(
        self,
        id: int,
        receipt: str,
        payload: bytes,
        targets: list[tuple[str, Optional[str], Optional[str], Optional[str]]],
    ) -> list[tuple[int, bool]]: ...
    def get(
        self,
        lease_ms: int,
        max_attempts: Optional[int] = None,
        busy_timeout_ms: Optional[int] = None,
    ) -> Optional[Lease]: ...
    def ack(self, id: int, receipt: str) -> None: ...
    def nack(
        self,
        id: int,
        receipt: str,
        delay_ms: int = 0,
        last_error: Optional[str] = None,
        failure_reason: Optional[str] = None,
    ) -> None: ...
    def fail(
        self,
        id: int,
        receipt: str,
        last_error: Optional[str] = None,
        failure_reason: Optional[str] = None,
        failure_category: Optional[str] = None,
    ) -> None: ...
    def extend_lease(self, id: int, receipt: str, extend_ms: int) -> int: ...
    def reclaim_expired(self, now: Optional[int] = None) -> int: ...
    def stats(self) -> Stats: ...
    def diagnostics(self) -> DiagnosticsSnapshot: ...
    def check_integrity(
        self, quick: bool = False, max_errors: int = 100
    ) -> IntegrityCheckSnapshot: ...
    def backup(self, destination: str) -> BackupSnapshot: ...
    def purge(self, older_than_ms: int, status: Optional[int] = None) -> int: ...
    def list_failed(self, limit: int = 100, offset: int = 0) -> list[FailedMessage]: ...
    def retry_failed(self, id: int) -> None: ...
    def vacuum(self) -> None: ...
    def close(self) -> None: ...

class LocalQueueError(Exception): ...
class Empty(LocalQueueError): ...
class Full(LocalQueueError): ...
class _FullImpossible(Full): ...
class LeaseExpired(LocalQueueError): ...
class DeduplicationConflict(LocalQueueError): ...
class CheckpointConflict(LocalQueueError): ...
