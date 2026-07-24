"""Type stubs for the native ``localqueue.localqueue`` module."""

from __future__ import annotations

from typing import Optional

__version__: str

class _FullImpossible(Exception): ...

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
    def get(self, lease_ms: int) -> Optional[Lease]: ...
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
class LeaseExpired(LocalQueueError): ...
