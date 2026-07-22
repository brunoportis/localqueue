"""Public diagnostics model and native snapshot conversion."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import metadata
from typing import Literal, Optional, Union

from localqueue import localqueue as _native

JsonScalar = Union[str, int, float, bool, None]
DurabilityMode = Literal["normal", "full", "unknown"]


@dataclass(frozen=True, slots=True)
class QueueDiagnostics:
    """Immutable, JSON-compatible operational snapshot for one logical queue."""

    schema_version: int
    package_version: str
    sqlite_version: str
    observed_at: float
    queue_name: str
    serializer_identity: str
    lease_seconds: float
    max_retries: int
    max_pending_jobs: Optional[int]
    journal_mode: str
    synchronous: int
    durability_mode: DurabilityMode
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
    pending_jobs: int
    available_slots: Optional[int]
    oldest_available_age_seconds: Optional[float]
    oldest_processing_updated_age_seconds: Optional[float]
    active_leases: int
    expired_leases: int
    oldest_expired_lease_age_seconds: Optional[float]

    def to_dict(self) -> dict[str, JsonScalar]:
        """Return a dictionary containing only JSON-serializable scalars."""
        return {
            "schema_version": self.schema_version,
            "package_version": self.package_version,
            "sqlite_version": self.sqlite_version,
            "observed_at": self.observed_at,
            "queue_name": self.queue_name,
            "serializer_identity": self.serializer_identity,
            "lease_seconds": self.lease_seconds,
            "max_retries": self.max_retries,
            "max_pending_jobs": self.max_pending_jobs,
            "journal_mode": self.journal_mode,
            "synchronous": self.synchronous,
            "durability_mode": self.durability_mode,
            "busy_timeout_ms": self.busy_timeout_ms,
            "database_size_bytes": self.database_size_bytes,
            "wal_size_bytes": self.wal_size_bytes,
            "shm_size_bytes": self.shm_size_bytes,
            "page_count": self.page_count,
            "page_size": self.page_size,
            "freelist_count": self.freelist_count,
            "ready": self.ready,
            "processing": self.processing,
            "acked": self.acked,
            "failed": self.failed,
            "pending_jobs": self.pending_jobs,
            "available_slots": self.available_slots,
            "oldest_available_age_seconds": self.oldest_available_age_seconds,
            "oldest_processing_updated_age_seconds": (
                self.oldest_processing_updated_age_seconds
            ),
            "active_leases": self.active_leases,
            "expired_leases": self.expired_leases,
            "oldest_expired_lease_age_seconds": (self.oldest_expired_lease_age_seconds),
        }


def build_diagnostics(
    snapshot: _native.DiagnosticsSnapshot,
    *,
    queue_name: str,
    serializer_identity: str,
    lease_seconds: Union[int, float],
    max_retries: int,
) -> QueueDiagnostics:
    """Convert the private native snapshot into the public Python model."""
    mode: DurabilityMode
    if snapshot.durability_mode in ("normal", "full"):
        mode = snapshot.durability_mode
    else:
        mode = "unknown"

    return QueueDiagnostics(
        schema_version=snapshot.schema_version,
        package_version=_package_version(),
        sqlite_version=snapshot.sqlite_version,
        observed_at=snapshot.observed_at_ms / 1000.0,
        queue_name=queue_name,
        serializer_identity=serializer_identity,
        lease_seconds=float(lease_seconds),
        max_retries=max_retries,
        max_pending_jobs=snapshot.max_pending_jobs,
        journal_mode=snapshot.journal_mode,
        synchronous=snapshot.synchronous,
        durability_mode=mode,
        busy_timeout_ms=snapshot.busy_timeout_ms,
        database_size_bytes=snapshot.database_size_bytes,
        wal_size_bytes=snapshot.wal_size_bytes,
        shm_size_bytes=snapshot.shm_size_bytes,
        page_count=snapshot.page_count,
        page_size=snapshot.page_size,
        freelist_count=snapshot.freelist_count,
        ready=snapshot.ready,
        processing=snapshot.processing,
        acked=snapshot.acked,
        failed=snapshot.failed,
        pending_jobs=snapshot.pending_jobs,
        available_slots=snapshot.available_slots,
        oldest_available_age_seconds=_seconds(snapshot.oldest_available_age_ms),
        oldest_processing_updated_age_seconds=_seconds(
            snapshot.oldest_processing_updated_age_ms
        ),
        active_leases=snapshot.active_leases,
        expired_leases=snapshot.expired_leases,
        oldest_expired_lease_age_seconds=_seconds(snapshot.oldest_expired_lease_age_ms),
    )


def _package_version() -> str:
    try:
        return metadata.version("localqueue")
    except metadata.PackageNotFoundError:
        # Source checkouts without installed distribution metadata are explicit.
        return "development"


def _seconds(milliseconds: Optional[int]) -> Optional[float]:
    if milliseconds is None:
        return None
    return milliseconds / 1000.0
