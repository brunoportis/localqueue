"""Typed results for explicit SQLite maintenance operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Union

from localqueue import localqueue as _native

IntegrityCheckMode = Literal["full", "quick"]
JsonValue = Union[str, int, float, bool, None, list[str]]


@dataclass(frozen=True, slots=True)
class IntegrityCheckResult:
    """Result of a read-only SQLite integrity check."""

    schema_version: int
    mode: IntegrityCheckMode
    max_errors: int
    ok: bool
    messages: tuple[str, ...]
    elapsed_seconds: float

    def to_dict(self) -> dict[str, JsonValue]:
        """Return a JSON-compatible representation."""
        return {
            "schema_version": self.schema_version,
            "mode": self.mode,
            "max_errors": self.max_errors,
            "ok": self.ok,
            "messages": list(self.messages),
            "elapsed_seconds": self.elapsed_seconds,
        }


@dataclass(frozen=True, slots=True)
class BackupResult:
    """Result of a completed online SQLite backup."""

    schema_version: int
    destination: str
    database_path: str
    elapsed_seconds: float
    pages_copied: int
    page_count: int
    database_size_bytes: int
    verified: bool
    verification_mode: str
    verification_messages: tuple[str, ...]

    def to_dict(self) -> dict[str, JsonValue]:
        """Return a JSON-compatible representation."""
        return {
            "schema_version": self.schema_version,
            "destination": self.destination,
            "database_path": self.database_path,
            "elapsed_seconds": self.elapsed_seconds,
            "pages_copied": self.pages_copied,
            "page_count": self.page_count,
            "database_size_bytes": self.database_size_bytes,
            "verified": self.verified,
            "verification_mode": self.verification_mode,
            "verification_messages": list(self.verification_messages),
        }


def build_integrity_result(
    snapshot: _native.IntegrityCheckSnapshot,
) -> IntegrityCheckResult:
    """Convert the private native result into the public model."""
    mode: IntegrityCheckMode = "quick" if snapshot.mode == "quick" else "full"
    return IntegrityCheckResult(
        schema_version=snapshot.schema_version,
        mode=mode,
        max_errors=snapshot.max_errors,
        ok=snapshot.ok,
        messages=tuple(snapshot.messages),
        elapsed_seconds=snapshot.elapsed_ms / 1000.0,
    )


def build_backup_result(
    snapshot: _native.BackupSnapshot,
    *,
    destination: str,
    database_path: str,
) -> BackupResult:
    """Convert the private native result into the public model."""
    return BackupResult(
        schema_version=snapshot.schema_version,
        destination=destination,
        database_path=database_path,
        elapsed_seconds=snapshot.elapsed_ms / 1000.0,
        pages_copied=snapshot.pages_copied,
        page_count=snapshot.page_count,
        database_size_bytes=snapshot.database_size_bytes,
        verified=snapshot.verified,
        verification_mode=snapshot.verification_mode,
        verification_messages=tuple(snapshot.verification_messages),
    )
