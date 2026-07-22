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

    ok: bool
    messages: tuple[str, ...]
    elapsed_seconds: float
    check_mode: IntegrityCheckMode

    def to_dict(self) -> dict[str, JsonValue]:
        """Return a JSON-compatible representation."""
        return {
            "ok": self.ok,
            "messages": list(self.messages),
            "elapsed_seconds": self.elapsed_seconds,
            "check_mode": self.check_mode,
        }


@dataclass(frozen=True, slots=True)
class BackupResult:
    """Result of a completed online SQLite backup."""

    destination: str
    overwritten: bool
    elapsed_seconds: float
    database_size_bytes: int

    def to_dict(self) -> dict[str, JsonValue]:
        """Return a JSON-compatible representation."""
        return {
            "destination": self.destination,
            "overwritten": self.overwritten,
            "elapsed_seconds": self.elapsed_seconds,
            "database_size_bytes": self.database_size_bytes,
        }


def build_integrity_result(
    snapshot: _native.IntegrityCheckSnapshot,
) -> IntegrityCheckResult:
    """Convert the private native result into the public model."""
    mode: IntegrityCheckMode = "quick" if snapshot.check_mode == "quick" else "full"
    return IntegrityCheckResult(
        ok=snapshot.ok,
        messages=tuple(snapshot.messages),
        elapsed_seconds=snapshot.elapsed_ms / 1000.0,
        check_mode=mode,
    )


def build_backup_result(
    snapshot: _native.BackupSnapshot,
    *,
    destination: str,
) -> BackupResult:
    """Convert the private native result into the public model."""
    return BackupResult(
        destination=destination,
        overwritten=snapshot.overwritten,
        elapsed_seconds=snapshot.elapsed_ms / 1000.0,
        database_size_bytes=snapshot.database_size_bytes,
    )
