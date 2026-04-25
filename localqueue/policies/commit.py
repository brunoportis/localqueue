from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Protocol


if TYPE_CHECKING:
    from ._types import CommitMode
    from ..results import ResultStore


class CommitPolicy(Protocol):
    @property
    def mode(self) -> CommitMode: ...

    @property
    def local_commit(self) -> bool: ...

    @property
    def coordinates_effects(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class LocalAtomicCommit:
    """Commit policy where result writes and queue acknowledgement stay local."""

    mode: CommitMode = "local-atomic"
    local_commit: bool = True
    coordinates_effects: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


LOCAL_ATOMIC_COMMIT = LocalAtomicCommit()


@dataclass(frozen=True, slots=True)
class TransactionalOutboxCommit:
    """Commit policy that models the transactional-outbox pattern."""

    mode: CommitMode = "transactional-outbox"
    local_commit: bool = True
    coordinates_effects: bool = True
    outbox_store: ResultStore | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "local_commit": self.local_commit,
            "coordinates_effects": self.coordinates_effects,
            "outbox_store": (
                None if self.outbox_store is None else type(self.outbox_store).__name__
            ),
        }


TRANSACTIONAL_OUTBOX_COMMIT = TransactionalOutboxCommit()


@dataclass(frozen=True, slots=True)
class TwoPhaseCommit:
    """Commit policy that models prepare/commit coordination."""

    mode: CommitMode = "two-phase"
    local_commit: bool = False
    coordinates_effects: bool = True
    prepare_store: ResultStore | None = None
    commit_store: ResultStore | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "local_commit": self.local_commit,
            "coordinates_effects": self.coordinates_effects,
            "prepare_store": (
                None
                if self.prepare_store is None
                else type(self.prepare_store).__name__
            ),
            "commit_store": (
                None if self.commit_store is None else type(self.commit_store).__name__
            ),
        }


TWO_PHASE_COMMIT = TwoPhaseCommit()


@dataclass(frozen=True, slots=True)
class SagaCommit:
    """Commit policy that models compensating actions across steps."""

    mode: CommitMode = "saga"
    local_commit: bool = False
    coordinates_effects: bool = True
    saga_store: ResultStore | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "local_commit": self.local_commit,
            "coordinates_effects": self.coordinates_effects,
            "saga_store": (
                None if self.saga_store is None else type(self.saga_store).__name__
            ),
        }
