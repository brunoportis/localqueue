from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol, TYPE_CHECKING


if TYPE_CHECKING:
    from ._types import OrderingGuarantee


class OrderingPolicy(Protocol):
    @property
    def guarantee(self) -> OrderingGuarantee: ...

    @property
    def ready_before_delayed(self) -> bool: ...

    @property
    def stable_for_same_timestamp(self) -> bool: ...

    @property
    def priority_before_sequence(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class FifoReadyOrdering:
    """Ordering policy for ready messages using availability then enqueue order."""

    guarantee: OrderingGuarantee = "fifo-ready"
    ready_before_delayed: bool = True
    stable_for_same_timestamp: bool = True
    priority_before_sequence: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


FIFO_READY_ORDERING = FifoReadyOrdering()


@dataclass(frozen=True, slots=True)
class PriorityOrdering:
    """Ordering policy where higher-priority ready messages are delivered first."""

    guarantee: OrderingGuarantee = "priority"
    ready_before_delayed: bool = True
    stable_for_same_timestamp: bool = True
    priority_before_sequence: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class BestEffortOrdering:
    """Ordering policy that does not promise stable delivery order."""

    guarantee: OrderingGuarantee = "best-effort"
    ready_before_delayed: bool = False
    stable_for_same_timestamp: bool = False
    priority_before_sequence: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)
