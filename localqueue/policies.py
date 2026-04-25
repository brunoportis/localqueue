from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal, Protocol

DeliveryGuarantee = Literal["at-least-once", "at-most-once", "effectively-once"]
Locality = Literal["local", "remote"]
RoutingPattern = Literal["point-to-point", "publish-subscribe"]
ConsumptionPattern = Literal["pull", "push"]
OrderingGuarantee = Literal["fifo-ready", "priority", "best-effort"]


@dataclass(frozen=True, slots=True)
class QueueSemantics:
    """Names the queueing concepts implemented by a queue configuration."""

    locality: Locality = "local"
    delivery: DeliveryGuarantee = "at-least-once"
    routing: RoutingPattern = "point-to-point"
    consumption: ConsumptionPattern = "pull"
    ordering: OrderingGuarantee = "fifo-ready"
    leases: bool = True
    acknowledgements: bool = True
    dead_letters: bool = True
    deduplication: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


LOCAL_AT_LEAST_ONCE = QueueSemantics()


class BackpressureStrategy(Protocol):
    @property
    def maxsize(self) -> int: ...

    def is_full(self, *, qsize: int) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class BoundedBackpressure:
    """Bounds ready-message capacity with queue.Queue-compatible semantics."""

    maxsize: int = 0

    def __post_init__(self) -> None:
        if self.maxsize < 0:
            raise ValueError("maxsize cannot be negative")

    def is_full(self, *, qsize: int) -> bool:
        return self.maxsize > 0 and qsize >= self.maxsize

    def as_dict(self) -> dict[str, object]:
        return {"type": "bounded", "maxsize": self.maxsize}
