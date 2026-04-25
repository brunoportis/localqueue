from __future__ import annotations

from dataclasses import asdict, dataclass

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._types import (
        ConsumptionPattern,
        DeliveryGuarantee,
        Locality,
        OrderingGuarantee,
        RoutingPattern,
    )


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
    subscriptions: bool = False
    notifications: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


LOCAL_AT_LEAST_ONCE = QueueSemantics()
