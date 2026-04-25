from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal, Protocol

AckTiming = Literal["before-delivery", "after-success", "manual"]
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


class DeliveryPolicy(Protocol):
    @property
    def guarantee(self) -> DeliveryGuarantee: ...

    @property
    def ack_timing(self) -> AckTiming: ...

    @property
    def uses_leases(self) -> bool: ...

    @property
    def redelivers_expired_leases(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class AtLeastOnceDelivery:
    """Delivery policy where messages are acknowledged after successful handling."""

    guarantee: DeliveryGuarantee = "at-least-once"
    ack_timing: AckTiming = "after-success"
    uses_leases: bool = True
    redelivers_expired_leases: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


AT_LEAST_ONCE_DELIVERY = AtLeastOnceDelivery()


@dataclass(frozen=True, slots=True)
class AtMostOnceDelivery:
    """Delivery policy where messages are removed before user handling."""

    guarantee: DeliveryGuarantee = "at-most-once"
    ack_timing: AckTiming = "before-delivery"
    uses_leases: bool = False
    redelivers_expired_leases: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


class ConsumptionPolicy(Protocol):
    @property
    def pattern(self) -> ConsumptionPattern: ...

    @property
    def consumer_requests_messages(self) -> bool: ...

    @property
    def producer_invokes_handler(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class PullConsumption:
    """Consumption policy where workers request messages from the queue."""

    pattern: ConsumptionPattern = "pull"
    consumer_requests_messages: bool = True
    producer_invokes_handler: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


PULL_CONSUMPTION = PullConsumption()


class RoutingPolicy(Protocol):
    @property
    def pattern(self) -> RoutingPattern: ...

    @property
    def single_consumer_per_message(self) -> bool: ...

    @property
    def fanout(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class PointToPointRouting:
    """Routing policy where each message is leased to one consumer at a time."""

    pattern: RoutingPattern = "point-to-point"
    single_consumer_per_message: bool = True
    fanout: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


POINT_TO_POINT_ROUTING = PointToPointRouting()


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
