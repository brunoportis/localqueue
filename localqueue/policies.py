from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Literal, Protocol

if TYPE_CHECKING:
    from .idempotency import IdempotencyStore
    from .results import ResultStore

AckTiming = Literal["before-delivery", "after-success", "manual"]
DeliveryGuarantee = Literal["at-least-once", "at-most-once", "effectively-once"]
Locality = Literal["local", "remote"]
RoutingPattern = Literal["point-to-point", "publish-subscribe"]
ConsumptionPattern = Literal["pull", "push"]
OrderingGuarantee = Literal["fifo-ready", "priority", "best-effort"]
CommitMode = Literal["local-atomic", "transactional-outbox", "two-phase", "saga"]


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


@dataclass(frozen=True, slots=True)
class ResultPolicy:
    stores_result: bool
    returns_cached_result: bool

    def as_dict(self) -> dict[str, object]:  # pragma: no cover
        raise NotImplementedError


class CommitPolicy(Protocol):
    @property
    def mode(self) -> CommitMode: ...

    @property
    def local_commit(self) -> bool: ...

    @property
    def coordinates_effects(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class NoResultPolicy(ResultPolicy):
    """Result policy that does not persist or return handler results."""

    stores_result: bool = False
    returns_cached_result: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "none",
            "stores_result": self.stores_result,
            "returns_cached_result": self.returns_cached_result,
        }


NO_RESULT_POLICY = NoResultPolicy()


@dataclass(frozen=True, slots=True)
class ReturnStoredResult(ResultPolicy):
    """Result policy that stores successful results and returns them on duplicates."""

    stores_result: bool = True
    returns_cached_result: bool = True
    result_store: ResultStore | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "return-stored",
            "stores_result": self.stores_result,
            "returns_cached_result": self.returns_cached_result,
            "result_store": (
                None if self.result_store is None else type(self.result_store).__name__
            ),
        }


RETURN_STORED_RESULT = ReturnStoredResult()


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


@dataclass(frozen=True, slots=True)
class EffectivelyOnceDelivery:
    """Delivery policy that requires idempotent enqueue keys."""

    guarantee: DeliveryGuarantee = "effectively-once"
    ack_timing: AckTiming = "after-success"
    uses_leases: bool = True
    redelivers_expired_leases: bool = True
    requires_dedupe_key: bool = True
    idempotency_store: IdempotencyStore | None = None
    result_policy: ResultPolicy = NO_RESULT_POLICY
    commit_policy: CommitPolicy = LOCAL_ATOMIC_COMMIT

    def as_dict(self) -> dict[str, object]:
        return {
            "guarantee": self.guarantee,
            "ack_timing": self.ack_timing,
            "uses_leases": self.uses_leases,
            "redelivers_expired_leases": self.redelivers_expired_leases,
            "requires_dedupe_key": self.requires_dedupe_key,
            "idempotency_store": (
                None
                if self.idempotency_store is None
                else type(self.idempotency_store).__name__
            ),
            "result_policy": self.result_policy.as_dict(),
            "commit_policy": self.commit_policy.as_dict(),
        }


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


@dataclass(frozen=True, slots=True)
class PushConsumption:
    """Consumption policy where producers or dispatchers invoke handlers."""

    pattern: ConsumptionPattern = "push"
    consumer_requests_messages: bool = False
    producer_invokes_handler: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


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


@dataclass(frozen=True, slots=True)
class PublishSubscribeRouting:
    """Routing policy where published messages fan out to subscribers."""

    pattern: RoutingPattern = "publish-subscribe"
    single_consumer_per_message: bool = False
    fanout: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


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


@dataclass(frozen=True, slots=True)
class QueuePolicySet:
    """Groups queue policies into one reusable configuration object."""

    semantics: QueueSemantics | None = None
    delivery_policy: DeliveryPolicy | None = None
    consumption_policy: ConsumptionPolicy | None = None
    ordering_policy: OrderingPolicy | None = None
    routing_policy: RoutingPolicy | None = None
    backpressure: BackpressureStrategy | None = None

    @classmethod
    def at_least_once(
        cls,
        *,
        consumption_policy: ConsumptionPolicy | None = None,
        ordering_policy: OrderingPolicy | None = None,
        routing_policy: RoutingPolicy | None = None,
        backpressure: BackpressureStrategy | None = None,
    ) -> "QueuePolicySet":
        return cls(
            delivery_policy=AT_LEAST_ONCE_DELIVERY,
            consumption_policy=consumption_policy,
            ordering_policy=ordering_policy,
            routing_policy=routing_policy,
            backpressure=backpressure,
        )

    @classmethod
    def at_most_once(
        cls,
        *,
        consumption_policy: ConsumptionPolicy | None = None,
        ordering_policy: OrderingPolicy | None = None,
        routing_policy: RoutingPolicy | None = None,
        backpressure: BackpressureStrategy | None = None,
    ) -> "QueuePolicySet":
        return cls(
            delivery_policy=AtMostOnceDelivery(),
            consumption_policy=consumption_policy,
            ordering_policy=ordering_policy,
            routing_policy=routing_policy,
            backpressure=backpressure,
        )

    @classmethod
    def effectively_once(
        cls,
        *,
        idempotency_store: IdempotencyStore | None = None,
        result_policy: ResultPolicy = NO_RESULT_POLICY,
        commit_policy: CommitPolicy = LOCAL_ATOMIC_COMMIT,
        consumption_policy: ConsumptionPolicy | None = None,
        ordering_policy: OrderingPolicy | None = None,
        routing_policy: RoutingPolicy | None = None,
        backpressure: BackpressureStrategy | None = None,
    ) -> "QueuePolicySet":
        delivery_policy = EffectivelyOnceDelivery(
            idempotency_store=idempotency_store,
            result_policy=result_policy,
            commit_policy=commit_policy,
        )
        return cls(
            delivery_policy=delivery_policy,
            consumption_policy=consumption_policy,
            ordering_policy=ordering_policy,
            routing_policy=routing_policy,
            backpressure=backpressure,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "semantics": None if self.semantics is None else self.semantics.as_dict(),
            "delivery_policy": (
                None if self.delivery_policy is None else self.delivery_policy.as_dict()
            ),
            "consumption_policy": (
                None
                if self.consumption_policy is None
                else self.consumption_policy.as_dict()
            ),
            "ordering_policy": (
                None if self.ordering_policy is None else self.ordering_policy.as_dict()
            ),
            "routing_policy": (
                None if self.routing_policy is None else self.routing_policy.as_dict()
            ),
            "backpressure": (
                None if self.backpressure is None else self.backpressure.as_dict()
            ),
        }
