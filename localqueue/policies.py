from __future__ import annotations

from dataclasses import asdict, dataclass
from collections.abc import Callable
from typing import TYPE_CHECKING, Literal, Protocol

if TYPE_CHECKING:
    from .idempotency import IdempotencyStore
    from .results import ResultStore
    from .stores import QueueMessage

AckTiming = Literal["before-delivery", "after-success", "manual"]
DeliveryGuarantee = Literal["at-least-once", "at-most-once", "effectively-once"]
Locality = Literal["local", "remote"]
RoutingPattern = Literal["point-to-point", "publish-subscribe"]
ConsumptionPattern = Literal["pull", "push"]
OrderingGuarantee = Literal["fifo-ready", "priority", "best-effort"]
CommitMode = Literal["local-atomic", "transactional-outbox", "two-phase", "saga"]
BackpressureOverflow = Literal["block", "reject"]
MessageHandler = Callable[["QueueMessage"], object]


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


class LocalityPolicy(Protocol):
    @property
    def locality(self) -> Locality: ...

    @property
    def co_located_state(self) -> bool: ...

    @property
    def crosses_network_boundary(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class LocalQueuePlacement:
    """Locality policy where queue state is local to the process host."""

    locality: Locality = "local"
    co_located_state: bool = True
    crosses_network_boundary: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


LOCAL_QUEUE_PLACEMENT = LocalQueuePlacement()


@dataclass(frozen=True, slots=True)
class RemoteQueuePlacement:
    """Locality policy where queue state lives behind a remote boundary."""

    locality: Locality = "remote"
    co_located_state: bool = False
    crosses_network_boundary: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


class LeasePolicy(Protocol):
    @property
    def timeout(self) -> float: ...

    @property
    def uses_leases(self) -> bool: ...

    @property
    def expires_inflight(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class FixedLeaseTimeout:
    """Lease policy where inflight messages expire after a fixed timeout."""

    timeout: float = 30.0
    uses_leases: bool = True
    expires_inflight: bool = True

    def __post_init__(self) -> None:
        if self.timeout <= 0:
            raise ValueError("lease timeout must be greater than zero")

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "fixed-timeout",
            "timeout": self.timeout,
            "uses_leases": self.uses_leases,
            "expires_inflight": self.expires_inflight,
        }


FIXED_LEASE_TIMEOUT = FixedLeaseTimeout()


class AcknowledgementPolicy(Protocol):
    @property
    def acknowledgements(self) -> bool: ...

    @property
    def explicit_ack(self) -> bool: ...

    @property
    def removes_on_ack(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class ExplicitAcknowledgement:
    """Acknowledgement policy where consumers explicitly remove completed work."""

    acknowledgements: bool = True
    explicit_ack: bool = True
    removes_on_ack: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


EXPLICIT_ACKNOWLEDGEMENT = ExplicitAcknowledgement()


class DeadLetterPolicy(Protocol):
    @property
    def dead_letters(self) -> bool: ...

    @property
    def stores_failures(self) -> bool: ...

    @property
    def supports_requeue(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class DeadLetterQueue:
    """Dead-letter policy where failed messages stay inspectable and requeueable."""

    dead_letters: bool = True
    stores_failures: bool = True
    supports_requeue: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


DEAD_LETTER_QUEUE = DeadLetterQueue()


class DeduplicationPolicy(Protocol):
    @property
    def deduplication(self) -> bool: ...

    @property
    def accepts_dedupe_key(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class DedupeKeySupport:
    """Deduplication policy where messages may carry an optional stable key."""

    deduplication: bool = True
    accepts_dedupe_key: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


DEDUPE_KEY_SUPPORT = DedupeKeySupport()


@dataclass(frozen=True, slots=True)
class NoDeduplication:
    """Deduplication policy where stable dedupe keys are not accepted."""

    deduplication: bool = False
    accepts_dedupe_key: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


NO_DEDUPLICATION = NoDeduplication()


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


class DispatchPolicy(Protocol):
    @property
    def dispatches(self) -> bool: ...

    @property
    def dispatches_on_put(self) -> bool: ...

    @property
    def handler_count(self) -> int: ...

    def dispatch(self, message: QueueMessage) -> None: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class NoDispatcher:
    """Dispatch policy where producers only enqueue messages."""

    dispatches: bool = False
    dispatches_on_put: bool = False
    handler_count: int = 0

    def dispatch(self, message: QueueMessage) -> None:
        _ = message

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


NO_DISPATCHER = NoDispatcher()


@dataclass(frozen=True, slots=True)
class CallbackDispatcher:
    """Dispatch policy that calls in-process handlers after enqueue."""

    handlers: tuple[MessageHandler, ...]
    dispatches: bool = True
    dispatches_on_put: bool = True

    def __post_init__(self) -> None:
        handlers = tuple(self.handlers)
        if not handlers:
            raise ValueError("handlers cannot be empty")
        object.__setattr__(self, "handlers", handlers)

    @property
    def handler_count(self) -> int:
        return len(self.handlers)

    def dispatch(self, message: QueueMessage) -> None:
        for handler in self.handlers:
            _ = handler(message)

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "callback",
            "dispatches": self.dispatches,
            "dispatches_on_put": self.dispatches_on_put,
            "handler_count": self.handler_count,
        }


class NotificationPolicy(Protocol):
    @property
    def notifies(self) -> bool: ...

    @property
    def notifies_on_put(self) -> bool: ...

    @property
    def listener_count(self) -> int: ...

    def notify(self, message: QueueMessage) -> None: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class NoNotification:
    """Notification policy where producers do not wake any listeners."""

    notifies: bool = False
    notifies_on_put: bool = False
    listener_count: int = 0

    def notify(self, message: QueueMessage) -> None:
        _ = message

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


NO_NOTIFICATION = NoNotification()


@dataclass(frozen=True, slots=True)
class CallbackNotification:
    """Notification policy that calls in-process listeners after enqueue."""

    listeners: tuple[MessageHandler, ...]
    notifies: bool = True
    notifies_on_put: bool = True

    def __post_init__(self) -> None:
        listeners = tuple(self.listeners)
        if not listeners:
            raise ValueError("listeners cannot be empty")
        object.__setattr__(self, "listeners", listeners)

    @property
    def listener_count(self) -> int:
        return len(self.listeners)

    def notify(self, message: QueueMessage) -> None:
        for listener in self.listeners:
            _ = listener(message)

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "callback",
            "notifies": self.notifies,
            "notifies_on_put": self.notifies_on_put,
            "listener_count": self.listener_count,
        }



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


class SubscriptionPolicy(Protocol):
    @property
    def subscriptions(self) -> bool: ...

    @property
    def fanout(self) -> bool: ...

    @property
    def subscriber_count(self) -> int: ...

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


@dataclass(frozen=True, slots=True)
class NoSubscriptions:
    """Subscription policy where messages are not fanned out to subscribers."""

    subscriptions: bool = False
    fanout: bool = False
    subscriber_count: int = 0

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


NO_SUBSCRIPTIONS = NoSubscriptions()


@dataclass(frozen=True, slots=True)
class StaticFanoutSubscriptions:
    """Subscription policy with a fixed set of subscriber names."""

    subscribers: tuple[str, ...]

    def __post_init__(self) -> None:
        subscribers = tuple(self.subscribers)
        if not subscribers:
            raise ValueError("subscribers cannot be empty")
        if any(not subscriber for subscriber in subscribers):
            raise ValueError("subscriber names cannot be empty")
        if len(set(subscribers)) != len(subscribers):
            raise ValueError("subscriber names must be unique")
        object.__setattr__(self, "subscribers", subscribers)

    @property
    def subscriptions(self) -> bool:
        return True

    @property
    def fanout(self) -> bool:
        return True

    @property
    def subscriber_count(self) -> int:
        return len(self.subscribers)

    def as_dict(self) -> dict[str, object]:
        return {
            "subscriptions": self.subscriptions,
            "fanout": self.fanout,
            "subscriber_count": self.subscriber_count,
            "subscribers": list(self.subscribers),
        }


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

    @property
    def overflow(self) -> BackpressureOverflow: ...

    def is_full(self, *, qsize: int) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class BoundedBackpressure:
    """Bounds ready-message capacity with queue.Queue-compatible semantics."""

    maxsize: int = 0
    overflow: BackpressureOverflow = "block"

    def __post_init__(self) -> None:
        if self.maxsize < 0:
            raise ValueError("maxsize cannot be negative")
        if self.overflow not in ("block", "reject"):
            raise ValueError("backpressure overflow must be 'block' or 'reject'")

    def is_full(self, *, qsize: int) -> bool:
        return self.maxsize > 0 and qsize >= self.maxsize

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "bounded",
            "maxsize": self.maxsize,
            "overflow": self.overflow,
        }


@dataclass(frozen=True, slots=True)
class RejectingBackpressure:
    """Bounds ready-message capacity and rejects producers immediately."""

    maxsize: int = 0
    overflow: BackpressureOverflow = "reject"

    def __post_init__(self) -> None:
        if self.maxsize < 0:
            raise ValueError("maxsize cannot be negative")
        if self.overflow != "reject":
            raise ValueError("rejecting backpressure overflow must be 'reject'")

    def is_full(self, *, qsize: int) -> bool:
        return self.maxsize > 0 and qsize >= self.maxsize

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "rejecting",
            "maxsize": self.maxsize,
            "overflow": self.overflow,
        }


@dataclass(frozen=True, slots=True)
class QueuePolicySet:
    """Groups queue policies into one reusable configuration object."""

    semantics: QueueSemantics | None = None
    locality_policy: LocalityPolicy | None = None
    lease_policy: LeasePolicy | None = None
    acknowledgement_policy: AcknowledgementPolicy | None = None
    dead_letter_policy: DeadLetterPolicy | None = None
    deduplication_policy: DeduplicationPolicy | None = None
    delivery_policy: DeliveryPolicy | None = None
    consumption_policy: ConsumptionPolicy | None = None
    dispatch_policy: DispatchPolicy | None = None
    notification_policy: NotificationPolicy | None = None
    ordering_policy: OrderingPolicy | None = None
    routing_policy: RoutingPolicy | None = None
    subscription_policy: SubscriptionPolicy | None = None
    backpressure: BackpressureStrategy | None = None

    @classmethod
    def at_least_once(
        cls,
        *,
        locality_policy: LocalityPolicy | None = None,
        lease_policy: LeasePolicy | None = None,
        acknowledgement_policy: AcknowledgementPolicy | None = None,
        dead_letter_policy: DeadLetterPolicy | None = None,
        deduplication_policy: DeduplicationPolicy | None = None,
        consumption_policy: ConsumptionPolicy | None = None,
        dispatch_policy: DispatchPolicy | None = None,
        notification_policy: NotificationPolicy | None = None,
        ordering_policy: OrderingPolicy | None = None,
        routing_policy: RoutingPolicy | None = None,
        subscription_policy: SubscriptionPolicy | None = None,
        backpressure: BackpressureStrategy | None = None,
    ) -> "QueuePolicySet":
        return cls(
            locality_policy=locality_policy,
            lease_policy=lease_policy,
            acknowledgement_policy=acknowledgement_policy,
            dead_letter_policy=dead_letter_policy,
            deduplication_policy=deduplication_policy,
            delivery_policy=AT_LEAST_ONCE_DELIVERY,
            consumption_policy=consumption_policy,
            dispatch_policy=dispatch_policy,
            notification_policy=notification_policy,
            ordering_policy=ordering_policy,
            routing_policy=routing_policy,
            subscription_policy=subscription_policy,
            backpressure=backpressure,
        )

    @classmethod
    def at_most_once(
        cls,
        *,
        locality_policy: LocalityPolicy | None = None,
        lease_policy: LeasePolicy | None = None,
        acknowledgement_policy: AcknowledgementPolicy | None = None,
        dead_letter_policy: DeadLetterPolicy | None = None,
        deduplication_policy: DeduplicationPolicy | None = None,
        consumption_policy: ConsumptionPolicy | None = None,
        dispatch_policy: DispatchPolicy | None = None,
        notification_policy: NotificationPolicy | None = None,
        ordering_policy: OrderingPolicy | None = None,
        routing_policy: RoutingPolicy | None = None,
        subscription_policy: SubscriptionPolicy | None = None,
        backpressure: BackpressureStrategy | None = None,
    ) -> "QueuePolicySet":
        return cls(
            locality_policy=locality_policy,
            lease_policy=lease_policy,
            acknowledgement_policy=acknowledgement_policy,
            dead_letter_policy=dead_letter_policy,
            deduplication_policy=deduplication_policy,
            delivery_policy=AtMostOnceDelivery(),
            consumption_policy=consumption_policy,
            dispatch_policy=dispatch_policy,
            notification_policy=notification_policy,
            ordering_policy=ordering_policy,
            routing_policy=routing_policy,
            subscription_policy=subscription_policy,
            backpressure=backpressure,
        )

    @classmethod
    def effectively_once(
        cls,
        *,
        idempotency_store: IdempotencyStore | None = None,
        result_policy: ResultPolicy = NO_RESULT_POLICY,
        commit_policy: CommitPolicy = LOCAL_ATOMIC_COMMIT,
        locality_policy: LocalityPolicy | None = None,
        lease_policy: LeasePolicy | None = None,
        acknowledgement_policy: AcknowledgementPolicy | None = None,
        dead_letter_policy: DeadLetterPolicy | None = None,
        deduplication_policy: DeduplicationPolicy | None = None,
        consumption_policy: ConsumptionPolicy | None = None,
        dispatch_policy: DispatchPolicy | None = None,
        notification_policy: NotificationPolicy | None = None,
        ordering_policy: OrderingPolicy | None = None,
        routing_policy: RoutingPolicy | None = None,
        subscription_policy: SubscriptionPolicy | None = None,
        backpressure: BackpressureStrategy | None = None,
    ) -> "QueuePolicySet":
        delivery_policy = EffectivelyOnceDelivery(
            idempotency_store=idempotency_store,
            result_policy=result_policy,
            commit_policy=commit_policy,
        )
        return cls(
            locality_policy=locality_policy,
            lease_policy=lease_policy,
            acknowledgement_policy=acknowledgement_policy,
            dead_letter_policy=dead_letter_policy,
            deduplication_policy=deduplication_policy,
            delivery_policy=delivery_policy,
            consumption_policy=consumption_policy,
            dispatch_policy=dispatch_policy,
            notification_policy=notification_policy,
            ordering_policy=ordering_policy,
            routing_policy=routing_policy,
            subscription_policy=subscription_policy,
            backpressure=backpressure,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "semantics": None if self.semantics is None else self.semantics.as_dict(),
            "locality_policy": (
                None if self.locality_policy is None else self.locality_policy.as_dict()
            ),
            "lease_policy": (
                None if self.lease_policy is None else self.lease_policy.as_dict()
            ),
            "acknowledgement_policy": (
                None
                if self.acknowledgement_policy is None
                else self.acknowledgement_policy.as_dict()
            ),
            "dead_letter_policy": (
                None
                if self.dead_letter_policy is None
                else self.dead_letter_policy.as_dict()
            ),
            "deduplication_policy": (
                None
                if self.deduplication_policy is None
                else self.deduplication_policy.as_dict()
            ),
            "delivery_policy": (
                None if self.delivery_policy is None else self.delivery_policy.as_dict()
            ),
            "consumption_policy": (
                None
                if self.consumption_policy is None
                else self.consumption_policy.as_dict()
            ),
            "dispatch_policy": (
                None if self.dispatch_policy is None else self.dispatch_policy.as_dict()
            ),
            "notification_policy": (
                None
                if self.notification_policy is None
                else self.notification_policy.as_dict()
            ),
            "ordering_policy": (
                None if self.ordering_policy is None else self.ordering_policy.as_dict()
            ),
            "routing_policy": (
                None if self.routing_policy is None else self.routing_policy.as_dict()
            ),
            "subscription_policy": (
                None
                if self.subscription_policy is None
                else self.subscription_policy.as_dict()
            ),
            "backpressure": (
                None if self.backpressure is None else self.backpressure.as_dict()
            ),
        }
