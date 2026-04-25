from __future__ import annotations

import time
from pathlib import Path
from collections.abc import Mapping
from queue import Empty, Full
from threading import Condition
from typing import Any, Generic, TypeVar, cast

from .paths import default_queue_store_path
from .policies import (
    AT_LEAST_ONCE_DELIVERY,
    DEAD_LETTER_QUEUE,
    EXPLICIT_ACKNOWLEDGEMENT,
    DEDUPE_KEY_SUPPORT,
    FIFO_READY_ORDERING,
    FIXED_LEASE_TIMEOUT,
    NO_DISPATCHER,
    NO_NOTIFICATION,
    NO_SUBSCRIPTIONS,
    POINT_TO_POINT_ROUTING,
    PULL_CONSUMPTION,
    AcknowledgementPolicy,
    ConsumptionPolicy,
    DeadLetterPolicy,
    DeliveryPolicy,
    DispatchPolicy,
    FixedLeaseTimeout,
    DeduplicationPolicy,
    LOCAL_QUEUE_PLACEMENT,
    LeasePolicy,
    LocalityPolicy,
    NotificationPolicy,
    OrderingPolicy,
    RoutingPolicy,
    SubscriptionPolicy,
    BackpressureStrategy,
    BoundedBackpressure,
    QueuePolicySet,
    QueueSemantics,
)
from .stores import QueueMessage, QueueStats, QueueStore, SQLiteQueueStore

T = TypeVar("T")
PolicyValue = TypeVar("PolicyValue")
_NEGATIVE_DELAY_ERROR = "delay cannot be negative"


class PersistentQueue(Generic[T]):
    name: str
    lease_timeout: float
    lease_policy: LeasePolicy
    acknowledgement_policy: AcknowledgementPolicy
    dead_letter_policy: DeadLetterPolicy
    deduplication_policy: DeduplicationPolicy
    maxsize: int
    semantics: QueueSemantics
    locality_policy: LocalityPolicy
    consumption_policy: ConsumptionPolicy
    delivery_policy: DeliveryPolicy
    dispatch_policy: DispatchPolicy
    notification_policy: NotificationPolicy
    ordering_policy: OrderingPolicy
    routing_policy: RoutingPolicy
    subscription_policy: SubscriptionPolicy
    backpressure: BackpressureStrategy
    _store: QueueStore | None
    _store_path: Path | None
    retry_defaults: dict[str, Any]
    _condition: Condition
    _unfinished: dict[str, QueueMessage]

    def __init__(
        self,
        name: str,
        *,
        store: QueueStore | None = None,
        store_path: str | Path | None = None,
        lease_timeout: float = 30.0,
        lease_policy: LeasePolicy | None = None,
        acknowledgement_policy: AcknowledgementPolicy | None = None,
        dead_letter_policy: DeadLetterPolicy | None = None,
        deduplication_policy: DeduplicationPolicy | None = None,
        maxsize: int = 0,
        retry_defaults: Mapping[str, Any] | None = None,
        semantics: QueueSemantics | None = None,
        locality_policy: LocalityPolicy | None = None,
        consumption_policy: ConsumptionPolicy | None = None,
        delivery_policy: DeliveryPolicy | None = None,
        dispatch_policy: DispatchPolicy | None = None,
        notification_policy: NotificationPolicy | None = None,
        ordering_policy: OrderingPolicy | None = None,
        routing_policy: RoutingPolicy | None = None,
        subscription_policy: SubscriptionPolicy | None = None,
        backpressure: BackpressureStrategy | None = None,
        policy_set: QueuePolicySet | None = None,
    ) -> None:
        if store is not None and store_path is not None:
            raise ValueError("pass either store= or store_path=, not both")
        (
            semantics,
            locality_policy,
            lease_policy,
            acknowledgement_policy,
            dead_letter_policy,
            deduplication_policy,
            consumption_policy,
            delivery_policy,
            dispatch_policy,
            notification_policy,
            ordering_policy,
            routing_policy,
            subscription_policy,
            backpressure,
        ) = _apply_policy_set(
            policy_set=policy_set,
            semantics=semantics,
            locality_policy=locality_policy,
            lease_policy=lease_policy,
            acknowledgement_policy=acknowledgement_policy,
            dead_letter_policy=dead_letter_policy,
            deduplication_policy=deduplication_policy,
            consumption_policy=consumption_policy,
            delivery_policy=delivery_policy,
            dispatch_policy=dispatch_policy,
            notification_policy=notification_policy,
            ordering_policy=ordering_policy,
            routing_policy=routing_policy,
            subscription_policy=subscription_policy,
            backpressure=backpressure,
            maxsize=maxsize,
        )
        if backpressure is not None and maxsize != 0:
            raise ValueError("pass either maxsize= or backpressure=, not both")
        if lease_policy is not None and lease_timeout != FIXED_LEASE_TIMEOUT.timeout:
            raise ValueError("pass either lease_timeout= or lease_policy=, not both")
        if lease_timeout <= 0:
            raise ValueError("lease_timeout must be greater than zero")
        if maxsize < 0:
            raise ValueError("maxsize cannot be negative")
        if retry_defaults is not None and not isinstance(retry_defaults, Mapping):
            raise TypeError("retry_defaults must be a mapping")
        _validate_retry_defaults(retry_defaults)
        resolved_delivery = (
            AT_LEAST_ONCE_DELIVERY if delivery_policy is None else delivery_policy
        )
        resolved_locality = (
            LOCAL_QUEUE_PLACEMENT if locality_policy is None else locality_policy
        )
        resolved_lease = (
            FixedLeaseTimeout(lease_timeout) if lease_policy is None else lease_policy
        )
        resolved_acknowledgement = (
            EXPLICIT_ACKNOWLEDGEMENT
            if acknowledgement_policy is None
            else acknowledgement_policy
        )
        resolved_dead_letter = (
            DEAD_LETTER_QUEUE if dead_letter_policy is None else dead_letter_policy
        )
        resolved_deduplication = (
            DEDUPE_KEY_SUPPORT if deduplication_policy is None else deduplication_policy
        )
        resolved_consumption = (
            PULL_CONSUMPTION if consumption_policy is None else consumption_policy
        )
        resolved_dispatch = (
            NO_DISPATCHER if dispatch_policy is None else dispatch_policy
        )
        resolved_notification = (
            NO_NOTIFICATION if notification_policy is None else notification_policy
        )
        resolved_ordering = (
            FIFO_READY_ORDERING if ordering_policy is None else ordering_policy
        )
        resolved_routing = (
            POINT_TO_POINT_ROUTING if routing_policy is None else routing_policy
        )
        resolved_subscription = (
            NO_SUBSCRIPTIONS if subscription_policy is None else subscription_policy
        )
        resolved_semantics = (
            semantics
            if semantics is not None
            else QueueSemantics(
                locality=resolved_locality.locality,
                delivery=resolved_delivery.guarantee,
                consumption=resolved_consumption.pattern,
                ordering=resolved_ordering.guarantee,
                routing=resolved_routing.pattern,
                leases=resolved_lease.uses_leases,
                acknowledgements=resolved_acknowledgement.acknowledgements,
                dead_letters=resolved_dead_letter.dead_letters,
                deduplication=resolved_deduplication.deduplication,
                subscriptions=resolved_subscription.subscriptions,
                notifications=resolved_notification.notifies,
            )
        )
        _validate_semantics(
            resolved_semantics,
            locality_policy=resolved_locality,
            lease_policy=resolved_lease,
            acknowledgement_policy=resolved_acknowledgement,
            dead_letter_policy=resolved_dead_letter,
            deduplication_policy=resolved_deduplication,
            delivery_policy=resolved_delivery,
            consumption_policy=resolved_consumption,
            ordering_policy=resolved_ordering,
            routing_policy=resolved_routing,
            subscription_policy=resolved_subscription,
            notification_policy=resolved_notification,
        )

        self.name = name
        self.lease_policy = resolved_lease
        self.acknowledgement_policy = resolved_acknowledgement
        self.dead_letter_policy = resolved_dead_letter
        self.deduplication_policy = resolved_deduplication
        self.lease_timeout = resolved_lease.timeout
        self.backpressure = (
            BoundedBackpressure(maxsize) if backpressure is None else backpressure
        )
        self.maxsize = self.backpressure.maxsize
        self.semantics = resolved_semantics
        self.locality_policy = resolved_locality
        self.consumption_policy = resolved_consumption
        self.delivery_policy = resolved_delivery
        self.dispatch_policy = resolved_dispatch
        self.notification_policy = resolved_notification
        self.ordering_policy = resolved_ordering
        self.routing_policy = resolved_routing
        self.subscription_policy = resolved_subscription
        self._store = store
        self._store_path = Path(store_path) if store_path is not None else None
        self.retry_defaults = dict(retry_defaults or {})
        self._condition = Condition()
        self._unfinished = {}

    def put(
        self,
        item: T,
        block: bool = True,
        timeout: float | None = None,
        *,
        delay: float = 0.0,
        dedupe_key: str | None = None,
        priority: int = 0,
    ) -> QueueMessage:
        if delay < 0:
            raise ValueError(_NEGATIVE_DELAY_ERROR)
        if dedupe_key is not None and not dedupe_key:
            raise ValueError("dedupe_key cannot be empty")
        if dedupe_key is not None and not self.deduplication_policy.accepts_dedupe_key:
            raise ValueError(
                "dedupe_key is not supported by the active deduplication_policy"
            )
        if _requires_dedupe_key(self.delivery_policy) and dedupe_key is None:
            raise ValueError("dedupe_key is required by the active delivery_policy")
        _validate_priority(priority)
        if priority > 0 and self.ordering_policy.guarantee != "priority":
            raise ValueError("priority requires PriorityOrdering")
        deadline = _deadline(timeout)
        with self._condition:
            while self.full():
                if not block or self.backpressure.overflow == "reject":
                    raise Full
                remaining = _remaining(deadline)
                if remaining is not None and remaining <= 0:
                    raise Full
                _ = self._condition.wait(remaining)
            message = self._get_store().enqueue(
                self.name,
                item,
                available_at=time.time() + delay,
                dedupe_key=dedupe_key,
                priority=priority,
            )
            self._condition.notify_all()
        self._run_post_put_hooks(message)
        return message

    def put_nowait(self, item: T) -> QueueMessage:
        return self.put(item, block=False)

    def get(
        self,
        block: bool = True,
        timeout: float | None = None,
        *,
        leased_by: str | None = None,
    ) -> T:
        message = self.get_message(block=block, timeout=timeout, leased_by=leased_by)
        if self.delivery_policy.ack_timing == "before-delivery":
            return cast("T", message.value)
        with self._condition:
            self._unfinished[message.id] = message
        return cast("T", message.value)

    def get_nowait(self) -> T:
        return self.get(block=False)

    def get_message(
        self,
        block: bool = True,
        timeout: float | None = None,
        *,
        leased_by: str | None = None,
    ) -> QueueMessage:
        deadline = _deadline(timeout)
        while True:
            with self._condition:
                message = self._get_store().dequeue(
                    self.name,
                    lease_timeout=self.lease_timeout,
                    now=time.time(),
                    leased_by=leased_by,
                )
                if message is not None:
                    if self.delivery_policy.ack_timing == "before-delivery":
                        _ = self._get_store().ack(self.name, message.id)
                    self._condition.notify_all()
                    return message
                if not block:
                    raise Empty
                remaining = _remaining(deadline)
                if remaining is not None and remaining <= 0:
                    raise Empty
                _ = self._condition.wait(_wait_time(remaining))

    def inspect(self, message_id: str) -> QueueMessage | None:
        with self._condition:
            return self._get_store().get(self.name, message_id)

    def ack(self, message: QueueMessage) -> bool:
        with self._condition:
            removed = self._get_store().ack(self.name, message.id)
            self._remove_unfinished(message.id)
            self._condition.notify_all()
            return removed

    def release(
        self,
        message: QueueMessage,
        *,
        delay: float = 0.0,
        error: BaseException | str | None = None,
    ) -> bool:
        if delay < 0:
            raise ValueError(_NEGATIVE_DELAY_ERROR)
        failed_at = time.time() if error is not None else None
        with self._condition:
            released = self._get_store().release(
                self.name,
                message.id,
                available_at=time.time() + delay,
                last_error=_error_payload(error),
                failed_at=failed_at,
            )
            self._remove_unfinished(message.id)
            self._condition.notify_all()
            return released

    def dead_letter(
        self, message: QueueMessage, *, error: BaseException | str | None = None
    ) -> bool:
        failed_at = time.time() if error is not None else None
        with self._condition:
            moved = self._get_store().dead_letter(
                self.name,
                message.id,
                last_error=_error_payload(error),
                failed_at=failed_at,
            )
            self._remove_unfinished(message.id)
            self._condition.notify_all()
            return moved

    def task_done(self) -> None:
        with self._condition:
            if not self._unfinished:
                raise ValueError("task_done() called too many times")
            message_id = next(iter(self._unfinished))
            message = self._unfinished.pop(message_id)
            self._get_store().ack(self.name, message.id)
            self._condition.notify_all()

    def join(self) -> None:
        with self._condition:
            while self._unfinished:
                _ = self._condition.wait()

    def qsize(self) -> int:
        return self._get_store().qsize(self.name, now=time.time())

    def stats(self) -> QueueStats:
        return self._get_store().stats(self.name, now=time.time())

    def record_worker_heartbeat(self, worker_id: str) -> None:
        if not worker_id:
            raise ValueError("worker_id cannot be empty")
        self._get_store().record_worker_heartbeat(self.name, worker_id, now=time.time())

    def dead_letters(self, *, limit: int | None = None) -> list[QueueMessage]:
        return self._get_store().dead_letters(self.name, limit=limit)

    def requeue_dead(self, message: QueueMessage, *, delay: float = 0.0) -> bool:
        if delay < 0:
            raise ValueError(_NEGATIVE_DELAY_ERROR)
        with self._condition:
            requeued = self._get_store().requeue_dead(
                self.name,
                message.id,
                available_at=time.time() + delay,
            )
            self._condition.notify_all()
            return requeued

    def prune_dead_letters(self, *, older_than: float) -> int:
        if older_than < 0:
            raise ValueError("older_than cannot be negative")
        with self._condition:
            removed = self._get_store().prune_dead_letters(
                self.name,
                older_than=older_than,
                now=time.time(),
            )
            self._condition.notify_all()
            return removed

    def count_dead_letters_older_than(self, *, older_than: float) -> int:
        if older_than < 0:
            raise ValueError("older_than cannot be negative")
        return self._get_store().count_dead_letters_older_than(
            self.name,
            older_than=older_than,
            now=time.time(),
        )

    def empty(self) -> bool:
        return self._get_store().empty(self.name, now=time.time())

    def full(self) -> bool:
        if self.backpressure.maxsize <= 0:
            return False
        return self.backpressure.is_full(qsize=self.qsize())

    def purge(self) -> int:
        with self._condition:
            self._unfinished.clear()
            count = self._get_store().purge(self.name)
            self._condition.notify_all()
            return count

    def _get_store(self) -> QueueStore:
        if self._store is None:
            path = (
                self._store_path
                if self._store_path is not None
                else default_queue_store_path()
            )
            self._store = SQLiteQueueStore(path)
        return self._store

    def _remove_unfinished(self, message_id: str) -> None:
        _ = self._unfinished.pop(message_id, None)

    def _run_post_put_hooks(self, message: QueueMessage) -> None:
        if self.notification_policy.notifies_on_put:
            self.notification_policy.notify(message)
        if self.dispatch_policy.dispatches_on_put:
            self.dispatch_policy.dispatch(message)


def _deadline(timeout: float | None) -> float | None:
    if timeout is None:
        return None
    if timeout < 0:
        raise ValueError("'timeout' must be a non-negative number")
    return time.monotonic() + timeout


def _remaining(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return deadline - time.monotonic()


def _apply_policy_set(
    *,
    policy_set: QueuePolicySet | None,
    semantics: QueueSemantics | None,
    locality_policy: LocalityPolicy | None,
    lease_policy: LeasePolicy | None,
    acknowledgement_policy: AcknowledgementPolicy | None,
    dead_letter_policy: DeadLetterPolicy | None,
    deduplication_policy: DeduplicationPolicy | None,
    consumption_policy: ConsumptionPolicy | None,
    delivery_policy: DeliveryPolicy | None,
    dispatch_policy: DispatchPolicy | None,
    notification_policy: NotificationPolicy | None,
    ordering_policy: OrderingPolicy | None,
    routing_policy: RoutingPolicy | None,
    subscription_policy: SubscriptionPolicy | None,
    backpressure: BackpressureStrategy | None,
    maxsize: int,
) -> tuple[
    QueueSemantics | None,
    LocalityPolicy | None,
    LeasePolicy | None,
    AcknowledgementPolicy | None,
    DeadLetterPolicy | None,
    DeduplicationPolicy | None,
    ConsumptionPolicy | None,
    DeliveryPolicy | None,
    DispatchPolicy | None,
    NotificationPolicy | None,
    OrderingPolicy | None,
    RoutingPolicy | None,
    SubscriptionPolicy | None,
    BackpressureStrategy | None,
]:
    if policy_set is None:
        return (
            semantics,
            locality_policy,
            lease_policy,
            acknowledgement_policy,
            dead_letter_policy,
            deduplication_policy,
            consumption_policy,
            delivery_policy,
            dispatch_policy,
            notification_policy,
            ordering_policy,
            routing_policy,
            subscription_policy,
            backpressure,
        )
    if policy_set.backpressure is not None and maxsize != 0:
        raise ValueError("pass either maxsize= or policy_set.backpressure, not both")
    return (
        _policy_value("semantics", semantics, policy_set.semantics),
        _policy_value(
            "locality_policy",
            locality_policy,
            policy_set.locality_policy,
        ),
        _policy_value(
            "lease_policy",
            lease_policy,
            policy_set.lease_policy,
        ),
        _policy_value(
            "acknowledgement_policy",
            acknowledgement_policy,
            policy_set.acknowledgement_policy,
        ),
        _policy_value(
            "dead_letter_policy",
            dead_letter_policy,
            policy_set.dead_letter_policy,
        ),
        _policy_value(
            "deduplication_policy",
            deduplication_policy,
            policy_set.deduplication_policy,
        ),
        _policy_value(
            "consumption_policy",
            consumption_policy,
            policy_set.consumption_policy,
        ),
        _policy_value("delivery_policy", delivery_policy, policy_set.delivery_policy),
        _policy_value("dispatch_policy", dispatch_policy, policy_set.dispatch_policy),
        _policy_value(
            "notification_policy",
            notification_policy,
            policy_set.notification_policy,
        ),
        _policy_value("ordering_policy", ordering_policy, policy_set.ordering_policy),
        _policy_value("routing_policy", routing_policy, policy_set.routing_policy),
        _policy_value(
            "subscription_policy",
            subscription_policy,
            policy_set.subscription_policy,
        ),
        _policy_value("backpressure", backpressure, policy_set.backpressure),
    )


def _wait_time(remaining: float | None) -> float:
    if remaining is None:
        return 0.05
    return min(max(remaining, 0.0), 0.05)


def _validate_retry_defaults(retry_defaults: Mapping[str, Any] | None) -> None:
    if retry_defaults is None:
        return
    if "max_tries" in retry_defaults and "stop" in retry_defaults:
        raise ValueError("retry_defaults cannot set both max_tries and stop")
    if "max_tries" in retry_defaults:
        max_tries = retry_defaults["max_tries"]
        if not isinstance(max_tries, int) or max_tries <= 0:
            raise ValueError("retry_defaults max_tries must be a positive integer")


def _validate_semantics(
    semantics: QueueSemantics,
    *,
    locality_policy: LocalityPolicy,
    lease_policy: LeasePolicy,
    acknowledgement_policy: AcknowledgementPolicy,
    dead_letter_policy: DeadLetterPolicy,
    deduplication_policy: DeduplicationPolicy,
    delivery_policy: DeliveryPolicy,
    consumption_policy: ConsumptionPolicy,
    ordering_policy: OrderingPolicy,
    routing_policy: RoutingPolicy,
    subscription_policy: SubscriptionPolicy,
    notification_policy: NotificationPolicy,
) -> None:
    if semantics.locality != locality_policy.locality:
        raise ValueError("semantics locality must match locality_policy locality")
    _validate_semantics_flags(
        semantics,
        lease_policy=lease_policy,
        acknowledgement_policy=acknowledgement_policy,
        dead_letter_policy=dead_letter_policy,
        deduplication_policy=deduplication_policy,
        subscription_policy=subscription_policy,
        notification_policy=notification_policy,
    )
    if semantics.delivery != delivery_policy.guarantee:
        raise ValueError("semantics delivery must match delivery_policy guarantee")
    if semantics.consumption != consumption_policy.pattern:
        raise ValueError("semantics consumption must match consumption_policy pattern")
    if semantics.ordering != ordering_policy.guarantee:
        raise ValueError("semantics ordering must match ordering_policy guarantee")
    if semantics.routing != routing_policy.pattern:
        raise ValueError("semantics routing must match routing_policy pattern")


def _validate_semantics_flags(
    semantics: QueueSemantics,
    *,
    lease_policy: LeasePolicy,
    acknowledgement_policy: AcknowledgementPolicy,
    dead_letter_policy: DeadLetterPolicy,
    deduplication_policy: DeduplicationPolicy,
    subscription_policy: SubscriptionPolicy,
    notification_policy: NotificationPolicy,
) -> None:
    if semantics.leases != lease_policy.uses_leases:
        raise ValueError("semantics leases must match lease_policy uses_leases")
    if semantics.acknowledgements != acknowledgement_policy.acknowledgements:
        raise ValueError(
            "semantics acknowledgements must match acknowledgement_policy "
            "acknowledgements"
        )
    if semantics.dead_letters != dead_letter_policy.dead_letters:
        raise ValueError(
            "semantics dead_letters must match dead_letter_policy dead_letters"
        )
    if semantics.deduplication != deduplication_policy.deduplication:
        raise ValueError(
            "semantics deduplication must match deduplication_policy deduplication"
        )
    if semantics.subscriptions != subscription_policy.subscriptions:
        raise ValueError(
            "semantics subscriptions must match subscription_policy subscriptions"
        )
    if semantics.notifications != notification_policy.notifies:
        raise ValueError(
            "semantics notifications must match notification_policy notifies"
        )


def _validate_priority(priority: int) -> None:
    if isinstance(priority, bool) or not isinstance(priority, int):
        raise TypeError("priority must be an integer")
    if priority < 0:
        raise ValueError("priority cannot be negative")


def _requires_dedupe_key(delivery_policy: DeliveryPolicy) -> bool:
    return bool(getattr(delivery_policy, "requires_dedupe_key", False))


def _policy_value(
    name: str,
    explicit: PolicyValue | None,
    bundled: PolicyValue | None,
) -> PolicyValue | None:
    if explicit is not None and bundled is not None:
        raise ValueError(f"pass either {name}= or policy_set.{name}, not both")
    return bundled if bundled is not None else explicit


def _error_payload(error: BaseException | str | None) -> dict[str, Any] | None:
    if error is None:
        return None
    if isinstance(error, BaseException):
        error_type = type(error)
        payload: dict[str, Any] = {
            "type": error_type.__name__,
            "module": error_type.__module__,
            "message": str(error),
        }
        for attr in ("command", "exit_code", "stdout", "stderr"):
            value = getattr(error, attr, None)
            if value is not None:
                payload[attr] = value
        return payload
    return {"type": None, "module": None, "message": str(error)}
