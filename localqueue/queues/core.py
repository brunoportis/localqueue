from __future__ import annotations

import time
from collections.abc import Mapping
from pathlib import Path
from queue import Empty, Full
from threading import Condition
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from ..policies import (
    AT_LEAST_ONCE_DELIVERY,
    DEAD_LETTER_QUEUE,
    DEDUPE_KEY_SUPPORT,
    EXPLICIT_ACKNOWLEDGEMENT,
    FIFO_READY_ORDERING,
    FIXED_LEASE_TIMEOUT,
    LOCAL_QUEUE_PLACEMENT,
    NO_DISPATCHER,
    NO_NOTIFICATION,
    NO_SUBSCRIPTIONS,
    POINT_TO_POINT_ROUTING,
    PULL_CONSUMPTION,
    AcknowledgementPolicy,
    BackpressureStrategy,
    BoundedBackpressure,
    ConsumptionPolicy,
    DeadLetterPolicy,
    DeliveryPolicy,
    DeduplicationPolicy,
    DispatchPolicy,
    FixedLeaseTimeout,
    LeasePolicy,
    LocalityPolicy,
    NotificationPolicy,
    OrderingPolicy,
    QueuePolicySet,
    QueueSemantics,
    RoutingPolicy,
    SubscriptionPolicy,
)
from .policies import _apply_policy_set
from .timing import _deadline, _remaining, _wait_time
from .validation import (
    _requires_dedupe_key,
    _validate_priority,
    _validate_retry_defaults,
    _validate_semantics,
)

if TYPE_CHECKING:
    from ..spec import QueueSpec
    from ..stores import QueueMessage, QueueStats, QueueStore

T = TypeVar("T")
_NEGATIVE_DELAY_ERROR = "delay cannot be negative"


def subscriber_queue_name(queue_name: str, subscriber: str) -> str:
    if not subscriber:
        raise ValueError("subscriber cannot be empty")
    return f"{queue_name}.{subscriber}"


def _fanout_dedupe_key(dedupe_key: str | None, subscriber: str) -> str | None:
    if dedupe_key is None:
        return None
    return f"{dedupe_key}:{subscriber}"


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

    @classmethod
    def from_spec(
        cls, spec: QueueSpec, **kwargs: Any
    ) -> "PersistentQueue[Any]":
        return spec.build_queue(**kwargs)

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
        if self.routing_policy.fanout:
            return self._put_fanout(
                item,
                block=block,
                timeout=timeout,
                delay=delay,
                dedupe_key=dedupe_key,
                priority=priority,
            )
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

    def subscriber_queue(self, subscriber: str) -> PersistentQueue[T]:
        subscribers = self._subscriber_names()
        if not subscribers:
            raise ValueError("queue does not have configured subscribers")
        if subscriber not in subscribers:
            raise ValueError(f"unknown subscriber {subscriber!r}")
        return PersistentQueue(
            subscriber_queue_name(self.name, subscriber),
            store=self._get_store(),
            lease_policy=self.lease_policy,
            acknowledgement_policy=self.acknowledgement_policy,
            dead_letter_policy=self.dead_letter_policy,
            deduplication_policy=self.deduplication_policy,
            retry_defaults=self.retry_defaults,
            locality_policy=self.locality_policy,
            consumption_policy=self.consumption_policy,
            delivery_policy=self.delivery_policy,
            dispatch_policy=self.dispatch_policy,
            notification_policy=self.notification_policy,
            ordering_policy=self.ordering_policy,
            routing_policy=POINT_TO_POINT_ROUTING,
            subscription_policy=NO_SUBSCRIPTIONS,
            backpressure=self.backpressure,
        )

    def subscriber_queue_name(self, subscriber: str) -> str:
        subscribers = self._subscriber_names()
        if not subscribers:
            raise ValueError("queue does not have configured subscribers")
        if subscriber not in subscribers:
            raise ValueError(f"unknown subscriber {subscriber!r}")
        return subscriber_queue_name(self.name, subscriber)

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
            from .. import queue as queue_module

            path = (
                self._store_path
                if self._store_path is not None
                else queue_module.default_queue_store_path()
            )
            self._store = queue_module.SQLiteQueueStore(path)
        return self._store

    def _subscriber_names(self) -> tuple[str, ...]:
        subscribers = getattr(self.subscription_policy, "subscribers", ())
        return cast("tuple[str, ...]", tuple(subscribers))

    def _remove_unfinished(self, message_id: str) -> None:
        _ = self._unfinished.pop(message_id, None)

    def _put_fanout(
        self,
        item: T,
        *,
        block: bool,
        timeout: float | None,
        delay: float,
        dedupe_key: str | None,
        priority: int,
    ) -> QueueMessage:
        subscribers = self._subscriber_names()
        if not subscribers:
            raise ValueError(
                "publish-subscribe routing requires configured subscribers"
            )
        deadline = _deadline(timeout)
        messages: list[QueueMessage] = []
        for subscriber in subscribers:
            remaining = _remaining(deadline)
            message = self.subscriber_queue(subscriber).put(
                item,
                block=block,
                timeout=remaining,
                delay=delay,
                dedupe_key=_fanout_dedupe_key(dedupe_key, subscriber),
                priority=priority,
            )
            messages.append(message)
        return messages[0]

    def _run_post_put_hooks(self, message: QueueMessage) -> None:
        if self.notification_policy.notifies_on_put:
            self.notification_policy.notify(message)
        if self.dispatch_policy.dispatches_on_put:
            self.dispatch_policy.dispatch(message)


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
