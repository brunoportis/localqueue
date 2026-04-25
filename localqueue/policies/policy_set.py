from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .commit import CommitPolicy, LOCAL_ATOMIC_COMMIT
from .delivery import (
    AT_LEAST_ONCE_DELIVERY,
    AtMostOnceDelivery,
    DeliveryPolicy,
    EffectivelyOnceDelivery,
)
from .results import NO_RESULT_POLICY, ResultPolicy

if TYPE_CHECKING:
    from .dead_letter import DeadLetterPolicy
    from .acknowledgement import AcknowledgementPolicy
    from .dispatch import DispatchPolicy
    from .deduplication import DeduplicationPolicy
    from .locality import LocalityPolicy
    from .notification import NotificationPolicy
    from .routing import RoutingPolicy
    from .lease import LeasePolicy
    from .semantics import QueueSemantics
    from .subscriptions import SubscriptionPolicy
    from .consumption import ConsumptionPolicy
    from .backpressure import BackpressureStrategy
    from .ordering import OrderingPolicy
    from ..idempotency import IdempotencyStore


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
