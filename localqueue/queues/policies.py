from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from ..policies import (
        AcknowledgementPolicy,
        BackpressureStrategy,
        ConsumptionPolicy,
        DeadLetterPolicy,
        DeduplicationPolicy,
        DeliveryPolicy,
        DispatchPolicy,
        LeasePolicy,
        LocalityPolicy,
        NotificationPolicy,
        OrderingPolicy,
        QueuePolicySet,
        QueueSemantics,
        RoutingPolicy,
        SubscriptionPolicy,
    )

PolicyValue = TypeVar("PolicyValue")


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
        _policy_value("locality_policy", locality_policy, policy_set.locality_policy),
        _policy_value("lease_policy", lease_policy, policy_set.lease_policy),
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


def _policy_value(
    name: str,
    explicit: PolicyValue | None,
    bundled: PolicyValue | None,
) -> PolicyValue | None:
    if explicit is not None and bundled is not None:
        raise ValueError(f"pass either {name}= or policy_set.{name}, not both")
    return bundled if bundled is not None else explicit
