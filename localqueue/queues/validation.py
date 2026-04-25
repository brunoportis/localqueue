from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ..policies import (
        AcknowledgementPolicy,
        ConsumptionPolicy,
        DeadLetterPolicy,
        DeduplicationPolicy,
        DeliveryPolicy,
        LeasePolicy,
        LocalityPolicy,
        NotificationPolicy,
        OrderingPolicy,
        QueueSemantics,
        RoutingPolicy,
        SubscriptionPolicy,
    )


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
