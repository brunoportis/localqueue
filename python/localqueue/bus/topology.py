"""Static routing topology for the event bus."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Union

from localqueue.bus.event import BaseEvent, event_type_of

NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]*$"
_NAME_RE = re.compile(NAME_PATTERN)

WILDCARD = "*"

EventPattern = Union[type[BaseEvent], str]


def validate_name(value: str, field: str) -> None:
    """Validate bus and subscription names consistently."""
    if not isinstance(value, str) or not _NAME_RE.match(value):
        raise ValueError(
            f"invalid '{field}': use {NAME_PATTERN} (non-empty and without ':')"
        )


def normalize_event_pattern(pattern: EventPattern) -> str:
    """Normalize and validate a topology or handler event pattern."""
    if isinstance(pattern, type) and issubclass(pattern, BaseEvent):
        return event_type_of(pattern)
    if not isinstance(pattern, str):
        raise TypeError(
            "event patterns must be BaseEvent subclasses or non-empty strings"
        )
    if not pattern.strip():
        raise ValueError("event type strings must be non-empty")
    if "*" in pattern and pattern != WILDCARD:
        raise ValueError(f"invalid wildcard event pattern {pattern!r}")
    return pattern


class BusTopology:
    """An immutable snapshot of subscription-to-event routing declarations."""

    def __init__(
        self,
        subscriptions: Mapping[str, Iterable[EventPattern]],
    ) -> None:
        if not isinstance(subscriptions, Mapping):
            raise TypeError("'subscriptions' must be a mapping")

        normalized: dict[str, frozenset[str]] = {}
        for subscription, patterns in subscriptions.items():
            validate_name(subscription, "subscription")
            if isinstance(patterns, (str, bytes)):
                raise TypeError(
                    "event patterns must be provided as an iterable of patterns"
                )
            try:
                patterns_iterator = iter(patterns)
            except TypeError as error:
                raise TypeError(
                    "event patterns must be provided as an iterable of patterns"
                ) from error
            event_types = frozenset(
                normalize_event_pattern(pattern) for pattern in patterns_iterator
            )
            if not event_types:
                raise ValueError(
                    f"subscription {subscription!r} must declare at least one "
                    "event pattern"
                )
            normalized[subscription] = event_types

        self._subscriptions = normalized
        self._subscription_names = tuple(sorted(normalized))

    @property
    def subscription_names(self) -> tuple[str, ...]:
        """Return declared subscription names in deterministic order."""
        return self._subscription_names

    def subscriptions_for(self, event_type: str) -> tuple[str, ...]:
        """Return subscriptions that route ``event_type`` in sorted order."""
        if not isinstance(event_type, str) or not event_type.strip():
            raise ValueError("'event_type' must be a non-empty string")
        return tuple(
            subscription
            for subscription in self._subscription_names
            if self.routes(subscription, event_type)
        )

    def has_subscription(self, subscription: str) -> bool:
        """Return whether ``subscription`` is declared."""
        return subscription in self._subscriptions

    def routes(self, subscription: str, event_type: str) -> bool:
        """Return whether the declared subscription routes ``event_type``."""
        patterns = self._subscriptions.get(subscription)
        return patterns is not None and (event_type in patterns or WILDCARD in patterns)
