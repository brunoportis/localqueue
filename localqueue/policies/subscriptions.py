from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol


class SubscriptionPolicy(Protocol):
    @property
    def subscriptions(self) -> bool: ...

    @property
    def fanout(self) -> bool: ...

    @property
    def subscriber_count(self) -> int: ...

    def as_dict(self) -> dict[str, object]: ...


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
