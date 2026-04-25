from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol, TYPE_CHECKING


if TYPE_CHECKING:
    from ._types import RoutingPattern


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
