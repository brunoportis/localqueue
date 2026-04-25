from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol, TYPE_CHECKING


if TYPE_CHECKING:
    from ._types import ConsumptionPattern


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
