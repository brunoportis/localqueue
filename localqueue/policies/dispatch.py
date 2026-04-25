from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Protocol


if TYPE_CHECKING:
    from ._types import MessageHandler
    from ..stores import QueueMessage


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
