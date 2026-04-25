from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from ..policies._types import MessageHandler
    from ..stores import QueueMessage


@dataclass(frozen=True, slots=True)
class CallbackDispatcher:
    """Dispatch adapter that calls in-process handlers after enqueue."""

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
