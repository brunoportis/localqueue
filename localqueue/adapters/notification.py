from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event
from typing import TYPE_CHECKING, cast


if TYPE_CHECKING:
    from ..policies._types import MessageHandler, MessageHandlers
    from ..stores import QueueMessage


@dataclass(frozen=True, slots=True)
class CallbackNotification:
    """Notification adapter that calls in-process listeners after enqueue."""

    listeners: MessageHandlers
    notifies: bool = True
    notifies_on_put: bool = True

    def __post_init__(self) -> None:
        listeners = (
            (self.listeners,) if callable(self.listeners) else tuple(self.listeners)
        )
        if not listeners:
            raise ValueError("listeners cannot be empty")
        object.__setattr__(self, "listeners", listeners)

    @property
    def listener_count(self) -> int:
        return len(cast("tuple[MessageHandler, ...]", self.listeners))

    def notify(self, message: QueueMessage) -> None:
        for listener in cast("tuple[MessageHandler, ...]", self.listeners):
            _ = listener(message)

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "callback",
            "scope": "in-process",
            "notifies": self.notifies,
            "notifies_on_put": self.notifies_on_put,
            "listener_count": self.listener_count,
        }


@dataclass(frozen=True, slots=True)
class InProcessNotification:
    """Notification adapter that wakes local threads with a threading.Event."""

    event: Event = field(default_factory=Event)
    notifies: bool = True
    notifies_on_put: bool = True
    listener_count: int = 1

    def notify(self, message: QueueMessage) -> None:
        _ = message
        self.event.set()

    def wait(self, timeout: float | None = None) -> bool:
        return self.event.wait(timeout)

    def clear(self) -> None:
        self.event.clear()

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "thread-event",
            "scope": "in-process",
            "notifies": self.notifies,
            "notifies_on_put": self.notifies_on_put,
            "listener_count": self.listener_count,
        }
