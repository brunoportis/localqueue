from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Protocol

from ..adapters.notification import (  # noqa: F401
    AsyncioNotification,
    CallbackNotification,
    InProcessNotification,
    WebSocketNotification,
)

if TYPE_CHECKING:
    from ..stores import QueueMessage


class NotificationPolicy(Protocol):
    @property
    def notifies(self) -> bool: ...

    @property
    def notifies_on_put(self) -> bool: ...

    @property
    def listener_count(self) -> int: ...

    def notify(self, message: QueueMessage) -> None: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class NoNotification:
    """Notification policy where producers do not wake any listeners."""

    notifies: bool = False
    notifies_on_put: bool = False
    listener_count: int = 0

    def notify(self, message: QueueMessage) -> None:
        _ = message

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


NO_NOTIFICATION = NoNotification()

__all__ = [
    "AsyncioNotification",
    "CallbackNotification",
    "InProcessNotification",
    "NO_NOTIFICATION",
    "NoNotification",
    "NotificationPolicy",
    "WebSocketNotification",
]
