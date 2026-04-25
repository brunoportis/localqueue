"""Local notification adapters: threading.Event and asyncio.Event."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncio
    import threading

    from ..stores import QueueMessage


@dataclass(frozen=True, slots=True)
class InProcessNotification:
    """Notification policy that signals a threading.Event after enqueue."""

    event: threading.Event
    notifies: bool = True
    notifies_on_put: bool = True

    @property
    def listener_count(self) -> int:
        return 1

    def notify(self, message: QueueMessage) -> None:
        _ = message
        self.event.set()

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "in-process-event",
            "notifies": self.notifies,
            "notifies_on_put": self.notifies_on_put,
            "listener_count": self.listener_count,
        }


@dataclass(frozen=True, slots=True)
class AsyncNotification:
    """Notification policy that signals an asyncio.Event after enqueue.

    To be safe when notify() is called from threads other than the event loop's
    thread, prefer passing the event loop explicitly via the ``loop`` argument.
    If ``loop`` is omitted, this adapter will try to locate the loop bound to
    the event and fall back to calling set() directly as a best-effort.
    """

    event: asyncio.Event
    loop: asyncio.AbstractEventLoop | None = None
    notifies: bool = True
    notifies_on_put: bool = True

    @property
    def listener_count(self) -> int:
        return 1

    def notify(self, message: QueueMessage) -> None:
        _ = message
        # Prefer an explicit loop if provided
        loop = self.loop or getattr(self.event, "_loop", None)
        if loop is not None:
            # Schedule a thread-safe set on the event's loop.
            try:
                loop.call_soon_threadsafe(self.event.set)
                return
            except RuntimeError:
                # Loop may be closed or in a bad state
                pass

        # Best-effort fallback (works if we're already on the event loop thread)
        try:
            self.event.set()
        except RuntimeError:
            # Last-resort: nothing sensible to do here; fail silently to keep
            # notify() non-blocking.
            pass

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "async-event",
            "notifies": self.notifies,
            "notifies_on_put": self.notifies_on_put,
            "listener_count": self.listener_count,
        }
