from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from threading import Event
from typing import TYPE_CHECKING, Any, Callable, cast


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


@dataclass(frozen=True, slots=True)
class AsyncioNotification:
    """Notification adapter that wakes local async tasks with an asyncio.Event."""

    event: asyncio.Event = field(default_factory=asyncio.Event)
    loop: asyncio.AbstractEventLoop | None = None
    notifies: bool = True
    notifies_on_put: bool = True
    listener_count: int = 1

    def __post_init__(self) -> None:
        if self.loop is None:
            try:
                object.__setattr__(self, "loop", asyncio.get_running_loop())
            except RuntimeError:
                pass

    def notify(self, message: QueueMessage) -> None:
        _ = message
        if self.loop is not None:
            self.loop.call_soon_threadsafe(self.event.set)
            return
        self.event.set()

    async def wait_async(self, timeout: float | None = None) -> bool:
        if timeout is None:
            await self.event.wait()
            return True
        try:
            await asyncio.wait_for(self.event.wait(), timeout)
        except TimeoutError:
            return False
        return True

    def clear(self) -> None:
        self.event.clear()

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "asyncio-event",
            "scope": "in-process",
            "notifies": self.notifies,
            "notifies_on_put": self.notifies_on_put,
            "listener_count": self.listener_count,
        }


def _default_websocket_message(message: QueueMessage) -> dict[str, Any]:
    return {
        "id": message.id,
        "queue": message.queue,
        "value": message.value,
        "state": message.state,
        "attempts": message.attempts,
        "created_at": message.created_at,
        "available_at": message.available_at,
        "leased_until": message.leased_until,
        "leased_by": message.leased_by,
        "last_error": message.last_error,
        "failed_at": message.failed_at,
        "attempt_history": list(message.attempt_history),
        "dedupe_key": message.dedupe_key,
        "priority": message.priority,
    }


@dataclass(frozen=True, slots=True)
class WebSocketNotification:
    """Notification adapter that broadcasts enqueue events over WebSocket-like objects."""

    connections: object | tuple[object, ...]
    serializer: Callable[[QueueMessage], object] = _default_websocket_message
    loop: asyncio.AbstractEventLoop | None = None
    notifies: bool = True
    notifies_on_put: bool = True

    def __post_init__(self) -> None:
        connections = (
            (self.connections,)
            if callable(getattr(self.connections, "send", None))
            or callable(getattr(self.connections, "send_text", None))
            or callable(getattr(self.connections, "send_json", None))
            else tuple(cast("Any", self.connections))
        )
        if not connections:
            raise ValueError("connections cannot be empty")
        object.__setattr__(self, "connections", connections)
        if self.loop is None:
            try:
                object.__setattr__(self, "loop", asyncio.get_running_loop())
            except RuntimeError:
                pass

    @property
    def listener_count(self) -> int:
        return len(cast("tuple[object, ...]", self.connections))

    def notify(self, message: QueueMessage) -> None:
        payload = self.serializer(message)
        for connection in cast("tuple[object, ...]", self.connections):
            coroutine = self._send(connection, payload)
            try:
                running_loop = asyncio.get_running_loop()
            except RuntimeError:
                running_loop = None
            if self.loop is not None and running_loop is self.loop:
                _ = self.loop.create_task(coroutine)
                continue
            if self.loop is not None:
                _ = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
                continue
            if running_loop is None:
                raise RuntimeError(
                    "WebSocketNotification requires an active event loop or loop="
                )
            _ = running_loop.create_task(coroutine)

    async def _send(self, connection: object, payload: object) -> None:
        if hasattr(connection, "send_json"):
            await cast("Any", connection).send_json(payload)
            return
        if hasattr(connection, "send_text") and isinstance(payload, str):
            await cast("Any", connection).send_text(payload)
            return
        await cast("Any", connection).send(payload)

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "websocket",
            "scope": "network",
            "notifies": self.notifies,
            "notifies_on_put": self.notifies_on_put,
            "listener_count": self.listener_count,
        }
