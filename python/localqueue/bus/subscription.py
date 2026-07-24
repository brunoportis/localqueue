"""Handler-registration facade for a declared bus subscription."""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, TypeVar, overload

from localqueue.bus.deadletter import FailedDelivery, inspect_delivery
from localqueue.bus.topology import EventPattern

if TYPE_CHECKING:
    from localqueue.bus.bus import EventBus, _EventHandlerDecorator
    from localqueue.bus.event import BaseEvent

_EventT = TypeVar("_EventT", bound="BaseEvent")
_HandlerResultT = TypeVar("_HandlerResultT")


class Subscription:
    """Bind local handlers to one statically declared subscription."""

    def __init__(self, bus: EventBus, name: str) -> None:
        self._bus = bus
        self.name = name

    @property
    def concurrency(self) -> int:
        """Return the subscription's current process-local concurrency bound."""
        return self._bus._concurrency_for(self.name)

    @overload
    def handler(
        self,
        pattern: type[_EventT],
        handler: None = None,
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> _EventHandlerDecorator[_EventT]: ...

    @overload
    def handler(
        self,
        pattern: type[_EventT],
        handler: Callable[[_EventT], _HandlerResultT],
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[_EventT], _HandlerResultT]: ...

    @overload
    def handler(
        self,
        pattern: str,
        handler: None = None,
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> _EventHandlerDecorator[BaseEvent]: ...

    @overload
    def handler(
        self,
        pattern: str,
        handler: Callable[[BaseEvent], _HandlerResultT],
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[BaseEvent], _HandlerResultT]: ...

    def handler(
        self,
        pattern: EventPattern,
        handler: object = None,
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> object:
        """Register a direct handler or return a handler decorator."""
        return self._bus._register_handler(
            self.name,
            pattern,
            handler,
            permanent_errors=permanent_errors,
            timeout=timeout,
        )

    def list_failed(self, limit: int = 100, offset: int = 0) -> list[FailedDelivery]:
        """Inspect this subscription's failed deliveries by increasing ID."""
        queue = self._bus._open_subscription_queue(self.name)
        try:
            return [
                inspect_delivery(self.name, message, self._bus.registry)
                for message in queue.list_failed(limit=limit, offset=offset)
            ]
        finally:
            queue.close()

    def retry_failed(self, message_id: int) -> None:
        """Replay one failed delivery from this subscription only."""
        queue = self._bus._open_subscription_queue(self.name)
        try:
            queue.retry_failed(message_id)
        finally:
            queue.close()
