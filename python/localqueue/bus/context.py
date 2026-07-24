"""Runtime capabilities and extensible handler contexts for the event bus."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Awaitable, Callable, TypeVar

from localqueue.bus.event import BaseEvent

ContextT = TypeVar("ContextT", bound="HandlerContext")
ContextFactory = Callable[["RuntimeContext"], ContextT | Awaitable[ContextT]]


@dataclass(frozen=True)
class RuntimeContext:
    """Read-only delivery metadata and runtime operations for one attempt."""

    event_id: str
    attempt: int
    handler_name: str
    _publish: Callable[[BaseEvent], Awaitable[object]] = field(repr=False)

    async def publish(self, event: BaseEvent) -> None:
        """Persist ``event`` through the bus that is handling this delivery."""
        await self._publish(event)


class HandlerContext:
    """Base context delivered to EventBus handlers.

    Custom contexts receive a :class:`RuntimeContext` and can add application
    dependencies explicitly. ``event_id``, ``attempt``, ``handler_name``, and
    ``publish`` are reserved runtime capabilities.
    """

    def __init__(self, runtime: RuntimeContext) -> None:
        self._runtime = runtime

    @property
    def event_id(self) -> str:
        return self._runtime.event_id

    @property
    def attempt(self) -> int:
        return self._runtime.attempt

    @property
    def handler_name(self) -> str:
        return self._runtime.handler_name

    async def publish(self, event: BaseEvent) -> None:
        """Publish an event using the current EventBus."""
        await self._runtime.publish(event)
