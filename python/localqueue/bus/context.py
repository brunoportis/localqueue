"""Runtime capabilities and extensible handler contexts for the event bus."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable, TypeVar

ContextT = TypeVar("ContextT", bound="HandlerContext")
ContextFactory = Callable[["RuntimeContext"], ContextT | Awaitable[ContextT]]


@dataclass(frozen=True)
class RuntimeContext:
    """Read-only delivery metadata for one attempt."""

    event_id: str
    attempt: int
    handler_name: str


class HandlerContext:
    """Base context delivered to EventBus handlers.

    Custom contexts receive a :class:`RuntimeContext` and can add application
    dependencies explicitly. ``event_id``, ``attempt``, and ``handler_name``
    are reserved runtime capabilities.
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
