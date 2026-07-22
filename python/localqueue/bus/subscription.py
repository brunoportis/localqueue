"""Handler-registration facade for a declared bus subscription."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from localqueue.bus.topology import EventPattern

if TYPE_CHECKING:
    from localqueue.bus.bus import EventBus


class Subscription:
    """Bind local handlers to one statically declared subscription."""

    def __init__(self, bus: EventBus, name: str) -> None:
        self._bus = bus
        self.name = name

    @property
    def concurrency(self) -> int:
        """Return the subscription's current process-local concurrency bound."""
        return self._bus._concurrency_for(self.name)

    def handler(
        self,
        pattern: EventPattern,
        handler: Callable[[Any], Any] | None = None,
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
    ) -> Callable[[Any], Any]:
        """Register a direct handler or return a handler decorator."""
        return self._bus._register_handler(
            self.name,
            pattern,
            handler,
            permanent_errors=permanent_errors,
        )
