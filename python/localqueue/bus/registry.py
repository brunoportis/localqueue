"""Registry of event classes by ``event_type``.

The registry is process-global. Consumers resolve an event class from its
persisted envelope, so the module defining that class only needs to be imported
through ``subscription.handler(EventClass, ...)`` or an explicit ``register``
call.
"""

from __future__ import annotations

from typing import Optional, TypeVar

from localqueue.bus.event import BaseEvent, event_type_of

_EventT = TypeVar("_EventT", bound=BaseEvent)


class EventRegistry:
    def __init__(self) -> None:
        self._classes: dict[str, type[BaseEvent]] = {}

    def register(self, cls: type[_EventT]) -> type[_EventT]:
        registered_cls = cls
        if not (isinstance(cls, type) and issubclass(cls, BaseEvent)):
            raise TypeError("'cls' must be a BaseEvent subclass")
        event_type = event_type_of(cls)
        existing = self._classes.get(event_type)
        if existing is not None and existing is not cls:
            raise ValueError(
                f"event_type {event_type!r} is already registered by "
                f"{existing.__module__}.{existing.__qualname__}"
            )
        self._classes[event_type] = cls
        return registered_cls

    def resolve(self, event_type: str) -> Optional[type[BaseEvent]]:
        return self._classes.get(event_type)


EVENT_REGISTRY = EventRegistry()
