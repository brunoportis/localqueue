"""Registry de classes de eventos por ``event_type``.

O registry é global ao processo: consumidores resolvem a classe do evento a
partir do envelope persistido, então basta que o módulo com a classe tenha
sido importado (via ``bus.on(Classe, ...)`` ou ``register`` explícito).
"""

from __future__ import annotations

from typing import Optional

from localqueue.bus.event import BaseEvent, event_type_of


class EventRegistry:
    def __init__(self) -> None:
        self._classes: dict[str, type[BaseEvent]] = {}

    def register(self, cls: type[BaseEvent]) -> type[BaseEvent]:
        if not (isinstance(cls, type) and issubclass(cls, BaseEvent)):
            raise TypeError("'cls' deve ser uma subclasse de BaseEvent")
        event_type = event_type_of(cls)
        existing = self._classes.get(event_type)
        if existing is not None and existing is not cls:
            raise ValueError(
                f"event_type {event_type!r} já registrado por "
                f"{existing.__module__}.{existing.__qualname__}"
            )
        self._classes[event_type] = cls
        return cls

    def resolve(self, event_type: str) -> Optional[type[BaseEvent]]:
        return self._classes.get(event_type)


EVENT_REGISTRY = EventRegistry()
