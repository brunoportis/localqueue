"""Barramento de eventos persistente sobre o localqueue (extra ``bus``)."""

try:
    import pydantic as _pydantic  # noqa: F401
except ImportError as error:  # pragma: no cover
    raise ImportError(
        'Install event bus support with:\n\n    pip install "localqueue[bus]"'
    ) from error

from localqueue.bus.bus import DispatchReceipt, EventBus, NoSubscribers
from localqueue.bus.event import BaseEvent
from localqueue.bus.registry import EVENT_REGISTRY, EventRegistry

__all__ = [
    "EVENT_REGISTRY",
    "BaseEvent",
    "DispatchReceipt",
    "EventBus",
    "EventRegistry",
    "NoSubscribers",
]
