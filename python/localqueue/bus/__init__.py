"""Persistent event bus built on localqueue (the optional ``bus`` extra)."""

try:
    import pydantic as _pydantic  # noqa: F401
except ImportError as error:  # pragma: no cover
    raise ImportError(
        'Install event bus support with:\n\n    pip install "localqueue[bus]"'
    ) from error

from localqueue.bus.bus import DispatchReceipt, EventBus, NoSubscribers
from localqueue.bus.event import BaseEvent
from localqueue.bus.registry import EVENT_REGISTRY, EventRegistry
from localqueue.bus.subscription import Subscription
from localqueue.bus.topology import BusTopology

__all__ = [
    "EVENT_REGISTRY",
    "BaseEvent",
    "BusTopology",
    "DispatchReceipt",
    "EventBus",
    "EventRegistry",
    "NoSubscribers",
    "Subscription",
]
