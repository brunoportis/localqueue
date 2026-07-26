"""Persistent event bus built on localqueue (the optional ``bus`` extra)."""

try:
    import pydantic as _pydantic  # noqa: F401
except ImportError as error:  # pragma: no cover
    raise ImportError(
        'Install event bus support with:\n\n    pip install "localqueue[bus]"'
    ) from error

from localqueue.bus.bus import DispatchReceipt, EventBus, NoSubscribers
from localqueue.bus.context import ContextFactory, HandlerContext, RuntimeContext
from localqueue.bus.control import Reject, Retry
from localqueue.bus.deadletter import FailedDelivery
from localqueue.bus.event import BaseEvent, InvalidEventIdentity, event
from localqueue.bus.ingestion import (
    CheckpointProgress,
    CheckpointState,
    IngestionCheckpoint,
    IngestionResult,
    SourceChanged,
)
from localqueue.bus.registry import EVENT_REGISTRY, EventRegistry
from localqueue.bus.retry import RetryPolicy
from localqueue.bus.sources import ResumableSource, SequenceSource, SourceRecord
from localqueue.bus.subscription import Subscription
from localqueue.bus.topology import BusTopology
from localqueue.exceptions import CheckpointConflict, DeduplicationConflict

__all__ = [
    "EVENT_REGISTRY",
    "BaseEvent",
    "event",
    "InvalidEventIdentity",
    "BusTopology",
    "CheckpointConflict",
    "CheckpointProgress",
    "CheckpointState",
    "ContextFactory",
    "DispatchReceipt",
    "DeduplicationConflict",
    "EventBus",
    "FailedDelivery",
    "HandlerContext",
    "IngestionCheckpoint",
    "IngestionResult",
    "EventRegistry",
    "NoSubscribers",
    "Reject",
    "ResumableSource",
    "Retry",
    "RetryPolicy",
    "RuntimeContext",
    "SequenceSource",
    "SourceChanged",
    "SourceRecord",
    "Subscription",
]
