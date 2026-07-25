"""Exceptions exposed by localqueue."""

from localqueue.localqueue import (
    DeduplicationConflict,
    Empty,
    Full,
    LeaseExpired,
    LocalQueueError,
)

__all__ = [
    "DeduplicationConflict",
    "Empty",
    "Full",
    "LeaseExpired",
    "LocalQueueError",
]
