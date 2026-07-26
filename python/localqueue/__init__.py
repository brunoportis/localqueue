"""localqueue: a persistent local SQLite queue with ACK, leases, and retries."""

from localqueue.core import (
    EnqueueItem,
    JsonSerializer,
    QueueStats,
    Serializer,
    SimpleQueue,
)
from localqueue.deadletter import FailedMessage, FailureReason
from localqueue.diagnostics import QueueDiagnostics
from localqueue.exceptions import (
    CheckpointConflict,
    DeduplicationConflict,
    Empty,
    Full,
    LeaseExpired,
    LocalQueueError,
)
from localqueue.job import Job
from localqueue.maintenance import BackupResult, IntegrityCheckResult
from localqueue.policies import DeliveryPolicy, DurabilityMode
from localqueue.worker import Worker

__all__ = [
    "Empty",
    "CheckpointConflict",
    "DeduplicationConflict",
    "Full",
    "BackupResult",
    "DeliveryPolicy",
    "DurabilityMode",
    "EnqueueItem",
    "FailedMessage",
    "FailureReason",
    "Job",
    "JsonSerializer",
    "IntegrityCheckResult",
    "LeaseExpired",
    "LocalQueueError",
    "QueueDiagnostics",
    "QueueStats",
    "Serializer",
    "SimpleQueue",
    "Worker",
]
