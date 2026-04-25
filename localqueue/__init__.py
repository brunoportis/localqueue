import importlib.metadata

try:
    __version__ = importlib.metadata.version("localqueue")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"

from .queue import PersistentQueue
from .policies import (
    AT_LEAST_ONCE_DELIVERY,
    FIFO_READY_ORDERING,
    LOCAL_AT_LEAST_ONCE,
    AtLeastOnceDelivery,
    BackpressureStrategy,
    BoundedBackpressure,
    DeliveryPolicy,
    FifoReadyOrdering,
    OrderingPolicy,
    QueueSemantics,
)
from .retry import (
    AttemptStore,
    AttemptStoreLockedError,
    LMDBAttemptStore,
    MemoryAttemptStore,
    PersistentAsyncRetrying,
    PersistentRetryExhausted,
    PersistentRetrying,
    RetryRecord,
    SQLiteAttemptStore,
    close_default_store,
    configure_default_store,
    configure_default_store_factory,
    idempotency_key_from_id,
    key_from_argument,
    key_from_attr,
    persistent_async_retry,
    persistent_retry,
)
from .stores import (
    LMDBQueueStore,
    MemoryQueueStore,
    QueueMessage,
    QueueStats,
    QueueStore,
    QueueStoreLockedError,
    SQLiteQueueStore,
)
from .worker import PersistentWorkerConfig, persistent_async_worker, persistent_worker

__all__ = [
    "AttemptStore",
    "AttemptStoreLockedError",
    "AT_LEAST_ONCE_DELIVERY",
    "AtLeastOnceDelivery",
    "BackpressureStrategy",
    "BoundedBackpressure",
    "DeliveryPolicy",
    "FIFO_READY_ORDERING",
    "FifoReadyOrdering",
    "LMDBAttemptStore",
    "LMDBQueueStore",
    "LOCAL_AT_LEAST_ONCE",
    "MemoryAttemptStore",
    "MemoryQueueStore",
    "PersistentAsyncRetrying",
    "PersistentQueue",
    "PersistentRetryExhausted",
    "PersistentRetrying",
    "PersistentWorkerConfig",
    "OrderingPolicy",
    "QueueMessage",
    "QueueSemantics",
    "QueueStats",
    "QueueStore",
    "QueueStoreLockedError",
    "RetryRecord",
    "SQLiteAttemptStore",
    "SQLiteQueueStore",
    "__version__",
    "close_default_store",
    "configure_default_store",
    "configure_default_store_factory",
    "idempotency_key_from_id",
    "key_from_argument",
    "key_from_attr",
    "persistent_async_retry",
    "persistent_async_worker",
    "persistent_retry",
    "persistent_worker",
]
