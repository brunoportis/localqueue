import importlib.metadata

try:
    __version__ = importlib.metadata.version("persistentretry")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"

from .core import (
    PersistentAsyncRetrying,
    PersistentRetryExhausted,
    PersistentRetrying,
    configure_default_store,
    configure_default_store_factory,
    idempotency_key_from_id,
    key_from_argument,
    key_from_attr,
    persistent_async_retry,
    persistent_retry,
)
from .store import (
    AttemptStore,
    AttemptStoreLockedError,
    LMDBAttemptStore,
    MemoryAttemptStore,
    RetryRecord,
    SQLiteAttemptStore,
)

__all__ = [
    "AttemptStore",
    "AttemptStoreLockedError",
    "LMDBAttemptStore",
    "MemoryAttemptStore",
    "PersistentAsyncRetrying",
    "PersistentRetryExhausted",
    "PersistentRetrying",
    "RetryRecord",
    "SQLiteAttemptStore",
    "__version__",
    "configure_default_store",
    "configure_default_store_factory",
    "idempotency_key_from_id",
    "key_from_argument",
    "key_from_attr",
    "persistent_async_retry",
    "persistent_retry",
]
