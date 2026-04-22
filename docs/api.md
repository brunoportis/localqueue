---
icon: lucide/braces
---

# API Reference

This page lists the public symbols exported by `localqueue` and
`localqueue.retry`.

## localqueue

### Queue classes

#### `PersistentQueue`

Persistent queue with `queue.Queue`-style methods and explicit message methods.

Constructor options:

| Option | Meaning |
| --- | --- |
| `name` | queue name inside the store |
| `store` | queue store instance |
| `store_path` | path for the default SQLite queue store |
| `lease_timeout` | seconds before an inflight message is redelivered |
| `maxsize` | maximum number of ready messages; `0` means unbounded |

Core methods:

| Method | Meaning |
| --- | --- |
| `put(item, block=True, timeout=None, delay=0.0)` | enqueue an item |
| `put_nowait(item)` | enqueue without blocking |
| `get(block=True, timeout=None, leased_by=None)` | get only the value |
| `get_nowait()` | get without blocking |
| `get_message(block=True, timeout=None, leased_by=None)` | lease and return a `QueueMessage` |
| `inspect(message_id)` | read one message without leasing it |
| `ack(message)` | remove a message permanently |
| `release(message, delay=0.0, error=None)` | return a message to ready delivery, optionally recording the failure |
| `dead_letter(message, error=None)` | move a message out of normal delivery, optionally recording the failure |
| `task_done()` | acknowledge the oldest unfinished `get()` message |
| `join()` | wait for unfinished `get()` messages |
| `qsize()` | count ready messages |
| `stats()` | count ready, delayed, inflight, dead, and total messages |
| `dead_letters(limit=None)` | list dead-letter messages |
| `requeue_dead(message, delay=0.0)` | return a dead-letter message to ready delivery |
| `empty()` | whether there are no ready messages |
| `full()` | whether ready capacity is reached |
| `purge()` | remove all queue records |

#### `PersistentWorkerConfig`

Reusable configuration for `persistent_worker()` and `persistent_async_worker()`.
It keeps worker behavior and retry options together so the same policy can be
shared by multiple queues.

```python
from localqueue import PersistentWorkerConfig
from tenacity import retry_if_exception_type, wait_fixed

config = PersistentWorkerConfig(
    max_tries=3,
    wait=wait_fixed(1),
    retry=retry_if_exception_type(ConnectionError),
    dead_letter_on_failure=False,
    release_delay=30,
)
```

Constructor options:

| Option | Meaning |
| --- | --- |
| `dead_letter_on_failure` | dead-letter final handler failures when `True` |
| `dead_letter_on_exhaustion` | compatibility alias for `dead_letter_on_failure` |
| `release_delay` | delay used when releasing failed messages |
| `**retry_kwargs` | forwarded to `PersistentRetrying` |

#### `QueueMessage`

Dataclass returned by `put()` and `get_message()`.

| Field | Meaning |
| --- | --- |
| `id` | generated message id |
| `value` | Python value stored in the queue |
| `queue` | queue name |
| `state` | current message state: `ready`, `inflight`, or `dead` |
| `attempts` | delivery attempt count |
| `created_at` | creation timestamp |
| `available_at` | earliest delivery timestamp |
| `leased_until` | lease expiration timestamp, if inflight |
| `leased_by` | optional worker id that currently owns the lease, if inflight |
| `attempt_history` | list of lease and outcome events recorded for this message |
| `last_error` | structured error from the most recent failed processing attempt, if recorded |
| `failed_at` | timestamp for `last_error`, if recorded |

For `localqueue queue exec` failures, `last_error` also includes `command`,
`exit_code`, `stdout`, and `stderr` fields so command workers can be inspected
from `queue inspect` and `queue dead`. `attempt_history` shows the lease and
terminal events that led to the current state.

#### `QueueStats`

Dataclass returned by `stats()`.

| Field | Meaning |
| --- | --- |
| `ready` | messages available for immediate delivery |
| `delayed` | ready-state messages whose `available_at` is in the future |
| `inflight` | leased messages not yet acknowledged, released, or dead-lettered |
| `dead` | dead-letter messages hidden from normal delivery |
| `total` | all messages still stored for the queue |
| `by_worker_id` | current inflight counts grouped by `leased_by` |

### Worker decorators

#### `persistent_worker(queue, config=None, **retry_kwargs)`

Builds a queue consumer around `PersistentRetrying`. The leased message id is
used as the persistent retry key. Worker handlers receive `message.value` as
their first argument.

```python
from localqueue import PersistentWorkerConfig, persistent_worker

config = PersistentWorkerConfig(max_tries=3)


@persistent_worker(queue, config=config)
def handle(payload: dict) -> None:
    ...
```

Direct keyword arguments are still accepted and override values from `config`.

#### `persistent_async_worker(queue, config=None, **retry_kwargs)`

Async equivalent backed by `PersistentAsyncRetrying`.

Options:

| Option | Meaning |
| --- | --- |
| `dead_letter_on_failure` | dead-letter final handler failures when `True` |
| `dead_letter_on_exhaustion` | compatibility alias for `dead_letter_on_failure` |
| `release_delay` | delay used when releasing failed messages |
| `**retry_kwargs` | forwarded to `PersistentRetrying` |

### Queue stores

#### `QueueStore`

Protocol for custom queue stores.

#### `SQLiteQueueStore`

SQLite-backed queue store. This is the default backend. Records are serialized
as versioned JSON; values must be JSON-serializable.

#### `LMDBQueueStore`

LMDB-backed queue store. Records are serialized as versioned JSON; values must be
JSON-serializable.

#### `MemoryQueueStore`

Thread-safe in-memory queue store for tests.

#### `QueueStoreLockedError`

Raised when LMDB reports that the queue store is locked by another process.

## localqueue.retry

### Retry decorators

#### `persistent_retry(**kwargs)`

Creates a decorator backed by `PersistentRetrying`.

```python
from localqueue.retry import key_from_argument, persistent_retry


@persistent_retry(key_fn=key_from_argument("job_id"), max_tries=3)
def run(job_id: str) -> None:
    ...
```

#### `persistent_async_retry(**kwargs)`

Creates a decorator backed by `PersistentAsyncRetrying`.

```python
from localqueue.retry import key_from_argument, persistent_async_retry


@persistent_async_retry(key_fn=key_from_argument("job_id"), max_tries=3)
async def run(job_id: str) -> None:
    ...
```

Both decorators require `key=` or `key_fn=` and accept the persistent options below plus Tenacity options such as `stop`, `wait`, `retry`, `before`, `after`, `before_sleep`, `retry_error_callback`, and `reraise`.

| Option | Meaning |
| --- | --- |
| `store` | attempt store instance |
| `store_path` | path for an LMDB attempt-store directory |
| `key` | fixed retry key |
| `key_fn` | function that derives a retry key from the call |
| `clear_on_success` | delete the attempt record after success |
| `max_tries` | alias for `stop_after_attempt(max_tries)` |

### Key helpers

#### `key_from_argument(name)`

Creates a `key_fn` that reads the retry key from a named function argument.

```python
from localqueue.retry import key_from_argument, persistent_retry


@persistent_retry(key_fn=key_from_argument("job_id"))
def run(*, job_id: str) -> None:
    ...
```

#### `key_from_attr(argument_name, attribute_name, *, prefix=None)`

Creates a `key_fn` that reads an attribute from a named function argument. Pass
`prefix=` when the same store may contain keys for different task domains.

```python
from localqueue.retry import key_from_attr, persistent_retry


@persistent_retry(key_fn=key_from_attr("task", "id", prefix="video"))
def run(task: VideoTask) -> None:
    ...
```

#### `idempotency_key_from_id(argument_name, *, prefix=None)`

Shortcut for `key_from_attr(argument_name, "id", prefix=prefix)`.

```python
from localqueue.retry import idempotency_key_from_id, persistent_retry


@persistent_retry(key_fn=idempotency_key_from_id("task", prefix="video"))
def run(task: VideoTask) -> None:
    ...
```

### Retry classes

#### `PersistentRetrying`

Synchronous retryer. It composes Tenacity's `Retrying` internally and exposes the
same call/decorator flow.

```python
from localqueue.retry import PersistentRetrying, key_from_argument

retryer = PersistentRetrying(key_fn=key_from_argument("job_id"), max_tries=5)
retryer(fn, "job:1")
```

Methods:

| Method | Meaning |
| --- | --- |
| `get_record(key)` | load the persisted `RetryRecord`, if any |
| `reset(key)` | delete the persisted retry record |
| `copy(**kwargs)` | copy the retryer, preserving persistent settings |

#### `PersistentAsyncRetrying`

Async retryer. It accepts coroutine functions and supports coroutine Tenacity callbacks and strategies where Tenacity supports them.

```python
from localqueue.retry import PersistentAsyncRetrying, key_from_argument

retryer = PersistentAsyncRetrying(key_fn=key_from_argument("job_id"), max_tries=5)
await retryer(fn, "job:1")
```

#### `PersistentRetryExhausted`

Raised before the wrapped function is called when a persisted key is already exhausted.

Attributes:

| Attribute | Meaning |
| --- | --- |
| `key` | exhausted retry key |
| `attempts` | persisted attempt count |

### Attempt stores

#### `RetryRecord`

Dataclass stored per retry key.

| Field | Type | Meaning |
| --- | --- | --- |
| `attempts` | `int` | number of failed attempts recorded |
| `first_attempt_at` | `float` | Unix timestamp of the first persisted attempt |
| `exhausted` | `bool` | whether the retry budget is exhausted |

#### `AttemptStore`

Protocol for custom attempt stores.

```python
class AttemptStore:
    def load(self, key: str) -> RetryRecord | None: ...
    def save(self, key: str, record: RetryRecord) -> None: ...
    def delete(self, key: str) -> None: ...
```

#### `LMDBAttemptStore`

LMDB-backed attempt store.

```python
from localqueue.retry import LMDBAttemptStore

store = LMDBAttemptStore("/var/lib/my-worker/retries")
```

#### `SQLiteAttemptStore`

SQLite-backed attempt store. This is the default backend and does not require LMDB's
native dependency.

```python
from localqueue.retry import SQLiteAttemptStore

store = SQLiteAttemptStore("/var/lib/my-worker/retries.sqlite3")
```

#### `MemoryAttemptStore`

Thread-safe in-memory attempt store for tests and local scenarios.

#### `AttemptStoreLockedError`

Raised when LMDB reports that the attempt store is locked by another process.
