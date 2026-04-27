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
| `store_path` | explicit path for the SQLite queue store |
| `lease_timeout` | seconds before an inflight message is redelivered |
| `lease_policy` | lease and visibility-timeout behavior; defaults from `lease_timeout` |
| `maxsize` | maximum number of ready messages; `0` means unbounded |
| `retry_defaults` | Tenacity retry keyword defaults inherited by workers |
| `semantics` | descriptive queue semantics; defaults to `LOCAL_AT_LEAST_ONCE` |
| `locality_policy` | locality behavior; defaults to `LOCAL_QUEUE_PLACEMENT` |
| `acknowledgement_policy` | acknowledgement behavior; defaults to `EXPLICIT_ACKNOWLEDGEMENT` |
| `dead_letter_policy` | dead-letter behavior; defaults to `DEAD_LETTER_QUEUE` |
| `deduplication_policy` | deduplication behavior; defaults to `DEDUPE_KEY_SUPPORT` |
| `consumption_policy` | consumption behavior; defaults to `PULL_CONSUMPTION` |
| `dispatch_policy` | in-process dispatch behavior; defaults to `NO_DISPATCHER` |
| `notification_policy` | wake-up notification behavior; defaults to `NO_NOTIFICATION` |
| `delivery_policy` | delivery behavior; defaults to `AT_LEAST_ONCE_DELIVERY` |
| `ordering_policy` | ready-message ordering behavior; defaults to `FIFO_READY_ORDERING` |
| `routing_policy` | message routing behavior; defaults to `POINT_TO_POINT_ROUTING` |
| `subscription_policy` | subscription fanout behavior; defaults to `NO_SUBSCRIPTIONS` |
| `backpressure` | strategy object for capacity checks; defaults from `maxsize` |
| `policy_set` | reusable bundle of queue policies; conflicts with explicit policy options |

Core methods:

| Method | Meaning |
| --- | --- |
| `put(item, block=True, timeout=None, delay=0.0, priority=0)` | enqueue an item |
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
| `prune_dead_letters(older_than)` | remove dead letters older than the given age |
| `count_dead_letters_older_than(older_than)` | count dead letters older than the given age without removing them |
| `empty()` | whether there are no ready messages |
| `full()` | whether ready capacity is reached |
| `purge()` | remove all queue records |

#### `QueueSemantics`

Descriptive value object for the queueing concepts implemented by a
configuration. The default `LOCAL_AT_LEAST_ONCE` describes the current
`PersistentQueue` behavior: local storage, at-least-once delivery,
point-to-point routing, pull consumption, ready-order delivery, leases,
acknowledgements, dead letters, dedupe-key support, no subscriptions, and no
notifications.

#### `QueuePolicySet`

Reusable bundle for queue policies. Pass `policy_set=` to `PersistentQueue` when
you want to keep locality, leases, acknowledgements, dead letters, delivery,
ordering, routing, consumption, dispatch, notifications, semantics, and
backpressure choices together as one configuration object. Explicit constructor
options remain available and conflict with the same option inside the policy set
so configuration stays unambiguous.

`QueuePolicySet.at_least_once(...)`, `QueuePolicySet.at_most_once(...)`, and
`QueuePolicySet.effectively_once(...)` build common delivery bundles with
optional locality, lease, acknowledgement, dead-letter, deduplication,
consumption, dispatch, notification, ordering, routing, subscription, and
backpressure policies. The effectively-once factory also accepts idempotency,
result, and commit policies.

#### `QoS`

Small enum for fluent queue specs. The built-in values are
`QoS.AT_LEAST_ONCE`, `QoS.AT_MOST_ONCE`, and `QoS.EFFECTIVELY_ONCE`.

#### `QueueSpec`

Fluent builder for common queue and worker settings. Use it when you want one
object to describe queue delivery defaults plus worker retry and pacing
defaults, then build the runtime objects explicitly with `build_queue()` and
`build_worker_config()`. `PersistentQueue.from_spec(spec)` and
`PersistentQueue(spec=spec)` are the matching queue entry points when you want
the queue type to own construction. Use `with_name(...)` when one base spec
should be reused across multiple queue names.

`with_qos(...)` maps to the queue delivery policy, `with_dead_letter_queue()`
maps to the queue dead-letter policy, `with_retry(...)` stores retry defaults
for the queue and worker config, and `with_dead_letter_on_failure(...)`,
`with_release_delay(...)`, `with_min_interval(...)`, and
`with_circuit_breaker(...)` map to
`PersistentWorkerConfig`.

#### `LocalityPolicy`

Protocol for naming where queue state lives relative to the process using the
queue.

#### `LocalQueuePlacement`

Locality policy used by default. It describes the current queue behavior: queue
state is co-located with the process host and no network boundary is crossed to
operate on the queue store.

#### `RemoteQueuePlacement`

Locality policy for queue definitions backed by a remote boundary. It names
`locality="remote"` in the queue configuration and makes the placement explicit
without turning the built-in local store into a distributed broker.

#### `LeasePolicy`

Protocol for naming how messages are leased while a consumer is processing
them.

#### `FixedLeaseTimeout`

Lease policy used by default. `FixedLeaseTimeout(timeout=30.0)` means an
inflight message can be redelivered after the fixed visibility timeout expires.
The `lease_timeout=` constructor option remains the simple shortcut for this
policy.

#### `AcknowledgementPolicy`

Protocol for naming how completed messages are acknowledged.

#### `ExplicitAcknowledgement`

Acknowledgement policy used by default. It describes the current queue behavior:
consumers explicitly acknowledge successful work, and acknowledged messages are
removed from normal queue storage.

#### `DeadLetterPolicy`

Protocol for naming how failed messages leave normal delivery.

#### `DeadLetterQueue`

Dead-letter policy used by default. It describes the current queue behavior:
failed messages can be moved to inspectable dead-letter storage and later
requeued.

#### `DeduplicationPolicy`

Protocol for naming whether the queue tracks stable dedupe keys.

#### `DedupeKeySupport`

Deduplication policy used by default. It describes the current queue behavior:
messages may carry a stable `dedupe_key` and duplicate keys can reuse the same
stored message.

#### `NoDeduplication`

Deduplication policy for queues that should not accept stable dedupe keys. It
names the `deduplication=False` contract explicitly while leaving the rest of
the queue configuration unchanged.

#### `AtLeastOnceDelivery`

Delivery policy used by default. It describes the current queue behavior:
messages are leased before handling, acknowledged after successful handling, and
redelivered if the lease expires before acknowledgement.

#### `AtMostOnceDelivery`

Delivery policy for workflows that prefer losing a message over processing it
more than once. The queue removes the message before returning it from `get()` or
`get_message()`, so handler failures are not redelivered or dead-lettered.

#### `EffectivelyOnceDelivery`

Delivery policy for idempotent workflows. It keeps the at-least-once processing
mechanics, but requires `dedupe_key` on `put()` so producers always provide a
stable identity for the work item. This is the base contract for higher-level
effectively-once features such as idempotency ledgers or cached results.
Pass `idempotency_store=` when you want to attach an explicit store interface for
dedupe/result coordination. When an attached store already marks a
`dedupe_key` as succeeded, worker helpers acknowledge the duplicate delivery and
skip handler execution. Until a future `ResultPolicy` exists, that short-circuit
path returns `None` by default. Pass `result_policy=ReturnStoredResult()` to
persist the successful handler result inline in the idempotency ledger and
return it on duplicate delivery. Pass `result_store=` to `ReturnStoredResult`
when you want result storage to live outside the idempotency ledger.
Pass `commit_policy=` when you want to name the coordination model explicitly.
`LocalAtomicCommit` is the default and matches the current local flow: the
worker records the final result and then acknowledges the queue message. The
other built-in policies are descriptive ports for outbox, two-phase, and
saga-style coordination.

#### `NoResultPolicy`

Default result policy for `EffectivelyOnceDelivery`. It keeps ledger state, but
does not persist or replay handler results.

#### `ReturnStoredResult`

Result policy for `EffectivelyOnceDelivery` that stores successful handler
results inline in the idempotency ledger and returns the cached value when a
duplicate delivery is skipped. When `result_store=` is attached, the policy saves
the handler result there and only keeps a `result_key` in the idempotency
ledger.

#### `CommitPolicy`

Protocol for naming how a successful handler result is coordinated with queue
acknowledgement and external side effects.

#### `LocalAtomicCommit`, `TransactionalOutboxCommit`, `TwoPhaseCommit`, `SagaCommit`

Built-in commit policy variants. `LocalAtomicCommit` is the default. The other
variants are explicit ports for outbox, two-phase, and saga-style coordination.
`TransactionalOutboxCommit` accepts `outbox_store=` for the durable outbox
envelope.
`TwoPhaseCommit` accepts `prepare_store=` and `commit_store=` for explicit
prepare/commit envelopes.
`SagaCommit` accepts `saga_store=` for forward and compensation envelopes.

## localqueue.results

#### `ResultStore`

Protocol for loading, saving, and deleting cached worker results by key.

#### `MemoryResultStore`, `SQLiteResultStore`, `LMDBResultStore`

Built-in result store adapters for in-memory, SQLite, and LMDB-backed cached
worker results.

#### `PullConsumption`

Consumption policy used by default. It describes the current queue behavior:
workers explicitly request messages with `get()`, `get_message()`, or the worker
helpers. Producers only enqueue work; they do not invoke handlers directly.

#### `PushConsumption`

Consumption policy for workflows that model push-based delivery, where a
producer or dispatcher invokes handlers instead of workers polling for messages.
It names the concept with `pattern="push"` and can be used in policy sets while
the current built-in worker helpers remain pull-based.

#### `DispatchPolicy`

Protocol for naming how in-process handlers are invoked after messages are
enqueued.

#### `NoDispatcher`

Dispatch policy used by default. Producers only persist messages; workers or
callers still pull work explicitly.

#### `CallbackDispatcher`

Dispatch policy for local push workflows. It calls one or more in-process
handlers after `put()` persists a message. This is not cross-process
notification; external wake-up mechanisms should be modeled by a notification
adapter.

#### `NotificationPolicy`

Protocol for naming how a queue wakes listeners after a message is persisted.

#### `NoNotification`

Notification policy used by default. The queue remains quiet after `put()`
unless an explicit notification policy is attached.

#### `CallbackNotification`

Notification policy for local push or polling hybrids. It calls in-process
listeners after persistence, which can be used to wake a worker loop or refresh
an in-memory subscriber. Cross-process wake-up still belongs in a separate
adapter.

#### `InProcessNotification`

Notification policy backed by `threading.Event`. `put()` sets the event after
the message is persisted, and local threads can call `wait()` and `clear()` on
the policy to coordinate producer wake-up with consumer polling. It is
in-process only and does not wake other processes.

#### `PointToPointRouting`

Routing policy used by default. It describes the current queue behavior: each
message is leased to one consumer at a time, and publishing a message does not
fan it out to multiple independent subscriber queues.

#### `PublishSubscribeRouting`

Routing policy for workflows that model publish/subscribe fanout. It names the
concept explicitly with `pattern="publish-subscribe"` and `fanout=True`. When
combined with a subscription policy that names subscribers, `put()` fans out
one durable message into each physical subscriber queue.

#### `SubscriptionPolicy`

Protocol for naming whether a queue definition has subscribers and whether
messages should be fanned out.

#### `NoSubscriptions`

Subscription policy used by default. It keeps point-to-point queue definitions
simple: there are no named subscribers and no fanout contract.

#### `StaticFanoutSubscriptions`

Subscription policy for publish/subscribe definitions with a fixed subscriber
set. Combined with `PublishSubscribeRouting`, it materializes one physical queue
per subscriber, for example `events.billing` and `events.audit`.

#### `FifoReadyOrdering`

Ordering policy used by default. It describes the current store ordering:
messages become eligible by `available_at`, and messages with the same
availability keep enqueue order.

#### `PriorityOrdering`

Ordering policy for queues that should deliver higher-priority ready messages
first. Pass `ordering_policy=PriorityOrdering()` to `PersistentQueue`, then use
`put(..., priority=n)` with non-negative integer priorities. Higher numbers are
delivered before lower numbers when messages are available at the same time.
Messages with the same priority keep enqueue order.

#### `BestEffortOrdering`

Ordering policy for workflows that do not require stable delivery order. It
names `guarantee="best-effort"` in the queue configuration and avoids promising
ready-before-delayed, same-timestamp stability, or priority ordering.

#### `BoundedBackpressure`

Capacity strategy used by `PersistentQueue.full()` and blocking `put()` calls.
`BoundedBackpressure(maxsize=0)` is unbounded. Positive values cap the number of
ready messages, matching the existing `maxsize` constructor option. The default
`overflow="block"` preserves `queue.Queue`-style producer behavior.

#### `RejectingBackpressure`

Capacity strategy for producers that should fail immediately when the queue is
full. It uses `overflow="reject"`, so `put()` raises `queue.Full` instead of
waiting for capacity even when `block=True`.

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
| `min_interval` | minimum seconds to wait between worker message starts |
| `circuit_breaker_failures` | consecutive recoverable failures before pausing the worker |
| `circuit_breaker_cooldown` | pause duration after the breaker opens |
| `**retry_kwargs` | forwarded to `PersistentRetrying` |

Queue-level retry defaults can also be attached to `PersistentQueue` and are
merged into worker retry kwargs before explicit worker overrides. That keeps
shared queue policies close to the queue definition while still letting a
worker override a specific retry parameter when needed.

`min_interval` is a per-worker rate limit. Set it when a handler talks to an
external service that should not be hit back-to-back. The circuit-breaker pair
(`circuit_breaker_failures` + `circuit_breaker_cooldown`) opens after repeated
recoverable failures and pauses the worker before it fetches the next message.

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
| `priority` | non-negative priority; higher values are delivered first with `PriorityOrdering` |
| `leased_until` | lease expiration timestamp, if inflight |
| `leased_by` | optional worker id that currently owns the lease, if inflight |
| `dedupe_key` | optional idempotency key used to reuse the same stored message |
| `attempt_history` | list of lease and outcome events recorded for this message |
| `last_error` | structured error from the most recent failed processing attempt, if recorded |
| `failed_at` | timestamp for `last_error`, if recorded |

For `localqueue queue exec` failures, `last_error` also includes `command`,
`exit_code`, `stdout`, and `stderr` fields so command workers can be inspected
from `queue inspect` and `queue dead`. `dedupe_key` is returned on inspection
and lets repeated enqueues reuse the same message until it is acknowledged or
cleaned up. `attempt_history` shows the lease and terminal events that led to
the current state.

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
| `leases_by_worker_id` | historical lease counts grouped by `leased_by`, a coarse throughput proxy |
| `last_seen_by_worker_id` | most recent heartbeat timestamp for each recorded worker id |
| `oldest_ready_age_seconds` | age of the oldest ready message currently waiting |
| `oldest_inflight_age_seconds` | age of the oldest current inflight lease |
| `average_inflight_age_seconds` | average age across current inflight leases |

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

The SQLite store tracks its on-disk schema version with `PRAGMA user_version`.
Current releases migrate older compatible versions and reject future versions
they do not know how to migrate yet.

#### `LMDBQueueStore`

LMDB-backed queue store. Records are serialized as versioned JSON; values must be
JSON-serializable.

#### `MemoryQueueStore`

Thread-safe in-memory queue store for tests.

#### `QueueStoreLockedError`

Raised when LMDB reports that the queue store is locked by another process.

Install `localqueue[cli]` when you want the CLI entry points, and
`localqueue[lmdb]` when you want the LMDB queue store backend.

## localqueue.idempotency

### `IdempotencyRecord`

Value object stored by idempotency adapters. It tracks `status`,
`first_seen_at`, optional `completed_at`, optional `result_key`, and free-form
`metadata`.

### `IdempotencyStore`

Protocol for loading, saving, deleting, and pruning idempotency records by
stable key.

### Built-in stores

- `MemoryIdempotencyStore`
- `SQLiteIdempotencyStore`
- `LMDBIdempotencyStore`

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
| `store_path` | explicit path for a SQLite attempt-store file |
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

#### `close_default_store(all_threads=False)`

Close the default retry store for the current thread. Pass `all_threads=True`
at process shutdown to close known factory-created default stores.

#### `AttemptStore`

Protocol for custom attempt stores.

```python
class AttemptStore:
    def load(self, key: str) -> RetryRecord | None: ...
    def save(self, key: str, record: RetryRecord) -> None: ...
    def delete(self, key: str) -> None: ...
    def prune_exhausted(self, *, older_than: float, now: float) -> int: ...
```

#### `LMDBAttemptStore`

LMDB-backed attempt store.

```python
from localqueue.retry import LMDBAttemptStore

store = LMDBAttemptStore("/var/lib/my-worker/retries")
```

#### `SQLiteAttemptStore`

SQLite-backed attempt store. This is the default backend and does not require
LMDB's native dependency. Retention cleanup uses indexed `exhausted` and
`first_attempt_at` columns instead of scanning every serialized retry record.

```python
from localqueue.retry import SQLiteAttemptStore

store = SQLiteAttemptStore("/var/lib/my-worker/retries.sqlite3")
```

#### `MemoryAttemptStore`

Thread-safe in-memory attempt store for tests and local scenarios.

#### `AttemptStoreLockedError`

Raised when LMDB reports that the attempt store is locked by another process.
