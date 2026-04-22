---
icon: lucide/inbox
---

# Persistent Queues

`localqueue` is the main workflow API in this project. It is a small queue
abstraction backed by SQLite by default and designed for durable, at-least-once
delivery of Python values.

It is local infrastructure: producers and consumers should run where they can
share the same queue store safely. It is not a distributed broker and does not
provide multi-host coordination.

## Creating a queue

```python
from localqueue import PersistentQueue

queue = PersistentQueue(
    "emails",
    store_path="./localqueue_queue.sqlite3",
    lease_timeout=30.0,
)
```

The queue name identifies an independent stream inside the store. Queue names cannot be empty and cannot contain `:`.

## Retry-aware workers

`persistent_worker()` connects a queue to `localqueue`. The queue message id
becomes the retry key, so retry budgets survive process restarts and remain tied
to the leased message.

```python
from localqueue import PersistentQueue, PersistentWorkerConfig, persistent_worker
from tenacity import retry_if_exception_type, stop_after_attempt, wait_exponential

queue = PersistentQueue("jobs")
queue.put({"name": "hello"})

worker_config = PersistentWorkerConfig(
    max_tries=3,
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(ConnectionError),
)


@persistent_worker(queue, config=worker_config)
def handle(job: dict[str, str]) -> str:
    return process(job["name"])


result = handle()
```

On success, the worker acknowledges the message. On final failure, it
dead-letters the message by default after the retry path raises. A final failure
can be retry-budget exhaustion or a non-retryable exception from the configured
Tenacity retry policy.
Worker handlers receive `message.value` as their first argument. Call the handler
with no arguments to consume the next queued message.

Change that behavior with `dead_letter_on_failure=False`. The older
`dead_letter_on_exhaustion` name is still accepted as a compatibility alias.

```python
release_config = PersistentWorkerConfig(
    max_tries=3,
    dead_letter_on_failure=False,
    release_delay=30,
)


@persistent_worker(queue, config=release_config)
def handle(job: dict) -> None:
    ...
```

For small examples, worker options can also be passed directly to the decorator.

Use `persistent_async_worker()` for async handlers. Queue operations are performed
off the event loop with `asyncio.to_thread()`.

## Queue-style usage

For simple workflows, use the familiar `put()`, `get()`, and `task_done()` methods.

```python
queue.put({"to": "user@example.com"})

item = queue.get()
send_email(item["to"])
queue.task_done()
```

`put_nowait()` and `get_nowait()` are available too. Blocking behavior and
timeouts follow Python's `queue.Queue` conventions.

Blocking consumers wait on an in-process condition and poll the store with a
short sleep. This keeps multiple processes compatible with the same store, but
there is no cross-process wake-up notification.

## Message lifecycle

Use the message API when you need explicit acknowledgement, release, or dead-letter behavior.

```python
message = queue.get_message()
try:
    process(message.value)
except RetryLater as exc:
    queue.release(message, delay=10, error=exc)
except Exception as exc:
    queue.dead_letter(message, error=exc)
    raise
else:
    queue.ack(message)
```

Messages move through these states:

| State | Meaning |
| --- | --- |
| ready | available for delivery |
| inflight | leased by a consumer |
| dead | removed from normal delivery |

`get_message()` leases a ready message and increments its delivery attempts. `ack()` removes it permanently. `release()` returns it to the ready queue, optionally after a delay. `dead_letter()` keeps it in storage but hides it from normal delivery. `requeue_dead()` returns a dead-letter message to ready delivery, optionally after a delay.

Use `inspect(message_id)` when you need to read one message without leasing it
or changing its state.

```python
message = queue.inspect("message-id")
```

Pass `error=` to `release()` or `dead_letter()` to persist the processing
failure on the message. Worker decorators and `localqueue queue process`
record this automatically. The next `QueueMessage` includes `last_error` with
the exception type, module, and message, plus `failed_at`.

`localqueue` provides at-least-once delivery. A message can be processed more
than once when a worker crashes, when a lease expires, or when a message is
released for retry. Make handlers idempotent: include a stable job id in the
payload, store side-effect completion in your own database, or pass idempotency
keys to external APIs when they support them.

There is an unavoidable failure window between a successful side effect and
`ack()`. If a process sends an email, charges a card, writes to another service,
or performs another non-idempotent action and then exits before the ack is
stored, the message can be delivered again.

## Leases

Leases protect against worker crashes. If a process leases a message and never acknowledges it, the message becomes ready again after `lease_timeout`.

```python
queue = PersistentQueue("jobs", lease_timeout=60.0)
message = queue.get_message(leased_by="worker-1")
```

`leased_by` is optional metadata for observability. It is stored while the
message is inflight and cleared when the message is acknowledged, released,
dead-lettered, requeued, or reclaimed after lease expiration. The CLI exposes
this as `--worker-id` on `queue pop` and `queue process`.

`get_message()` reclaims expired leases before checking available messages. `qsize()`
counts ready messages at the time of the store read. Expired leases are reclaimed
by `get_message()`, so a message whose lease has expired may not appear as ready
until a consumer attempts to lease work.

## Delayed delivery

Delay initial delivery with `put(..., delay=seconds)`.

```python
queue.put({"id": 1}, delay=30)
```

Delay redelivery with `release(..., delay=seconds)`.

```python
message = queue.get_message()
queue.release(message, delay=5)
```

Delay reprocessing of a dead-letter message with `requeue_dead(..., delay=seconds)`.

```python
message = queue.dead_letters(limit=1)[0]
queue.requeue_dead(message, delay=30)
```

Negative delays raise `ValueError`.

## Stores

The default queue store is SQLite at `./localqueue_queue.sqlite3`.

```python
from localqueue import PersistentQueue, SQLiteQueueStore

store = SQLiteQueueStore("/var/lib/my-worker/queue.sqlite3")
queue = PersistentQueue("jobs", store=store)
```

Install the optional LMDB extra when you want the LMDB backend explicitly.

```bash
pip install "localqueue[lmdb]"
```

```python
from localqueue import LMDBQueueStore, PersistentQueue

store = LMDBQueueStore("/var/lib/my-worker/queue")
queue = PersistentQueue("jobs", store=store)
```

Use `MemoryQueueStore` for tests.

```python
from localqueue import MemoryQueueStore, PersistentQueue

queue = PersistentQueue("jobs", store=MemoryQueueStore())
```

Persistent queue stores serialize records as versioned JSON. Queue values must be
JSON-serializable.

Prefer small payloads. Store large files, blobs, or generated artifacts outside
the queue and enqueue references such as paths, object keys, or database ids.

## Operational boundaries

The SQLite backend is the default because it keeps local workers easy to deploy:
one file persists ready, delayed, inflight, and dead-letter messages. That
simplicity also defines the operating limits.

- Use one shared local SQLite file for producers and consumers on the same host
  or storage boundary.
- Expect SQLite writes to serialize. Multiple lightweight producers and
  consumers are fine for local workloads, but busy workers can contend on the
  same queue file.
- Test your own producer and consumer concurrency before relying on a single
  SQLite file for busy workloads.
- Treat message ordering as best effort when multiple producers or consumers are
  active.
- Use `lease_timeout` values longer than the normal handler runtime, and prefer
  explicit `release(..., delay=...)` for planned retry delays.
- Monitor `stats()`, `dead_letters()`, and retry records for long-running
  processes.
- Back up the queue and retry SQLite files if losing them would lose important
  work. Restore by stopping workers and replacing both files from the same
  backup point.
- Move to Postgres, Redis, SQS, RabbitMQ, Kafka, or another broker when you need
  multi-host coordination, high throughput, retention controls, built-in
  metrics, or mature operational tooling.

## Capacity and cleanup

Set `maxsize` to limit ready queue capacity.

```python
queue = PersistentQueue("jobs", maxsize=1000)
```

Inspect and clean a queue with:

```python
queue.qsize()
queue.stats()
queue.inspect(message_id)
queue.dead_letters()
queue.requeue_dead(message)
queue.empty()
queue.full()
queue.purge()
```

`qsize()` counts messages available for immediate delivery. `stats()` returns
ready, delayed, inflight, dead, and total counts. `inspect()` reads one message
by id without changing state. `dead_letters()` lists dead-letter messages for
inspection. `requeue_dead()` moves a dead-letter message back to the ready queue.
`purge()` removes all records for that queue, including ready, inflight, and
dead-letter records.
