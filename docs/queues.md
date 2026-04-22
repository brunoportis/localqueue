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

`localqueue` treats validation-style failures such as `ValueError`, `TypeError`,
`KeyError`, `IndexError`, and missing command execution as permanent failures.
Those are dead-lettered even when the worker is configured to release on final
failure.

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

## Command workers

Use `localqueue queue exec` when a queued message should be processed by an
external command instead of an importable Python function.

```bash
localqueue queue exec jobs -- python scripts/process_job.py
localqueue queue exec webhooks -- curl -X POST https://example.com/hook -d @-
localqueue queue exec jobs -- sh -c 'jq -r .id | xargs -I{} ./process-job {}'
localqueue queue exec webhooks -- sh examples/process_webhook.sh
```

The command receives `message.value` on stdin as JSON. `localqueue` captures the
command output so the CLI can keep printing its own JSON status. Exit code `0`
acks the message. Any other exit code raises a command failure and applies the
same retry, release, or dead-letter behavior as `queue process`.

Command failures are stored in `last_error` with structured fields:

| Field | Meaning |
| --- | --- |
| `type` | `_CommandExecutionError` |
| `message` | human-readable command failure |
| `command` | argv list that was executed |
| `exit_code` | command exit code |
| `stdout` | captured stdout, truncated for inspection |
| `stderr` | captured stderr, truncated for inspection |

Commands are executed without an implicit shell. Use `sh -c` explicitly when you
want pipes, redirects, shell expansion, or multiple commands.
See `examples/process_webhook.sh` for a shell worker that reads JSON, extracts
fields with `jq`, and posts them with `curl`.

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
the exception type, module, and message, plus `failed_at`. It also carries an
`attempt_history` list with lease and outcome events so inspection can show the
path a message took through the queue.

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

### SQLite housekeeping

SQLite files can keep running for a long time, but they still need normal file
maintenance.

- Keep the main `*.sqlite3` file and its retry store together when you back up
  or restore a queue.
- If you use WAL mode, expect `-wal` and `-shm` files alongside the database
  file. Restore the whole set, not just the main file.
- Run `VACUUM` when you want to reclaim space after a lot of deletes or
  dead-letter churn.
- Stop producers and consumers before copying the store files to another
  location.
- If the store looks corrupted or partially written, restore from the latest
  known-good backup instead of trying to patch records by hand.

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
ready, delayed, inflight, dead, and total counts, plus `by_worker_id` for
current inflight leases and `leases_by_worker_id` for historical lease counts.
It also reports `oldest_ready_age_seconds`, `oldest_inflight_age_seconds`, and
`average_inflight_age_seconds` so you can spot backlog and stuck workers from
the terminal. `oldest_inflight_age_seconds` is a coarse proxy for handler
duration on messages that are still running.
`inspect()` reads one message by id without changing state. `dead_letters()`
lists dead-letter messages for inspection. `requeue_dead()` moves a dead-letter
message back to the ready queue. `purge()` removes all records for that queue,
including ready, inflight, and dead-letter records.

From the CLI, use `queue stats --watch` to monitor those counts while workers
are running:

```bash
localqueue queue stats jobs --watch --interval 1
```

Each sample is printed as JSON with `ready`, `delayed`, `inflight`, `dead`, and
`total` counts, plus `by_worker_id` for current inflight leases and
`leases_by_worker_id` for historical lease counts. The historical count is a
coarse throughput proxy, not a completion metric. Stop the watch with `Ctrl-C`.

Use `queue dead --watch` to keep an eye on recent failures:

```bash
localqueue queue dead jobs --watch --interval 2
```

Use `queue dead --summary` when you want a quick aggregate view, and combine it
with `--min-attempts`, `--max-attempts`, `--error-contains`, or
`--failed-within` when the dead-letter list is noisy. The summary groups by
error type, attempt count, and worker id that last leased the message.

## Safe shutdown

`queue process` and `queue exec` handle `SIGINT` and `SIGTERM` by finishing the
current message and then stopping before the next lease attempt. That keeps the
lease, retry state, and final ack/release/dead-letter transition consistent.

```bash
localqueue queue process emails myapp.workers:send_email \
  --forever \
  --block \
  --worker-id worker-1 \
  --max-tries 5
```

Use `Ctrl-C` for interactive workers. Use `SIGTERM` for supervised workers
started by systemd, Docker, or another process manager.

The CLI rejects `--forever` together with `--max-jobs`; use batch mode or
continuous mode, not both.

When you need a controlled stop in your own wrapper, forward the signal and let
the worker exit on its own instead of killing the process immediately.

```bash
#!/usr/bin/env bash
set -euo pipefail

localqueue queue exec emails --forever --block -- python scripts/send_email.py
```

## Health checks

There is no separate health endpoint in the library. For local workers, a small
command check is usually enough:

```bash
localqueue queue stats emails --json
```

Use that to confirm the queue store is reachable and the queue counts can be
read. For a more specific check, inspect a known message id:

```bash
localqueue queue inspect emails <message-id> --json
```

If you want to check the worker loop itself, combine `queue stats --watch` with
`queue dead --watch` while the worker is running and verify that new messages
move through ready, inflight, and acked states as expected.

When the underlying problem is fixed, `queue requeue-dead --all` moves every
dead letter back to ready delivery:

```bash
localqueue queue requeue-dead jobs --all
```
