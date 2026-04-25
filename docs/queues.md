---
icon: lucide/inbox
---

# Persistent Queues

`localqueue` queues are for local workflows where you want to accept work now,
run it later on the same machine, and keep enough state to inspect and recover
failures from the terminal.

This is the main workflow API in the project. It is a small queue abstraction
backed by SQLite by default and designed for durable, at-least-once delivery of
Python values.

It is local infrastructure: producers and consumers should run where they can
share the same queue store safely. It is not a distributed broker and does not
provide multi-host coordination.

Best-fit queue workflows:

- local outbox patterns for email, webhooks, uploads, or report generation
- CLI-driven workers that should survive restarts
- jobs that may need dead-letter inspection and replay

Start somewhere else if the queue itself needs to be a distributed system
boundary. `localqueue` keeps delivery local, recovery local, and operations
simple.

If you have independent workloads, give them separate queue names instead of
loading everything into one queue. Each queue keeps its own stats, dead
letters, retention settings, and worker identity history.

## Creating a queue

```python
from localqueue import PersistentQueue

queue = PersistentQueue(
    "emails",
    lease_timeout=30.0,
    retry_defaults={
        "max_tries": 3,
    },
)
```

The queue name identifies an independent stream inside the store. Queue names
cannot be empty and cannot contain `:`.
Queue-level retry defaults are merged into worker retry kwargs before explicit
worker overrides, so the same policy can be reused across several workers that
consume the same queue.

By default, a queue also exposes the semantics it implements:

```python
queue.semantics.as_dict()
```

The default semantics are local, at-least-once, point-to-point, pull-based
delivery with leases, acknowledgements, dead letters, and optional dedupe keys.
This is intentionally descriptive: it names the queueing concepts behind the
small API without requiring users to configure them before they need to.

The delivery behavior is also available as a policy object:

```python
queue.delivery_policy.as_dict()
```

The default `AT_LEAST_ONCE_DELIVERY` policy means a message is leased before
handling, acknowledged after successful handling, and redelivered if the lease
expires before acknowledgement.

Use `AtMostOnceDelivery` when duplicate processing is worse than losing work.

```python
from localqueue import AtMostOnceDelivery, PersistentQueue

queue = PersistentQueue("telemetry", delivery_policy=AtMostOnceDelivery())
```

With this policy, `get()` and `get_message()` remove the message before returning
it. If the handler crashes after delivery, the message is not redelivered and is
not moved to dead letters.

Use `EffectivelyOnceDelivery` when producers can provide a stable idempotency key.

```python
from localqueue import EffectivelyOnceDelivery, PersistentQueue

queue = PersistentQueue("payments", delivery_policy=EffectivelyOnceDelivery())
queue.put({"payment_id": "pay_123"}, dedupe_key="payment:pay_123")
```

This policy currently makes `dedupe_key` mandatory on `put()`. It keeps the
at-least-once delivery flow, but turns stable enqueue identity into an explicit
contract so future idempotency/result stores can build on it.

If you want to start wiring that coordination explicitly, attach an
`idempotency_store` to the delivery policy:

```python
from localqueue import EffectivelyOnceDelivery, PersistentQueue, SQLiteIdempotencyStore

queue = PersistentQueue(
    "payments",
    delivery_policy=EffectivelyOnceDelivery(
        idempotency_store=SQLiteIdempotencyStore("payments-idempotency.sqlite3")
    ),
)
```

With an attached store, worker helpers now record `pending`, `succeeded`, and
`failed` states keyed by `dedupe_key`. If a duplicate delivery arrives for a key
already marked `succeeded`, the worker acknowledges it and skips handler
execution.

By default, that short-circuit returns `None`. If you want the worker to replay
the stored success value, attach `ReturnStoredResult()`:

```python
from localqueue import (
    EffectivelyOnceDelivery,
    PersistentQueue,
    ReturnStoredResult,
    SQLiteIdempotencyStore,
)

queue = PersistentQueue(
    "payments",
    delivery_policy=EffectivelyOnceDelivery(
        idempotency_store=SQLiteIdempotencyStore("payments-idempotency.sqlite3"),
        result_policy=ReturnStoredResult(),
    ),
)
```

This first version stores the successful handler result inline in the
idempotency ledger, so built-in stores expect it to be JSON-serializable.

If you want the result payload stored separately from the ledger, attach a
`result_store` to the result policy:

```python
from localqueue import (
    EffectivelyOnceDelivery,
    PersistentQueue,
    ReturnStoredResult,
    SQLiteIdempotencyStore,
    SQLiteResultStore,
)

queue = PersistentQueue(
    "payments",
    delivery_policy=EffectivelyOnceDelivery(
        idempotency_store=SQLiteIdempotencyStore("payments-idempotency.sqlite3"),
        result_policy=ReturnStoredResult(
            result_store=SQLiteResultStore("payments-results.sqlite3")
        ),
    ),
)
```

In that mode, the idempotency ledger keeps the status and `result_key`, and the
result payload itself lives in the configured result store.

The commit policy is available too:

```python
from localqueue import (
    EffectivelyOnceDelivery,
    LocalAtomicCommit,
    PersistentQueue,
    SQLiteIdempotencyStore,
)

queue = PersistentQueue(
    "payments",
    delivery_policy=EffectivelyOnceDelivery(
        idempotency_store=SQLiteIdempotencyStore("payments-idempotency.sqlite3"),
        commit_policy=LocalAtomicCommit(),
    ),
)
```

`LocalAtomicCommit` matches the current local behavior. The other built-in
commit policies are named ports for outbox, two-phase, and saga coordination.
`TransactionalOutboxCommit(outbox_store=...)` writes a durable outbox envelope
before the queue message is acknowledged.
`TwoPhaseCommit(prepare_store=..., commit_store=...)` writes explicit prepare
and commit envelopes before the queue message is acknowledged.

The consumption behavior is available as a policy object:

```python
queue.consumption_policy.as_dict()
```

The default `PULL_CONSUMPTION` policy means workers explicitly request messages.
Producers enqueue work, but they do not invoke handlers directly.

The routing behavior is available as a policy object:

```python
queue.routing_policy.as_dict()
```

The default `POINT_TO_POINT_ROUTING` policy means each message is leased to one
consumer at a time. Enqueueing a message does not fan it out to multiple
independent subscriber queues.

The ready-message ordering is available as a policy object too:

```python
queue.ordering_policy.as_dict()
```

The default `FIFO_READY_ORDERING` policy means messages become eligible by
`available_at`, and messages with the same availability keep enqueue order.

Use `PriorityOrdering` when some ready messages should be delivered before
others.

```python
from localqueue import PersistentQueue, PriorityOrdering

queue = PersistentQueue("jobs", ordering_policy=PriorityOrdering())
queue.put({"kind": "report"}, priority=10)
queue.put({"kind": "cleanup"}, priority=1)
```

Higher priority values are delivered first when messages are available at the
same time. Messages with the same priority keep enqueue order.

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

For shared queue-wide retry defaults, attach `retry_defaults` to the
`PersistentQueue` and let workers inherit those Tenacity arguments unless the
worker passes explicit overrides.

`localqueue` keeps built-in permanent-failure classification conservative.
Import/name-resolution failures and missing command execution are dead-lettered
even when the worker is configured to release on final failure. Runtime
validation errors such as `ValueError`, `TypeError`, and `KeyError` follow the
configured worker policy because they can represent either bad input or a
recoverable transient condition.

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

When a worker talks to a slow or fragile external service, add a small
`min_interval` to keep requests from landing back-to-back. If the service keeps
failing, use `circuit_breaker_failures` with `circuit_breaker_cooldown` so the
worker pauses before it fetches the next message.

```python
from localqueue import PersistentQueue, PersistentWorkerConfig, persistent_worker
from tenacity import retry_if_exception_type, wait_fixed

queue = PersistentQueue("webhooks")
queue.put({"url": "https://example.com/hook"})

config = PersistentWorkerConfig(
    max_tries=3,
    wait=wait_fixed(2),
    retry=retry_if_exception_type((ConnectionError, TimeoutError)),
    min_interval=0.5,
    circuit_breaker_failures=5,
    circuit_breaker_cooldown=30,
)


@persistent_worker(queue, config=config)
def deliver_webhook(payload: dict[str, str]) -> None:
    post_json(payload["url"], payload)
```

Use a small `min_interval` when the downstream API should not be hit in quick
bursts. Use the breaker when repeated recoverable failures should pause the
worker long enough for the dependency to recover or for an operator to inspect
the failure mode.

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

## Operational flow

A small end-to-end queue flow usually looks like this:

```bash
localqueue queue add webhooks --value '{"url":"https://example.com/hook"}'
localqueue queue exec webhooks -- sh examples/process_webhook.sh
localqueue queue health webhooks
localqueue queue dead webhooks --summary
localqueue retry prune --dry-run --older-than 604800
```

The same flow also works as a shell script when you want a repeatable terminal
recipe:

```bash
#!/usr/bin/env bash
set -euo pipefail

localqueue queue add webhooks --value '{"url":"https://example.com/hook"}'
localqueue queue exec webhooks -- sh examples/process_webhook.sh
localqueue queue stats webhooks --watch --interval 1
```

Pass `--log-events` on the CLI when you want structured JSON transition events
on stderr for enqueue, lease, ack, release, dead-letter, requeue, and purge.

If the producer can replay work, pass a stable `dedupe_key` so repeated
enqueues reuse the same stored message until it is acknowledged or cleaned up.

```bash
localqueue queue add webhooks \
  --dedupe-key webhook-123 \
  --value '{"url":"https://example.com/hook"}'
```

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

## Retry timing

`lease_timeout` should outlive the normal handler runtime, including any retry
work that happens inside the handler. `release_delay` is for when the worker
hands the message back to the queue and you want the next delivery to wait a
bit. Tenacity `wait` is for backoff between retry attempts inside one handler
call. In practice:

- raise `lease_timeout` when the work is simply taking longer than expected
- use Tenacity `wait` when a retryable error should pause before the next
  attempt inside the same call
- use `release_delay` when the queue should hold the message before the next
  delivery after the worker gives up for now

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

The default queue store is SQLite at
`$XDG_DATA_HOME/localqueue/queue.sqlite3`. When `XDG_DATA_HOME` is not set,
the fallback is `~/.local/share/localqueue/queue.sqlite3`.

Pass `store_path=` when the queue file must live somewhere explicit, such as a
service data directory.

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

When you use `dedupe_key`, the key is stored with the message and returned on
inspection. Reusing the same key returns the original message for that queue
until the stored record is removed by `ack()`, `purge()`, or retention cleanup.

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

The SQLite store keeps a schema version in `PRAGMA user_version`. Current
releases migrate older compatible versions and reject future versions they do
not know how to migrate yet.

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

For code that wants to make backpressure a named strategy, use the equivalent
`BoundedBackpressure` object:

```python
from localqueue import BoundedBackpressure, PersistentQueue

queue = PersistentQueue("jobs", backpressure=BoundedBackpressure(1000))
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

Use `queue dead --prune-older-than` to remove old dead letters after you have
inspected them:

```bash
localqueue queue dead emails --prune-older-than 86400
```

Use `--dry-run` with cleanup commands when you want to preview how many records
would be removed without touching the store yet:

```bash
localqueue queue dead emails --dry-run --prune-older-than 86400
localqueue retry prune --dry-run --older-than 604800
```

If you want cleanup defaults to live in config instead of on the command line,
set `dead_letter_ttl_seconds` and `retry_record_ttl_seconds` with
`localqueue config set`.
Negative TTL values are rejected at config update time.

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

For a compact one-shot summary, use `queue health`:

```bash
localqueue queue health emails --stale-after 120
```

It combines queue stats, worker liveness, dead-letter summary, and the
configured retention values so you can see the state and the cleanup policy in
one pass. `queue stats --stale-after 120` exposes the same worker liveness view
inline with the queue counters when you want a watch loop instead of a one-shot
summary.
