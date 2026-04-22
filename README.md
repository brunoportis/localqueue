# `localqueue`

[![Tests](https://github.com/brunoportis/localqueue/actions/workflows/tests.yml/badge.svg)](https://github.com/brunoportis/localqueue/actions/workflows/tests.yml)
![Coverage](https://img.shields.io/badge/coverage-%E2%89%A595%25-brightgreen)

Durable local queues for Python workers, with persistent retry state powered by
Tenacity.

`localqueue` provides two small building blocks for reliable job processing in
scripts, CLIs, cron jobs, and small worker processes:

- a SQLite-backed queue with at-least-once delivery, leases, delayed delivery,
  acknowledgements, release, and dead-letter records
- `localqueue.retry`: a Tenacity-backed retry adapter that persists retry
  budgets across process restarts

Use the queue when one machine needs to persist jobs and process them later. Use
the retry layer directly when another system already delivers work and you only
need durable retry state.

## Why this exists

Python has good retry tools and in-memory queues, but many worker scripts need a
small durable local queue without bringing in Celery, Redis, RabbitMQ, SQS, or
another broker. This project focuses on that local worker shape:

```text
enqueue job -> lease message -> run handler with retry -> ack or dead-letter
```

Tenacity already provides the right retry model:

- `Retrying` and `AsyncRetrying`
- configurable `stop`, `wait`, and `retry` strategies
- callback hooks such as `before`, `after`, and `before_sleep`
- decorator wrappers with `retry_with`

This library keeps that model and uses it as the retry engine for queue workers
and lower-level retry wrappers.

`localqueue` is not distributed coordination. The default store is a local
SQLite file, so it is best suited to workers running on the same host or against
the same local filesystem. If the workload needs multi-host scheduling, high
write throughput, broker-managed retention, stream processing, or hard
cross-service ordering guarantees, use a broker or database designed for that
operating model.

## Install

```bash
pip install localqueue
```

`localqueue` requires Python 3.11 or newer.

Install the optional CLI dependencies with:

```bash
pip install "localqueue[cli]"
```

Install `localqueue[lmdb]` when you want the optional LMDB queue or retry
stores.

## CLI

The CLI reads YAML configuration from
`~/.config/localqueue/config.yaml`. `XDG_CONFIG_HOME` is respected when set.

```bash
localqueue config init --store-path ./localqueue_queue.sqlite3
localqueue config show
localqueue config set retry_store_path ./localqueue_retries.sqlite3
```

Example config:

```yaml
store_path: ./localqueue_queue.sqlite3
retry_store_path: ./localqueue_retries.sqlite3
```

`store_path` is the SQLite queue file. `retry_store_path` is the SQLite file
used by `queue process` to persist retry attempts.

The CLI starts with queue management commands. Values are JSON by default.

```bash
localqueue queue add emails --value '{"to":"user@example.com"}'
echo '{"to":"user@example.com"}' | localqueue queue add emails
localqueue queue size emails
localqueue queue stats emails
localqueue queue stats emails --watch --interval 1
localqueue queue inspect emails <message-id>
localqueue queue dead emails
localqueue queue requeue-dead emails <message-id>
localqueue queue pop emails --worker-id worker-1
localqueue queue ack emails <message-id>
localqueue queue exec emails -- python scripts/send_email.py
```

Use `--raw` when the queue value should be stored as a string:

```bash
localqueue queue add emails --value user@example.com --raw
```

Queue values are stored as JSON in the queue store, so values must be
JSON-serializable.

To process queued messages, pass an importable handler in `module:function`
format. The handler receives the message value as its first argument. Successful
handlers ack the message. Failing handlers release the message unless the
persistent retry budget is exhausted, in which case the message is moved to
dead-letter storage by default.

```bash
localqueue queue process emails myapp.workers:send_email --max-tries 5
```

Use `queue exec` when the handler is an external command. The message value is
written to the command's stdin as JSON. Exit code `0` acknowledges the message;
any other exit code is treated as a processing failure and follows the same
retry, release, and dead-letter rules as `queue process`.

```bash
localqueue queue exec emails -- python scripts/send_email.py
localqueue queue exec webhooks -- curl -X POST https://example.com/hook -d @-
localqueue queue exec emails -- sh -c 'jq -r .to | xargs -I{} curl https://example.com/{}'
```

Command output is captured so the CLI can keep printing its own JSON status.
When a command fails, `last_error` includes the command, exit code, stdout, and
stderr, truncated for inspection.

Use `--forever` for a long-running worker. When interrupted with `SIGINT` or
`SIGTERM`, the CLI finishes the current message before stopping.

```bash
localqueue queue process emails myapp.workers:send_email \
  --forever \
  --block \
  --worker-id worker-1 \
  --max-tries 5
```

`--worker-id` is recorded on leased messages as `leased_by`, which makes
`queue inspect` useful when multiple workers consume the same queue.

## Quickstart

Run this from the repository root after installing the optional CLI
dependencies. It enqueues one email job, processes it with the example handler,
and leaves no external services running.

```bash
localqueue queue add emails \
  --store-path /tmp/localqueue-demo \
  --value '{"to":"user@example.com"}'

localqueue queue process emails examples.email_worker:send_email \
  --store-path /tmp/localqueue-demo \
  --retry-store-path /tmp/localqueue-demo-retries.sqlite3 \
  --worker-id worker-1 \
  --max-tries 3
```

Inspect worker ownership while a message is leased:

```bash
localqueue queue inspect emails <message-id> \
  --store-path /tmp/localqueue-demo
```

## Queue worker

```python
from localqueue import PersistentQueue, PersistentWorkerConfig, persistent_worker
from tenacity import retry_if_exception_type, stop_after_attempt, wait_exponential


queue = PersistentQueue("emails", store_path="./localqueue_queue.sqlite3")
queue.put({"to": "user@example.com"})

worker_config = PersistentWorkerConfig(
    max_tries=5,
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type(ConnectionError),
)


@persistent_worker(queue, config=worker_config)
def send_email_job(job: dict[str, str]) -> None:
    send_email(job["to"])


send_email_job()
```

The worker leases one message, runs the handler with persistent retry state, and
acknowledges the message on success. If processing keeps failing until the retry
budget is exhausted, the message is moved to dead-letter storage by default.
Worker handlers receive `message.value` as their first argument.
Pass `dead_letter_on_failure=False` to `PersistentWorkerConfig` or
`persistent_worker()` when final handler failures should release the message
instead.

Use `persistent_async_worker()` for async handlers.

## Manual queue control

```python
from localqueue import PersistentQueue

queue = PersistentQueue("emails", store_path="./localqueue_queue.sqlite3")

queue.put({"to": "user@example.com"})

message = queue.get_message()
try:
    send_email(message.value["to"])
except RetryLater as exc:
    queue.release(message, delay=30, error=exc)
except Exception as exc:
    queue.dead_letter(message, error=exc)
    raise
else:
    queue.ack(message)
```

`localqueue` uses at-least-once delivery:

- `put()` persists before returning
- `get_message()` leases a message and moves it to `inflight`
- `ack()` removes the message permanently
- `release()` makes it available again, optionally after a delay
- expired leases are returned to the ready queue
- `dead_letter()` moves a message out of normal delivery
- `requeue_dead()` returns a dead-letter message to ready delivery

Handlers must be safe to run more than once. A worker can crash after an
external side effect succeeds but before `ack()` is persisted, and the message
will be delivered again after the lease expires. Store an idempotency key in the
payload, pass it to external APIs when possible, or check your own database
before repeating non-idempotent side effects.

When a worker fails, `last_error` and `failed_at` are stored on the message so
CLI failures and redelivered messages show why processing failed.
Use `queue stats` when you need ready, delayed, inflight, dead-letter, and total
counts instead of only ready messages. Use `queue inspect` to inspect one
message by id, `queue dead` to list dead-letter messages, and
`queue requeue-dead` to retry one after the failure cause is fixed.
Use `queue stats --watch` to print those counts repeatedly while local workers
are running.

## Operational boundaries

The default SQLite backend is intentionally simple. It is practical for local
worker processes, small teams, development tools, and operational scripts, but
it is not a drop-in replacement for a distributed queue.

- Run producers and consumers where they can safely share the same SQLite file.
- Use small JSON payloads; store large files externally and enqueue references.
- Treat ordering as best effort once multiple producers or consumers are active.
- Keep handlers idempotent because delivery is at least once.
- Monitor `queue stats`, `queue dead`, and store file growth for long-running
  deployments.
- Back up the SQLite files if queued work or retry state matters after host
  loss.
- Move to Postgres, Redis, SQS, RabbitMQ, Kafka, or a similar system when you
  need multi-host coordination, high concurrency, retention controls, metrics,
  or managed operations.

The [Operational maturity](docs/operational-maturity.md) checklist tracks the
remaining hardening work before describing this as a mature production queue
system.

## Persistent retry layer

Use `localqueue.retry` directly when the job source is not `localqueue`.

```python
from localqueue.retry import key_from_argument, persistent_retry
from tenacity import retry_if_exception_type, stop_after_attempt, wait_fixed


@persistent_retry(
    key_fn=key_from_argument("email_id"),
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(ConnectionError),
)
def send_email(email_id: str, address: str) -> None:
    deliver(address)
```

Every persistent retry needs an explicit key. Use one of these forms:

- `key="email:42"` for a retryer bound to one logical job
- `key_fn=key_from_argument("email_id")` for scalar IDs
- `key_fn=key_from_attr("task", "id", prefix="process-video")` for object attributes
- `key_fn=idempotency_key_from_id("task", prefix="process-video")` as shorthand for `task.id`

When a retry budget is exhausted, later calls with the same key raise
`PersistentRetryExhausted` before the wrapped function is called again.

## Stores

The default queue store is SQLite at `./localqueue_queue.sqlite3`.

The default retry store is SQLite at `./localqueue_retries.sqlite3`.
The CLI `retry_store_path` setting also uses SQLite. In the Python retry API,
`PersistentRetrying(store_path=...)` selects an optional LMDB attempt-store
directory; pass `store=SQLiteAttemptStore("retries.sqlite3")` when you want a
SQLite file explicitly.

LMDB remains available as an optional backend through `localqueue[lmdb]`
and explicit `LMDBQueueStore` or `LMDBAttemptStore` usage.

For tests, use `MemoryQueueStore` and `MemoryAttemptStore`.

## Documentation

- [Persistent queues](docs/queues.md): message lifecycle, workers, leases, delay, and dead-letter behavior.
- [Persistent retries](docs/retries.md): decorators, low-level retryers, keys, stores, and exhaustion behavior.
- [API reference](docs/api.md): exported classes, functions, and protocols.
- [Operational maturity](docs/operational-maturity.md): documented limits and checklist for future production-hardening work.
- [Release checklist](docs/release.md): manual versioning, build, smoke test, and publish steps.

## License

`localqueue` is distributed under the MIT license. See [LICENSE](LICENSE).
