# `persistentretry`

[![Tests](https://github.com/brunoportis/persistentretry/actions/workflows/tests.yml/badge.svg)](https://github.com/brunoportis/persistentretry/actions/workflows/tests.yml)
![Coverage](https://img.shields.io/badge/coverage-%E2%89%A595%25-brightgreen)

Durable queues for Python, with persistent retry state powered by Tenacity.

`persistentretry` provides two small building blocks for reliable local job
processing:

- `persistentqueue`: a SQLite-backed queue with at-least-once delivery, leases,
  delayed delivery, acknowledgements, release, and dead-letter records.
- `persistentretry`: a retry layer around
  [`tenacity`](https://tenacity.readthedocs.io/en/latest/) that persists retry
  budgets across process restarts.

Use the queue when you have jobs to deliver and process. Use the retry layer
directly when you already have a delivery mechanism and only need durable retry
state.

## Why this exists

Python has good retry tools and in-memory queues, but many worker scripts need a
small durable queue without bringing in a broker. This project focuses on that
local worker shape:

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

## Install

```bash
pip install persistentretry
```

`persistentretry` requires Python 3.11 or newer.

Install the optional CLI dependencies with:

```bash
pip install "persistentretry[cli]"
```

Install `persistentretry[lmdb]` when you want the optional LMDB queue or retry
stores.

## CLI

The CLI reads YAML configuration from
`~/.config/persistentretry/config.yaml`. `XDG_CONFIG_HOME` is respected when set.

```bash
persistentretry config init --store-path ./persistence_queue.sqlite3
persistentretry config show
persistentretry config set retry_store_path ./persistence_retries.sqlite3
```

Example config:

```yaml
store_path: ./persistence_queue.sqlite3
retry_store_path: ./persistence_retries.sqlite3
```

`store_path` is the SQLite queue file. `retry_store_path` is the SQLite file
used by `queue process` to persist retry attempts.

The CLI starts with queue management commands. Values are JSON by default.

```bash
persistentretry queue add emails --value '{"to":"user@example.com"}'
echo '{"to":"user@example.com"}' | persistentretry queue add emails
persistentretry queue size emails
persistentretry queue stats emails
persistentretry queue inspect emails <message-id>
persistentretry queue dead emails
persistentretry queue requeue-dead emails <message-id>
persistentretry queue pop emails --worker-id worker-1
persistentretry queue ack emails <message-id>
```

Use `--raw` when the queue value should be stored as a string:

```bash
persistentretry queue add emails --value user@example.com --raw
```

Queue values are stored as JSON in the queue store, so values must be
JSON-serializable.

To process queued messages, pass an importable handler in `module:function`
format. The handler receives the message value as its first argument. Successful
handlers ack the message. Failing handlers release the message unless the
persistent retry budget is exhausted, in which case the message is moved to
dead-letter storage by default.

```bash
persistentretry queue process emails myapp.workers:send_email --max-tries 5
```

Use `--forever` for a long-running worker. When interrupted with `SIGINT` or
`SIGTERM`, the CLI finishes the current message before stopping.

```bash
persistentretry queue process emails myapp.workers:send_email \
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
persistentretry queue add emails \
  --store-path /tmp/persistentretry-demo \
  --value '{"to":"user@example.com"}'

persistentretry queue process emails examples.email_worker:send_email \
  --store-path /tmp/persistentretry-demo \
  --retry-store-path /tmp/persistentretry-demo-retries.sqlite3 \
  --worker-id worker-1 \
  --max-tries 3
```

Inspect worker ownership while a message is leased:

```bash
persistentretry queue inspect emails <message-id> \
  --store-path /tmp/persistentretry-demo
```

## Queue worker

```python
from persistentqueue import PersistentQueue, PersistentWorkerConfig, persistent_worker
from tenacity import retry_if_exception_type, stop_after_attempt, wait_exponential


queue = PersistentQueue("emails", store_path="./persistence_queue.sqlite3")
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
from persistentqueue import PersistentQueue

queue = PersistentQueue("emails", store_path="./persistence_queue.sqlite3")

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

`persistentqueue` uses at-least-once delivery:

- `put()` persists before returning
- `get_message()` leases a message and moves it to `inflight`
- `ack()` removes the message permanently
- `release()` makes it available again, optionally after a delay
- expired leases are returned to the ready queue
- `dead_letter()` moves a message out of normal delivery
- `requeue_dead()` returns a dead-letter message to ready delivery

When a worker fails, `last_error` and `failed_at` are stored on the message so
CLI failures and redelivered messages show why processing failed.
Use `queue stats` when you need ready, delayed, inflight, dead-letter, and total
counts instead of only ready messages. Use `queue inspect` to inspect one
message by id, `queue dead` to list dead-letter messages, and
`queue requeue-dead` to retry one after the failure cause is fixed.

## Persistent retry layer

Use `persistentretry` directly when the job source is not `persistentqueue`.

```python
from persistentretry import key_from_argument, persistent_retry
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

The default queue store is SQLite at `./persistence_queue.sqlite3`.

The default retry store is SQLite at `./persistence_db.sqlite3`.
The CLI `retry_store_path` setting also uses SQLite. In the Python retry API,
`PersistentRetrying(store_path=...)` selects an optional LMDB attempt-store
directory; pass `store=SQLiteAttemptStore("retries.sqlite3")` when you want a
SQLite file explicitly.

LMDB remains available as an optional backend through `persistentretry[lmdb]`
and explicit `LMDBQueueStore` or `LMDBAttemptStore` usage.

For tests, use `MemoryQueueStore` and `MemoryAttemptStore`.

## Documentation

- [Persistent queues](docs/queues.md): message lifecycle, workers, leases, delay, and dead-letter behavior.
- [Persistent retries](docs/retries.md): decorators, low-level retryers, keys, stores, and exhaustion behavior.
- [API reference](docs/api.md): exported classes, functions, and protocols.
- [Release checklist](docs/release.md): manual versioning, build, smoke test, and publish steps.

## License

`persistentretry` is distributed under the MIT license. See [LICENSE](LICENSE).
