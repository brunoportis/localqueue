---
icon: lucide/inbox
---

# localqueue

`localqueue` provides durable local queues for Python workers, with
persistent retry state powered by [Tenacity](https://tenacity.readthedocs.io/en/latest/).

The main entry point is `localqueue.PersistentQueue`: a SQLite-backed queue for
at-least-once job delivery. The lower-level `localqueue.retry` subdomain remains
available when you only need durable retry budgets around an existing delivery
mechanism.

The default model is local-file storage, not distributed coordination. Use it
for scripts, CLIs, cron jobs, development tools, and small worker processes that
can share one local store. Use a broker or external database when jobs need
multi-host scheduling, high write concurrency, managed retention, or stronger
cross-service delivery guarantees.

## Install

```bash
pip install localqueue
```

The package requires Python 3.11 or newer.

## Basic queue worker

Install the optional CLI dependencies when you want to operate queues from the
terminal.

```bash
pip install "localqueue[cli]"
```

Run one queued message with an importable handler:

```bash
localqueue queue process emails myapp.workers:send_email --max-tries 5
```

Run one queued message with an external command:

```bash
localqueue queue exec emails -- python scripts/send_email.py
```

`queue exec` writes the message value to the command's stdin as JSON. Exit code
`0` acknowledges the message. Any other exit code is treated as a failed handler
attempt and follows the configured retry, release, and dead-letter policy.
Command failures are recorded in `last_error` with the command, exit code,
stdout, and stderr.

Run a continuous local worker with `--forever`. `SIGINT` and `SIGTERM` request a
graceful stop after the current message finishes.

```bash
localqueue queue process emails myapp.workers:send_email \
  --forever \
  --block \
  --worker-id worker-1 \
  --max-tries 5
```

Watch queue counts while workers run:

```bash
localqueue queue stats emails --watch --interval 1
```

For a local smoke test, enqueue one job and process it with the bundled example
handler:

```bash
localqueue queue add emails \
  --store-path /tmp/localqueue-demo.sqlite3 \
  --value '{"to":"user@example.com"}'

localqueue queue process emails examples.email_worker:send_email \
  --store-path /tmp/localqueue-demo.sqlite3 \
  --retry-store-path /tmp/localqueue-demo-retries.sqlite3 \
  --worker-id worker-1 \
  --max-tries 3
```

```python
from localqueue import PersistentQueue, PersistentWorkerConfig, persistent_worker
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt, wait_exponential


queue = PersistentQueue("emails", store_path="./localqueue_queue.sqlite3")
queue.put({"to": "user@example.com"})

worker_config = PersistentWorkerConfig(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(ConnectionError),
)


@persistent_worker(queue, config=worker_config)
def send_email_job(job: dict[str, str]) -> None:
    deliver(job["to"])
```

Calling `send_email_job()` leases one message, runs the handler with a persistent
retry budget, and acknowledges the message on success. If the process exits, the
message lease and retry state survive in storage. Worker handlers receive
`message.value` as their first argument.

Delivery is at least once. Handlers should be idempotent because a worker can
finish an external side effect and then crash before `ack()` is stored.

## Manual queue control

Use the explicit message API when the handler needs to decide between
acknowledging, releasing, or dead-lettering a message.

```python
from localqueue import PersistentQueue

queue = PersistentQueue("emails", store_path="./localqueue_queue.sqlite3")
queue.put({"to": "user@example.com"})

message = queue.get_message()
try:
    send_email(message.id, message.value["to"])
except Exception:
    queue.release(message)
else:
    queue.ack(message)
```

Use `ack()` after successful processing. Use `release()` to make a leased message available again. Use `dead_letter()` when the message should leave normal delivery.

## Direct retry usage

Use `localqueue.retry` directly when the queued message lifecycle is handled by
another system and you only need retry state to survive restarts.

```python
from localqueue.retry import idempotency_key_from_id, persistent_retry
from tenacity import stop_after_attempt, wait_exponential


class EmailTask:
    def __init__(self, task_id: str, address: str) -> None:
        self.id = task_id
        self.address = address


@persistent_retry(
    key_fn=idempotency_key_from_id("task", prefix="email"),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=8),
)
def send_email(task: EmailTask) -> None:
    deliver(task.address)
```

Prefer explicit retry keys in new code. `idempotency_key_from_id("task", prefix="email")`
uses the `id` attribute from the named `task` argument and stores retry state under
keys like `email:42`. `localqueue` requires either `key=` or `key_fn=`;
it does not infer a key from argument names or positions.

## What persists

| Component | Default storage | Persistence model |
| --- | --- | --- |
| `localqueue.retry` | `./localqueue_retries.sqlite3` | retry attempts per key |
| `localqueue` | `./localqueue_queue.sqlite3` | ready, inflight, and dead-letter messages |

The default retry store and default queue store are SQLite-backed.
Tests and in-memory workflows can use `MemoryAttemptStore` and `MemoryQueueStore`.
The CLI `retry_store_path` setting is a SQLite file path. In the Python retry
API, `store_path=` selects an optional LMDB attempt-store directory; use
`store=SQLiteAttemptStore("retries.sqlite3")` for an explicit SQLite file.
LMDB queue storage is still available through `localqueue[lmdb]` and
explicit `LMDBQueueStore` usage.

## Operational boundaries

The queue store is deliberately small and local. Keep payloads JSON-serializable
and reasonably small, monitor ready/inflight/dead-letter counts, and back up the
SQLite files if queued work is important. Ordering is best effort under
concurrent producers or consumers, and there is no worker heartbeat beyond lease
expiration.

See [Operational maturity](operational-maturity.md) for the current production
readiness checklist and the hardening work still outside the library's scope.

## Which API to use

| Need | Use |
| --- | --- |
| Durable jobs with ack, release, leases, and dead-letter records | `localqueue` |
| A function decorator with retry state across process restarts | `localqueue.retry` |
| Queue consumers that should retry before ack/dead-letter | `persistent_worker()` or `persistent_async_worker()` |
| Custom broker or scheduler that already delivers work | `PersistentRetrying` or `persistent_retry()` |

## Next steps

- [Persistent queues](queues.md): message lifecycle, leases, delayed delivery, and workers.
- [Persistent retries](retries.md): decorators, low-level retryers, keys, stores, and exhaustion behavior.
- [API reference](api.md): exported classes, functions, and protocols.
- [Operational maturity](operational-maturity.md): supported local-worker model and future hardening checklist.
- [Release checklist](release.md): manual versioning, build, smoke test, and publish steps.

## License

`localqueue` is distributed under the MIT license.
