---
icon: lucide/inbox
---

# persistentretry

`persistentretry` provides durable local queues for Python workers, with
persistent retry state powered by [Tenacity](https://tenacity.readthedocs.io/en/latest/).

The main entry point is `persistentqueue`: an LMDB-backed queue for at-least-once
job delivery. The lower-level `persistentretry` package remains available when
you only need durable retry budgets around an existing delivery mechanism.

## Install

```bash
pip install persistentretry
```

The package requires Python 3.11 or newer.

## Basic queue worker

Install the optional CLI dependencies when you want to operate queues from the
terminal.

```bash
pip install "persistentretry[all]"
```

Run one queued message with an importable handler:

```bash
persistentretry queue process emails myapp.workers:send_email --max-tries 5
```

Run a continuous local worker with `--forever`. `SIGINT` and `SIGTERM` request a
graceful stop after the current message finishes.

```bash
persistentretry queue process emails myapp.workers:send_email \
  --forever \
  --block \
  --worker-id worker-1 \
  --max-tries 5
```

For a local smoke test, enqueue one job and process it with the bundled example
handler:

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

```python
from persistentqueue import PersistentQueue, PersistentWorkerConfig, persistent_worker
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt, wait_exponential


queue = PersistentQueue("emails", store_path="./persistence_db")
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

## Manual queue control

Use the explicit message API when the handler needs to decide between
acknowledging, releasing, or dead-lettering a message.

```python
from persistentqueue import PersistentQueue

queue = PersistentQueue("emails", store_path="./persistence_db")
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

Use `persistentretry` directly when the queued message lifecycle is handled by
another system and you only need retry state to survive restarts.

```python
from persistentretry import idempotency_key_from_id, persistent_retry
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
keys like `email:42`. `persistentretry` requires either `key=` or `key_fn=`;
it does not infer a key from argument names or positions.

## What persists

| Component | Default storage | Persistence model |
| --- | --- | --- |
| `persistentretry` | `./persistence_db.sqlite3` | retry attempts per key |
| `persistentqueue` | `./persistence_db` | ready, inflight, and dead-letter messages |

The default retry store is SQLite; the default queue store remains LMDB-based.
Tests and in-memory workflows can use `MemoryAttemptStore` and `MemoryQueueStore`.

## Which API to use

| Need | Use |
| --- | --- |
| Durable jobs with ack, release, leases, and dead-letter records | `persistentqueue` |
| A function decorator with retry state across process restarts | `persistentretry` |
| Queue consumers that should retry before ack/dead-letter | `persistent_worker()` or `persistent_async_worker()` |
| Custom broker or scheduler that already delivers work | `PersistentRetrying` or `persistent_retry()` |

## Next steps

- [Persistent queues](queues.md): message lifecycle, leases, delayed delivery, and workers.
- [Persistent retries](retries.md): decorators, low-level retryers, keys, stores, and exhaustion behavior.
- [API reference](api.md): exported classes, functions, and protocols.
- [Release checklist](release.md): manual versioning, build, smoke test, and publish steps.
