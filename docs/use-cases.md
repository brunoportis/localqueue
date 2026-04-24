---
icon: lucide/briefcase
---

# Use Cases

These examples are meant to be copied and run from the repository root.
They keep state under `/tmp/localqueue-use-cases` so they do not interfere with
your normal localqueue data.

## Prerequisites

Install the project with CLI support first:

```bash
uv sync --extra cli
mkdir -p /tmp/localqueue-use-cases
```

## 1. Local email spooler

This is the simplest shape for `localqueue`: one process enqueues work and
another process handles it later on the same machine.

Enqueue one email job:

```bash
uv run python examples/enqueue_email.py \
  user@example.com \
  --store-path /tmp/localqueue-use-cases/emails.sqlite3
```

Inspect the queue before processing:

```bash
uv run localqueue queue stats emails \
  --store-path /tmp/localqueue-use-cases/emails.sqlite3
```

Process one queued message with the example worker:

```bash
uv run localqueue queue exec emails \
  --store-path /tmp/localqueue-use-cases/emails.sqlite3 \
  -- python examples/email_worker.py
```

Check that the queue is empty again:

```bash
uv run localqueue queue stats emails \
  --store-path /tmp/localqueue-use-cases/emails.sqlite3
```

Use this pattern when your app should accept work quickly and let a local
worker perform the external side effect later.

## 2. Dead-letter and recover a failed command

This recipe shows a practical operator loop: enqueue a job that fails, inspect
the dead-letter list, then requeue it after fixing the payload.

Create a failing job:

```bash
uv run python examples/enqueue_email.py \
  broken@example.com \
  --fail \
  --store-path /tmp/localqueue-use-cases/failing-emails.sqlite3
```

Process it with a small retry budget so it reaches dead-letter quickly:

```bash
uv run localqueue queue exec emails \
  --store-path /tmp/localqueue-use-cases/failing-emails.sqlite3 \
  --retry-store-path /tmp/localqueue-use-cases/failing-emails-retries.sqlite3 \
  --max-tries 2 \
  -- python examples/email_worker.py
```

Inspect the dead-letter summary and full record:

```bash
uv run localqueue queue dead emails \
  --store-path /tmp/localqueue-use-cases/failing-emails.sqlite3 \
  --summary

uv run localqueue queue dead emails \
  --store-path /tmp/localqueue-use-cases/failing-emails.sqlite3
```

If you want to replay everything in the dead-letter queue:

```bash
uv run localqueue queue requeue-dead emails \
  --store-path /tmp/localqueue-use-cases/failing-emails.sqlite3 \
  --all
```

This is a good fit for webhook delivery, notification pipelines, and other
single-host jobs where operators need to inspect and replay failed work from
the terminal.

## 3. Persistent retries without a queue

Sometimes another system already delivers the job and all you need is retry
state that survives restarts. In that case, use `localqueue.retry` directly.

Run this once:

```bash
uv run python - <<'PY'
from localqueue.retry import PersistentRetryExhausted, persistent_retry


@persistent_retry(
    key="invoice:1001",
    store_path="/tmp/localqueue-use-cases/retries.sqlite3",
    max_tries=3,
)
def flaky() -> None:
    print("calling remote API")
    raise ConnectionError("temporary upstream failure")


try:
    flaky()
except PersistentRetryExhausted as exc:
    print(f"exhausted: {exc.key} after {exc.attempts} attempts")
except Exception as exc:
    print(f"failed this run: {exc}")
PY
```

Run the same command again. The retry state is reused because the key is
stable and the attempt store is on disk. After the budget is exhausted,
`PersistentRetryExhausted` is raised before the wrapped function runs again.

You can later inspect or prune exhausted retry records:

```bash
uv run localqueue retry prune \
  --retry-store-path /tmp/localqueue-use-cases/retries.sqlite3 \
  --dry-run \
  --older-than 0
```

Use this shape when work already arrives from somewhere else, such as a cron
trigger, another queue, or an HTTP handler, and you only want durable retry
budgets.
