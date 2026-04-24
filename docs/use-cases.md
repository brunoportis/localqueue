---
icon: lucide/briefcase
---

# Use Cases

These examples are meant to be copied and run from any directory.
They keep state under `/tmp/localqueue-use-cases` so they do not interfere with
your normal localqueue data.

## Choose a launcher

The shell commands below use `localqueue` as the CLI name. You can provide that
command in any of these ways:

```bash
pip install "localqueue[cli]"
```

```bash
pipx run --spec 'localqueue[cli]' localqueue --help
```

```bash
uvx --from 'localqueue[cli]' localqueue --help
```

If you are using `pipx run` or `uvx`, replace `localqueue` in the examples with
the full launcher command.

The Python snippets need the `localqueue` package to be importable. Use
`python` in an environment where `localqueue` is installed, or use:

```bash
uv run --with localqueue python --version
```

## Setup

Create a scratch directory and a small worker script:

```bash
mkdir -p /tmp/localqueue-use-cases
cat > /tmp/localqueue-use-cases/email_worker.py <<'PY'
from __future__ import annotations

import json
import sys


payload = json.load(sys.stdin)
address = payload["to"]
if payload.get("fail"):
    raise ConnectionError(f"could not deliver email to {address}")
print(f"sent email to {address}")
PY
```

## 1. Local email spooler

This is the simplest shape for `localqueue`: one process enqueues work and
another process handles it later on the same machine.

Enqueue one email job:

```bash
localqueue queue add emails \
  --store-path /tmp/localqueue-use-cases/emails.sqlite3 \
  --value '{"to":"user@example.com"}'
```

Inspect the queue before processing:

```bash
localqueue queue stats emails \
  --store-path /tmp/localqueue-use-cases/emails.sqlite3
```

Process one queued message with the worker script:

```bash
localqueue queue exec emails \
  --store-path /tmp/localqueue-use-cases/emails.sqlite3 \
  -- python /tmp/localqueue-use-cases/email_worker.py
```

Check that the queue is empty again:

```bash
localqueue queue stats emails \
  --store-path /tmp/localqueue-use-cases/emails.sqlite3
```

Use this pattern when your app should accept work quickly and let a local
worker perform the external side effect later.

## 2. Dead-letter and recover a failed command

This recipe shows a practical operator loop: enqueue a job that fails, inspect
the dead-letter list, then requeue it after fixing the payload.

Create a failing job:

```bash
localqueue queue add emails \
  --store-path /tmp/localqueue-use-cases/failing-emails.sqlite3 \
  --value '{"to":"broken@example.com","fail":true}'
```

Process it with a small retry budget so it reaches dead-letter quickly:

```bash
localqueue queue exec emails \
  --store-path /tmp/localqueue-use-cases/failing-emails.sqlite3 \
  --retry-store-path /tmp/localqueue-use-cases/failing-emails-retries.sqlite3 \
  --max-tries 2 \
  -- python /tmp/localqueue-use-cases/email_worker.py
```

Inspect the dead-letter summary and full record:

```bash
localqueue queue dead emails \
  --store-path /tmp/localqueue-use-cases/failing-emails.sqlite3 \
  --summary

localqueue queue dead emails \
  --store-path /tmp/localqueue-use-cases/failing-emails.sqlite3
```

If you want to replay everything in the dead-letter queue:

```bash
localqueue queue requeue-dead emails \
  --store-path /tmp/localqueue-use-cases/failing-emails.sqlite3 \
  --all
```

This is a good fit for webhook delivery, notification pipelines, and other
single-host jobs where operators need to inspect and replay failed work from
the terminal.

## 3. Persistent retries without a queue

Sometimes another system already delivers the job and all you need is retry
state that survives restarts. In that case, use `localqueue.retry` directly.

Run this once. If you do not have `localqueue` installed in the current Python
environment, replace `python` with `uv run --with localqueue python`.

```bash
python - <<'PY'
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
localqueue retry prune \
  --retry-store-path /tmp/localqueue-use-cases/retries.sqlite3 \
  --dry-run \
  --older-than 0
```

Use this shape when work already arrives from somewhere else, such as a cron
trigger, another queue, or an HTTP handler, and you only want durable retry
budgets.
