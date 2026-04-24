---
icon: lucide/briefcase
---

# Use Cases

`localqueue` shines when the work can stay on one machine and the value is in
durable retries, local recovery, and simple terminal operations.

The use cases below focus on workflows that are already well supported by the
current queue, worker, dead-letter, and persistent-retry APIs. They are meant
to be copied and run from any directory.

By default, `localqueue` stores queue and retry state under the usual XDG data
location for your user.

## Command style

The examples below use the direct CLI form:

```bash
localqueue queue exec ...
```

Install `localqueue[cli]` first if the `localqueue` command is not available.
If you prefer `pipx run` or `uvx`, translate the command locally after you
understand the workflow.

## Setup

Create a scratch directory and a small worker script once:

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

## 1. Local outbox for external side effects

This is the clearest `localqueue` story: your app or script accepts work
quickly, stores it durably, and lets a local worker perform the slow or fragile
side effect later.

Good fits:

- email delivery from a web app or admin script
- webhook fan-out from a local integration process
- report generation, uploads, or notifications that should not block the caller

Why `localqueue` fits:

- no broker, Redis, or extra service to operate
- queue state survives process restarts
- the worker can be a Python function or an external command

What to keep in mind:

- this is a single-host pattern, not a distributed outbox
- delivery is at least once, so handlers should be idempotent

Enqueue one email job:

```bash
echo '{"to":"user@example.com"}' | localqueue queue add emails
```

Inspect the queue before processing:

```bash
localqueue queue stats emails
```

Process one queued message with the worker script:

```bash
localqueue queue exec emails \
  -- python /tmp/localqueue-use-cases/email_worker.py
```

Check that the queue is empty again:

```bash
localqueue queue stats emails
```

This is the right shape when the caller should return now and the side effect
can happen a few seconds later on the same machine.

## 2. Operator loop for failed jobs

Many local automations fail in boring, recoverable ways: a remote API is down,
a credential is wrong, or a payload needs a small fix. In those cases, the
useful feature is not just retry. It is being able to inspect failures and
replay them from the terminal.

Good fits:

- webhook delivery with occasional upstream outages
- notification pipelines that need human inspection on failure
- CLI-driven maintenance jobs where operators need a dead-letter queue

Why `localqueue` fits:

- failed jobs can move to dead-letter after a small retry budget
- operators can inspect summary and full records locally
- replay is one command instead of an ad hoc recovery script

Create a failing job:

```bash
echo '{"to":"broken@example.com","fail":true}' | localqueue queue add webhooks
```

Process it with a small retry budget so it reaches dead-letter quickly:

```bash
localqueue queue exec webhooks \
  --max-tries 2 \
  -- python /tmp/localqueue-use-cases/email_worker.py
```

Inspect the dead-letter summary and full record:

```bash
localqueue queue dead webhooks --summary

localqueue queue dead webhooks
```

If you want to replay everything in the dead-letter queue:

```bash
localqueue queue requeue-dead webhooks --all
```

This is the strongest terminal-driven workflow in the project today: run work,
inspect what failed, then requeue after the underlying issue is fixed.

## 3. Persistent retries without a queue

Sometimes another system already delivers the job and all you need is retry
state that survives restarts. In that case, use `localqueue.retry` directly
instead of introducing a queue just to count attempts.

Good fits:

- cron jobs that call an external API
- HTTP handlers that already receive the work but need durable retry budgets
- consumers from another queueing system that want persistent retry state

Why `localqueue` fits:

- retry budgets survive process restarts
- a stable key prevents the same logical job from being retried forever
- exhausted jobs can be pruned later from the CLI

What to keep in mind:

- this is retry state, not a full message lifecycle
- if you need ack, release, lease, or dead-letter handling, start with
  `PersistentQueue`

Run this once in an environment where `localqueue` is importable.

```bash
python - <<'PY'
from localqueue.retry import PersistentRetryExhausted, persistent_retry


@persistent_retry(
    key="invoice:1001",
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
  --dry-run \
  --older-than 0
```

Use this shape when work already arrives from somewhere else, such as a cron
trigger, another queue, or an HTTP handler, and you only want durable retry
budgets.

## When these use cases do not fit

Use a broker or managed queue instead when:

- producers and consumers run on different machines
- strict global ordering matters
- very high write concurrency is expected
- queue retention, fan-out, or cross-service coordination is a first-class need

If you want these examples to use isolated files instead of the default XDG
paths, add `--store-path` or `--retry-store-path` explicitly.
