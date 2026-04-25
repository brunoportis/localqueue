---
icon: lucide/rotate-cw
---

# Persistent Retries

Use this pattern when another part of your system already delivers the work and
the missing piece is a retry budget that survives process restarts.

This page is about persistent attempt tracking, not a full queue lifecycle. If
you need ack, release, leases, or dead-letter handling, start with
`PersistentQueue` instead.

## Best fit

- cron jobs that call an external API
- HTTP handlers that already receive the work but need durable retry budgets
- consumers from another queueing system that want persistent retry state

## Why `localqueue` fits

- retry budgets survive process restarts
- a stable key prevents the same logical job from being retried forever
- exhausted jobs can be pruned later from the CLI

## Boundaries

- this is retry state, not a full message lifecycle
- each logical job needs a stable retry key

## Minimal flow

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

Inspect or prune exhausted retry records later:

```bash
localqueue retry prune \
  --dry-run \
  --older-than 0
```

## When to choose something else

Use a queue instead when work must be stored for later delivery, acknowledged
after success, released for another attempt, or moved to dead-letter on final
failure.
