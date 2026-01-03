---
icon: lucide/rotate-cw
---

# Persistent Retries

`persistentretry` is the durable retry layer used by queue workers and by code
that already has its own delivery mechanism. It wraps Tenacity's `Retrying` and
`AsyncRetrying` classes. You still configure `stop`, `wait`, `retry`, callbacks,
and `retry_with()` the same way; the wrapper adds a durable attempt record before
each stop decision.

If you need a full job lifecycle with ack, release, leases, and dead-letter
records, start with [Persistent queues](queues.md).

## Decorator API

```python
from persistentretry import key_from_argument, persistent_retry
from tenacity import retry_if_exception_type, stop_after_attempt, wait_fixed


@persistent_retry(
    key_fn=key_from_argument("job_id"),
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(ConnectionError),
)
def sync_task(job_id: str) -> str:
    return call_remote_service(job_id)
```

For async functions, use `persistent_async_retry`.

```python
from persistentretry import key_from_argument, persistent_async_retry
from tenacity import stop_after_attempt


@persistent_async_retry(
    key_fn=key_from_argument("job_id"),
    stop=stop_after_attempt(3),
)
async def async_task(job_id: str) -> dict:
    return await fetch_payload(job_id)
```

## Retry keys

Each persistent retry needs a stable key. Pass `key=` when the retryer is bound
to one logical job, or pass `key_fn=` when the key must be derived from call
data. Calls without either option raise `ValueError` before the wrapped function
is called. Pass `key=` when the retryer is bound to one logical job.

```python
from persistentretry import PersistentRetrying

retryer = PersistentRetrying(key="invoice:1001", max_tries=5)
retryer(generate_invoice)
```

Use a documented key factory when the key must be derived from call data.

```python
from persistentretry import idempotency_key_from_id
from persistentretry import key_from_argument
from persistentretry import key_from_attr
from persistentretry import persistent_retry


def retry_key(fn, args, kwargs) -> str:
    tenant = kwargs["tenant_id"]
    invoice = kwargs["invoice_id"]
    return f"{tenant}:invoice:{invoice}"


@persistent_retry(key_fn=retry_key, max_tries=5)
def export_invoice(*, tenant_id: str, invoice_id: str) -> None:
    ...


@persistent_retry(key_fn=key_from_argument("job_id"), max_tries=5)
def export_job(*, job_id: str) -> None:
    ...


@persistent_retry(key_fn=key_from_attr("task", "id", prefix="video"), max_tries=5)
def process_video(task: VideoTask) -> None:
    ...


@persistent_retry(
    key_fn=idempotency_key_from_id("task", prefix="video"),
    max_tries=5,
)
def transcode_video(task: VideoTask) -> None:
    ...
```

If no key can be derived, the wrapper raises `ValueError`.

## Attempt budgets

`max_tries=` is a convenience alias for `stop=stop_after_attempt(...)`.

```python
from persistentretry import key_from_argument, persistent_retry


@persistent_retry(key_fn=key_from_argument("payment_id"), max_tries=4)
def charge_card(payment_id: str) -> None:
    ...
```

Do not pass `max_tries=` and `stop=` together. If both are supplied, `persistentretry` raises `ValueError` because there would be two sources of truth for the retry budget.

When a retry budget is exhausted, the attempt record is marked as exhausted. A later call with the same key raises `PersistentRetryExhausted` before calling the wrapped function again.

```python
from persistentretry import PersistentRetryExhausted

try:
    charge_card("payment:123")
except PersistentRetryExhausted as exc:
    print(exc.key, exc.attempts)
```

## Stores

The default attempt store is SQLite at `./persistence_db.sqlite3`.

Choose a path explicitly with `store_path=`.

```python
from persistentretry import PersistentRetrying, key_from_argument

retryer = PersistentRetrying(
    store_path="/var/lib/my-worker/retries",
    max_tries=5,
    key_fn=key_from_argument("job_id"),
)
```

Provide a store instance with `store=` when you need full control or want in-memory tests.

```python
from persistentretry import MemoryAttemptStore, key_from_argument, persistent_retry

store = MemoryAttemptStore()


@persistent_retry(
    store=store,
    key_fn=key_from_argument("job_id"),
    max_tries=2,
    wait=lambda state: 0,
)
def flaky(job_id: str) -> str:
    ...
```

`store=` and `store_path=` are mutually exclusive.

## State and callbacks

Tenacity callbacks and strategies receive a state object that behaves like Tenacity's `RetryCallState`, with persistent attempt numbering.

```python
def before_sleep(state) -> None:
    print(state.attempt_number, state.seconds_since_start)
```

`attempt_number` includes attempts loaded from the store. `start_time` and `seconds_since_start` are based on the first persisted attempt for the key.

## Clearing and resetting

Successful calls clear retry state by default.

```python
from persistentretry import PersistentRetrying, key_from_argument

retryer = PersistentRetrying(key_fn=key_from_argument("job_id"), max_tries=3)
result = retryer(process, "job:1")
```

Use `clear_on_success=False` when you want to inspect records after success.

```python
retryer = PersistentRetrying(
    key_fn=key_from_argument("job_id"),
    max_tries=3,
    clear_on_success=False,
)
```

Retryers also expose helpers for operational workflows.

```python
record = retryer.get_record("job:1")
retryer.reset("job:1")
```

## Low-level API

Use `PersistentRetrying` directly when decorators are not a good fit.

```python
from persistentretry import PersistentRetrying, key_from_argument

retryer = PersistentRetrying(key_fn=key_from_argument("job_id"), max_tries=5)
result = retryer(run_job, "job:123", {"priority": "high"})
```

Use `PersistentAsyncRetrying` for coroutine functions.

```python
from persistentretry import PersistentAsyncRetrying, key_from_argument

retryer = PersistentAsyncRetrying(key_fn=key_from_argument("job_id"), max_tries=5)
result = await retryer(run_async_job, "job:123")
```

The decorated function keeps Tenacity's `retry_with()` pattern.

```python
urgent = sync_task.retry_with(max_tries=1)
urgent("job:urgent")
```
