# Event bus

`localqueue.bus` is an optional, persistent publish/subscribe layer built on
top of the same SQLite-backed queues. It requires the `bus` extra:

```bash
uv add "localqueue[bus]"   # or: pip install "localqueue[bus]"
```

Importing `localqueue` alone never requires pydantic; importing
`localqueue.bus` without the extra raises a clear `ImportError`.

## Event types

Events are pydantic models:

```python
from localqueue.bus import BaseEvent

class UserCreated(BaseEvent):
    user_id: str
```

Each event carries an `event_id` (UUID) and `event_created_at` (UTC datetime)
automatically. The persisted envelope is:

```json
{
  "event_id": "...",
  "event_type": "UserCreated",
  "event_schema": "UserCreated@1",
  "event_created_at": "2026-01-01T00:00:00+00:00",
  "payload": {"user_id": "123"}
}
```

- `event_type` defaults to the class name. Override it with the class variable
  `event_name = "user.created"` (must be a non-empty string) to decouple the
  persisted name from the Python class.
- `schema_version` (class variable, default `1`) is recorded in
  `event_schema` as `<event_type>@<version>` for future evolution.

## Subscriptions and handlers

Handlers are registered per subscription with `bus.on`, either directly or as
a decorator. The pattern can be an event class, an `event_type` string, or
`"*"` (wildcard):

```python
@bus.on(UserCreated, subscription="email")
async def send_welcome(event: UserCreated) -> None: ...

bus.on("UserCreated", track, subscription="analytics")

@bus.on("*", subscription="audit")
def audit(event: BaseEvent) -> None: ...
```

- Each **subscription** is a durable queue named
  `__bus__:{bus}:{subscription}` in the same `localqueue.db`. One delivery per
  subscription, no matter how many handlers match within it.
- Registering the same `(subscription, pattern)` twice is rejected. An exact
  pattern wins over `"*"` inside the same subscription — only one handler
  runs per delivery.
- `name` (bus) and `subscription` must match
  `^[A-Za-z0-9][A-Za-z0-9_.-]*$` (no `:`, never empty).

## Consumer groups

Workers in multiple processes that call `bus.run_subscription("email")` (or
`bus.run()`) compete for the same subscription queue: deliveries are claimed
with leases, so each one is processed by a single worker. Add processes to
scale out — there is nothing to coordinate.

## Atomic dispatch

```python
receipt = bus.dispatch(UserCreated(user_id="123"))
```

`dispatch` resolves the interested subscriptions, serializes the envelope
**once**, and writes to all target queues in **one SQLite transaction**. It
only returns after the commit, and the receipt carries the subscriptions and
the internal message ids:

```python
DispatchReceipt(event_id=..., event_type="UserCreated",
                subscriptions=("analytics", "audit", "email"),
                message_ids=(11, 12, 13))
```

The `event_id` is used as the queue `job_id`, so re-dispatching the same
event instance is deduplicated per subscription.

If no subscription matches, `require_subscribers=True` (the default) raises
`NoSubscribers`; with `require_subscribers=False` the dispatch returns an
empty receipt and writes nothing. `await bus.dispatch_async(event)` is the
async variant (`asyncio.to_thread`).

## Consuming

```python
await bus.run()                          # all subscriptions registered in this process
await bus.run_subscription("email")      # just one
```

Both run until cancelled — `CancelledError` closes the queues cleanly and
propagates. For tests, pass `idle_timeout=seconds` to stop after the queue
has been empty for that long.

While a handler runs, a background heartbeat renews the delivery lease
(roughly every `lease_seconds/3`), so long handlers are not redelivered
mid-execution. If the lease is lost anyway (or expires before the final
ack/nack/fail), the outcome is discarded and logged — a `LeaseExpired` never
terminates the consumer. Blocking `get` calls and synchronous handlers run
in worker threads, so consumers never stall the asyncio event loop.

Each delivery is reconstructed into the event class (resolved from the
process-wide registry) and validated with pydantic before the handler runs.
Handlers may be sync or async:

```python
result = handler(event)
if inspect.isawaitable(result):
    await result
```

Outcome mapping (reusing the underlying queue semantics — no parallel retry
logic in the bus):

| Outcome | Action |
| --- | --- |
| Handler returns | `ack` |
| Handler raises any other exception | `nack` (transient: retried up to `max_retries`, then dead-letter) |
| Handler raises an exception in `permanent_errors` | `fail` straight to dead-letter |
| Unknown `event_type` | `fail` with descriptive `last_error` (payload kept in dead-letter) |
| Payload fails pydantic validation | `fail` (permanent — retrying would not help) |

## Guarantees and idempotency

Delivery is **at-least-once**. There is an important distinction between:

- **event persisted** — `dispatch` returning means the event was committed to
  every interested subscription queue; consumers will eventually see it, even
  across process crashes; and
- **handler completed** — a handler can perform an external side effect and
  crash before the `ack`, causing redelivery.

Make handlers idempotent (for example, key side effects on `event.event_id`)
when duplicate effects matter.

## MVP limitations

- **In-memory registration.** Handlers and event classes live in process
  memory; nothing about subscriptions is stored in SQL. A consumer process
  must import the modules containing the event classes and the `bus.on`
  decorators (directly or via explicit imports) before calling `run()`.
  Unknown event types go to the dead-letter queue instead of blocking.
- **Local machine only.** This is not a distributed broker: there is no
  network protocol, clustering, cross-machine replication, or broker-managed
  consumer offsets. All processes share the same `localqueue.db` file.

## Out of scope

Deliberately not implemented in this iteration: `event_result` / RPC,
`expect()`, child events, replay, priorities, SQL-persisted subscriptions,
autodiscovery, per-subscription configuration, and dashboards.
