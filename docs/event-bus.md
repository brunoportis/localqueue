# Event bus

`localqueue.bus` is an optional, persistent publish/subscribe layer built on
the same SQLite-backed queues. Install the `bus` extra:

```bash
uv add "localqueue[bus]"   # or: pip install "localqueue[bus]"
```

Importing `localqueue` alone does not require Pydantic. Importing
`localqueue.bus` without the extra raises an `ImportError` with the required
install command.

> The topology decides where events are persisted. Handler registration
> decides what the current process can execute.

These are separate configurations. A producer loads event definitions and the
static topology, but does not import or register consumer handlers.

## Define events

Put event contracts in a module shared by producers and consumers:

```python
# events.py
from localqueue.bus import BaseEvent


class UserCreated(BaseEvent):
    event_name = "user.created"

    user_id: str


class OrderPlaced(BaseEvent):
    event_name = "order.placed"

    order_id: str
```

Events are Pydantic models. Every event automatically carries an `event_id`
(UUID), a `correlation_id`, an optional `causation_id`, and an
`event_created_at` (UTC datetime). `event_type` defaults to the class name;
setting `event_name` gives it a stable name independent of the Python class.
`schema_version` defaults to `1` and is recorded in `event_schema` as
`<event_type>@<version>`.

## Correlate derived events

Use `from_parent()` as the single supported way to create a derived event:

```python
class UserCreated(BaseEvent):
    user_id: str


class WelcomeEmailRequested(BaseEvent):
    user_id: str


root = UserCreated(user_id="42")

child = WelcomeEmailRequested.from_parent(
    root,
    user_id=root.user_id,
)
```

The identifiers form a direct causal chain:

```text
root:       event_id=A  correlation_id=A  causation_id=None
child:      event_id=B  correlation_id=A  causation_id=A
grandchild: event_id=C  correlation_id=A  causation_id=B
```

`correlation_id` identifies the complete logical workflow; it is not a
deduplication key. `causation_id` points only to the direct parent. All three
identifiers are immutable after event construction, while business payload
fields keep their existing Pydantic behavior. Historical envelopes without
correlation or causation metadata are reconstructed as root events, with
`correlation_id == event_id` and `causation_id is None`.

This metadata does not provide automatic distributed-tracing integration.
`event_name` and `schema_version` remain independent of the Python class name
and are preserved when derived events are persisted and reconstructed.

## Declare the static topology

Declare every subscription and the event types routed to it in another shared
module:

```python
# topology.py
from localqueue.bus import BusTopology

from .events import OrderPlaced, UserCreated


TOPOLOGY = BusTopology(
    {
        "email": [UserCreated],
        "analytics": [UserCreated, OrderPlaced],
        "audit": ["*"],
    }
)
```

Event patterns may be `BaseEvent` subclasses, exact event-type strings, or
`"*"`. The wildcard routes every event type to that subscription. Subscription
names must match `^[A-Za-z0-9][A-Za-z0-9_.-]*$`.

`BusTopology` copies and normalizes its input when constructed. Later changes
to the caller's dictionary or lists do not affect routing. Matching
subscription names are always returned in sorted order.

## Run an independent producer

The producer imports no consumer code and registers no handlers:

```python
# producer.py
from localqueue.bus import EventBus

from .events import UserCreated
from .topology import TOPOLOGY


bus = EventBus("./data", name="app", topology=TOPOLOGY)
try:
    receipt = bus.dispatch(UserCreated(user_id="123"))
finally:
    bus.close()
```

This dispatch persists one delivery in each matching durable queue:

```text
__bus__:app:analytics
__bus__:app:audit
__bus__:app:email
```

`dispatch()` serializes the envelope once and writes all targets with the
existing native `fanout()` call in one SQLite transaction. It returns only
after commit. The receipt contains the event id, event type, sorted
subscriptions, and internal message ids. Re-dispatching the same event id is
deduplicated independently in each subscription queue.

If no route matches, `require_subscribers=True` raises `NoSubscribers`. With
`require_subscribers=False`, dispatch returns an empty receipt and writes
nothing. `await bus.dispatch_async(event)` runs dispatch outside the event-loop
thread.

## Run an independent consumer

A consumer loads the same topology, then registers only the handlers it owns:

```python
# consumer.py
import asyncio

from localqueue.bus import EventBus

from .events import UserCreated
from .topology import TOPOLOGY


bus = EventBus("./data", name="app", topology=TOPOLOGY)
email = bus.subscription("email", concurrency=8)
billing = bus.subscription("billing", concurrency=1)


@email.handler(UserCreated)
async def send_welcome_email(event: UserCreated) -> None:
    ...


@billing.handler(OrderPlaced)
async def charge(event: OrderPlaced) -> None:
    ...


asyncio.run(bus.run())
```

Direct registration is also supported:

```python
email.handler(UserCreated, send_welcome_email)
```

`bus.on(...)` remains a compatibility convenience and delegates to the same
binder:

```python
bus.on(UserCreated, send_welcome_email, subscription="email")
```

Neither form declares a subscription or changes dispatch routing. The
canonical API is `bus.subscription(...).handler(...)`.

An exact handler may be registered only when its subscription declares that
event type or `"*"`. A wildcard handler may be registered for any declared
subscription, but it is only a runtime fallback for deliveries the topology
already routes. Inside one subscription, an exact handler wins over the
wildcard handler.

## Consumption and consumer groups

```python
await bus.run()                       # subscriptions with local handlers only
await bus.run_subscription("email")  # one locally handled subscription
```

`run()` intentionally ignores declared subscriptions for which the current
process has no handler. Loading the complete topology therefore does not let
an email worker consume and dead-letter analytics deliveries.
`run_subscription()` fails immediately if the subscription is undeclared or
has no handler registered in the current process.

Multiple processes running the same subscription compete for its durable
queue as a consumer group. Claims use leases, so each delivery is processed by
one worker at a time. Delivery remains at least once: a handler can complete
an external side effect and crash before `ack()`, causing redelivery. Make
handlers idempotent when duplicate effects matter.

Handlers may be synchronous or asynchronous. Blocking queue operations and
synchronous handlers run outside the asyncio event-loop thread. A background
heartbeat renews the delivery lease while a handler runs. Handler returns are
acked; transient exceptions are retried up to `max_retries`; exceptions listed
in `permanent_errors`, unknown event types, and invalid payloads go directly to
dead letter.

## Async handler timeouts

Set a positive, finite timeout in seconds on an individual `async def` handler:

```python
@email.handler(UserCreated, timeout=30.0)
async def send_welcome_email(event: UserCreated) -> None:
    await email_provider.send(event.user_id)
```

The timer is local, in-memory handler configuration. It is neither persisted
nor shared with other processes, and it does not change subscription
concurrency, the delivery envelope, topology, or any other handler.

For every timed handler, EventBus creates a handler task and a timer task, then
waits for the first to finish. A completed handler wins a simultaneous finish.
If the timer wins, the deadline is recorded explicitly, the handler task is
cancelled, and EventBus awaits any cooperative cleanup before proceeding. A
handler that suppresses `CancelledError` and returns is still a timeout.
Likewise, a cleanup exception is observed and logged without replacing the
timeout result or creating another transition.

An internal timeout NACKs the delivery with a `last_error` beginning `handler
timeout after ... seconds`; normal retry and dead-letter policy then applies.
This is distinct from a handler-raised `TimeoutError`: that is an ordinary
handler exception, so it follows `permanent_errors` when configured or the
normal transient NACK path otherwise.

The heartbeat remains active through cooperative cleanup. After cleanup,
EventBus cancels and awaits the heartbeat, checks for lease loss, and only then
NACKs the internal timeout when the lease remains valid. A registered
`permanent_errors` exception raised before the deadline is still a permanent
failure.

External cancellation of `run()` or `run_subscription()` takes precedence over
the handler timeout and is propagated as `CancelledError`, without being
reported as a timeout. Lease loss takes precedence over a successful handler
result: EventBus never ACKs a result after it knows the lease is lost, and the
receipt-fenced queue transition also rejects any lease lost concurrently.

Timeouts intentionally do not apply to synchronous handlers. Registering a
timeout for a non-`async def` handler raises `TypeError`; Python cannot safely
stop arbitrary code running in a thread. Use a process-isolated worker if hard
execution limits are required.

## Per-subscription concurrency

Each process can bound simultaneous deliveries for a subscription when creating
its local binder:

```python
email = bus.subscription("email", concurrency=8)
billing = bus.subscription("billing", concurrency=1)
```

`concurrency` is a positive integer and defaults to `1`. It is process-local,
in-memory configuration: it is not stored in the topology or SQLite. Reusing a
subscription binder keeps its configured value; assigning a conflicting value
in the same process raises `ValueError`. Configure it before the first
`run()` or `run_subscription()` for that subscription; later explicit changes
raise `RuntimeError` rather than resizing active work.

At most that many deliveries are claimed and handled by this process at once.
When all slots are occupied, the consumer does not claim another delivery
until a handler reaches its ACK, NACK, permanent-failure, or lease-loss path.
Every active delivery retains its own heartbeat and receipt-fenced transition.
Other processes still compete normally for the same durable subscription
queue, so this setting is not a global limit.

Within one `EventBus` instance, a subscription has one active consumer runner.
Starting that same subscription again while it runs raises `RuntimeError`; this
prevents two local claim loops from multiplying its configured bound.

With the default `concurrency=1`, one process claims and processes deliveries
sequentially: a delivery completes its transition before the next claim. With
a larger value, claims follow the queue's available order, but handler and ACK
completion order are intentionally unspecified; retries can change it further.
Choose a value from the concurrency the downstream dependency and SQLite can
safely absorb (for example, email-provider and database connection limits),
considering external I/O and lease duration, then measure and adjust. Raising
the limit does not guarantee more throughput; it is not automatic CPU sizing
and it is not a handler timeout.

This bound limits active local handlers, not durable backlog or producer rate.
Use `SimpleQueue(max_pending_jobs=...)` for producer-side backlog limits and
the resulting `Full` backpressure policy. EventBus fan-out itself remains
unlimited.

Cancelling `run()` or `run_subscription()` stops further claims, cancels active
async delivery tasks, closes the subscription queue, and propagates
`CancelledError`. A synchronous handler already running in `asyncio.to_thread()`
cannot be forcibly stopped; its result is not transitioned after cancellation,
its heartbeat stops, and normal lease expiry/retry recovery applies. Such
handlers must be idempotent; hard isolation or handler timeouts are outside
this API.

## Four distinct concepts

- **Topology configuration:** the static in-memory declaration used by
  producers for fan-out and by consumers to validate handler compatibility.
- **Handler registry:** process-local callable registrations that determine
  what that process can execute and which subscriptions `run()` consumes.
- **Subscription queue:** the durable SQLite queue
  `__bus__:{bus}:{subscription}` containing one delivery per routed event.
- **Consumer group:** multiple processes competing for deliveries from the
  same subscription queue.

## Static-topology limitations

- Every producer must load the same topology configuration.
- Topology changes are deployment or configuration changes.
- Subscription declarations are not persisted in SQLite.
- A consumer created later does not receive events published before its
  subscription existed in the producer's topology.
- Topology consistency across separately deployed processes is the
  application's responsibility; there is no automatic synchronization or
  topology-version negotiation.
- Event classes used for reconstruction must be registered in each consumer
  process through a class-based handler or `bus.register(EventClass)`.
- Communication is limited to processes sharing one local database on one
  machine. There is no network protocol, cross-machine replication, replay,
  retention, offsets, partitions, or dynamic subscription discovery.
