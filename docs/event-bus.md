# Event bus

## Durable event identity

Use `@event(identity=...)` when independently constructed occurrences represent
the same logical operation:

```python
from localqueue.bus import BaseEvent, EventBus, event


@event(identity="cnpj")
class ContactCreationRequested(BaseEvent):
    cnpj: str
    nome: str


@event(identity=("tenant_id", "cnpj"))
class TenantContactCreationRequested(BaseEvent):
    tenant_id: str
    cnpj: str
    nome: str
```

`event_id` remains the random UUID of the occurrence passed to `dispatch`.
Identity creates a separate SHA-256 deduplication key. It is namespaced by
`event_type@schema_version`, canonicalized from validated JSON field values,
and unique per full queue name—therefore per bus name and subscription. Two
processes using the same database coordinate through SQLite's unique index;
different databases do not share identities.

Identity fields must be present in the persisted business payload. Statically
excluded fields are rejected by the decorator; conditional exclusions and
values that cannot produce deterministic finite JSON raise
`InvalidEventIdentity` before any insert.

Identity is opt-in for each concrete event class. A subclass does not inherit a
parent's identity declaration; decorate the subclass explicitly when it should
also use durable business identity.

The payload fingerprint excludes `event_id`, creation time, correlation, and
causation metadata. Equal identity and equal business payload reuse the
existing message ID without replacing its envelope. `DispatchReceipt.inserted`
is aligned with `subscriptions` and `message_ids`; mixed fanout can report
`(False, True)`. A different payload for the same identity raises
`DeduplicationConflict` and rolls back the complete fanout.

ACKed and failed rows continue to reserve their identity. A duplicate does not
reactivate them or clear failure state. `retry_failed` reuses the same row;
`purge` deletes the row and releases both its `job_id` and identity.

### Choosing a good identity

Identity should name an idempotent operation or logical event, not necessarily
an entity forever. An import row ID or `(tenant_id, cnpj)` can be appropriate
for a one-time contact-creation request. `account_id` alone is dangerous for
`AccountBalanceChanged` when several legitimate changes can occur; prefer
`(account_id, transaction_id)` or include a revision.

This is durable local ingestion deduplication, not exactly-once execution. It
does not deduplicate lease-expiration deliveries or HTTP side effects. Handlers
must still make external calls idempotent, commonly with
`idempotency_key=ctx.event_id`.

Failed deliveries are inspectable and replayable without discovering internal
queue names:

```python
subscription = bus.subscription("payments")
for delivery in subscription.list_failed():
    print(delivery.reason, delivery.event_type, delivery.raw_payload)
    if delivery.inspection_error is not None:
        print(delivery.inspection_error)
    subscription.retry_failed(delivery.id)
```

See [Dead-letter inspection and replay](dead-letters.md) for the structured
failure reasons, corrupt-envelope behavior, and at-least-once replay warning.

For deployment boundaries and operational limits, see the
[operational envelope](operational-envelope.md).

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

The common API declares both together. The advanced API keeps them separate so
a producer can load event definitions and topology without importing consumer
handlers.

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

## Happy path: declare routes with handlers

Topology is optional when one process owns both routing and handlers:

```python
from localqueue.bus import EventBus


bus = EventBus(
    "./data",
    name="contacts",
    concurrency=20,
)


@bus.handler(
    ContactCreationRequested,
    concurrency=100,
)
async def create_contact(event, ctx):
    contact = await ctx.contacts.create(event)
    return ContactCreated(contact_id=contact.id)


@bus.handler(ContactCreated)
async def record_contact(event, ctx):
    await ctx.read_model.record(event)


await bus.run()
```

`EventBus.handler` registers the callable and adds its event route to a new
immutable `BusTopology` snapshot. The default durable subscription name is the
event type: normally the class name, or `event_name` when set. Renaming the
Python function therefore does not create a new queue. If that event type is
not a valid subscription name, registration fails instead of silently
sanitizing it; provide an explicit name such as
`subscription="contact-requested"`.

The constructor's `concurrency` is the process-local default for each
subscription. The handler argument overrides it for that handler's
subscription. It is not a global sum: two subscriptions configured with
`concurrency=100` can run up to 200 deliveries in one process. Each process has
its own limit. The default is one subscription per handler, so the limit
usually feels handler-local, but concurrency always belongs to the
subscription.

Registering the same event twice with default names is rejected. Independent
handlers for one event need distinct durable subscriptions:

```python
@bus.handler(ContactCreated, subscription="update-read-model")
async def update_read_model(event): ...


@bus.handler(ContactCreated, subscription="send-notification")
async def send_notification(event): ...
```

Dispatch fans out to both subscriptions, whose ACK, retry, DLQ and concurrency
state remain independent. Different event types may deliberately share one
explicit subscription; they then share one queue, consumer group and
concurrency bound:

```python
@bus.handler(ContactCreated, subscription="contact-projector", concurrency=10)
async def on_created(event): ...


@bus.handler(ContactUpdated, subscription="contact-projector", concurrency=10)
async def on_updated(event): ...
```

Failures remain subscription-scoped:

```python
failed = bus.subscription("ContactCreationRequested").list_failed()
```

An explicit subscription uses its explicit name for inspection. Returning a
`BaseEvent` from an ergonomic handler uses the same atomic local ACK plus
fanout path described below; routes added by other ergonomic handlers
participate automatically.

The `path` argument still names the directory containing `localqueue.db`.
Names such as `"contacts"` or `"contacts.db"` are not given any new path
resolution semantics by the ergonomic API.

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

## Advanced path: declare topology separately

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

This path remains useful for remote subscriptions handled by another process,
producer-only topology, wildcard and string patterns, subscriptions consuming
multiple event types, and fully explicit operational configuration.
`BusTopology` is not deprecated.

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

## Emit one event from a handler

A handler may return one `BaseEvent`:

```python
@bus.subscription("contacts").handler(ContactCreationRequested)
async def create_contact(event, ctx):
    response = await ctx.http.post(...)
    if response.status_code == 201:
        return ContactCreated(
            cnpj=event.cnpj,
            contact_id="123",
        )
    if response.status_code == 409:
        return ContactAlreadyExists(cnpj=event.cnpj)
    if response.status_code == 422:
        raise Reject(response.text, category="validation")
    raise Retry()
```

`None`, including the implicit result of a function without `return`, means a
normal ACK without emission. A returned `BaseEvent` means fanout to every
matching subscription plus ACK of the current delivery in one local SQLite
transaction. Only one event is accepted: lists, tuples, generators, batches,
responses, and arbitrary objects are programming errors and move the delivery
immediately to DLQ with `PERMANENT_HANDLER_ERROR`. `Retry` and `Reject` remain
explicit control-flow exceptions. There is no `ctx.publish()` API.

The returned object is not mutated. If its `correlation_id` was omitted, the
persisted copy inherits the parent's correlation. If its `causation_id` was
omitted, the persisted copy points to the parent's `event_id`. Explicit values
are preserved, as are the child's own `event_id`, creation time, and business
fields. `BaseEvent.from_parent()` remains available when lineage should be set
at construction time.

When the returned event has no route, `require_subscribers=True` moves the
current delivery immediately to DLQ without emitting anything.
`require_subscribers=False` permits the dropped emission and ACKs normally.

### Local atomicity guarantee

The atomic commit covers only local EventBus state: the delivery ACK and the
fanout of the returned event. Either all local targets and the ACK commit, or
none do.

It does **not** make exactly-once the handler's HTTP request, an external
database write, email delivery, or any other side effect performed before the
return. Handler execution remains at-least-once. External integrations still
need idempotency, for example:

```python
await client.post(..., idempotency_key=ctx.event_id)
```

## Delivery and durability policies

EventBus shares the same immutable delivery policy and durability intent as
`SimpleQueue`:

```python
from localqueue import DeliveryPolicy, DurabilityMode
from localqueue.bus import EventBus


bus = EventBus(
    "./data",
    name="app",
    topology=TOPOLOGY,
    delivery=DeliveryPolicy(
        lease_seconds=30,
        max_retries=5,
    ),
    durability=DurabilityMode.DURABLE,
)
```

The policy applies both to the native queue used for atomic fanout and to every
subscription queue opened for consumption. `DurabilityMode.RELAXED` is the
throughput-oriented default and selects SQLite `synchronous=NORMAL`;
`DurabilityMode.DURABLE` selects `synchronous=FULL` for stronger protection of
recent commits. Neither mode promises survival across every filesystem,
kernel, drive cache, controller, or hardware failure.

`require_subscribers` remains a boolean because it controls one stable
dispatch decision. Serializer and event registry remain explicit strategy
objects. Topology can be supplied explicitly or grown immutably by
`EventBus.handler`.

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
async def send_welcome_email(event: UserCreated) -> None: ...


@billing.handler(OrderPlaced)
async def charge(event: OrderPlaced) -> None: ...


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

Neither form declares a subscription or changes dispatch routing. Use
`EventBus.handler` when registration should also declare the route, and these
explicit binders when topology is managed separately.

Handlers may also accept a second `HandlerContext` argument. Applications can
use an `EventBus[AppContext]` with `context_factory=` to add explicitly managed
dependencies with static typing. The factory runs for every attempt; its errors
use the normal retry policy. See [Custom handler contexts](custom-handler-contexts.md).

An exact handler may be registered only when its subscription declares that
event type or `"*"`. A wildcard handler may be registered for any declared
subscription, but it is only a runtime fallback for deliveries the topology
already routes. Inside one subscription, an exact handler wins over the
wildcard handler.

## Consumption and consumer groups

```python
await bus.run()  # subscriptions with local handlers only
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
reported as a timeout, including while a timed-out handler is performing
cooperative cleanup. Lease loss takes precedence over a successful handler
result: EventBus never ACKs a result after it knows the lease is lost, and the
receipt-fenced queue transition also rejects any lease lost concurrently.

Timeouts intentionally do not apply to synchronous handlers. Registering a
timeout for a non-`async def` handler raises `TypeError`; Python cannot safely
stop arbitrary code running in a thread. Use a process-isolated worker if hard
execution limits are required.

## Per-subscription concurrency

Each process can bound simultaneous deliveries for a subscription either with
the EventBus default, an ergonomic handler override, or the explicit binder:

```python
bus = EventBus("./data", concurrency=4)


@bus.handler(UserCreated, concurrency=8)
async def index_user(event): ...


email = bus.subscription("email", concurrency=8)
billing = bus.subscription("billing", concurrency=1)
```

`concurrency` is a positive integer and defaults to `1`. The EventBus value is
the fallback for every subscription; a handler or binder value is that
subscription's override. It is process-local, in-memory configuration: it is
not stored in the topology or SQLite. Reusing a subscription keeps its
configured value; assigning a conflicting value in the same process raises
`ValueError`. Configure handlers and overrides before the first `run()` or
`run_subscription()` for that subscription; later registrations or explicit
changes raise `RuntimeError` rather than resizing active work.

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
## Explicit handler control flow

Handlers and context factories can request a retry or reject a delivery:

```python
from localqueue.bus import Reject, Retry

raise Retry()
raise Retry("rate limited", after=30)
raise Reject("invalid input", category="validation")
```

`Retry` uses the normal persistent NACK path and still observes
`DeliveryPolicy.max_retries`. With no `after`, it preserves the configured
delivery behavior; `after` changes only the next attempt's persisted
availability time. `Reject` immediately moves the delivery to the subscription
DLQ without an automatic retry. Its reason and optional category remain
inspectable through `FailedDelivery` and survive database reopen and manual
replay.

## Declarative retry policies

`RetryPolicy` configures a durable subscription's retry budget and delay:

```python
from localqueue.bus import Retry, RetryPolicy


@bus.handler(
    ContactCreationRequested,
    retry=RetryPolicy.exponential(
        max_attempts=8,
        initial_delay=0.5,
        max_delay=60,
    ),
)
async def create_contact(event, ctx):
    try:
        ...
    except RateLimited as error:
        raise Retry(
            "rate limited",
            after=error.retry_after,
        ) from error
```

`RetryPolicy.fixed(max_attempts=5, delay=10)` always uses the same delay.
Exponential backoff starts at `initial_delay`, multiplies after each failed
attempt, and saturates at `max_delay`. Its default `jitter=True` uses full
jitter: a random delay between zero and the current capped base delay.

`max_attempts` counts the initial execution. A value of `1` permits that
execution but no retry; `8` permits at most one initial execution and seven
retries. Without an explicit policy, `DeliveryPolicy.max_retries + 1` remains
the persisted attempt budget and retries retain their legacy zero-delay
behavior.

The policy belongs to the subscription, not the Python function. Exact and
wildcard handlers sharing a subscription must therefore specify structurally
compatible policies. Omitting `retry=` on another handler inherits an already
configured policy. All consumers of the same durable subscription should use
consistent configuration; localqueue does not coordinate policy configuration
between processes.

After a failure, EventBus calculates the delay once and persists it in
`available_at`; the worker does not sleep and is free to claim other work.
`Retry(after=...)` overrides only that next persisted delay, without jitter or
the exponential cap, and does not extend an exhausted attempt budget. This is
appropriate for values obtained from `Retry-After` while keeping the policy
independent of HTTP.

Retry policies do not know about HTTP clients, status codes, or domain
exceptions, and do not decide whether a failure is retryable. Handlers continue
to use `Retry` and `Reject` for explicit classification; configured
`permanent_errors` also remain permanent. When a retryable failure exhausts the
policy budget, the terminal failure reason is `RETRIES_EXHAUSTED`;
`last_error` preserves the concrete timeout or exception from the final
attempt.

The budget is written during each atomic claim so lease recovery observes it
even if the worker crashes before the handler starts. Consequently, changing a
subscription policy between deployments can change a message's persisted
budget at its next claim. Deploy consistent policy configuration across all
workers competing for that subscription.
