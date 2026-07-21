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
(UUID) and `event_created_at` (UTC datetime). `event_type` defaults to the
class name; setting `event_name` gives it a stable name independent of the
Python class. `schema_version` defaults to `1` and is recorded in
`event_schema` as `<event_type>@<version>`.

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
email = bus.subscription("email")


@email.handler(UserCreated)
async def send_welcome_email(event: UserCreated) -> None:
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
