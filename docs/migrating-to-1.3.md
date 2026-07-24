# Migrating to 1.3

Version 1.3 intentionally changes the Python API. It does not provide aliases,
deprecated signatures, or shims for the removed calls. Persisted database
compatibility is a separate, narrower policy; see
[Storage compatibility](storage-compatibility.md).

## Configuration

`lease_seconds`, `max_retries`, and `fsync` are no longer public constructor
parameters on `SimpleQueue` or `EventBus`. Put delivery lifecycle values in
`DeliveryPolicy`, and select durability by intent with `DurabilityMode`.

```python
# v1.2
queue = SimpleQueue(
    path,
    name="emails",
    lease_seconds=30,
    max_retries=5,
    fsync=True,
    serializer=serializer,
    max_pending_jobs=100,
)

# v1.3
queue = SimpleQueue(
    path,
    name="emails",
    delivery=DeliveryPolicy(lease_seconds=30, max_retries=5),
    durability=DurabilityMode.DURABLE,
    serializer=serializer,
    max_pending_jobs=100,
)
```

The same positional `path`, `name`, `topology`, serializer, registry, and
capacity concepts remain explicit for `EventBus`; replace its old
`lease_seconds`, `max_retries`, and `fsync` keywords in the same way.
`queue.lease_seconds` and `queue.max_retries` are now
`queue.delivery.lease_seconds` and `queue.delivery.max_retries`.

## Generic payloads and handlers

Annotations now carry the payload relationship without changing runtime
serialization:

```python
from dataclasses import dataclass

from localqueue import Job, SimpleQueue, Worker


@dataclass(frozen=True)
class Payload:
    value: str


queue: SimpleQueue[Payload] = SimpleQueue("./data", serializer=payload_serializer)


def handle(job: Job[Payload]) -> None:
    print(job.data.value)


worker: Worker[Payload] = Worker(queue, handle)
```

`SimpleQueue[Payload]`, `Job[Payload]`, and `Worker[Payload]` improve type
inference only. A serializer still owns runtime reconstruction. EventBus class
handlers similarly receive their declared Pydantic event subtype:

```python
@bus.on(UserCreated, subscription="emails")
def handle_user_created(event: UserCreated) -> None:
    print(event.user_id)
```

## Queue dead letters

`queue.list_failed()` returns `FailedMessage[PayloadT]`, whose `reason` is a
stable `FailureReason`. A decoding failure is isolated to that record: its
`decoded` flag is false, `decode_error` explains the problem, and
`raw_payload` keeps the original bytes as evidence.

```python
failed = queue.list_failed()
record = failed[0]
print(record.id, record.reason, record.raw_payload)
queue.retry_failed(record.id)
```

Replay preserves the message ID, `job_id`, `created_at`, and raw payload. It
resets attempts and the failure state, then makes the row ready subject to the
queue's current capacity limit.

## Subscription dead letters

Use the subscription binder, not an internal queue name:

```python
subscription = bus.subscription("emails")
failed = subscription.list_failed()
delivery = failed[0]
print(delivery.event_type, delivery.event, delivery.raw_payload, delivery.reason)
subscription.retry_failed(delivery.id)
```

`FailedDelivery` reconstructs the typed envelope when possible and preserves
raw evidence when it is not. Its classification distinguishes invalid
envelopes, unknown event types, invalid payloads, and handler failures.

## Replay semantics

Replay is at-least-once. A process can execute an external effect and fail
before it confirms the delivery, so a replay can execute that effect again.
Replay does not provide exactly-once behavior; handlers remain responsible for
idempotency where duplicate effects matter.

## Breaking Python API changes

- `fsync`, `lease_seconds`, and `max_retries` were removed from public queue
  and EventBus constructors.
- Delivery settings moved to `DeliveryPolicy`; durability intent moved to
  `DurabilityMode`.
- Queue, job, worker, serializer, and failed-message APIs are generic.
- Failed records are typed public models rather than unstructured dictionaries.
- Subscription dead-letter inspection and replay are public binder operations.
