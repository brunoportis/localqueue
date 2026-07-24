# Migrating to v1.3

v1.3 intentionally tightens the Python API while preserving forward opening of
persisted databases from the 1.x storage lineage. Treat these as separate
contracts:

- Python source may require mechanical edits when moving between minor releases;
- persisted SQLite databases are covered by the release compatibility matrix.

No deprecation aliases or runtime compatibility shims are included for the
removed Python calls below.

## Queue and EventBus configuration

Replace storage mechanics with semantic policies.

```python
# v1.2
SimpleQueue(path, fsync=True)

# v1.3
from localqueue import DurabilityMode, SimpleQueue

SimpleQueue(path, durability=DurabilityMode.DURABLE)
```

```python
# v1.2
SimpleQueue(path, lease_seconds=30, max_retries=5)

# v1.3
from localqueue import DeliveryPolicy, SimpleQueue

SimpleQueue(
    path,
    delivery=DeliveryPolicy(
        lease_seconds=30,
        max_retries=5,
    ),
)
```

The same `delivery=` and `durability=` arguments apply to `EventBus`.
Configuration attributes move under the semantic objects:

| v1.2 | v1.3 |
| --- | --- |
| `queue.lease_seconds` | `queue.delivery.lease_seconds` |
| `queue.max_retries` | `queue.delivery.max_retries` |
| `bus.lease_seconds` | `bus.delivery.lease_seconds` |
| `bus.max_retries` | `bus.delivery.max_retries` |
| `bus.fsync` | `bus.durability` |

`DurabilityMode.RELAXED` preserves the previous throughput-oriented default.
Use `DurabilityMode.DURABLE` when the stronger SQLite synchronization intent is
required. Neither mode is an absolute physical-power-loss guarantee.

## Generic queue and handler types

Payload relationships are now explicit:

```text
Serializer[PayloadT]
        -> SimpleQueue[PayloadT]
        -> Job[PayloadT]
        -> Worker[PayloadT]
        -> handler(Job[PayloadT])
```

Existing runtime payload bytes are unchanged. A type annotation does not validate
old rows or reconstruct an application class; the configured serializer still
owns runtime decoding and validation.

For typed application payloads, annotate the queue and provide a serializer that
round-trips that type. EventBus class-pattern handlers now retain their concrete
event subtype. See [Static typing](typing.md).

## Dead-letter inspection

`SimpleQueue.list_failed()` no longer returns dictionaries. It returns immutable
`FailedMessage[PayloadT]` records:

```python
from localqueue import FailureReason, SimpleQueue

queue: SimpleQueue[dict[str, object]] = SimpleQueue(path)
for record in queue.list_failed():
    print(record.id, record.reason, record.raw_payload)
    if record.decoded:
        print(record.data)
    else:
        print(record.decode_error)

    if record.reason is FailureReason.RETRIES_EXHAUSTED:
        queue.retry_failed(record.id)
```

EventBus exposes the same operational workflow without internal queue names:

```python
subscription = bus.subscription("payments")
for delivery in subscription.list_failed():
    print(delivery.reason, delivery.event_type, delivery.raw_payload)
    subscription.retry_failed(delivery.id)
```

Replay is at least once. External side effects can happen again, corrupt payloads
may fail again, and unknown event types remain unknown until registered. See
[Dead-letter inspection and replay](dead-letters.md).

## Persisted database migration

v1.3 adds one nullable column:

```sql
messages.failure_reason TEXT
```

Opening an older 1.x database checks the schema read-only first. Only a database
missing the column enters a `BEGIN IMMEDIATE` migration; the schema is checked
again after acquiring the writer lock before one `ALTER TABLE` is attempted.
Existing rows are not rewritten. Null, future, and unrecognized stored values are
reported as `FailureReason.LEGACY_UNKNOWN`.

The release matrix exercises real databases created by published 1.0.0, 1.0.1,
1.1.0, 1.1.1, 1.1.2, and 1.2.0 wheels. This is a forward-opening guarantee for
the tested 1.x lineage, not a downgrade or concurrent mixed-version guarantee.
Custom serializers remain responsible for decoding their historical payloads.

Before upgrading:

1. stop or coordinate all queue processes onto one version;
2. create a backup or copy of `localqueue.db`;
3. install the matching v1.3 wheel;
4. open the queue normally and run an integrity check when the database matters;
5. do not run old and new package versions against the same database concurrently.

See [Storage compatibility](storage-compatibility.md) for the maintained policy
and reproducible matrix command.
