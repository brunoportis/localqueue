# Dead-letter inspection and replay

Failed records remain useful even when their serializer or EventBus envelope
is no longer readable. Inspection returns immutable typed snapshots and always
retains the exact bytes stored in SQLite:

```python
queue: SimpleQueue[dict[str, object]] = SimpleQueue("./data")
failed = queue.list_failed()

for record in failed:
    print(record.id, record.reason, record.raw_payload)
    if record.decoded:
        print(record.data)  # a valid payload may itself be None
    else:
        print(record.decode_error)

queue.retry_failed(failed[0].id)
```

`list_failed(limit=100, offset=0)` orders by increasing message ID. Pages are
deterministic for a stable failed set, without snapshot isolation during
concurrent mutations. `created_at` is original creation time; `updated_at`
while failed is the last transition relevant to that state. Both are Unix
seconds.

`FailureReason` is the stable automation contract:

| Reason | Meaning |
| --- | --- |
| `RETRIES_EXHAUSTED` | Normal attempts were exhausted, including final lease expiration. |
| `PERMANENT_HANDLER_ERROR` | A configured permanent Worker or EventBus exception. |
| `UNKNOWN_EVENT_TYPE` | The event is absent from the current registry. |
| `INVALID_ENVELOPE` | The minimum EventBus envelope is unreadable. |
| `INVALID_PAYLOAD` | Pydantic cannot construct a known event. |
| `HANDLER_TIMEOUT` | A timeout transition exhausted delivery. |
| `EXPLICIT_PERMANENT_FAILURE` | Public `SimpleQueue.fail()` was called. |
| `NO_HANDLER` | No matching handler exists in the consuming process. |
| `LEGACY_UNKNOWN` | A legacy, null, future, or unrecognized value. |

`last_error` is for people; do not parse it for automation. A failed decode has
`decoded == False`, `data is None`, a non-null `decode_error`, and unchanged
`raw_payload`. One corrupt record does not block the rest of a page.

## EventBus subscriptions

```python
subscription = bus.subscription("payments")
failed = subscription.list_failed()

for delivery in failed:
    print(delivery.subscription, delivery.reason)
    print(delivery.event_type, delivery.event)
    print(delivery.raw_payload, delivery.inspection_error)

subscription.retry_failed(failed[0].id)
```

`event_type` remains available when the envelope exposes it, even if the type
is unknown or its payload is invalid. `inspection_error` describes what the
current serializer and registry can inspect; it never rewrites the historical
reason. Listing is read-only and does not require running consumers.

## Replay guarantees

Replay atomically changes only the selected failed row in the selected queue.
It preserves ID, `job_id`, creation time, and exact payload bytes; resets
attempts; clears receipt, lease, `last_error`, and `failure_reason`; and makes
the row ready under the current capacity limit. It never deserializes,
validates, repairs, or executes the payload.

Replay is at-least-once: handler side effects may happen again, and it is not
exactly-once. Corrupt payloads may fail again. Unknown events may fail until
their class is registered. Two concurrent replays have one winner; the other
receives `LocalQueueError("job not found")`. The same applies to a second
replay or an ID belonging to another subscription.

Bulk replay, filtering, cursor pagination, retention automation, and retry-all
are outside this API.
