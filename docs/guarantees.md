# Delivery guarantees

`localqueue` is a persistent local queue backed by SQLite. It provides
**at-least-once** delivery: a message may be delivered again when a previous
attempt did not confirm processing.

## What `put()` guarantees

When `put()` returns successfully, the message was inserted in a SQLite
transaction and assigned an internal ID. The message remains in the database
until it reaches a terminal state (`acked` or `failed`) or is removed by a
maintenance operation such as `purge()`.

`DurabilityMode.RELAXED`, the default, uses `synchronous=NORMAL`. This protects
the state against normal process crashes, but abrupt host, kernel, or power
failure may lose recent transactions. `DurabilityMode.DURABLE` configures
`synchronous=FULL` for stronger protection of recent commits. The effective
guarantee still depends on SQLite, the filesystem, kernel, drive cache,
controller, and hardware.

## Delivery lifecycle

```text
ready -> leased -> acked
             |
             +-> ready, after NACK or lease expiration
             |
             +-> failed, after the attempt limit is reached
```

A `get()` creates a lease and a unique receipt for that delivery. If the worker
dies, does not call `ack()`, or lets the lease expire, another worker may receive
the same message.

The `max_retries` limit is finite. At-least-once delivery therefore does not
mean that a message is attempted indefinitely: after the limit, it moves to the
dead-letter state (`failed`) and can be inspected or explicitly requeued.

See [Dead-letter inspection and replay](dead-letters.md) for typed inspection,
raw evidence, and replay safety.

## Why delivery is not exactly once

There is no single transaction that covers both the queue and the handler's
external effects. For example:

```text
worker applies a change to an external service
    |
    +-- worker dies before calling ack()
    |
    +-- the lease expires and the message is delivered again
```

The external effect may therefore happen twice. Handlers should be idempotent
or use an idempotency key accepted by the external system. Include that key in
the payload when the handler needs access to it.

## Fencing token (receipt)

Each delivery receives a different receipt. `ack()`, `nack()`, `fail()`, and
lease extension are accepted only with the current receipt and before the lease
expires.

This prevents an old worker from acknowledging the current delivery after
another worker has already received the message. The fencing token rejects
stale transitions, but it does not eliminate duplicate external effects.

## Deduplication with `job_id`

When provided, `job_id` is unique within the named queue while its record
exists. A new `put()` with the same `job_id` returns the existing internal ID
and does not replace the original payload.

This prevents duplicate inserts, but it does not make processing exactly once.
After `purge()` removes the record, the same `job_id` can be used again.

## What the queue does not guarantee

- exactly-once delivery for effects outside SQLite;
- absence of duplicate deliveries during lease expiration and recovery;
- infinite retries after `max_retries` is reached;
- survival of every recent commit under abrupt failure in `RELAXED` mode;
- indefinite retention of messages removed by maintenance.

An optional `max_pending_jobs` contract atomically bounds ready + processing
jobs for participating producers. It does not bound disk usage or persist a
database policy. See [Bounded backlog and producer backpressure](backpressure.md).
