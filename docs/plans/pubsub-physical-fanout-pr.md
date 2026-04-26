---
icon: lucide/git-branch-plus
---

# Physical Fanout For Static Subscriptions

This PR turns the existing publish/subscribe policy surface into concrete queue
behavior.

Today the code can describe publish/subscribe intent with:

- `PublishSubscribeRouting`
- `StaticFanoutSubscriptions`

But the store still behaves like one concrete queue at a time. This change
implements physical fanout so a published message is materialized into one
subscriber queue per configured subscriber.

## Goal

Add static physical fanout with these properties:

- publishing to a pub/sub queue creates one durable message per subscriber
- each subscriber consumes, retries, acknowledges, and dead-letters
  independently
- ordering remains per subscriber queue
- the root queue acts as a publisher namespace, not as a competing-consumer
  queue

## Proposed Behavior

Given:

```python
from localqueue import (
    PersistentQueue,
    PublishSubscribeRouting,
    StaticFanoutSubscriptions,
)

events = PersistentQueue(
    "events",
    routing_policy=PublishSubscribeRouting(),
    subscription_policy=StaticFanoutSubscriptions(("billing", "audit")),
)
```

Then:

```python
events.put({"kind": "invoice-paid", "invoice_id": "inv-1"})
```

Creates one durable message in each physical subscriber queue:

- `events.billing`
- `events.audit`

Each subscriber queue is consumed independently:

```python
billing = events.subscriber_queue("billing")
audit = events.subscriber_queue("audit")
```

## API Shape

### New helper

Add a public helper on `PersistentQueue`:

```python
queue.subscriber_queue("billing")
```

This returns a new `PersistentQueue` instance pointing at the physical queue for
that subscriber and reusing the same underlying store configuration.

### Physical queue naming

Use a centralized naming helper, with:

```python
events.billing
```

as the default physical queue name.

This avoids duplicating string logic across queue creation, tests, and docs.

## Data And Semantics

### Payload

Each subscriber copy preserves:

- `value`
- delay
- priority

### Message identity

Each physical copy gets its own message id from the active store.

### Dedupe

If the root publish call includes a `dedupe_key`, each subscriber copy should
derive a subscriber-local key, for example:

```text
<original-dedupe-key>:<subscriber>
```

This prevents one subscriber from suppressing another.

### Retry And Failure Handling

Ack, release, retry, dead-letter, idempotency, and result storage remain
per-physical-queue behavior. A failure in `events.billing` must not affect
`events.audit`.

## Validation Rules

The queue contract should reject misleading configurations where appropriate.

At minimum:

- publish/subscribe without subscribers should fail clearly
- `subscriber_queue("unknown")` should fail clearly

This PR does not need dynamic subscriber registration.

## Implementation Plan

1. Add queue naming helpers for subscriber queues.
2. Add `PersistentQueue.subscriber_queue(subscriber)`.
3. Update `put()` so pub/sub queues fan out into one physical queue per
   configured subscriber.
4. Preserve delay, priority, and subscriber-local dedupe keys during fanout.
5. Keep normal queue behavior unchanged for point-to-point queues.
6. Add tests for independent ack/release/dead-letter behavior across
   subscribers.
7. Document the behavior and a minimal usage example.

## Test Plan

Required tests:

- publishing to a queue with two static subscribers creates two physical
  messages
- `subscriber_queue("billing")` and `subscriber_queue("audit")` resolve to the
  expected queue names
- ack in one subscriber queue does not remove the other subscriber copy
- release in one subscriber queue does not affect the other subscriber copy
- dead-letter in one subscriber queue does not affect the other subscriber copy
- subscriber-local dedupe keys do not collide across subscribers
- point-to-point queues preserve current behavior
- invalid subscriber lookup raises a clear error

## Explicit Non-Goals

This PR does not implement:

- dynamic subscribers
- wildcard topic matching
- remote fanout
- replay semantics
- consumer groups
- cross-process notification changes

## Closure Criteria

This work is complete when:

- the existing publish/subscribe policies produce real physical fanout
- each subscriber gets an independent durable queue copy
- point-to-point behavior remains unchanged
- tests and docs describe the new behavior concretely
