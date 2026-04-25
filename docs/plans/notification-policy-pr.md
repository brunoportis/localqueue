---
icon: lucide/bell-ring
---

# Notification Policy PR Plan

This plan describes the notification policy PR that makes local wake-up
behavior explicit in the queue contract without changing the simple default
queue path.

The architectural intent is narrow:

- dispatch invokes handlers
- notification wakes listeners
- storage persists queue state

The default queue should continue to be storage-first and quiet:

```python
from localqueue import PersistentQueue

queue = PersistentQueue("jobs")
```

That constructor should still mean "persist messages locally and do nothing
else unless a policy says otherwise."

## Problem Statement

The queue docs and semantics already distinguish pull consumption from local
push-style workflows, but the runtime policy surface needs the same separation.
Today, a user can reasonably want one of these behaviors:

- persist only
- persist and notify a local waiter
- persist and dispatch to in-process handlers
- persist, notify, and dispatch

Without an explicit notification policy, these cases are easy to blur together.
That makes it harder to document what is local, what is synchronous, and what
is merely a future architectural direction.

## Goal

Add a notification layer that is separate from dispatch:

- `DispatchPolicy` calls handlers.
- `NotificationPolicy` wakes or signals listeners.
- `PersistentQueue("jobs")` remains quiet by default.
- SQLite remains storage only; it is not treated as a notification broker.

This PR should make the contract visible in both code and docs:

- notification is optional
- notification happens after persistence
- notification is not a delivery guarantee
- notification is not cross-process by default

## Scope

- Add a `NotificationPolicy` protocol with `notify(message)`,
  `notifies_on_put`, and serializable metadata.
- Add `NoNotification` as the default policy.
- Add `CallbackNotification` for in-process wake-up callbacks.
- Add `InProcessNotification` for local thread wake-up through
  `threading.Event`.
- Add `QueueSemantics.notifications` so the queue contract can advertise
  whether notification is part of the configuration.
- Add `notification_policy` to `PersistentQueue` and `QueuePolicySet`.
- Run notification after `put()` persists the message, before any configured
  dispatch.
- Document the difference between local notification and local dispatch.
- Cover constructor defaults, semantic validation, policy-set conflicts, and
  callback notification behavior with tests.

## Proposed API Shape

The minimal API should mirror the existing policy style used elsewhere in the
queue architecture.

### Policy protocol

`NotificationPolicy` should expose:

- `notify(message)`: perform the local wake-up action
- `notifies_on_put`: advertise whether `put()` triggers notification
- `as_dict()`: serialize the contract for queue semantics and docs

The metadata should stay small and descriptive. This PR should not try to model
transport-specific configuration for future adapters.

### Built-in policies

The PR should introduce:

- `NoNotification`
- `CallbackNotification`
- `InProcessNotification`

`NoNotification` remains the default and should advertise an inactive local
policy, for example with a shape equivalent to:

```python
{
    "kind": "notification",
    "mode": "none",
    "notifies_on_put": False,
}
```

`CallbackNotification` should advertise an active in-process policy, for
example:

```python
{
    "kind": "notification",
    "mode": "callback",
    "scope": "in-process",
    "notifies_on_put": True,
}
```

Exact field names can follow the existing policy conventions, but the important
part is that queue semantics can clearly distinguish "quiet by default" from
"local wake-up configured."

`InProcessNotification` should provide the first concrete waitable local
adapter. It is backed by `threading.Event`, sets the event after persistence,
and lets local consumers call `wait()` and `clear()` explicitly.

### Queue wiring

`PersistentQueue` should accept:

```python
from localqueue import CallbackNotification, PersistentQueue

queue = PersistentQueue(
    "jobs",
    notification_policy=CallbackNotification(on_notify),
)
```

`QueuePolicySet` should also accept `notification_policy` so a reusable queue
contract can include wake-up semantics without requiring ad hoc constructor
arguments.

### Runtime ordering

`put()` should execute in this order:

1. Persist the message.
2. Run the notification policy, if configured.
3. Run the dispatch policy, if configured.

This keeps the state transition honest:

- listeners are only notified about persisted messages
- dispatch does not become the implicit notification mechanism
- notification failure cannot imply persistence rollback

## Semantics And Validation

This PR should extend queue semantics so the contract is visible at inspection
time, not only in constructor code.

### Semantics surface

`QueueSemantics.notifications` should describe whether the queue includes a
notification contract. That makes the difference between these configurations
observable:

- `PersistentQueue("jobs")`
- `PersistentQueue("jobs", notification_policy=CallbackNotification(...))`

The semantics output should make it obvious that notification is local and
explicit, not something inferred from SQLite or from push consumption alone.

### Validation rules

Validation should stay strict where the model would otherwise be misleading:

- a `QueuePolicySet` must reject conflicting notification policies
- constructor defaults must preserve the current quiet queue behavior
- semantics must not imply cross-process guarantees for callback notification

This PR does not need to reject every unusual combination. It only needs to
prevent combinations that misstate the queue contract.

## Review Points

- The default behavior must stay unchanged: no notification and no dispatch.
- Notification failures happen after persistence. They should remain visible to
  the caller, but they must not imply the message was rolled back.
- `CallbackNotification` is intentionally in-process only. It must not promise
  cross-process wake-up.
- `NotificationPolicy` should be small enough for future adapters such as
  thread events, asyncio events, sockets, signals, HTTP callbacks, SSE,
  WebSockets, and Redis pub/sub.

## Documentation Changes

The docs should explain the distinction in plain language:

- dispatch runs handlers
- notification wakes something up
- neither changes the storage role of SQLite

The queue docs should be able to point to notification as the local wake-up
primitive for push or polling-hybrid workflows, while still stating that actual
message delivery, leasing, acknowledgement, retry, and dead-letter behavior
remain store-driven.

## Test Plan

Tests should cover the user-visible contract rather than only internal
plumbing:

- `PersistentQueue("jobs")` has `NoNotification` by default
- `QueueSemantics.notifications` reflects the configured policy
- `put()` persists before notification runs
- notification runs before dispatch when both are configured
- notification exceptions surface to the caller after persistence
- `CallbackNotification` invokes the configured callback with the persisted
  message
- `InProcessNotification` sets and clears a local thread event
- policy-set composition rejects notification conflicts cleanly

## Out Of Scope

- Cross-process wake-up adapters.
- Async notification adapters.
- Threaded or pooled dispatch.
- Physical publish/subscribe fanout.
- Changes to worker retry, ack, lease, or dead-letter behavior.

## Acceptance Criteria

This PR is complete when:

- the queue exposes a first-class `NotificationPolicy`
- the default queue remains silent
- local callback notification is supported and documented
- local thread-event notification is supported and documented
- queue semantics advertise notification explicitly
- docs clearly separate notification from dispatch
- tests prove persistence-first ordering and failure behavior

## Validation

The PR should pass:

```bash
uv run ruff format localqueue tests docs
uv run ruff check localqueue tests
uv run basedpyright localqueue tests
uv run pytest --cov=localqueue --cov-report=term-missing --cov-fail-under=100
```
