---
icon: lucide/bell-ring
---

# Notification Policy PR Plan

This plan describes the current notification policy PR. The goal is to make
push-style wake-up behavior explicit without changing the simple default queue
path.

## Goal

Add a notification layer that is separate from dispatch:

- `DispatchPolicy` calls handlers.
- `NotificationPolicy` wakes or signals listeners.
- `PersistentQueue("jobs")` remains quiet by default.
- SQLite remains storage only; it is not treated as a notification broker.

## Scope

- Add a `NotificationPolicy` protocol with `notify(message)`,
  `notifies_on_put`, and serializable metadata.
- Add `NoNotification` as the default policy.
- Add `CallbackNotification` for in-process wake-up callbacks.
- Add `QueueSemantics.notifications` so the queue contract can advertise
  whether notification is part of the configuration.
- Add `notification_policy` to `PersistentQueue` and `QueuePolicySet`.
- Run notification after `put()` persists the message, before any configured
  dispatch.
- Document the difference between local notification and local dispatch.
- Cover constructor defaults, semantic validation, policy-set conflicts, and
  callback notification behavior with tests.

## Review Points

- The default behavior must stay unchanged: no notification and no dispatch.
- Notification failures happen after persistence. They should remain visible to
  the caller, but they must not imply the message was rolled back.
- `CallbackNotification` is intentionally in-process only. It must not promise
  cross-process wake-up.
- `NotificationPolicy` should be small enough for future adapters such as
  thread events, asyncio events, sockets, signals, HTTP callbacks, SSE,
  WebSockets, and Redis pub/sub.

## Out Of Scope

- Cross-process wake-up adapters.
- Async notification adapters.
- Threaded or pooled dispatch.
- Physical publish/subscribe fanout.
- Changes to worker retry, ack, lease, or dead-letter behavior.

## Validation

The PR should pass:

```bash
uv run ruff format localqueue tests docs
uv run ruff check localqueue tests
uv run basedpyright localqueue tests
uv run pytest --cov=localqueue --cov-report=term-missing --cov-fail-under=100
```
