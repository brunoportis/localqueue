---
icon: lucide/map
---

# Queue Architecture Roadmap

This roadmap tracks the remaining work for the queue architecture direction:
keep the default queue simple, while exposing the major queue-system concepts as
clear policies, adapters, and operational choices.

## Current Shape

The queue already has explicit policy objects for:

- locality
- delivery
- commit/result/idempotency
- consumption
- dispatch
- notification
- routing
- subscriptions
- ordering
- leases
- acknowledgements
- dead letters
- deduplication
- backpressure

The important boundary is that policies should describe concepts cleanly, while
adapters provide concrete runtime behavior.

## Next PRs

1. Add `InProcessNotification`.

   Provide a local wake-up primitive based on `threading.Event` or an equivalent
   in-process signal. This should let a consumer wait for producer activity
   without treating SQLite as an event source.

2. Add `AsyncNotification`.

   Provide the asyncio counterpart for async workers and event-loop based apps.
   Keep it local and explicit, with no cross-process promise.

3. Add `ThreadedDispatcher`.

   Let handlers run outside the producer call stack when a synchronous
   `CallbackDispatcher` would block the producer for too long.

4. Add a small wait/dispatch helper.

   Consider `wait_for_notification()` or `dispatch_pending()` only after the
   notification adapters exist, so the helper API follows real usage instead of
   guessing too early.

## Adapter Families

Local adapters should come first:

- callback notification
- thread event notification
- asyncio event notification
- inline dispatch
- threaded dispatch
- worker-pool dispatch

Cross-process notification adapters should come after the local contract is
stable:

- Unix signal notification
- local socket notification
- HTTP webhook notification
- SSE notification
- WebSocket notification
- Redis pub/sub notification

Cross-process adapters should usually wake a consumer. The consumer should still
lease the message from the store so ack, retry, lease timeout, and dead-letter
behavior stay centralized.

## Larger Features

- Implement physical fanout for `PublishSubscribeRouting` and
  `StaticFanoutSubscriptions`.
- Add stronger operational metrics around enqueue, lease, ack, release,
  dispatch, notification, retries, and dead letters.
- Expand storage guidance and optional backends when SQLite or LMDB are no
  longer the right fit.
- Keep effectively-once features honest: idempotency, result stores, commit
  policies, outbox, two-phase commit, and sagas should be available, but the
  project should not imply generic distributed exactly-once delivery.

## Closure Criteria

This queue architecture phase is complete when:

- simple usage remains `PersistentQueue("jobs")`
- every major queue-system concept has a visible policy or adapter
- local push can work without polling a database loop
- cross-process wake-up is adapter-based and clearly documented
- publish/subscribe has both descriptive policies and a concrete fanout path
- docs explain which guarantees are local, best-effort, at-least-once,
  effectively-once, or out of scope
