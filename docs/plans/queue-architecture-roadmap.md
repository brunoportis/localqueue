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

## Missing Coverage To Track

These areas need explicit decisions before this architecture can be considered
complete:

- a guarantee matrix that states which combinations are descriptive, local,
  best-effort, at-least-once, effectively-once, or out of scope
- policy compatibility checks for combinations such as push without dispatch or
  notification, publish/subscribe without subscriptions, and priority values
  without priority ordering
- worker integration for notification-driven waits, graceful shutdown, handler
  failures, leases, acknowledgements, and dead letters
- concrete adapter contracts for local, cross-process, and remote notification
  mechanisms
- explicit point-to-point guidance for competing consumers and single-consumer
  delivery
- physical fanout semantics for subscriber queues, including whether fanout
  copies messages or stores subscriber cursors
- priority and best-effort ordering guidance under concurrency
- additional backpressure decisions such as throttling, drop-newest, or
  drop-oldest when the store can make those choices explicit
- examples and recipes that show how the policies compose in real workflows
- adapter contract tests so new implementations can prove they preserve queue
  invariants

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

5. Add a policy compatibility matrix.

   Keep the matrix mostly documentary at first, then turn the risky combinations
   into constructor validation where the project can reject misuse without
   blocking valid advanced use cases.

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

Remote adapters should be treated as a separate family:

- remote queue placement
- remote queue store adapters
- remote notification adapters
- remote result/idempotency stores
- remote commit coordinators

`RemoteQueuePlacement` should only name the boundary until a concrete adapter is
attached. It should not imply distributed coordination by itself.

## Worker Integration

Notification-driven workers should still use the normal lifecycle:

- wait for notification or timeout
- lease from the queue store
- run the handler
- commit result/idempotency state when configured
- acknowledge, release, or dead-letter through the queue

This keeps push-style wake-up from bypassing the parts that make retries,
leases, and recovery predictable.

Useful worker-facing additions to evaluate:

- `wait_for_notification(timeout=...)`
- `dispatch_pending(limit=...)`
- `persistent_worker(..., notification_policy=...)`
- `persistent_async_worker(..., notification_policy=...)`
- structured shutdown behavior for dispatchers and notification listeners

## Publish/Subscribe Plan

The descriptive publish/subscribe policies are already visible. The physical
fanout implementation still needs a design decision:

- copy-on-publish creates one durable message per subscriber queue
- cursor-based fanout stores one message and tracks each subscriber position
- hybrid fanout copies only when subscribers need independent retention,
  retries, or dead-letter handling

The first implementation should prefer correctness and inspection over raw
throughput. Subscriber queues should preserve lease, ack, retry, dedupe, and
dead-letter behavior independently.

## Ordering And Backpressure

Ordering should stay explicit per queue:

- `FifoReadyOrdering` for normal ready-message ordering
- `PriorityOrdering` when producers intentionally choose priority values
- `BestEffortOrdering` when concurrency or adapter behavior prevents a stable
  promise

Backpressure should also stay explicit. The current model covers blocking and
rejecting producers. Future overflow strategies should only be added when their
data-loss behavior can be named clearly:

- drop the newest message
- drop the oldest ready message
- delay or throttle producers
- route overflow to a dead-letter or overflow queue

## Guarantees And Recipes

The docs should include concrete recipes for the main combinations users will
actually choose:

- simple local at-least-once queue
- local at-most-once queue
- point-to-point queue with competing consumers
- effectively-once with idempotency and stored results
- transactional outbox for side effects
- saga-style compensation
- pull worker with lease recovery
- push local worker with notification-driven wake-up
- publish/subscribe with named subscribers
- priority queue with explicit ordering
- bounded queue with blocking producers
- bounded queue with rejecting producers
- remote boundary with explicit store and notification adapters

Each recipe should state the guarantee, the failure window, and what the user is
responsible for.

## Larger Features

- Implement physical fanout for `PublishSubscribeRouting` and
  `StaticFanoutSubscriptions`.
- Add adapter contract tests for notification, dispatch, store, result,
  idempotency, and commit adapters.
- Add compatibility tests for policy combinations and `QueuePolicySet` presets.
- Add stronger operational metrics around enqueue, lease, ack, release,
  dispatch, notification, retries, and dead letters.
- Add optional structured event hooks for enqueue, lease, ack, release,
  dead-letter, notification, and dispatch transitions.
- Expand storage guidance and optional backends when SQLite or LMDB are no
  longer the right fit.
- Evaluate batching as a separate consumption concern, not as a replacement for
  the current single-message API.
- Evaluate message expiration or TTL separately from dead-letter retention.
- Evaluate serializer or codec policies only if payload compatibility becomes a
  user-facing concern.
- Keep effectively-once features honest: idempotency, result stores, commit
  policies, outbox, two-phase commit, and sagas should be available, but the
  project should not imply generic distributed exactly-once delivery.

## Long-Term Future

These concepts should have an architectural place, but they should not be
presented as near-term promises or implied by the local default queue:

- clustering, replication, leader election, and consensus
- partitioning, rebalancing, consumer groups, and offset management for
  stream-style workloads
- broker protocol compatibility such as AMQP, MQTT, Redis Streams, Kafka, SQS,
  or RabbitMQ APIs
- access control, authentication, authorization, multitenancy, and tenant-level
  quotas
- schema registry, payload compatibility policies, and migration workflows for
  message formats
- remote store adapters such as Postgres, Redis, SQS, RabbitMQ, or cloud queue
  services
- stream processing features such as replay windows, compacted topics,
  materialized views, and stateful processors
- cross-region replication, disaster recovery, and managed retention policies

The near-term goal is not to become every broker. It is to make the concepts
visible, name the boundaries honestly, and keep the simple local API useful
while advanced adapters can be added deliberately.

## Closure Criteria

This queue architecture phase is complete when:

- simple usage remains `PersistentQueue("jobs")`
- every major queue-system concept has a visible policy or adapter
- local push can work without polling a database loop
- cross-process wake-up is adapter-based and clearly documented
- publish/subscribe has both descriptive policies and a concrete fanout path
- point-to-point and priority use cases have direct recipes
- policy combinations have documented guarantees and validation for risky cases
- worker helpers can use notification without bypassing lease, ack, retry, or
  dead-letter behavior
- adapters have shared contract tests
- common recipes are documented with failure windows and user responsibilities
- docs explain which guarantees are local, best-effort, at-least-once,
  effectively-once, or out of scope
