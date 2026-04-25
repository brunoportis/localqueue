---
icon: lucide/shield-check
---

# Stability

`localqueue` is still a beta project, so the public surface is intentionally
small. The goal is to keep the parts people already use predictable while leaving
room to improve the rest.

## Stable surface

These names are treated as the main public contracts today:

- `PersistentQueue`
- `QueueSemantics`
- `QueuePolicySet`
- `LocalityPolicy` and the built-in locality policies
- `LeasePolicy` and the built-in lease policies
- `AcknowledgementPolicy` and the built-in acknowledgement policies
- `DeadLetterPolicy` and the built-in dead-letter policies
- `PullConsumption`
- `PushConsumption`
- `PointToPointRouting`
- `PublishSubscribeRouting`
- `AtLeastOnceDelivery`
- `AtMostOnceDelivery`
- `EffectivelyOnceDelivery`
- `NoResultPolicy`
- `ReturnStoredResult`
- `IdempotencyRecord`
- `IdempotencyStore` and the built-in idempotency stores
- `ResultStore` and the built-in result stores
- `CommitPolicy` and the built-in commit policies
- `FifoReadyOrdering`
- `PriorityOrdering`
- `BestEffortOrdering`
- `BoundedBackpressure`
- `QueueMessage`
- `QueueStats`
- `QueueStore` and the built-in queue stores
- `PersistentWorkerConfig`
- `persistent_worker`
- `persistent_async_worker`
- `persistent_retry`
- `persistent_async_retry`
- `RetryRecord`
- `AttemptStore` and the built-in retry stores
- `QueueStoreLockedError`
- `AttemptStoreLockedError`

The CLI command families are also treated as part of the supported surface:

- `queue add`, `pop`, `inspect`, `ack`, `release`, `dead`, `requeue-dead`,
  `purge`, `size`, `stats`, `process`, `exec`, `health`
- `retry prune`
- `config init`, `config set`, `config show`, `config path`

## Compatibility aliases

Some options stay available as compatibility aliases while the project is still
early:

- `dead_letter_on_exhaustion` remains accepted as an alias for
  `dead_letter_on_failure`
- `max_tries` remains accepted as an alias for `stop_after_attempt(max_tries)`

Aliases stay documented until they are removed. When an alias is planned for
removal, the project should announce the change in the changelog and keep a
compatibility window long enough for users to update their code.

## Deprecation policy

Deprecation is simple on purpose:

1. announce the change in docs and changelog
2. keep the old name working for one release cycle when practical
3. prefer warning-free compatibility first, then removal later

Because the project is beta, some internals can still change. The intent is not
to promise permanent stability for every helper, but to avoid surprising users
who already rely on the main queue, retry, and CLI entry points.
