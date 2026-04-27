---
icon: lucide/shield-check
---

# API Contracts

This page records the public API surface and the compatibility aliases that are
currently supported.

## Stable surface

These names are treated as the main public contracts today:

- `PersistentQueue`
- `QueueSemantics`
- `QueuePolicySet`
- `LocalityPolicy` and the built-in locality policies
- `LeasePolicy` and the built-in lease policies
- `AcknowledgementPolicy` and the built-in acknowledgement policies
- `DeadLetterPolicy` and the built-in dead-letter policies
- `DeduplicationPolicy` and the built-in deduplication policies
- `PullConsumption`
- `PushConsumption`
- `DispatchPolicy` and the built-in dispatch policies
- `NotificationPolicy` and the built-in notification policies
- `PointToPointRouting`
- `PublishSubscribeRouting`
- `SubscriptionPolicy` and the built-in subscription policies
- `subscriber_queue_name`
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
- `RejectingBackpressure`
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

These aliases remain available for compatibility:

- `dead_letter_on_exhaustion` remains accepted as an alias for
  `dead_letter_on_failure`
- `max_tries` remains accepted as an alias for `stop_after_attempt(max_tries)`

Aliases stay documented until they are removed.

## Deprecation policy

Deprecation follows a short documented cycle:

1. announce the change in docs and changelog
2. keep the old name working for one release cycle when practical
3. remove it after that compatibility window
