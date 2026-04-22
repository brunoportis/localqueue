---
icon: lucide/list-checks
---

# Operational Maturity Checklist

This project is intentionally small: durable local queues for Python workers,
plus persistent retry state powered by Tenacity. The checklist below tracks the
work needed before describing it as a mature production queue system instead of
a local worker library.

## Concurrency and locking

- [x] Document practical SQLite concurrency limits for producers and workers.
- [ ] Add stress tests for multiple producer and consumer processes sharing one
  SQLite queue file.
- [ ] Measure throughput and lock contention under WAL mode.
- [ ] Define guidance for when users should move to Postgres, Redis, SQS,
  RabbitMQ, or another external broker.

## Distributed operation

- [ ] Decide whether multi-host operation is in scope.
- [x] Add explicit documentation that the default model is local-file storage,
  not distributed coordination.
- [ ] Evaluate worker heartbeats beyond lease expiration.
- [ ] Evaluate worker identity, liveness inspection, and stale-worker reporting.
- [ ] Evaluate sharding or namespacing guidance for independent queues.

## Delivery semantics

- [x] Document at-least-once delivery prominently in the README and queue docs.
- [x] Add idempotency guidance for handlers.
- [x] Provide examples for idempotent job keys and external side effects.
- [ ] Evaluate deduplication support for enqueue operations.
- [x] Evaluate stronger ordering guarantees, or document why ordering is best
  effort under concurrency.
- [x] Document the failure window between a successful handler side effect and
  `ack()`.

## Retry and worker policies

- [ ] Add examples for different policies by exception type.
- [ ] Evaluate built-in permanent-failure classification.
- [ ] Evaluate queue-level retry defaults that can be reused by many workers.
- [ ] Evaluate rate limiting for handlers that call external services.
- [ ] Evaluate circuit-breaker integration or documentation.
- [ ] Add clearer guidance for `release_delay`, Tenacity `wait`, and
  `lease_timeout` interactions.

## Observability

- [ ] Add optional Prometheus or OpenTelemetry metrics.
- [ ] Track per-message attempt history, not only the latest error.
- [ ] Track queue latency, handler duration, and time spent inflight.
- [ ] Track throughput by queue and worker id.
- [ ] Add aggregate failure summaries for dead-letter inspection.
- [ ] Evaluate structured event logging for enqueue, lease, ack, release, and
  dead-letter transitions.

## Storage operations

- [ ] Define a schema migration strategy for SQLite stores.
- [x] Document backup and restore expectations.
- [ ] Add maintenance guidance for `VACUUM`, WAL files, and long-lived stores.
- [ ] Add retention or cleanup controls for dead-letter records.
- [ ] Add cleanup controls for old retry records.
- [ ] Document recovery behavior for corrupted or partially written store files.
- [ ] Add compatibility tests for store files created by older package versions.

## API stability

- [ ] Decide which APIs are stable before a `1.0` release.
- [ ] Document compatibility aliases and their planned lifetime.
- [ ] Add deprecation policy.
- [ ] Increase coverage margin above the current 95% gate.
- [ ] Keep docs aligned with defaults for SQLite, optional LMDB, and CLI extras.

## Guardrails against misuse

- [x] Document recommended payload size limits.
- [x] Add clearer lease-timeout guidance for slow jobs.
- [ ] Add examples for safe shutdown and long-running workers.
- [ ] Add health-check examples for worker processes.
- [ ] Add configuration validation for risky combinations where possible.
- [x] Add warnings or docs for unbounded queue growth.

## Positioning

- [x] Keep public wording focused on durable local queues for scripts and small
  workers.
- [x] Avoid presenting the project as a replacement for distributed brokers.
- [ ] Add comparison docs explaining when to use this library versus Celery, RQ,
  Dramatiq, SQS, RabbitMQ, Redis Streams, or Kafka.
- [x] Add production-readiness notes that distinguish supported use cases from
  future work.
