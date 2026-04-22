---
icon: lucide/list-checks
---

# Operational Maturity Checklist

This project is intentionally small: durable local queues for Python workers,
plus persistent retry state powered by Tenacity. The checklist below tracks the
work needed before describing it as a mature production queue system instead of
a local worker library.

## Near-term focus

The next release should improve two things that matter for real local workers:

- performance, especially under multiple producers and consumers sharing one
  SQLite file
- guarantees, especially where the project is intentionally at-least-once and
  best-effort rather than distributed or strongly ordered

That means measuring the current ceiling before promising more:

- benchmark WAL contention and queue throughput with concurrent processes
- use `examples/sqlite_concurrency_benchmark.py` to get a repeatable local
  throughput snapshot
- a quick local run on the development machine stayed in the low thousands of
  messages per second and handled a few producers and consumers without failing
  the queue, which is useful as a local ceiling check but not a production
  guarantee
- exercise abrupt consumer exit and lease recovery with
  `examples/sqlite_process_harness.py --mode crash-recovery`
- document the practical limits for a single host and a single SQLite file
- keep ordering language explicit: best effort under concurrency, not strict
  global ordering
- keep delivery language explicit: at least once, not exactly once
- keep recovery language explicit: inspection and requeue are local operations,
  not cross-cluster coordination

## Concurrency and locking

- [x] Document practical SQLite concurrency limits for producers and workers.
- [x] Add stress tests for multiple producer and consumer processes sharing one
  SQLite queue file.
- [x] Measure throughput and lock contention under WAL mode.
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
- [x] Evaluate deduplication support for enqueue operations.
- [x] Evaluate stronger ordering guarantees, or document why ordering is best
  effort under concurrency.
- [x] Document the failure window between a successful handler side effect and
  `ack()`.

## Retry and worker policies

- [x] Add examples for different policies by exception type.
- [x] Evaluate built-in permanent-failure classification.
- [x] Evaluate queue-level retry defaults that can be reused by many workers.
- [ ] Evaluate rate limiting for handlers that call external services.
- [ ] Evaluate circuit-breaker integration or documentation.
- [ ] Add clearer guidance for `release_delay`, Tenacity `wait`, and
  `lease_timeout` interactions.
- [x] Reject ambiguous worker-loop combinations such as `--forever` with
  `--max-jobs`.

## Observability

- [ ] Add optional Prometheus or OpenTelemetry metrics.
- [x] Track per-message attempt history, not only the latest error.
- [x] Track queue latency, handler duration, and time spent inflight.
- [x] Track throughput by queue and worker id.
- [x] Add aggregate failure summaries for dead-letter inspection.
- [x] Evaluate structured event logging for enqueue, lease, ack, release, and
  dead-letter transitions.

## Storage operations

- [ ] Define a schema migration strategy for SQLite stores.
- [x] Document backup and restore expectations.
- [x] Add maintenance guidance for `VACUUM`, WAL files, and long-lived stores.
- [x] Add retention or cleanup controls for dead-letter records.
- [x] Add cleanup controls for old retry records.
- [x] Add preview mode before cleanup removes records.
- [x] Add configurable retention defaults for dead letters and retry records.
- [x] Add a one-shot queue health summary for day-to-day checks.
- [x] Document recovery behavior for corrupted or partially written store files.
- [x] Add compatibility tests for SQLite store files created by older package
  versions.

## API stability

- [x] Decide which APIs are stable before a `1.0` release.
- [x] Document compatibility aliases and their planned lifetime.
- [x] Add deprecation policy.
- [ ] Increase coverage margin above the current 95% gate.
- [ ] Keep docs aligned with defaults for SQLite, optional LMDB, and CLI extras.

## Guardrails against misuse

- [x] Document recommended payload size limits.
- [x] Add clearer lease-timeout guidance for slow jobs.
- [x] Add examples for safe shutdown and long-running workers.
- [x] Add health-check examples for worker processes.
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
