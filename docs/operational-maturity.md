---
icon: lucide/list-checks
---

# Operations Guide

This page focuses on day-to-day runtime behavior for `localqueue`: how it runs,
what to measure, and which built-in tools help operate a queue on one machine.

## Concurrency and storage

The default queue store is SQLite. It works well for scripts, CLI tools, and
small workers that share a local queue file.

For local throughput and lock-contention checks, use:

- `examples/sqlite_concurrency_benchmark.py`
- `examples/sqlite_process_harness.py --mode throughput`
- `examples/sqlite_process_harness.py --mode crash-recovery`

These exercises are useful when tuning producer and consumer counts, lease
timeouts, and worker pacing for a concrete host.

## Delivery and recovery

Queue delivery is at least once. Under concurrency, ready-message ordering is
best effort. Design handlers to be idempotent and to tolerate retries after a
successful side effect but before `ack()`.

The CLI provides the core local recovery loop:

- `localqueue queue stats <queue>`
- `localqueue queue health <queue>`
- `localqueue queue dead <queue>`
- `localqueue queue requeue-dead <queue>`
- `localqueue queue inspect <queue>`

Use dead-letter inspection and requeue to recover failed work without losing
the original payload or latest error details.

## Worker policy tuning

`QueueSpec` and `PersistentWorkerConfig` cover the most common runtime tuning:

- retry budgets with `with_retry(...)`
- delayed redelivery with `with_release_delay(...)`
- pacing with `with_min_interval(...)`
- breaker pauses with `with_circuit_breaker(...)`
- dead-letter behavior with `with_dead_letter_on_failure(...)`

Use queue-level defaults when several workers should share the same delivery and
retry behavior. Override per worker only when one consumer needs different
timing or error handling.

## Observability

The built-in queue state is designed to support inspection from Python and the
CLI.

- queue stats include ready, inflight, delayed, and dead-letter counts
- message records keep attempt counters and latest error data
- health output summarizes queue activity for routine checks
- worker identity and inflight timing support lease and recovery inspection

For long-lived deployments, pair these commands with host-level logging and
process supervision.

## Maintenance

The SQLite store keeps its schema version in `PRAGMA user_version`. Releases can
migrate older compatible versions and reject newer unknown versions explicitly.

For operational hygiene:

- back up queue files before host-level maintenance
- vacuum long-lived SQLite files during maintenance windows
- monitor dead-letter and retry retention over time
- use preview modes before destructive cleanup commands
