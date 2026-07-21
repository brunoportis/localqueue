# Changelog

All notable changes to `localqueue` are documented here.

## [1.1.0] - 2026-07-21

### Added

- `SimpleQueue.put_many(...)` for atomic batch enqueueing, with optional
  per-item deduplication through `EnqueueItem`.
- The optional `localqueue.bus` package, exposing `BaseEvent` and `EventBus`
  when installed with `localqueue[bus]`.
- Atomic event fan-out to durable subscriptions, wildcard handlers, consumer
  groups across processes, lease heartbeats, retries, and dead-letter handling.
- Fan-out benchmarks and multiprocess coverage for the event bus.

### Changed

- Blocking queue operations and synchronous event handlers are moved off the
  asyncio event loop.
- Idle polling avoids repeated SQLite writer transactions while queues are
  empty, reducing lock contention across many idle subscriptions.
- Release wheels cover CPython 3.10 through 3.14 on Linux x86-64/aarch64,
  macOS x86-64/arm64, and Windows x86-64.

### Compatibility

- Existing `SimpleQueue` and `Worker` APIs remain backward compatible.
- Event bus support is optional and requires Pydantic 2 via the `bus` extra.

[1.1.0]: https://github.com/brunoportis/localqueue/compare/v1.0.1...v1.1.0
