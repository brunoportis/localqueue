# Changelog

All notable changes to `localqueue` are documented here.

<!-- version list -->

## [1.1.1] - 2026-07-21

### Added

- Zensical documentation site with a durable, two-process getting-started
  guide and GitHub Pages deployment workflow.
- Python branch-coverage reporting, Ruff, Pyrefly, pre-commit, Gitleaks, and
  cargo-deny quality gates.
- Manual Conventional Commits release workflow with Python, Cargo, lockfile,
  tag, and wheel-release consistency checks.

### Changed

- The README quickstart now demonstrates a producer persisting a job before a
  later worker process consumes it.
- CI caches Rust build outputs and uses debug builds for test jobs while wheel
  builds remain optimized release builds.

## [1.1.0] - 2026-07-21

### Added

- `SimpleQueue.put_many(...)` for atomic batch enqueueing, with optional
  per-item deduplication through `EnqueueItem`.
- The optional `localqueue.bus` package, exposing `BaseEvent` and `EventBus`
  when installed with `localqueue[bus]`.
- Atomic event fan-out to durable subscriptions, wildcard handlers, consumer
  groups across processes, lease heartbeats, retries, and dead-letter handling.
- Fan-out benchmarks and multiprocess coverage for the event bus.
- Event routing now uses an explicit static `BusTopology`, allowing producers
  and consumers to run in separate processes without producers importing
  consumer handlers.
- Handler registration is separate from subscription declaration through
  `bus.subscription(...).handler(...)`.

### Changed

- Blocking queue operations and synchronous event handlers are moved off the
  asyncio event loop.
- Idle polling avoids repeated SQLite writer transactions while queues are
  empty, reducing lock contention across many idle subscriptions.
- Public docstrings, log messages, validation errors, and native queue errors
  are now consistently written in English.
- Release wheels cover CPython 3.10 through 3.14 on Linux x86-64/aarch64,
  macOS x86-64/arm64, and Windows x86-64.

### Fixed

- Reject worker heartbeat intervals that are not shorter than the queue lease,
  non-positive lease extensions, and negative NACK delays.

### Compatibility

- Existing `SimpleQueue` and `Worker` APIs remain backward compatible.
- Event bus support is optional and requires Pydantic 2 via the `bus` extra.

## Migrating from 0.5.0

Version 1.0.0 was a complete, backward-incompatible reimplementation of the
legacy `localqueue` package. Upgrading from 0.5.0 is not an in-place library or
storage migration:

- Replace `PersistentQueue` with `SimpleQueue` and adapt producers and
  consumers to the explicit `put` → `get` → `ack`/`nack` lifecycle.
- The Tenacity-backed persistent retry API and the `cli`, `lmdb`, and `sqlite`
  extras from 0.5.0 are not part of 1.x. Version 1.x uses its bundled SQLite
  engine and Rust extension; install `localqueue[bus]` only when the event bus
  is needed.
- Do not reuse a 0.5.0 database with 1.x. Drain or export pending work with
  0.5.0 first, create a new 1.x queue directory, and enqueue any work that must
  be retained.
- Imports, configuration, storage layout, and operational commands from 0.5.0
  are not guaranteed to work in 1.x. Test the migration in a separate virtual
  environment before replacing a production worker.
- Version 1.x supports Python 3.10 and newer and is licensed under Apache-2.0;
  version 0.5.0 required Python 3.11 and used the MIT license.

## [1.0.1] - 2026-07-20

### Changed

- Expanded the public README with installation, delivery guarantees,
  configuration, API, architecture, and development guidance.
- Declared and validated support for Python 3.10 through 3.14 across Linux,
  macOS, and Windows wheels.

## [1.0.0] - 2026-07-20

### Changed

- Reimplemented `localqueue` around `SimpleQueue`, a bundled SQLite database,
  and a native Rust extension.
- Replaced the legacy 0.5.0 API and storage model with explicit ACK/NACK,
  leases, bounded retries, dead-letter handling, receipt fencing, and
  multiprocess safety on one machine.

[1.1.1]: https://github.com/brunoportis/localqueue/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/brunoportis/localqueue/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/brunoportis/localqueue/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/brunoportis/localqueue/releases/tag/v1.0.0
