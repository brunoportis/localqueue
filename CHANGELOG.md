# Changelog

## [0.4.0](https://github.com/brunoportis/localqueue/compare/v0.3.4...v0.4.0) (2026-04-24)


### Features

* add release-please automation and CLI --version option ([f8db976](https://github.com/brunoportis/localqueue/commit/f8db9767f45fc1eb5055ef0e7eb946aae3af3129))


### Bug Fixes

* **release:** target uv.lock version explicitly ([3e71f44](https://github.com/brunoportis/localqueue/commit/3e71f4450e50de69488b18c04b018a6af00bf962))
* **release:** target uv.lock version explicitly ([31b45ce](https://github.com/brunoportis/localqueue/commit/31b45ce2ba3deea070d8d49c8bebebadcbc9fb37))
* **release:** track uv.lock in release-please ([897b86a](https://github.com/brunoportis/localqueue/commit/897b86aa33d9b38cba82da7b0e2623473488b94d))
* **release:** track uv.lock in release-please ([637ef7f](https://github.com/brunoportis/localqueue/commit/637ef7fca945f1153b643f27b0d6df6ccc39f778))
* **workflow:** configure git identity for release sync ([9872e7a](https://github.com/brunoportis/localqueue/commit/9872e7a85d1382a968f344444009e721da926e50))
* **workflow:** configure git identity for release sync ([2ef15fd](https://github.com/brunoportis/localqueue/commit/2ef15fdd3b5a877569b46980ef795442c1f3089c))
* **workflow:** sync uv.lock in release-please ([f26b1fa](https://github.com/brunoportis/localqueue/commit/f26b1fac2603f5ae0016ce994f5065291b6a6ad0))
* **workflow:** sync uv.lock in release-please ([193b4c6](https://github.com/brunoportis/localqueue/commit/193b4c6d5dba208ec43d7ea6ada402c5c4f1c2a5))


### Documentation

* **use-cases:** simplify CLI examples ([84b92f2](https://github.com/brunoportis/localqueue/commit/84b92f2a420ddeb46dc8db2e6c1493fc45430bff))

## 0.3.4

- Keep `queue exec` and `queue process` alive in `--forever --block` mode when the queue starts empty.
- Preserve the batch empty-queue exit behavior for non-forever workers.

## 0.3.3

- Split queue and retry store implementations into dedicated modules.
- Refactor the CLI worker option wiring and centralize worker helpers.
- Add a SonarCloud workflow and stabilize the SQLite concurrency queue test.

## 0.3.2

- Add a CLI Docker image and publish it to GHCR on version tags.
- Raise the coverage gate to 100% and align the README badge.

## 0.3.1

- Use XDG data directories for default SQLite queue and retry store files.
- Move the exploratory retry demo from `main.py` to `examples/retry_demo.py`.
- Make `PersistentQueue` unfinished-message tracking O(1) by message id.
- Add an end-to-end SQLite worker test covering queue ack and retry cleanup.
- Make `store_path=` for persistent retries open a SQLite attempt store.
- Tighten permanent-failure classification to avoid dead-lettering common
  transient application errors such as `ValueError` and `KeyError`.
- Optimize SQLite queue stats and dead-letter retention queries with
  materialized columns and schema migration support.
- Add indexed SQLite retry-store retention for exhausted records.

## 0.3.0

- Add a concurrent SQLite stress test for multiple producers and consumers.
- Add `examples/sqlite_concurrency_benchmark.py` for measuring local queue throughput.
- Make the SQLite benchmark reset its store and retry transient lock contention.
- Add `examples/sqlite_process_harness.py` for process-level throughput and crash-recovery checks.
- Add process-based stress tests for SQLite producer and consumer coordination.
- Add dead-letter filters and summaries for `queue dead`.
- Add enqueue deduplication with `--dedupe-key` and `dedupe_key=`.
- Add queue-level retry defaults that workers can inherit from `PersistentQueue`.
- Add worker rate limiting and circuit-breaker controls.
- Add usage docs for rate limiting, circuit breaker, and queue positioning.
- Add a short-term maturity note focused on performance and guarantees.

## 0.2.0

- Add `queue stats --watch` for monitoring queue counts while workers run.
- Add `queue dead --watch` for repeatedly listing dead letters.
- Add `queue requeue-dead --all` for bulk recovery after a fix.
- Add structured `command not found` handling for `queue exec`.
- Add `examples/process_webhook.sh` as a shell/curl worker example.

## 0.1.1

- Add project URLs for PyPI metadata.
- Document `queue exec` command-failure fields stored in `last_error`.

## 0.1.0

- Add persistent retry wrappers for sync and async Tenacity retryers.
- Add SQLite-backed durable local queues with leases, delayed delivery,
  acknowledgements, release, dead-letter records, and requeue from dead-letter
  storage. LMDB remains available as an optional backend.
- Add CLI commands for config, queue add/pop/ack/release/dead-letter/stats,
  inspect, dead-letter listing, dead-letter requeue, and continuous processing.
- Add `queue exec` for processing messages with external commands that receive
  the message value as JSON on stdin.
- Add worker identity metadata with `--worker-id` and `leased_by` for inflight
  message inspection.
- Document local-worker operational boundaries, at-least-once delivery, and
  idempotency guidance.
- Add focused examples for enqueueing and processing email jobs locally.
- Add MIT license metadata for package distribution.
- Add `dead_letter_on_failure` as the preferred worker failure policy option,
  keeping `dead_letter_on_exhaustion` as a compatibility alias.
- Enable SQLite WAL journal mode and normal synchronous mode for improved
  concurrency in `SQLiteAttemptStore`.
