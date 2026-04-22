# Changelog

## Unreleased

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
