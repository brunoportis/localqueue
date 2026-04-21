# Changelog

## 0.1.0

- Add persistent retry wrappers for sync and async Tenacity retryers.
- Add LMDB-backed persistent queues with leases, delayed delivery,
  acknowledgements, release, dead-letter records, and requeue from dead-letter
  storage.
- Add CLI commands for config, queue add/pop/ack/release/dead-letter/stats,
  inspect, dead-letter listing, dead-letter requeue, and continuous processing.
- Add worker identity metadata with `--worker-id` and `leased_by` for inflight
  message inspection.
- Add focused examples for enqueueing and processing email jobs locally.
- Add MIT license metadata for package distribution.
- Add `dead_letter_on_failure` as the preferred worker failure policy option,
  keeping `dead_letter_on_exhaustion` as a compatibility alias.
