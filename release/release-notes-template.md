# localqueue v{{ version }}

This candidate consolidates the changes since v1.3.0. The final public claim is
deliberately left to the human promotion gate and must not exceed the collected
evidence.

## Breaking Python API changes

- Python 3.10 is no longer supported; localqueue now requires Python 3.11 or newer.

## New public APIs

- Durable event identity, resumable source ingestion, and finite
  `EventBus.execute()` operations.
- Typed handler contexts, explicit retry/reject control flow, and CSV sources.

## Typing improvements

- Typed EventBus sources, handler contexts, and execution results.

## Dead-letter inspection and replay

- Subscription-scoped `FailedDelivery` inspection and replay.

## Persisted database compatibility

- A transactional SQLite/Rust core with documented atomicity, durability, lease,
  retry, and at-least-once guarantees.
- Bounded producer backpressure and typed runtime diagnostics.
- Integrity checking and online backup operations.
- Canonical single-process and multiprocess benchmarks with correctness gates.
- Additive, idempotent `messages.failure_reason` migration and release-to-release
  online/offline validation.
- EventBus correlation/causality metadata and bounded per-subscription concurrency.
- Explicit timeout behavior for asynchronous EventBus handlers.

See the [operational envelope](../docs/operational-envelope.md) and
[storage compatibility policy](../docs/storage-compatibility.md) before upgrading.

## Replay guarantees and limitations

Upgrade from v1.3.0 using the wheel matching the supported Python and platform
matrix. Back up queue databases before upgrading and follow the documented
compatibility procedure. Replay is at-least-once; handlers must remain idempotent
when duplicate external effects matter.

## Artifacts and supported platforms

The evidence inventory records each validated distribution and its smoke status.
Linux ARM64 is build/QEMU-validated only unless evidence explicitly says otherwise.

## Known limitations

- NFS, SMB, other network filesystems, and multi-host access are unsupported.
- Delivery is at least once; exactly-once processing is not provided.
- Process-crash evidence is not physical power-loss evidence.
- Physical ARM64 hardware and abrupt-power validation remain outstanding in #31;
  Linux ARM64 artifacts are validated under emulation, not called a physical smoke test.
- Guarantees remain bounded by SQLite, the filesystem, storage hardware, and the
  documented single-host operational envelope.

## Release evidence

The complete evidence manifest, distribution inventory, checksums, CI summary,
soak/crash/chaos reports, compatibility reports, benchmarks, documentation audit,
open-issue audit, and security audit will be attached to the GitHub Release after
successful human-approved promotion.

## Proposed public wording

The maintainer must select one exact option at the promotion gate:

- `production-grade transactional core`
- `production-ready for documented single-host workloads`
- `validated for documented single-host workloads` (more conservative)

No option in this candidate note is a definitive production-readiness claim.
