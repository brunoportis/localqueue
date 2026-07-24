# localqueue v{{ version }}

This candidate consolidates the changes since v1.2.0. The final public claim is
deliberately left to the human promotion gate and must not exceed the collected
evidence.

## Highlights

- Semantic queue configuration through `DeliveryPolicy` and `DurabilityMode`.
- Generic payload flow across `Serializer`, `SimpleQueue`, `Job`, `Worker`, and
  typed EventBus handlers.
- First-class typed dead-letter inspection and replay for queues and EventBus
  subscriptions, including exact stored bytes and structured failure reasons.
- Executable Ruff, Pyrefly, and architectural import contracts in CI.
- A persisted `failure_reason` field with an idempotent, concurrency-safe schema
  migration for databases created by earlier 1.x releases.

See the [v1.3 migration guide](../docs/migrating-to-1.3.md),
[dead-letter guide](../docs/dead-letters.md),
[operational envelope](../docs/operational-envelope.md), and
[storage compatibility policy](../docs/storage-compatibility.md) before upgrading.

## Breaking Python API changes

Minor releases in the current 1.x series may intentionally change the Python API.
This release removes the old constructor mechanics rather than preserving aliases:

- `fsync=` becomes `durability=DurabilityMode.RELAXED|DURABLE`;
- `lease_seconds=` and `max_retries=` move into `delivery=DeliveryPolicy(...)`;
- public queue, serializer, job, worker, failed-message, and handler annotations are
  generic and stricter;
- `list_failed()` returns immutable typed records rather than dictionaries.

No deprecation aliases or compatibility shims are included. Follow the migration
guide for mechanical before/after examples.

## Persisted database compatibility

The Python API policy above is separate from storage compatibility. v1.3 adds one
nullable column, `messages.failure_reason TEXT`, to the existing 1.x schema.
Opening an older 1.x database performs an idempotent migration; existing rows are
not rewritten and missing, future, or unrecognized reasons surface as
`FailureReason.LEGACY_UNKNOWN`.

The release matrix opens and exercises real databases created by published
1.0.0, 1.0.1, 1.1.0, 1.1.1, 1.1.2, and 1.2.0 wheels. This is a forward-opening
release check and foundation for later releases, not a downgrade, mixed-version,
or arbitrary custom-serializer guarantee. Back up the database and coordinate all
processes onto one version when upgrading.

## Dead-letter replay

Replay preserves the stored row identity and exact payload bytes, resets delivery
state, and makes the record ready again under the current capacity policy. Replay
is at least once: external side effects can happen again. It is not exactly once,
and corrupt payloads or unknown event types may fail again.

## Limits and unsupported deployment modes

- NFS, SMB, other network filesystems, and multi-host access are unsupported.
- Delivery and replay are at least once; exactly-once processing is not provided.
- Process-crash evidence is not physical power-loss evidence.
- Physical ARM64 hardware and abrupt-power validation remain outstanding; Linux
  ARM64 artifacts are validated under emulation, not called a physical smoke test.
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
