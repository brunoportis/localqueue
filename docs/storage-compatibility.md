# Storage compatibility

The [operational envelope](operational-envelope.md) summarizes the operational
scope and limitations of this compatibility policy.

Version 1.0.0 starts the current storage lineage. Databases made by 0.5.0 do
not have an in-place upgrade path to 1.x; preserve the existing 0.5.0 migration
guidance in the changelog when moving from that release.

We test forward opening and normal queue operations from published wheels
`v1.0.0`, `v1.0.1`, `v1.1.0`, `v1.1.1`, `v1.1.2`, and `v1.2.0` to the
candidate wheel. Each public release added to this matrix becomes a baseline
for later releases. This is deliberately a persisted-storage contract, not a
promise of Python API compatibility between minor releases.

The matrix uses real public wheels, not hand-authored SQLite fixtures. Its
Linux x86_64 / CPython 3.14 scope creates ready, leased, ACKed, delayed,
dead-letter, error, and deduplicated queue records. EventBus fixtures apply to
the 1.1.x baselines. Custom serializers remain the application's
responsibility: current code must still deserialize its older payloads.

Run the same check locally:

```bash
uv run python compatibility/run_matrix.py --current . --output compatibility-report.json
```

The report records verified wheel names and hashes, isolation paths, fixture and
operation assertions, SQLite integrity, schema fingerprint, and explicit
limitations. A future schema change must update the policy, matrix or
incompatibility rationale, and tests together.

## v1.3 migration

Version 1.3 adds nullable `failure_reason TEXT NULL` to the `messages` table.
Opening an older database first inspects the schema without taking a writer
lock. Only when the column is absent does it open `BEGIN IMMEDIATE`; it checks
again inside that transaction and runs `ALTER TABLE` only when still necessary.
The migration is therefore additive and idempotent. Reopening an already
migrated database does not take a writer lock just to migrate.

Existing rows are not rewritten: their `failure_reason` remains `NULL`, and the
compatible existing classification exposes legacy or unknown values as
`FailureReason.LEGACY_UNKNOWN`.

## Guaranteed

- A database made by a published, tested baseline can be opened by v1.3.
- Existing messages, IDs, and metadata remain present.
- Older failed records remain inspectable.
- The matrix uses verified published wheels to create representative pending,
  leased, acknowledged, retried, failed, deduplicated, timestamped, and
  `job_id` records before v1.3 opens them.

## Not guaranteed

- Downgrading a v1.3 database with older code.
- Different versions writing the same database concurrently, or a
  mixed-version deployment.
- Databases shared over NFS or SMB.
- Decoding historical application payloads with a serializer that is no longer
  compatible with those bytes.
- Exactly-once processing or replay.
