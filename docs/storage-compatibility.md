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

## v1.4 migration

Version 1.4 adds nullable `failure_category TEXT NULL` to the `messages` table
for structured handler rejections. Opening an older database follows the same
additive, idempotent migration protocol used by `failure_reason`: inspect
without a writer lock, acquire `BEGIN IMMEDIATE` only when the column is
missing, and check again inside the transaction before `ALTER TABLE`.

Existing rows remain unchanged with a `NULL` category. Rejected deliveries
persist their reason in `last_error`, their stable classification as
`FailureReason.REJECTED`, and their optional category in `failure_category`.

## v1.5 migration

Version 1.5 adds `event_bus_executions` and the many-to-many
`event_bus_execution_deliveries` table. The membership key is
`(execution_id, message_id)` with cascading foreign keys to executions and
messages, plus an index from a message to all of its execution memberships.
Opening an older database creates only these tables and indexes in one
transaction; it neither rebuilds nor rewrites `messages`.

Online backups copy these tables as part of the SQLite database snapshot, and
normal SQLite integrity checks include their foreign-key structure.

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
