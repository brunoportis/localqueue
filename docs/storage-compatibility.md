# Storage compatibility

The [operational envelope](operational-envelope.md) summarizes the operational
scope and limitations of this compatibility policy.

Version 1.0.0 starts the current storage lineage. Databases made by 0.5.0 do
not have an in-place upgrade path to 1.x; preserve the existing 0.5.0 migration
guidance in the changelog when moving from that release.

We test forward opening and normal queue operations from published wheels
`v1.0.0`, `v1.0.1`, `v1.1.0`, `v1.1.1`, and `v1.1.2` to the candidate wheel.
This is not a downgrade guarantee. Copy or back up a database before upgrading,
coordinate all processes onto one version during the upgrade, and do not run
mixed versions concurrently.

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

The v1.3 schema adds nullable `messages.failure_reason TEXT`. Opening an older
database first checks the column read-only. Already-migrated databases return
through that fast path without acquiring SQLite's writer lock. `BEGIN
IMMEDIATE` is acquired only when the column appears absent; after acquiring the
lock, the migration checks again before running `ALTER TABLE`. This double-check
lets concurrent old-database openers serialize safely without attempting the
same schema change twice. Existing rows are not rewritten; null and
unrecognized values are exposed as `FailureReason.LEGACY_UNKNOWN`. Older
releases use explicit insert/select columns and tolerate the additional column.
