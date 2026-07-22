# Operational SQLite chaos campaign

The canonical command is:

```bash
uv run python stress/run_chaos.py --profile ci --output chaos-report.json
```

To preserve all child logs, databases, and WAL/SHM snapshots in a stable
location, use:

```bash
uv run python stress/run_chaos.py \
  --profile ci \
  --output artifacts/chaos-report.json \
  --artifacts-dir artifacts/scenarios
```

Use `--scenario NAME` to run one entry from the versioned catalog. The `ci`
profile runs ten isolated scenarios; `smoke` runs a short subset. The command
does not rely on the current working directory and always attempts to write a
JSON report. A non-zero exit means that the report is not fully passing.

## Evidence and limits

The campaign creates canonical databases and performs product operations through
`SimpleQueue`; direct SQL is limited to inducing conditions, locks, PRAGMAs,
integrity/evidence inspection, and the internal backup fixture. Disk-full uses
an SQLite connection-local page limit applied by a `__crash_test`-only native
hook, not an attempt to fill the runner. The hook exists because
`max_page_count` set by an external connection does not constrain the product's
separate connection. Read-only uses actual
POSIX permissions and is skipped with a reason when the runner cannot enforce
them (for example, root). Lock contention uses a separate SQLite connection
and reads the configured `busy_timeout` from the actual Rust connection; it
does not infer lock ownership from sleep. Connection-local `synchronous` and
`busy_timeout` values from external Python connections are never reported as
product configuration.

WAL recovery requires a real `localqueue.db-wal` file, records SHM
separately, and proves committed state can be reopened and checkpointed. It is
not physical power-loss evidence. `synchronous=NORMAL` is the
default crash-oriented mode; `FULL` requests stronger SQLite synchronization,
but neither mode makes `SIGKILL` equivalent to a power cut through hardware,
host caches, or storage firmware.

An operation that returned an error is recorded as unconfirmed. Retry is marked
safe only when the fixture performs a successful retry and validates the
result. Errors are retained in normalized form instead of being hidden by an
automatic retry. Corrupt input fails explicitly and is never reset or
recreated; arbitrary media corruption is not promised to be recoverable.

Producer and maintenance termination use an explicit child synchronization
line before `SIGKILL`, followed by fresh-process integrity validation. They
prove transactional rollback for the selected boundary, not external side
effects or exactly-once delivery. The internal backup/restore fixture uses
`sqlite3.Connection.backup`; it validates a consistent SQLite copy only. It is
not the public backup API planned for issue #22.

## Report

Report schema version 1 includes the profile, platform metadata, summary, each
scenario's status, expected outcome, confirmation state, error, pragmas,
counts, integrity result, invariants, artifacts, and limitations. Timestamps,
PIDs, and absolute paths are intentionally not used for comparisons. A skip
always contains a reason, and a missing or failed scenario makes `passed`
false.

The scheduled and campaign-path pull-request Linux workflow builds a separate
`__crash_test` extension and preserves the report plus scenario directory with
`if: always()`. The normal extension is checked first to ensure test-only hooks
are absent from ordinary builds.
