# Runtime diagnostics

`SimpleQueue.diagnostics()` captures a bounded, read-only operational report
for one logical queue without requiring direct SQLite access:

```python
import json

from localqueue import SimpleQueue


queue = SimpleQueue("./data")
report = queue.diagnostics()

print(report)
print(json.dumps(report.to_dict(), indent=2))
queue.close()
```

The return value is a frozen, slotted `QueueDiagnostics` dataclass. Its
`to_dict()` method contains only JSON scalar values and `None`; it never exposes
the private native snapshot. Report format `schema_version` starts at `1`.

## Fields

All names and types in schema version 1 are public. **Stable** means the field's
meaning is intended for programmatic use. **Informational** means callers may
display or record the value, but should not use its exact value as a portable
policy or consistency boundary.

| Field | Type | Class | Source and meaning |
| --- | --- | --- | --- |
| `schema_version` | `int` | Stable | Diagnostics format version; currently `1`. |
| `package_version` | `str` | Informational | Installed `localqueue` distribution metadata. A source checkout without distribution metadata reports `"development"`; other metadata errors propagate. |
| `sqlite_version` | `str` | Informational | SQLite library version linked into the native extension. |
| `observed_at` | `float` | Stable | Unix wall-clock seconds captured once by the native operation. |
| `queue_name` | `str` | Stable | Selected logical queue. |
| `serializer_identity` | `str` | Stable | `localqueue.JsonSerializer` for the default, otherwise the serializer class's deterministic `module.qualname`; no `repr()` or internal state. |
| `lease_seconds` | `float` | Stable | Lease duration configured on this Python queue object. |
| `max_retries` | `int` | Stable | Retry limit configured on this Python queue object. |
| `journal_mode` | `str` | Informational | Effective `PRAGMA journal_mode` read from the queue's Rust-owned SQLite connection. |
| `synchronous` | `int` | Informational | Effective numeric `PRAGMA synchronous` from that same connection. SQLite currently reports `1` for NORMAL and `2` for FULL. |
| `durability_mode` | `"normal" \| "full" \| "unknown"` | Stable | Portable mapping of the observed synchronous value. Unrecognized values map to `"unknown"`. |
| `busy_timeout_ms` | `int` | Informational | Effective connection-local `PRAGMA busy_timeout`, read from the real Rust connection. |
| `database_size_bytes` | `int \| None` | Informational | Best-effort main database file size. |
| `wal_size_bytes` | `int \| None` | Informational | Best-effort WAL sidecar size. |
| `shm_size_bytes` | `int \| None` | Informational | Best-effort shared-memory sidecar size. |
| `page_count` | `int` | Informational | `PRAGMA page_count` in the SQL read snapshot. |
| `page_size` | `int` | Informational | `PRAGMA page_size` in the SQL read snapshot. |
| `freelist_count` | `int` | Informational | `PRAGMA freelist_count` in the SQL read snapshot. |
| `ready` | `int` | Stable | All ready records in the selected queue, including delayed records. |
| `processing` | `int` | Stable | Leased records in the selected queue, including expired leases not yet reclaimed. |
| `acked` | `int` | Stable | Acknowledged records in the selected queue. |
| `failed` | `int` | Stable | Dead-letter records in the selected queue. |
| `oldest_available_age_seconds` | `float \| None` | Stable | Age since `available_at` of the oldest ready record that was claimable at `observed_at`; future delayed jobs are excluded. |
| `oldest_processing_updated_age_seconds` | `float \| None` | Stable | Age since the oldest leased record's last `updated_at`, not its original processing start. `extend_lease()` updates this timestamp. |
| `active_leases` | `int` | Stable | Processing records whose `lease_until` is after `observed_at`. |
| `expired_leases` | `int` | Stable | Processing records whose `lease_until` is at or before `observed_at`; diagnostics does not reclaim them. |
| `oldest_expired_lease_age_seconds` | `float \| None` | Stable | Time since the earliest currently expired `lease_until`, or `None` when no lease is expired. |

`None` is also returned for an age when no matching message exists. Optional
file sizes use `None` when a file is absent or its metadata is temporarily
unavailable.

## Time semantics

Persisted queue timestamps use the system wall clock. The native operation
captures one `observed_at` and uses it for every age and lease classification
in that report. Ages are clamped to zero if the wall clock moves backwards.
They are not monotonic between processes or between calls: NTP, administrator
changes, virtual-machine clock corrections, and suspend/resume can change the
result. A monotonic clock is suitable for measuring the local duration of a
call, but cannot be compared with the persisted wall-clock timestamps.

The current schema does not preserve the original processing start across
lease extensions. `oldest_processing_updated_age_seconds` therefore states
exactly what can be known: time since the last processing-state update. It does
not invent a processing duration.

## Snapshot and filesystem limits

Counts, ages, lease classifications, page values, and connection-local PRAGMAs
come from the Rust-owned `SimpleQueue` connection. Counts and temporal
aggregates are bounded SQL aggregates executed in one read transaction; no
payload is materialized or deserialized. A separate Python `sqlite3` connection
is not used as the source for `synchronous` or `busy_timeout_ms`.

The main database, WAL, SHM, and page metrics describe the file shared by every
logical queue in the directory; only logical counts and ages are filtered by
`queue_name`. File metadata is read best effort after the coherent SQL
snapshot. WAL and SHM can appear, disappear, or change size concurrently, so
their sizes are not promised to represent the exact SQL snapshot instant. No
absolute path is included in the report.

NORMAL and FULL describe SQLite synchronization policy. FULL requests more
synchronization than NORMAL, but the report makes no claim about physical
power-loss behavior through the operating system, filesystem, controller,
firmware, or hardware.

`diagnostics()` does not reclaim leases, alter receipts or timestamps, retry
failed work, checkpoint WAL, run `integrity_check`, change PRAGMAs, purge,
VACUUM, or accept arbitrary SQL. Integrity checking and online backup are
separate maintenance APIs planned in issue #22. Calling diagnostics after
`close()` raises `LocalQueueError("queue is closed")` and never reopens the
database.
