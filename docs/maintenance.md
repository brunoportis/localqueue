# Integrity checks and online backups

`SimpleQueue` exposes explicit maintenance APIs for checking a live SQLite
database and creating a consistent point-in-time backup without direct access
to the queue's connection:

```python
from pathlib import Path

from localqueue import SimpleQueue


queue = SimpleQueue("./data")

integrity = queue.check_integrity()
if not integrity.ok:
    for message in integrity.messages:
        print(message)

backup = queue.backup(Path("./backups/localqueue.db"))
print(backup.destination, backup.database_size_bytes)
queue.close()
```

Both return frozen, slotted dataclasses with JSON-compatible `to_dict()`
methods. Maintenance failures raise `LocalQueueError`, except invalid arguments
(`ValueError`) and an existing destination (`FileExistsError`). Neither API
repairs or recreates a damaged source database.

## Integrity checking

```python
full = queue.check_integrity()
quick = queue.check_integrity(mode="quick")
```

`mode="full"` runs SQLite `PRAGMA integrity_check`. It checks low-level
database formatting, table and index consistency, constraints, and freelist
ownership. `mode="quick"` runs `PRAGMA quick_check`; it is faster but skips
UNIQUE and index-content consistency checks. SQLite does not include foreign
key violations in either operation.

`IntegrityCheckResult` contains:

| Field | Type | Meaning |
| --- | --- | --- |
| `ok` | `bool` | True only when SQLite returned the single message `"ok"`. |
| `messages` | `tuple[str, ...]` | Every diagnostic row returned by SQLite, unchanged and in order. |
| `elapsed_seconds` | `float` | Monotonic elapsed time spent executing and reading the check. |
| `check_mode` | `"full" \| "quick"` | The selected SQLite check. |

The integrity check uses the queue's active SQLite connection. It waits for
that connection's mutex and holds it until all messages have been read. A full
check is proportional to database and index size and can delay operations made
through the same `SimpleQueue` instance. Other SQLite connections may also
experience filesystem/cache pressure while the scan runs.

An unsuccessful result is operational evidence, not an exception: inspect and
retain `messages` before deciding how to recover. SQLite execution and I/O
failures still raise `LocalQueueError` with the underlying message.

## Online backup

```python
backup = queue.backup("./backups/localqueue.db")

# Replacing a destination is never implicit.
replacement = queue.backup("./backups/localqueue.db", overwrite=True)
assert replacement.overwritten
```

The destination is a database **file**, not a directory. Its parent directory
must already exist. By default, an existing path raises `FileExistsError`
without changing its contents. `overwrite=True` replaces an existing regular
file only after a complete temporary backup has passed an independent
`PRAGMA integrity_check`. A destination equal to the active source database or
a destination directory is rejected.

`BackupResult` contains the caller-facing destination string, whether a file
existed when the operation began, monotonic elapsed time, and final database
size. It does not expose the source database's absolute internal path.

The implementation uses SQLite's Online Backup API through a separate
read-only source connection. It copies bounded groups of pages and releases
SQLite read locks between steps, so producers and consumers using other
connections can continue. Concurrent commits may make SQLite restart copying
some pages, but a completed destination is one valid point-in-time snapshot.
Heavy continuous writes can increase latency substantially. In a pathological
workload that changes the source faster than SQLite can copy it, the operation
may not converge; this API does not impose a total-duration timeout.

The temporary file is created beside the destination, keeping publication on
one filesystem. The filesystem must support normal SQLite files and hard links
for collision-safe first publication. Ensure enough free space for the full
database plus temporary filesystem overhead. Permission, capacity, lock, and
other SQLite/I/O failures are surfaced and the unfinished temporary file is
removed. Overwrite publication is atomic on Unix-like systems; Windows cannot
portably replace an open/existing file with the same rename primitive, so the
old destination is removed immediately before the completed temporary file is
renamed.

## Verify and recover

Treat a backup as a recovery candidate until it has been verified in the
environment where it will be restored. The public API already runs a full
integrity check before publishing, and operators can independently verify both
integrity and logical state:

```python
import sqlite3


with sqlite3.connect("file:./backups/localqueue.db?mode=ro", uri=True) as db:
    assert db.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    counts = dict(
        db.execute(
            "SELECT status, COUNT(*) FROM messages GROUP BY status ORDER BY status"
        )
    )
    print(counts)
```

For a recovery drill, stop writers to the recovery location, place the verified
snapshot at `<new-directory>/localqueue.db`, open that directory with
`SimpleQueue`, call `check_integrity()` again, and compare `stats()` and sample
payload lifecycles with the expected recovery point. Keep the original database
and its sidecars untouched until the recovered queue has been accepted. These
APIs do not repair corruption, schedule backups, encrypt files, or provide
incremental/remote backups.
