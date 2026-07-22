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

backup = queue.backup(Path("./backups/2026-07-22"))
print(backup.destination, backup.database_path)
queue.close()
```

Both return frozen, slotted dataclasses with JSON-compatible `to_dict()`
methods. Maintenance failures raise `LocalQueueError`, except invalid arguments
(`ValueError` or `TypeError`) and an existing destination (`FileExistsError`).
Neither API repairs or recreates a damaged source database.

## Integrity checking

```python
full = queue.check_integrity()
quick = queue.check_integrity(mode="quick", max_errors=25)
```

`mode="full"` runs SQLite `PRAGMA integrity_check`. It checks low-level
database formatting, table and index consistency, constraints, and freelist
ownership. `mode="quick"` runs `PRAGMA quick_check`; it is faster but skips
UNIQUE and index-content consistency checks. SQLite does not include foreign
key violations in either operation. `max_errors` limits the reported
diagnostics and must be an integer from 1 through 1000, inclusive.

`IntegrityCheckResult` contains:

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | `int` | Public result schema version; currently `1`. |
| `mode` | `"full" \| "quick"` | The selected SQLite check. |
| `max_errors` | `int` | Validated maximum number of diagnostic rows. |
| `ok` | `bool` | True only when SQLite returned the single message `"ok"`. |
| `messages` | `tuple[str, ...]` | Every diagnostic row returned by SQLite, unchanged and in order. |
| `elapsed_seconds` | `float` | Monotonic elapsed time spent executing and reading the check. |

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
backup = queue.backup("./backups/2026-07-22")
assert backup.database_path == "./backups/2026-07-22/localqueue.db"
```

The destination is a new **directory**, not a database filename. Its parent
must already exist and be a directory. The operation reserves the final
directory with one exclusive creation call; an existing file or directory
raises `FileExistsError` without changing that path. A destination equal to or
inside the active source directory is rejected. There is no overwrite mode.

`BackupResult` schema version 1 contains the caller-facing destination and
`localqueue.db` paths, monotonic elapsed time, real Online Backup API page
progress, final database size, and the full verification result. Tuple fields
become lists in `to_dict()`. It never exposes the source database path.

The implementation uses SQLite's Online Backup API through a separate
read-only source connection. It copies bounded groups of pages and releases
SQLite read locks between steps, so producers and consumers using other
connections can continue. Concurrent commits may make SQLite restart copying
some pages, but a completed destination is one valid point-in-time snapshot.
Heavy continuous writes can increase latency. A continuous `Busy` or `Locked`
period uses bounded exponential backoff and fails after the same 5-second busy
timeout used by product connections; successful page-copy progress resets that
lock deadline.

Within the exclusively owned directory, SQLite first writes
`.localqueue.db.incomplete`. The destination connection is closed, the
temporary database passes a full integrity check, and a normal rename publishes
`localqueue.db`. No hard links or replacement operation are required. On any
failure, the temporary database and its sidecars are removed, followed by the
operation-owned directory when it is empty. Pre-existing paths are never
deleted.

## Verify and recover

Treat a backup as a recovery candidate until it has been verified in the
environment where it will be restored. The public API already runs a full
integrity check before publishing, and operators can independently verify both
integrity and logical state:

```python
from localqueue import SimpleQueue


recovered = SimpleQueue("./backups/2026-07-22")
assert recovered.check_integrity(mode="full").ok
print(recovered.stats())
recovered.close()
```

For a recovery drill, open the returned destination directory directly with
`SimpleQueue`, compare `stats()` and sample payload lifecycles with the expected
recovery point, and retain the original database until the recovered queue has
been accepted. These APIs do not repair corruption, schedule backups, encrypt
files, or provide incremental or remote backups.
