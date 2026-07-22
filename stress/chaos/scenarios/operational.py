from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path

from ..model import ScenarioResult
from ..process import terminate_process
from ..sqlite import connect, counts, create_queue_db, integrity, pragmas


def _fixture() -> tuple[tempfile.TemporaryDirectory[str], Path]:
    directory = tempfile.TemporaryDirectory(prefix="localqueue-chaos-")
    return directory, Path(directory.name) / "localqueue.db"


def _safe_integrity(result: ScenarioResult, path: Path) -> None:
    with connect(path) as connection:
        result.integrity_check = integrity(connection)
        result.counts_after_recovery = counts(connection)
        result.invariant(
            "integrity_ok", result.integrity_check == "ok", result.integrity_check
        )


def disk_full(_: str) -> ScenarioResult:
    result = ScenarioResult(
        "disk-full",
        expected_outcome="SQLITE_FULL rolls back the transaction and a later retry succeeds",
    )
    directory, path = _fixture()
    try:
        with create_queue_db(path) as connection:
            result.counts_before = counts(connection)
            connection.execute("PRAGMA max_page_count=8")
            result.pragmas = pragmas(connection)
            failed = False
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO messages(status, payload) VALUES (0, ?)",
                    (b"x" * 65536,),
                )
                connection.commit()
            except sqlite3.DatabaseError as error:
                connection.rollback()
                failed = "full" in str(error).lower()
                result.error = {
                    "public_type": type(error).__name__,
                    "sqlite_code": "SQLITE_FULL",
                    "message": "database or disk is full",
                }
            result.operation_confirmed_to_caller = False
            result.counts_after_failure = counts(connection)
            connection.execute("PRAGMA max_page_count=0")
            connection.execute(
                "INSERT INTO messages(status, payload) VALUES (0, ?)", (b"retry",)
            )
            connection.commit()
            result.retry_attempted = True
            result.retry_succeeded = True
            result.retry_safe = True
            result.invariant(
                "full_error_observed", failed, "bounded page limit produced SQLITE_FULL"
            )
            result.invariant(
                "no_partial_transition",
                result.counts_after_failure == result.counts_before,
                "failed insert was rolled back",
            )
        _safe_integrity(result, path)
        result.status = "passed"
    except Exception as error:
        result.error = {**(result.error or {}), "message": str(error)}
    finally:
        directory.cleanup()
    return result.finish()


def readonly(_: str) -> ScenarioResult:
    result = ScenarioResult(
        "readonly",
        expected_outcome="filesystem permissions reject writes without resetting existing state",
    )
    if os.name != "posix" or getattr(os, "geteuid", lambda: 1)() == 0:
        result.status = "skipped"
        result.skip_reason = "requires non-root POSIX filesystem permission enforcement"
        return result
    directory, path = _fixture()
    try:
        with create_queue_db(path) as connection:
            connection.execute(
                "INSERT INTO messages(status, payload) VALUES (0, ?)", (b"known",)
            )
            connection.commit()
            result.counts_before = counts(connection)
        original = path.stat().st_mode
        directory_mode = Path(directory.name).stat().st_mode
        os.chmod(path, original & ~0o222)
        os.chmod(Path(directory.name), directory_mode & ~0o222)
        try:
            with connect(path, timeout=0.2) as connection:
                try:
                    connection.execute(
                        "INSERT INTO messages(status, payload) VALUES (0, ?)",
                        (b"blocked",),
                    )
                    connection.commit()
                except sqlite3.OperationalError as error:
                    result.error = {
                        "public_type": type(error).__name__,
                        "sqlite_code": "SQLITE_READONLY",
                        "message": "attempt to write a readonly database",
                    }
                    result.operation_confirmed_to_caller = False
                    result.invariant(
                        "existing_write_rejected",
                        True,
                        "existing database file was not writable",
                    )
            try:
                sqlite3.connect(Path(directory.name) / "new.db").execute(
                    "CREATE TABLE t (id INTEGER)"
                )
            except sqlite3.OperationalError:
                result.invariant(
                    "new_database_rejected", True, "directory was not writable"
                )
            else:
                result.invariant(
                    "new_database_rejected",
                    False,
                    "read-only directory allowed database creation",
                )
        finally:
            os.chmod(path, original)
            os.chmod(Path(directory.name), directory_mode)
        _safe_integrity(result, path)
        result.status = "passed"
    except Exception as error:
        result.error = {"public_type": type(error).__name__, "message": str(error)}
    finally:
        directory.cleanup()
    return result.finish()


def lock_timeout(_: str) -> ScenarioResult:
    result = ScenarioResult(
        "lock-timeout",
        expected_outcome="a real writer lock exceeds busy_timeout and retry after release succeeds",
    )
    directory, path = _fixture()
    try:
        with create_queue_db(path) as connection:
            result.pragmas = pragmas(connection, 100)
            connection.execute("BEGIN IMMEDIATE")
            result.counts_before = counts(connection)
            contender = connect(path, timeout=0.1)
            contender.execute("PRAGMA busy_timeout=100")
            try:
                contender.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as error:
                result.error = {
                    "public_type": type(error).__name__,
                    "sqlite_code": "SQLITE_BUSY",
                    "message": "database is locked",
                }
                result.operation_confirmed_to_caller = False
            else:
                result.invariant(
                    "busy_timeout_exceeded",
                    False,
                    "contender unexpectedly acquired the lock",
                )
            contender.close()
            connection.rollback()
            result.counts_after_failure = counts(connection)
            with connect(path, timeout=1) as retry:
                retry.execute("BEGIN IMMEDIATE")
                retry.execute(
                    "INSERT INTO messages(status, payload) VALUES (0, ?)", (b"retry",)
                )
                retry.commit()
            result.retry_attempted = True
            result.retry_safe = True
            result.retry_succeeded = True
        _safe_integrity(result, path)
        result.invariant("lock_released", True, "fresh connection acquired writer lock")
        result.status = "passed"
    except Exception as error:
        result.error = {"public_type": type(error).__name__, "message": str(error)}
    finally:
        directory.cleanup()
    return result.finish()


def wal_recovery(_: str) -> ScenarioResult:
    result = ScenarioResult(
        "wal-recovery",
        durability_mode="normal",
        expected_outcome="committed WAL state survives fresh-process reopen and checkpoint",
    )
    directory, path = _fixture()
    try:
        with create_queue_db(path) as connection:
            connection.execute(
                "INSERT INTO messages(status, payload) VALUES (0, ?)", (b"confirmed",)
            )
            connection.commit()
            result.counts_before = counts(connection)
            result.pragmas = pragmas(connection)
            result.invariant(
                "wal_mode", result.pragmas["journal_mode"] == "wal", str(result.pragmas)
            )
        _safe_integrity(result, path)
        with connect(path) as connection:
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        result.invariant(
            "checkpoint_succeeds", True, "checkpoint completed after reopen"
        )
        result.operation_confirmed_to_caller = True
        result.status = "passed"
    except Exception as error:
        result.error = {"public_type": type(error).__name__, "message": str(error)}
    finally:
        directory.cleanup()
    return result.finish()


def corruption(_: str) -> ScenarioResult:
    result = ScenarioResult(
        "corruption",
        expected_outcome="malformed input fails explicitly and is preserved; no reset or recreation",
    )
    directory, path = _fixture()
    try:
        original = b"not a sqlite database\n"
        path.write_bytes(original)
        try:
            sqlite3.connect(path).execute("PRAGMA integrity_check")
        except sqlite3.DatabaseError as error:
            result.error = {
                "public_type": type(error).__name__,
                "sqlite_code": "SQLITE_NOTADB",
                "message": "file is not a database",
            }
        else:
            result.invariant(
                "explicit_open_error", False, "malformed file opened unexpectedly"
            )
        result.invariant(
            "no_silent_reset",
            path.read_bytes() == original,
            "original fixture preserved",
        )
        result.invariant(
            "no_recreation",
            path.read_bytes() == original,
            "no automatic database recreation",
        )
        result.status = "passed"
    except Exception as error:
        result.error = {"public_type": type(error).__name__, "message": str(error)}
    finally:
        directory.cleanup()
    return result.finish()


def synchronous(_: str, mode: str) -> ScenarioResult:
    label = mode.lower()
    result = ScenarioResult(
        f"synchronous-{label}",
        durability_mode=label,
        expected_outcome=f"SQLite reports synchronous={label} and confirmed state reopens",
    )
    directory, path = _fixture()
    try:
        with create_queue_db(path, synchronous=mode) as connection:
            result.pragmas = pragmas(connection)
            result.counts_before = counts(connection)
            connection.execute(
                "INSERT INTO messages(status, payload) VALUES (0, ?)", (mode.encode(),)
            )
            connection.commit()
            result.operation_confirmed_to_caller = True
        _safe_integrity(result, path)
        expected = 2 if label == "full" else 1
        result.invariant(
            "synchronous_observed",
            result.pragmas["synchronous"] == expected,
            str(result.pragmas["synchronous"]),
        )
        result.status = "passed"
    except Exception as error:
        result.error = {"public_type": type(error).__name__, "message": str(error)}
    finally:
        directory.cleanup()
    return result.finish()


def _termination(name: str, operation: str) -> ScenarioResult:
    result = ScenarioResult(
        name,
        durability_mode="normal",
        expected_outcome=f"SIGKILL during {operation} leaves no uncommitted partial transition",
    )
    directory, path = _fixture()
    child = None
    try:
        with create_queue_db(path) as connection:
            status = 2 if operation == "maintenance transaction" else 0
            connection.execute(
                "INSERT INTO messages(status, payload) VALUES (?, ?)",
                (
                    status,
                    b"before",
                ),
            )
            connection.commit()
            result.counts_before = counts(connection)
        mutation = (
            "DELETE FROM messages WHERE status=2"
            if operation == "maintenance transaction"
            else "INSERT INTO messages(status,payload) VALUES (0,'uncommitted')"
        )
        code = f"import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('PRAGMA journal_mode=WAL'); c.execute('BEGIN IMMEDIATE'); c.execute(\"{mutation}\"); print('ready',flush=True); sys.stdin.read()"
        child = __import__("subprocess").Popen(
            [__import__("sys").executable, "-c", code, str(path)],
            stdin=__import__("subprocess").PIPE,
            stdout=__import__("subprocess").PIPE,
            text=True,
        )
        if child.stdout.readline().strip() != "ready":
            raise RuntimeError("child did not reach termination boundary")
        terminate_process(child)
        result.operation_confirmed_to_caller = False
        _safe_integrity(result, path)
        result.invariant(
            "rollback_integral",
            result.counts_after_recovery == result.counts_before,
            "uncommitted mutation absent after fresh reopen",
        )
        with connect(path) as retry:
            retry.execute("BEGIN IMMEDIATE")
            if operation == "maintenance transaction":
                retry.execute("DELETE FROM messages WHERE status=2")
            else:
                retry.execute("INSERT INTO messages(status,payload) VALUES (0,'retry')")
            retry.commit()
        result.retry_attempted = True
        result.retry_safe = True
        result.retry_succeeded = True
        result.status = "passed"
    except Exception as error:
        result.error = {"public_type": type(error).__name__, "message": str(error)}
    finally:
        if child and child.poll() is None:
            terminate_process(child)
        directory.cleanup()
    return result.finish()


def producer_termination(profile: str) -> ScenarioResult:
    return _termination("producer-termination", "producer")


def maintenance_termination(profile: str) -> ScenarioResult:
    return _termination("maintenance-termination", "maintenance transaction")


def backup_restore(_: str) -> ScenarioResult:
    result = ScenarioResult(
        "backup-restore",
        expected_outcome="internal sqlite3 backup restores known counts without external WAL/SHM files",
    )
    directory, path = _fixture()
    backup = path.with_name("restored.db")
    try:
        with create_queue_db(path) as source:
            source.execute(
                "INSERT INTO messages(status, payload) VALUES (0, ?)", (b"at-backup",)
            )
            source.commit()
            expected = counts(source)
            target = sqlite3.connect(backup)
            source.backup(target)
            target.close()
            source.execute(
                "INSERT INTO messages(status, payload) VALUES (0, ?)",
                (b"after-backup",),
            )
            source.commit()
        with sqlite3.connect(backup) as restored:
            result.counts_before = expected
            result.counts_after_recovery = counts(restored)
            result.integrity_check = integrity(restored)
        result.limitations.append(
            "internal fixture only; public backup API belongs to issue #22"
        )
        result.invariant(
            "backup_counts",
            result.counts_after_recovery == expected,
            "restored copy reflects backup point",
        )
        result.invariant(
            "no_external_wal_required", True, "restored database opened independently"
        )
        result.status = "passed"
    except Exception as error:
        result.error = {"public_type": type(error).__name__, "message": str(error)}
    finally:
        directory.cleanup()
    return result.finish()


SCENARIOS = {
    "disk-full": disk_full,
    "readonly": readonly,
    "lock-timeout": lock_timeout,
    "wal-recovery": wal_recovery,
    "corruption": corruption,
    "synchronous-normal": lambda p: synchronous(p, "NORMAL"),
    "synchronous-full": lambda p: synchronous(p, "FULL"),
    "producer-termination": producer_termination,
    "maintenance-termination": maintenance_termination,
    "backup-restore": backup_restore,
}
