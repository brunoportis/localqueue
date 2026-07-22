from __future__ import annotations

import shutil
import sqlite3
import sys
from pathlib import Path

from localqueue import SimpleQueue

from ..model import ScenarioResult
from ..process import wait_for_notification
from ..sqlite import checkpoint
from .common import ScenarioContext, counts, run_queue_operation, validate_queue

COMMITTED_CHILD = r"""
import socket
import sys
from localqueue import SimpleQueue

queue_dir, address = sys.argv[1:3]
queue = SimpleQueue(queue_dir)
queue.put({"kind": "confirmed"})
host, port = address.split(":")
control = socket.create_connection((host, int(port)), timeout=2)
control.sendall(b"committed\n")
control.settimeout(10)
control.recv(1)
queue.close()
"""


def _snapshot_sidecars(context: ScenarioContext) -> None:
    for suffix in ("-wal", "-shm"):
        source = Path(f"{context.db_path}{suffix}")
        if source.exists():
            shutil.copy2(source, context.path / f"localqueue.db{suffix}.snapshot")


def record_sidecar_evidence(result: ScenarioResult, database: Path) -> None:
    wal = Path(f"{database}-wal")
    shm = Path(f"{database}-shm")
    wal_present = wal.exists()
    shm_present = shm.exists()
    result.pragmas.update(
        {
            "wal_present": wal_present,
            "shm_present": shm_present,
            "wal_size_bytes": wal.stat().st_size if wal_present else None,
            "shm_size_bytes": shm.stat().st_size if shm_present else None,
        }
    )
    result.invariant(
        "wal_present",
        wal_present,
        "localqueue.db-wal existed while the committed process remained alive",
    )


FAILPOINT_CHILD = r"""
import sys
from localqueue import SimpleQueue

queue_dir, failpoint, address, operation = sys.argv[1:5]
queue = SimpleQueue(queue_dir)
queue._native._test_configure_failpoint(failpoint, address)
if operation == "put":
    queue.put({"kind": "unconfirmed"})
elif operation == "purge":
    queue.purge(0)
else:
    raise ValueError(operation)
"""


def _failpoint_child(context: ScenarioContext, failpoint: str, operation: str):
    return wait_for_notification(
        [
            sys.executable,
            "-c",
            FAILPOINT_CHILD,
            str(context.queue_dir),
            failpoint,
            "{control_address}",
            operation,
        ],
        expected=failpoint,
        timeout=5.0,
        artifacts_dir=context.path / failpoint,
    )


def producer_termination(_: str, artifacts_dir: Path) -> ScenarioResult:
    result = ScenarioResult(
        "producer-termination",
        durability_mode="normal",
        expected_outcome="SIGKILL at enqueue-before-commit rolls back SimpleQueue.put",
        required_invariants=frozenset(
            {
                "failpoint_reached",
                "no_partial_transition",
                "integrity_ok",
                "retry_visible",
            }
        ),
        required_fields=frozenset(
            {
                "operation_confirmed_to_caller",
                "counts_before",
                "counts_after_failure",
                "counts_after_recovery",
                "integrity_check",
            }
        ),
        fresh_process_required=True,
    )
    context = ScenarioContext(artifacts_dir, result.name)
    child = None
    try:
        queue = SimpleQueue(str(context.queue_dir))
        result.counts_before = counts(queue.stats())
        queue.close()
        child = _failpoint_child(context, "enqueue-before-commit", "put")
        result.invariant(
            "failpoint_reached", True, "Rust emitted enqueue-before-commit"
        )
        child.kill_and_collect()
        child = None
        result.operation_confirmed_to_caller = False
        validation = validate_queue(context)
        result.fresh_process_validated = True
        result.counts_after_failure = counts(validation["stats_before"])
        result.counts_after_recovery = result.counts_after_failure
        result.integrity_check = str(validation["integrity_check"])
        result.invariant(
            "no_partial_transition",
            result.counts_after_recovery == result.counts_before,
            "uncommitted SimpleQueue.put is absent after reopen",
        )
        result.invariant(
            "integrity_ok", result.integrity_check == "ok", result.integrity_check
        )
        retry = run_queue_operation(context, "put", {"kind": "retry"}, label="retry")
        result.retry_attempted = True
        result.retry_succeeded = bool(retry["ok"])
        result.retry_safe = result.retry_succeeded
        after_retry = validate_queue(context, label="retry-validator")
        result.invariant(
            "retry_visible",
            counts(after_retry["stats_before"])["ready"] == 1,
            "real SimpleQueue retry is visible",
        )
        result.status = "passed"
    except BaseException as error:
        result.error = {
            "public_type": type(error).__name__,
            "sqlite_code": "",
            "message": str(error),
        }
    finally:
        if child is not None:
            child.kill_and_collect()
    result.artifacts = context.artifacts()
    return result.finish()


def wal_recovery(_: str, artifacts_dir: Path) -> ScenarioResult:
    result = ScenarioResult(
        "wal-recovery",
        durability_mode="normal",
        expected_outcome="confirmed SimpleQueue WAL write survives SIGKILL and failpoint write stays absent",
        required_invariants=frozenset(
            {
                "commit_returned",
                "wal_present",
                "confirmed_preserved",
                "unconfirmed_absent",
                "integrity_ok",
                "checkpoint_succeeded",
            }
        ),
        required_fields=frozenset(
            {
                "operation_confirmed_to_caller",
                "counts_before",
                "counts_after_failure",
                "counts_after_recovery",
                "integrity_check",
            }
        ),
        fresh_process_required=True,
    )
    context = ScenarioContext(artifacts_dir, result.name)
    child = None
    try:
        queue = SimpleQueue(str(context.queue_dir))
        result.counts_before = counts(queue.stats())
        queue.close()
        child = wait_for_notification(
            [
                sys.executable,
                "-c",
                COMMITTED_CHILD,
                str(context.queue_dir),
                "{control_address}",
            ],
            expected="committed",
            timeout=5.0,
            artifacts_dir=context.path / "committed-child",
        )
        result.invariant(
            "commit_returned", True, "child reported after SimpleQueue.put returned"
        )
        record_sidecar_evidence(result, context.db_path)
        _snapshot_sidecars(context)
        child.kill_and_collect()
        child = _failpoint_child(context, "enqueue-before-commit", "put")
        child.kill_and_collect()
        child = None
        result.operation_confirmed_to_caller = False
        validation = validate_queue(context, consume=True)
        result.fresh_process_validated = True
        result.counts_after_failure = counts(validation["stats_before"])
        result.counts_after_recovery = result.counts_after_failure
        result.integrity_check = str(validation["integrity_check"])
        result.invariant(
            "confirmed_preserved",
            validation["payload"] == {"kind": "confirmed"},
            "fresh SimpleQueue process received the confirmed payload",
        )
        result.invariant(
            "unconfirmed_absent",
            result.counts_after_recovery
            == {"ready": 1, "processing": 0, "acked": 0, "failed": 0},
            "only the confirmed write was visible before consume",
        )
        result.invariant(
            "integrity_ok", result.integrity_check == "ok", result.integrity_check
        )
        checkpoint_result = checkpoint(context.db_path)
        result.pragmas["checkpoint"] = list(checkpoint_result)
        result.invariant(
            "checkpoint_succeeded",
            checkpoint_result[0] == 0,
            f"wal_checkpoint(TRUNCATE) returned {checkpoint_result}",
        )
        result.status = "passed"
    except BaseException as error:
        result.error = {
            "public_type": type(error).__name__,
            "sqlite_code": "",
            "message": str(error),
        }
    finally:
        if child is not None:
            child.kill_and_collect()
    result.artifacts = context.artifacts()
    return result.finish()


def maintenance_termination(_: str, artifacts_dir: Path) -> ScenarioResult:
    result = ScenarioResult(
        "maintenance-termination",
        durability_mode="normal",
        expected_outcome="SIGKILL at purge-before-commit rolls back public SimpleQueue.purge",
        required_invariants=frozenset(
            {"failpoint_reached", "rollback_integral", "integrity_ok", "retry_removed"}
        ),
        required_fields=frozenset(
            {
                "operation_confirmed_to_caller",
                "counts_before",
                "counts_after_failure",
                "counts_after_recovery",
                "integrity_check",
            }
        ),
        fresh_process_required=True,
    )
    context = ScenarioContext(artifacts_dir, result.name)
    child = None
    try:
        queue = SimpleQueue(str(context.queue_dir))
        queue.put({"maintenance": True})
        job = queue.get(block=False)
        queue.ack(job)
        result.counts_before = counts(queue.stats())
        queue.close()
        with sqlite3.connect(context.db_path) as evidence:
            evidence.execute("UPDATE messages SET updated_at = 0 WHERE status = 2")
            evidence.commit()
        child = _failpoint_child(context, "purge-before-commit", "purge")
        result.invariant("failpoint_reached", True, "Rust emitted purge-before-commit")
        child.kill_and_collect()
        child = None
        result.operation_confirmed_to_caller = False
        validation = validate_queue(context)
        result.fresh_process_validated = True
        result.counts_after_failure = counts(validation["stats_before"])
        result.counts_after_recovery = result.counts_after_failure
        result.integrity_check = str(validation["integrity_check"])
        result.invariant(
            "rollback_integral",
            result.counts_after_recovery == result.counts_before,
            "acked message remains after interrupted public purge",
        )
        result.invariant(
            "integrity_ok", result.integrity_check == "ok", result.integrity_check
        )
        result.retry_attempted = True
        retry = run_queue_operation(context, "purge", 0, label="retry")
        result.retry_succeeded = bool(retry["ok"] and retry["operation_result"] == 1)
        result.retry_safe = result.retry_succeeded
        after_retry = validate_queue(context, label="retry-validator")
        result.invariant(
            "retry_removed",
            counts(after_retry["stats_before"])["acked"] == 0,
            "repeated SimpleQueue.purge removed the original acked message",
        )
        result.status = "passed"
    except BaseException as error:
        result.error = {
            "public_type": type(error).__name__,
            "sqlite_code": "",
            "message": str(error),
        }
    finally:
        if child is not None:
            child.kill_and_collect()
    result.artifacts = context.artifacts()
    return result.finish()
