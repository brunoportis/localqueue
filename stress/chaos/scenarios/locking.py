from __future__ import annotations

import sys
import time
from pathlib import Path

from localqueue import SimpleQueue

from ..model import ScenarioResult
from ..process import wait_for_notification
from ..sqlite import inspect_pragmas
from .common import (
    ScenarioContext,
    counts,
    public_error,
    run_queue_operation,
    validate_queue,
)

HOLDER_CODE = r"""
import socket
import sqlite3
import sys

database, address = sys.argv[1:3]
host, port = address.split(":")
connection = sqlite3.connect(database)
connection.execute("BEGIN IMMEDIATE")
control = socket.create_connection((host, int(port)), timeout=2)
control.sendall(b"locked\n")
control.settimeout(12)
control.recv(1)
connection.rollback()
connection.close()
control.close()
"""


def run(_: str, artifacts_dir: Path) -> ScenarioResult:
    result = ScenarioResult(
        "lock-timeout",
        expected_outcome="SimpleQueue.put reports lock timeout and succeeds after explicit release",
        required_invariants=frozenset(
            {
                "lock_confirmed",
                "busy_error_observed",
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
    holder = None
    try:
        queue = SimpleQueue(str(context.queue_dir))
        result.counts_before = counts(queue.stats())
        queue.close()
        holder = wait_for_notification(
            [
                sys.executable,
                "-c",
                HOLDER_CODE,
                str(context.db_path),
                "{control_address}",
            ],
            expected="locked",
            timeout=3.0,
            artifacts_dir=context.path / "holder",
        )
        result.invariant(
            "lock_confirmed",
            True,
            "external process reported BEGIN IMMEDIATE acquisition",
        )
        started = time.monotonic()
        operation = run_queue_operation(
            context, "put", {"locked": True}, timeout=8.0, label="contender"
        )
        duration = time.monotonic() - started
        result.pragmas = inspect_pragmas(context.db_path)
        result.pragmas["observed_wait_ms"] = round(duration * 1000)
        result.operation_confirmed_to_caller = bool(operation.get("confirmed"))
        result.error = public_error(operation)
        result.invariant(
            "busy_error_observed",
            not operation["ok"] and "locked" in str(result.error).lower(),
            "public SimpleQueue operation returned the underlying lock failure",
        )
        result.counts_after_failure = result.counts_before
        holder.release_and_collect()
        holder = None
        validation = validate_queue(context)
        result.fresh_process_validated = True
        result.counts_after_recovery = counts(validation["stats_before"])
        result.integrity_check = str(validation["integrity_check"])
        result.invariant(
            "no_partial_transition",
            result.counts_after_recovery == result.counts_before,
            "timed-out put was not visible",
        )
        result.invariant(
            "integrity_ok", result.integrity_check == "ok", result.integrity_check
        )
        retry = run_queue_operation(context, "put", {"retry": True}, label="retry")
        result.retry_attempted = True
        result.retry_succeeded = bool(retry["ok"])
        result.retry_safe = result.retry_succeeded
        after_retry = validate_queue(context, label="retry-validator")
        result.invariant(
            "retry_visible",
            counts(after_retry["stats_before"])["ready"] == 1,
            "retry through SimpleQueue is visible after reopen",
        )
        result.status = "passed"
    except BaseException as error:
        result.error = result.error or {
            "public_type": type(error).__name__,
            "sqlite_code": "",
            "message": str(error),
        }
    finally:
        if holder is not None:
            holder.kill_and_collect()
    result.artifacts = context.artifacts()
    return result.finish()
