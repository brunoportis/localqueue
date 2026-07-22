#!/usr/bin/env python3
"""Run one deterministic SQLite transaction crash scenario.

The crash scenarios are Linux-only because the CI contract uses SIGKILL. The
control scenarios and the JSON schema are portable. No timing is used to
detect a failpoint: the child sends a socket notification from Rust and then
blocks until the parent has received it.
"""

from __future__ import annotations

import argparse
import json
import platform
import signal
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
COUNTS_ZERO = {"ready": 0, "processing": 0, "acked": 0, "failed": 0}

SCENARIOS: dict[str, dict[str, Any]] = {
    "enqueue-after-begin": {
        "failpoint": "enqueue-after-begin",
        "operation": "enqueue",
        "initial": COUNTS_ZERO,
        "expected": COUNTS_ZERO,
    },
    "enqueue-before-commit": {
        "failpoint": "enqueue-before-commit",
        "operation": "enqueue",
        "initial": COUNTS_ZERO,
        "expected": COUNTS_ZERO,
    },
    "claim-before-commit": {
        "failpoint": "claim-before-commit",
        "operation": "claim",
        "initial": {"ready": 1, "processing": 0, "acked": 0, "failed": 0},
        "expected": {"ready": 1, "processing": 0, "acked": 0, "failed": 0},
    },
    "ack-before-commit": {
        "failpoint": "ack-before-commit",
        "operation": "ack",
        "initial": {"ready": 0, "processing": 1, "acked": 0, "failed": 0},
        "expected": {"ready": 0, "processing": 1, "acked": 0, "failed": 0},
    },
    "nack-before-commit": {
        "failpoint": "nack-before-commit",
        "operation": "nack",
        "initial": {"ready": 0, "processing": 1, "acked": 0, "failed": 0},
        "expected": {"ready": 0, "processing": 1, "acked": 0, "failed": 0},
    },
    "fail-before-commit": {
        "failpoint": "fail-before-commit",
        "operation": "fail",
        "initial": {"ready": 0, "processing": 1, "acked": 0, "failed": 0},
        "expected": {"ready": 0, "processing": 1, "acked": 0, "failed": 0},
    },
    "control-enqueue": {
        "failpoint": None,
        "operation": "enqueue",
        "initial": COUNTS_ZERO,
        "expected": {"ready": 1, "processing": 0, "acked": 0, "failed": 0},
    },
    "control-claim": {
        "failpoint": None,
        "operation": "claim",
        "initial": {"ready": 1, "processing": 0, "acked": 0, "failed": 0},
        "expected": {"ready": 0, "processing": 1, "acked": 0, "failed": 0},
    },
    "control-ack": {
        "failpoint": None,
        "operation": "ack",
        "initial": {"ready": 0, "processing": 1, "acked": 0, "failed": 0},
        "expected": {"ready": 0, "processing": 0, "acked": 1, "failed": 0},
    },
    "control-nack": {
        "failpoint": None,
        "operation": "nack",
        "initial": {"ready": 0, "processing": 1, "acked": 0, "failed": 0},
        "expected": {"ready": 1, "processing": 0, "acked": 0, "failed": 0},
    },
    "control-fail": {
        "failpoint": None,
        "operation": "fail",
        "initial": {"ready": 0, "processing": 1, "acked": 0, "failed": 0},
        "expected": {"ready": 0, "processing": 0, "acked": 0, "failed": 1},
    },
}

CHILD_CODE = r"""
from localqueue import SimpleQueue
queue = SimpleQueue(DB_PATH, lease_seconds=60.0, max_retries=3)
if FAILPOINT:
    hook = getattr(queue._native, "_test_configure_failpoint")
    hook(FAILPOINT, CONTROL_ADDRESS)

if OPERATION == "enqueue":
    queue.put({"scenario": SCENARIO})
elif OPERATION == "claim":
    queue.get(block=False)
else:
    job = queue.get(block=False)
    if OPERATION == "ack":
        queue.ack(job)
    elif OPERATION == "nack":
        queue.nack(job, last_error="crash-harness")
    elif OPERATION == "fail":
        queue.fail(job, "crash-harness")
    else:
        raise ValueError(OPERATION)
queue.close()
"""

VALIDATOR_CODE = r"""
import json
import sqlite3
from localqueue import LeaseExpired, SimpleQueue

with sqlite3.connect(DB_PATH + "/localqueue.db") as connection:
    integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    rows = connection.execute(
        "SELECT status, COUNT(*) FROM messages WHERE queue = 'default' GROUP BY status"
    ).fetchall()
counts = {"ready": 0, "processing": 0, "acked": 0, "failed": 0}
for status, count in rows:
    counts[{0: "ready", 1: "processing", 2: "acked", 3: "failed"}[status]] = count
recovery = {"processable_after_reopen": True, "stale_receipt_rejected": True}
queue = SimpleQueue(DB_PATH, lease_seconds=60.0, max_retries=3)
if counts["ready"]:
    job = queue.get(block=False)
    queue.ack(job)
elif CHECK_RECEIPT and counts["processing"]:
    with sqlite3.connect(DB_PATH + "/localqueue.db") as connection:
        message_id, receipt, lease_until = connection.execute(
            "SELECT id, receipt, lease_until FROM messages "
            "WHERE queue = 'default' AND status = 1"
        ).fetchone()
    queue._native.reclaim_expired(lease_until + 1)
    try:
        queue.ack(type("OldJob", (), {"id": message_id, "receipt": receipt})())
    except LeaseExpired:
        pass
    else:
        recovery["stale_receipt_rejected"] = False
    with sqlite3.connect(DB_PATH + "/localqueue.db") as connection:
        recovery["processable_after_reopen"] = connection.execute(
            "SELECT status FROM messages WHERE id = ?", (message_id,)
        ).fetchone()[0] == 0
else:
    recovery["processable_after_reopen"] = True
queue.close()
print(json.dumps({"integrity_check": integrity, "observed_counts": counts, "recovery": recovery}))
"""


def _invariant(name: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"name": name, "passed": passed, "detail": detail}


def _prepare(path: Path, operation: str) -> None:
    from localqueue import SimpleQueue

    queue = SimpleQueue(str(path), lease_seconds=60.0, max_retries=3)
    if operation != "enqueue":
        queue.put({"scenario": "crash-harness"})
    queue.close()


def _validate(path: Path, check_receipt: bool) -> dict[str, Any]:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            VALIDATOR_CODE.replace("DB_PATH", repr(str(path))).replace(
                "CHECK_RECEIPT", repr(check_receipt)
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(f"fresh validator process failed: {result.stderr.strip()}")
    return json.loads(result.stdout)


def run(scenario: str, output: Path) -> int:
    definition = SCENARIOS[scenario]
    crash = definition["failpoint"] is not None
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "scenario": scenario,
        "failpoint": definition["failpoint"],
        "operation": definition["operation"],
        "platform": platform.system().lower(),
        "child_exit_mode": None,
        "child_return_code": None,
        "signal": None,
        "synchronization_reached": False,
        "integrity_check": None,
        "initial_counts": definition["initial"],
        "expected_counts": definition["expected"],
        "observed_counts": None,
        "invariants": [],
        "passed": False,
    }
    try:
        if crash and platform.system() != "Linux":
            raise RuntimeError(
                "crash scenarios require Linux SIGKILL; use a control scenario"
            )

        with tempfile.TemporaryDirectory(prefix="localqueue-crash-") as directory:
            path = Path(directory)
            _prepare(path, definition["operation"])
            listener: socket.socket | None = None
            address = ""
            if crash:
                listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                listener.bind(("127.0.0.1", 0))
                listener.listen(1)
                listener.settimeout(10.0)
                address = f"127.0.0.1:{listener.getsockname()[1]}"

            code = (
                CHILD_CODE.replace("DB_PATH", repr(str(path)))
                .replace("CONTROL_ADDRESS", repr(address))
                .replace("FAILPOINT", repr(definition["failpoint"]))
                .replace("OPERATION", repr(definition["operation"]))
                .replace("SCENARIO", repr(scenario))
            )
            child = subprocess.Popen(
                [sys.executable, "-c", code],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            connection: socket.socket | None = None
            try:
                if crash:
                    assert listener is not None
                    connection, _ = listener.accept()
                    connection.settimeout(2.0)
                    notification = b""
                    while not notification.endswith(b"\n"):
                        notification += connection.recv(128)
                    reached = notification.decode().strip()
                    report["synchronization_reached"] = (
                        reached == definition["failpoint"]
                    )
                    if report["synchronization_reached"]:
                        child.kill()
                        report["child_exit_mode"] = "signal"
                child.wait(timeout=10)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=5)
                if connection is not None:
                    connection.close()
                if listener is not None:
                    listener.close()

            report["child_return_code"] = child.returncode
            if child.returncode is not None and child.returncode < 0:
                report["signal"] = signal.Signals(-child.returncode).name
            if not crash:
                report["child_exit_mode"] = "normal"

            validation = _validate(path, crash)
            report["integrity_check"] = validation["integrity_check"]
            report["observed_counts"] = validation["observed_counts"]
            report["recovery"] = validation["recovery"]
            report["invariants"] = [
                _invariant(
                    "synchronization_reached",
                    (not crash) or report["synchronization_reached"],
                    "Rust notification matched the selected failpoint",
                ),
                _invariant(
                    "child_terminated_as_expected",
                    (child.returncode is not None and child.returncode < 0)
                    if crash
                    else child.returncode == 0,
                    "child exit mode is deterministic",
                ),
                _invariant(
                    "integrity_check_is_ok",
                    report["integrity_check"] == "ok",
                    "fresh process returned PRAGMA integrity_check = ok",
                ),
                _invariant(
                    "logical_counts_match",
                    report["observed_counts"] == report["expected_counts"],
                    "uncommitted work is not visible after reopen",
                ),
                _invariant(
                    "recovery_remains_processable",
                    report["recovery"]["processable_after_reopen"]
                    and report["recovery"]["stale_receipt_rejected"],
                    "reclaim permits processing and rejects the old receipt",
                ),
            ]
            report["passed"] = all(item["passed"] for item in report["invariants"])
    except Exception as error:  # Always attempt to leave machine-readable evidence.
        report["error"] = str(error)
        report["invariants"].append(
            _invariant("harness_completed", False, "scenario failed")
        )

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0 if report["passed"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    return run(args.scenario, args.output)


if __name__ == "__main__":
    raise SystemExit(main())
