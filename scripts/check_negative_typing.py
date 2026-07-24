"""Prove that Pyrefly rejects each intentionally invalid public API use."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_DIAGNOSTICS = (
    "localqueue.core.SimpleQueue.put",
    "localqueue.core.SimpleQueue.ack",
    "localqueue.worker.Worker.__init__",
    "localqueue.bus.bus.EventBus.on",
    "localqueue.bus.subscription.Subscription.handler",
)


def main() -> int:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = "python"
    result = subprocess.run(
        [
            "pyrefly",
            "check",
            "typing_tests/negative.py",
            "--progress-bar",
            "no",
            "--output-format",
            "min-text",
        ],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    output = result.stdout + result.stderr
    if result.returncode == 0:
        print("negative typing fixture unexpectedly passed", file=sys.stderr)
        return 1

    missing = [
        diagnostic for diagnostic in EXPECTED_DIAGNOSTICS if diagnostic not in output
    ]
    error_count = output.count("ERROR typing_tests/negative.py:")
    if missing or error_count != len(EXPECTED_DIAGNOSTICS):
        print(output, file=sys.stderr)
        print(
            f"expected {len(EXPECTED_DIAGNOSTICS)} diagnostics, got {error_count}; "
            f"missing={missing}",
            file=sys.stderr,
        )
        return 1

    print("negative typing fixture produced the 5 expected incompatibility diagnostics")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
