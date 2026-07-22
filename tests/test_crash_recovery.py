"""Deterministic subprocess crash evidence for issue #18."""

from __future__ import annotations

import json
import platform
import subprocess
import sys
from pathlib import Path

import pytest
from localqueue import localqueue as native

HARNESS = Path(__file__).with_name("crash_harness.py")
CRASH_SCENARIOS = (
    "enqueue-after-begin",
    "enqueue-before-commit",
    "claim-before-commit",
    "ack-before-commit",
    "nack-before-commit",
    "fail-before-commit",
)
CONTROL_SCENARIOS = (
    "control-enqueue",
    "control-claim",
    "control-ack",
    "control-nack",
    "control-fail",
)


def _run_harness(scenario: str, output: Path) -> dict[str, object]:
    result = subprocess.run(
        [sys.executable, str(HARNESS), "--scenario", scenario, "--output", str(output)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    report = json.loads(output.read_text(encoding="utf-8"))
    assert result.returncode == 0, result.stderr or report
    return report


@pytest.mark.parametrize("scenario", CRASH_SCENARIOS)
def test_precommit_crash_recovery_is_atomic(scenario: str, tmp_path: Path) -> None:
    if platform.system() != "Linux":
        pytest.skip("SIGKILL crash scenarios run in the dedicated Linux job")
    if not hasattr(native.NativeQueue, "_test_configure_failpoint"):
        pytest.skip("requires the explicit __crash_test extension build")
    report = _run_harness(scenario, tmp_path / f"{scenario}.json")
    assert report["schema_version"] == 1
    assert report["failpoint"] == scenario
    assert report["child_exit_mode"] == "signal"
    assert report["synchronization_reached"] is True
    assert report["integrity_check"] == "ok"
    assert report["passed"] is True


@pytest.mark.parametrize("scenario", CONTROL_SCENARIOS)
def test_committed_transition_survives_reopen(scenario: str, tmp_path: Path) -> None:
    report = _run_harness(scenario, tmp_path / f"{scenario}.json")
    assert report["failpoint"] is None
    assert report["child_exit_mode"] == "normal"
    assert report["integrity_check"] == "ok"
    assert report["passed"] is True
