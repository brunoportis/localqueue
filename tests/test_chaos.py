from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

from stress import run_chaos
from stress.chaos.model import SCENARIO_NAMES, ScenarioResult
from stress.chaos.process import ProcessFailure, run_process, wait_for_notification
from stress.chaos.report import make_report, normalize_error
from stress.chaos.scenarios.corruption import evaluate_open
from stress.chaos.scenarios.readonly import evaluate as evaluate_readonly

RUNNER = Path(__file__).parents[1] / "stress" / "run_chaos.py"


def test_success_report_is_versioned_and_complete():
    result = ScenarioResult(
        "example",
        status="passed",
        required_invariants=frozenset({"example"}),
    )
    result.invariant("example", True, "ok")
    report = make_report("ci", [result])
    assert report["schema_version"] == 1
    assert report["passed"] is True
    assert report["summary"] == {"total": 1, "passed": 1, "failed": 0, "skipped": 0}


def test_failure_report_is_not_passed():
    result = ScenarioResult("example", status="failed")
    report = make_report("ci", [result])
    assert report["passed"] is False
    assert report["summary"]["failed"] == 1


def test_skip_has_explicit_reason_and_does_not_pass_campaign():
    result = ScenarioResult(
        "readonly",
        status="skipped",
        skip_reason="filesystem permissions cannot be enforced",
    )
    report = make_report("ci", [result])
    assert report["passed"] is False
    assert report["scenarios"][0]["skip"] == {
        "reason": "filesystem permissions cannot be enforced"
    }


def test_scenario_without_invariants_cannot_pass():
    result = ScenarioResult("unsafe", status="passed").finish()
    assert result.status == "failed"


def test_missing_required_field_cannot_pass():
    result = ScenarioResult(
        "unsafe",
        status="passed",
        required_invariants=frozenset({"checked"}),
        required_fields=frozenset({"integrity_check"}),
    )
    result.invariant("checked", True, "check ran")
    assert result.finish().status == "failed"


def test_retry_safe_requires_real_attempt_and_success():
    result = ScenarioResult(
        "unsafe", status="passed", required_invariants=frozenset({"checked"})
    )
    result.invariant("checked", True, "check ran")
    result.retry_safe = True
    assert result.finish().status == "failed"


def test_readonly_fails_when_write_is_unexpectedly_accepted():
    result = ScenarioResult(
        "readonly",
        status="passed",
        required_invariants=frozenset(
            {"existing_write_rejected", "new_database_rejected"}
        ),
    )
    evaluate_readonly(
        result,
        {"ok": True, "confirmed": True},
        {"ok": False, "confirmed": False, "error": {}},
    )
    assert result.finish().status == "failed"


def test_corruption_fails_when_simplequeue_opens_without_error():
    result = ScenarioResult(
        "corruption",
        status="passed",
        required_invariants=frozenset({"malformed_explicit_error"}),
    )
    evaluate_open(result, "malformed", None)
    assert result.finish().status == "failed"


def test_error_normalization_is_stable():
    assert (
        normalize_error(RuntimeError("database is locked: /tmp/123"))["message"]
        == "database is locked"
    )


def test_scenario_catalog_is_explicit():
    assert len(SCENARIO_NAMES) == 10
    assert "disk-full" in SCENARIO_NAMES


def test_unknown_scenario_writes_report(tmp_path: Path):
    output = tmp_path / "report.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--profile",
            "ci",
            "--scenario",
            "unknown",
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode != 0
    report = json.loads(output.read_text())
    assert report["passed"] is False
    assert report["scenarios"][0]["name"] == "harness"


def test_internal_harness_error_still_writes_failed_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    def fail_scenario(profile: str, artifacts_dir: Path) -> ScenarioResult:
        raise RuntimeError("internal fixture failure")

    monkeypatch.setitem(run_chaos.SCENARIOS, "corruption", fail_scenario)
    output = tmp_path / "failed.json"
    code = run_chaos.main(
        [
            "--profile",
            "ci",
            "--scenario",
            "corruption",
            "--output",
            str(output),
        ]
    )
    report = json.loads(output.read_text())
    assert code == 1
    assert report["passed"] is False
    assert report["scenarios"][0]["error"]["public_type"] == "RuntimeError"


def test_scenario_selection_runs_only_one(tmp_path: Path):
    output = tmp_path / "report.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--profile",
            "ci",
            "--scenario",
            "corruption",
            "--output",
            str(output),
        ],
        check=False,
    )
    assert completed.returncode in (0, 1)
    report = json.loads(output.read_text())
    assert [scenario["name"] for scenario in report["scenarios"]] == ["corruption"]
    assert report["scenarios"][0]["status"] in ("passed", "failed", "skipped")


def test_runner_works_outside_repository(tmp_path: Path):
    output = tmp_path / "outside.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(RUNNER),
            "--profile",
            "smoke",
            "--scenario",
            "corruption",
            "--output",
            str(output),
        ],
        cwd=tmp_path,
        check=False,
    )
    assert completed.returncode in (0, 1)
    report = json.loads(output.read_text())
    assert report["scenarios"][0]["name"] == "corruption"
    assert report["scenarios"][0]["status"] in ("passed", "failed", "skipped")


def test_subprocess_timeout_is_bounded():
    started = time.monotonic()
    with pytest.raises(TimeoutError, match="timeout"):
        run_process([sys.executable, "-c", "import time; time.sleep(2)"], timeout=0.05)
    assert time.monotonic() - started < 1


def test_child_exit_before_readiness_is_reported_and_stderr_preserved(tmp_path: Path):
    with pytest.raises(ProcessFailure, match="before synchronization") as failure:
        wait_for_notification(
            [sys.executable, "-c", "import sys; print('child-error', file=sys.stderr)"],
            expected="ready",
            timeout=1,
            artifacts_dir=tmp_path,
        )
    assert "child-error" in failure.value.stderr
    assert (tmp_path / "child.stderr.txt").read_text().strip() == "child-error"


@pytest.mark.parametrize("missing", [("--profile",), ("--output",)])
def test_required_cli_arguments(missing: tuple[str]):
    args = [
        sys.executable,
        str(RUNNER),
        "--profile",
        "ci",
        "--output",
        "/tmp/unused-chaos.json",
    ]
    for flag in missing:
        args.remove(flag)
    completed = subprocess.run(args, capture_output=True, text=True)
    assert completed.returncode == 2
