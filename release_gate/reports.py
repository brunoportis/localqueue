from __future__ import annotations

from pathlib import PurePath
from typing import Mapping


class ReportError(ValueError):
    pass


CRASH_SCENARIOS = {
    "enqueue-after-begin",
    "enqueue-before-commit",
    "claim-before-commit",
    "ack-before-commit",
    "nack-before-commit",
    "fail-before-commit",
    "control-enqueue",
    "control-claim",
    "control-ack",
    "control-nack",
    "control-fail",
}
CHAOS_SCENARIOS = {
    "disk-full",
    "readonly",
    "lock-timeout",
    "wal-recovery",
    "corruption",
    "synchronous-normal",
    "synchronous-full",
    "producer-termination",
    "maintenance-termination",
    "backup-restore",
}


def validate_report(
    report: Mapping[str, object],
    candidate_sha: str,
    version: str,
    candidate_ref: str,
    *,
    require_wheel: bool = False,
) -> None:
    subject = report.get("subject")
    if not isinstance(subject, Mapping):
        raise ReportError("report subject is missing")
    expected = {
        "candidate_sha": candidate_sha,
        "package_version": version,
        "native_version": version,
        "candidate_ref": candidate_ref,
    }
    for key, value in expected.items():
        if subject.get(key) != value:
            raise ReportError(f"report {key} differs or is missing")
    if report.get("status", report.get("passed")) not in (
        "passed",
        "manual_confirmation_required",
        True,
    ):
        raise ReportError("report did not pass")
    if require_wheel:
        module = str(subject.get("installed_module_path", "")).replace("\\", "/")
        if "site-packages/" not in module:
            raise ReportError("wheel report import must come from site-packages")
        if any(part in {"python", "checkout"} for part in PurePath(module).parts[:-2]):
            raise ReportError("wheel report imported from checkout")


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ReportError(f"{label} is missing")
    return value


def _passed_scenarios(report: Mapping[str, object], expected: set[str]) -> None:
    scenarios = report.get("scenarios")
    if not isinstance(scenarios, list):
        raise ReportError("scenario list is missing")
    names = {
        str(item.get("scenario", item.get("name", "")))
        for item in scenarios
        if isinstance(item, Mapping)
    }
    if names != expected:
        raise ReportError(f"scenario catalog differs: expected {sorted(expected)}")
    if any(
        not isinstance(item, Mapping)
        or item.get("status", "passed" if item.get("passed") else "failed") != "passed"
        for item in scenarios
    ):
        raise ReportError("one or more required scenarios did not pass")


def validate_report_semantics(filename: str, report: Mapping[str, object]) -> None:
    if filename == "ci-summary.json":
        if _mapping(report.get("summary"), "CI summary").get("name") != "complete-ci":
            raise ReportError("CI summary does not represent complete CI")
    elif filename == "wheel-smoke-summary.json":
        if (
            _mapping(report.get("summary"), "wheel summary").get("name")
            != "wheel-smoke"
        ):
            raise ReportError("wheel smoke summary is incomplete")
    elif filename == "multiprocess-soak.json":
        configuration = _mapping(report.get("configuration"), "soak configuration")
        expected = {
            "duration_seconds": 1800.0,
            "messages": 50_000,
            "producers": 4,
            "consumers": 8,
            "crash_rate": 0.01,
        }
        if any(configuration.get(key) != value for key, value in expected.items()):
            raise ReportError("soak did not use the scheduled release profile")
        database = _mapping(report.get("database"), "soak database result")
        if report.get("success") is not True or database.get("integrity") != "ok":
            raise ReportError("soak correctness or integrity failed")
    elif filename == "deterministic-crash.json":
        _passed_scenarios(report, CRASH_SCENARIOS)
        if _mapping(report.get("summary"), "crash summary").get("total") != len(
            CRASH_SCENARIOS
        ):
            raise ReportError("crash summary count differs")
    elif filename == "chaos.json":
        if report.get("profile") != "ci" or report.get("passed") is not True:
            raise ReportError("chaos report is not the complete passing ci profile")
        _passed_scenarios(report, CHAOS_SCENARIOS)
    elif filename in {"compatibility-online.json", "compatibility-offline.json"}:
        current = _mapping(report.get("current_wheel"), "compatibility wheel")
        if current.get("source_kind") != "explicit wheel" or not current.get("sha256"):
            raise ReportError("compatibility did not use an explicit candidate wheel")
        baselines = report.get("baselines")
        if (
            not isinstance(baselines, list)
            or not baselines
            or any(
                not isinstance(item, Mapping) or item.get("status") != "passed"
                for item in baselines
            )
        ):
            raise ReportError("compatibility baseline matrix is incomplete")
    elif filename in {"benchmark-standard.json", "benchmark-multiprocess-release.json"}:
        expected_profile = (
            "standard"
            if filename == "benchmark-standard.json"
            else "multiprocess-release"
        )
        profile = _mapping(report.get("profile"), "benchmark profile")
        run = _mapping(report.get("run"), "benchmark run")
        scenarios = report.get("scenarios")
        canonical = (
            profile.get("warmups") == 3 and profile.get("samples") == 25
            if expected_profile == "standard"
            else profile.get("canonical") is True
        )
        if (
            profile.get("name") != expected_profile
            or not canonical
            or run.get("status") != "passed"
            or not isinstance(scenarios, list)
            or not scenarios
            or any(
                not isinstance(item, Mapping) or item.get("status") != "passed"
                for item in scenarios
            )
        ):
            raise ReportError(
                f"{expected_profile} benchmark is incomplete or non-canonical"
            )
    elif filename == "documentation-summary.json":
        if (
            _mapping(report.get("summary"), "documentation summary").get("name")
            != "documentation"
        ):
            raise ReportError("documentation summary is incomplete")
    elif filename == "open-issue-audit.json":
        if report.get("blockers") or not isinstance(report.get("reviewed"), list):
            raise ReportError("open-issue audit is incomplete or blocked")
    elif filename == "security-audit.json":
        if report.get("gitleaks") != "passed" or report.get("cargo_deny") != "passed":
            raise ReportError("security tools did not pass")
        if report.get("private_vulnerability_reporting") not in {
            "enabled",
            "manual_confirmation_required",
        }:
            raise ReportError("private vulnerability reporting is disabled")
