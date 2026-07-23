#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from release_gate.markdown import evidence_markdown  # noqa: E402
from release_gate.subject import installed_subject  # noqa: E402
from stress.chaos.model import (  # noqa: E402
    SCENARIO_NAMES,
    SMOKE_SCENARIOS,
    ScenarioResult,
)
from stress.chaos.report import make_report, normalize_error, write_report  # noqa: E402
from stress.chaos.scenarios import SCENARIOS  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run deterministic SQLite operational chaos scenarios"
    )
    parser.add_argument("--profile", required=True, choices=("ci", "smoke"))
    parser.add_argument("--scenario")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--artifacts-dir", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    parser.add_argument("--candidate-sha")
    parser.add_argument("--candidate-ref")
    parser.add_argument("--candidate-version")
    parser.add_argument("--require-wheel", action="store_true")
    args = parser.parse_args(argv)
    artifacts_dir = args.artifacts_dir or args.output.parent / "scenarios"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    names = (
        [args.scenario]
        if args.scenario
        else list(SMOKE_SCENARIOS if args.profile == "smoke" else SCENARIO_NAMES)
    )
    unknown = [name for name in names if name not in SCENARIO_NAMES]
    if unknown:
        result = ScenarioResult(
            "harness", expected_outcome="unknown scenario is rejected"
        )
        result.error = {
            "public_type": "ValueError",
            "sqlite_code": "",
            "message": f"unknown scenario: {unknown[0]}",
        }
        result.invariant(
            "scenario_name_valid", False, "scenario is not in the versioned catalog"
        )
        result.status = "failed"
        report = make_report(args.profile, [result])
        write_report(args.output, report)
        print(f"chaos: rejected unknown scenario {unknown[0]}", file=sys.stderr)
        return 1
    results: list[ScenarioResult] = []
    try:
        for name in names:
            try:
                results.append(SCENARIOS[name](args.profile, artifacts_dir))
            except Exception as error:
                failure = ScenarioResult(
                    name, expected_outcome="harness must report internal failures"
                )
                failure.error = normalize_error(error)
                results.append(failure)
    except Exception as error:
        failure = ScenarioResult(
            "harness", expected_outcome="harness failure is visible in JSON"
        )
        failure.error = normalize_error(error)
        results.append(failure)
    subject = None
    identity = (args.candidate_sha, args.candidate_ref, args.candidate_version)
    if any(identity):
        if not all(identity):
            parser.error("candidate SHA, ref and version must be provided together")
        subject = installed_subject(
            args.candidate_sha,
            args.candidate_ref,
            args.candidate_version,
            require_wheel=args.require_wheel,
        )
    report = make_report(args.profile, results, subject)
    try:
        write_report(args.output, report)
        if args.markdown_output:
            args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
            args.markdown_output.write_text(
                evidence_markdown("Operational SQLite chaos", report), encoding="utf-8"
            )
    except Exception as error:
        print(f"could not write chaos report: {error}", file=sys.stderr)
        return 2
    print(f"chaos: {report['summary']} -> {args.output}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
