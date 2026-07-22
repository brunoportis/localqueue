"""Command-line interface for the canonical benchmark."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from localqueue.benchmark.config import BenchmarkConfig
from localqueue.benchmark.errors import sanitize_error_message
from localqueue.benchmark.runner import run_profile


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the canonical single-process localqueue benchmark."
    )
    parser.add_argument("--profile", choices=("smoke", "standard"), required=True)
    parser.add_argument("--durability", choices=("normal", "full"))
    parser.add_argument("--workdir", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        report = run_profile(
            BenchmarkConfig.from_profile(args.profile, args.durability),
            args.output,
            args.workdir,
        )
    except (OSError, ValueError) as error:
        print(
            f"benchmark usage error: {type(error).__name__}: {error}", file=sys.stderr
        )
        return 2
    except RuntimeError as error:
        message = sanitize_error_message(
            str(error),
            tuple(
                path
                for path in (args.workdir, args.output, args.output.parent)
                if path is not None
            ),
        )
        print(f"benchmark failed: {type(error).__name__}: {message}", file=sys.stderr)
        return 1
    profile = report.profile
    print(
        f"profile={profile['name']} durability={profile['durability'].upper()} package={report.subject['package_version']} commit={report.subject['commit_sha'] or 'unavailable'}"
    )
    print(
        "scenario                         ops/s       p50 ns       p95 ns       p99 ns  correctness"
    )
    for scenario in report.scenarios:
        summary = scenario.summary
        if summary is None:
            print(f"{scenario.scenario_id:<32} failed")
        else:
            print(
                f"{scenario.scenario_id:<32} {summary.operations_per_second:10.2f} {summary.p50_ns:12d} {summary.p95_ns:12d} {summary.p99_ns:12d} {scenario.status}"
            )
    print(f"report={args.output}")
    return 0
