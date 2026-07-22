"""Command-line interface for the canonical benchmark."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from localqueue.benchmark.config import BenchmarkConfig
from localqueue.benchmark.errors import sanitize_error_message
from localqueue.benchmark.multiprocess import run_multiprocess_profile
from localqueue.benchmark.multiprocess_models import MultiprocessConfig
from localqueue.benchmark.render import render_file
from localqueue.benchmark.runner import run_profile


def _atomic_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, sort_keys=True, allow_nan=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if argv and argv[0] == "render":
        parser = argparse.ArgumentParser()
        parser.add_argument("input", type=Path)
        parser.add_argument("--output", type=Path, required=True)
        try:
            args = parser.parse_args(argv[1:])
            render_file(args.input, args.output)
            return 0
        except (OSError, ValueError, json.JSONDecodeError) as error:
            print(
                f"benchmark render error: {type(error).__name__}: {error}",
                file=sys.stderr,
            )
            return 2
    parser = argparse.ArgumentParser(
        description="Run the canonical single-process localqueue benchmark."
    )
    parser.add_argument(
        "--profile",
        choices=("smoke", "standard", "multiprocess-ci", "multiprocess-release"),
        required=True,
    )
    parser.add_argument("--durability", choices=("normal", "full"))
    parser.add_argument("--workdir", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--large-db-rows", type=int, default=1_000_000)
    args = parser.parse_args(argv)
    try:
        if args.profile.startswith("multiprocess-"):
            report = run_multiprocess_profile(
                MultiprocessConfig(
                    profile=args.profile,
                    messages=200 if args.profile.endswith("ci") else 5000,
                    durability=args.durability,
                    large_db_rows=args.large_db_rows,
                ),
                args.output,
                args.workdir,
            )
        else:
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
    assert report is not None
    profile = report.profile
    if args.profile.startswith("multiprocess-"):
        print(
            f"profile={profile['name']} canonical={profile['canonical']} "
            f"package={report.subject['package_version']}"
        )
        for scenario in report.scenarios:
            print(f"{scenario.scenario_id} {scenario.status}")
        return 0 if report.run["status"] == "passed" else 1
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
