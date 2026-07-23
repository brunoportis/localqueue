#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from release_gate.markdown import evidence_markdown  # noqa: E402
from release_gate.subject import installed_subject  # noqa: E402
from tests.crash_harness import SCENARIOS  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--markdown-output", type=Path, required=True)
    parser.add_argument("--scenario-dir", type=Path, required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--candidate-ref", required=True)
    parser.add_argument("--candidate-version", required=True)
    args = parser.parse_args()
    args.scenario_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, object]] = []
    for name in sorted(SCENARIOS):
        destination = args.scenario_dir / f"{name}.json"
        completed = subprocess.run(
            [
                sys.executable,
                "tests/crash_harness.py",
                "--scenario",
                name,
                "--output",
                str(destination),
            ],
            check=False,
        )
        if destination.is_file():
            result = json.loads(destination.read_text(encoding="utf-8"))
        else:
            result = {
                "scenario": name,
                "passed": False,
                "error": "scenario report missing",
            }
        result["runner_return_code"] = completed.returncode
        result["status"] = (
            "passed" if result.get("passed") and completed.returncode == 0 else "failed"
        )
        results.append(result)
    passed = all(item["status"] == "passed" for item in results)
    report = {
        "schema_version": 1,
        "subject": installed_subject(
            args.candidate_sha,
            args.candidate_ref,
            args.candidate_version,
            require_wheel=False,
        ),
        "summary": {
            "total": len(results),
            "passed": sum(item["status"] == "passed" for item in results),
            "failed": sum(item["status"] == "failed" for item in results),
        },
        "scenarios": results,
        "status": "passed" if passed else "failed",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    args.markdown_output.write_text(
        evidence_markdown("Deterministic crash campaign", report), encoding="utf-8"
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
