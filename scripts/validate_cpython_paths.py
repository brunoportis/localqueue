"""Validate explicit CPython paths supplied to the release wheel build."""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
from pathlib import Path
from typing import Sequence

EXPECTED = {
    "cp310": "3.10",
    "cp311": "3.11",
    "cp312": "3.12",
    "cp313": "3.13",
    "cp314": "3.14",
}
PROBE = (
    "import json, sys, sysconfig; "
    "print(json.dumps({'implementation': sys.implementation.name, "
    "'version': '.'.join(map(str, sys.version_info[:3])), "
    "'gil_disabled': bool(sysconfig.get_config_var('Py_GIL_DISABLED') or 0)}))"
)


class InterpreterPathError(ValueError):
    pass


def parse_interpreters(values: Sequence[str]) -> dict[str, Path]:
    parsed: dict[str, Path] = {}
    for value in values:
        key, separator, raw_path = value.partition("=")
        if not separator or key not in EXPECTED or not raw_path:
            raise InterpreterPathError(f"invalid interpreter specification: {value!r}")
        if key in parsed:
            raise InterpreterPathError(f"duplicate interpreter specification: {key}")
        parsed[key] = Path(raw_path)
    if set(parsed) != set(EXPECTED):
        raise InterpreterPathError(
            f"expected interpreter IDs {sorted(EXPECTED)}, got {sorted(parsed)}"
        )
    return parsed


def validate_interpreters(interpreters: dict[str, Path]) -> list[dict[str, str]]:
    resolved: set[Path] = set()
    results: list[dict[str, str]] = []
    for key in sorted(EXPECTED):
        path = interpreters[key]
        if not path.is_absolute():
            raise InterpreterPathError(f"{key} path is not absolute: {path}")
        if not path.is_file():
            raise InterpreterPathError(f"{key} path does not exist: {path}")
        resolved_path = path.resolve()
        if resolved_path in resolved:
            raise InterpreterPathError(
                f"{key} path duplicates another interpreter: {path}"
            )
        resolved.add(resolved_path)
        expected_version = EXPECTED[key]
        version_result = subprocess.run(
            [str(path), "--version"], check=True, capture_output=True, text=True
        )
        returned_version = (version_result.stdout or version_result.stderr).strip()
        if not returned_version.startswith(f"Python {expected_version}."):
            raise InterpreterPathError(
                f"{key} returned {returned_version!r}, expected Python {expected_version}.x"
            )
        completed = subprocess.run(
            [str(path), "-c", PROBE], check=True, capture_output=True, text=True
        )
        try:
            probe = json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise InterpreterPathError(
                f"{key} did not return valid probe data"
            ) from error
        version = str(probe.get("version", ""))
        if not version.startswith(f"{expected_version}."):
            raise InterpreterPathError(
                f"{key} returned Python {version}, expected {expected_version}.x"
            )
        if probe.get("implementation") != "cpython":
            raise InterpreterPathError(
                f"{key} is not CPython: {probe.get('implementation')}"
            )
        if probe.get("gil_disabled"):
            raise InterpreterPathError(f"{key} is CPython free-threaded")
        results.append(
            {
                "id": key,
                "expected": expected_version,
                "path": str(resolved_path),
                "version": returned_version,
            }
        )
    return results


def render_summary(results: Sequence[dict[str, str]]) -> str:
    lines = [
        "## Explicit CPython interpreter paths",
        "",
        f"- Runner platform: `{platform.system()}`",
        "",
    ]
    for result in results:
        lines.append(
            f"- `{result['id']}`: expected `{result['expected']}.x`, resolved path `{result['path']}`, returned `{result['version']}`"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interpreter", action="append", required=True)
    args = parser.parse_args()
    results = validate_interpreters(parse_interpreters(args.interpreter))
    summary = render_summary(results)
    print(summary, end="")
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as stream:
            stream.write(summary)


if __name__ == "__main__":
    main()
