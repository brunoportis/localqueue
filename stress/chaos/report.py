from __future__ import annotations

import json
import platform
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

from . import SCHEMA_VERSION
from .model import ScenarioResult


def normalize_error(error: BaseException) -> dict[str, str]:
    name = type(error).__name__
    message = " ".join(str(error).split())
    lower = message.lower()
    if "database is locked" in lower or "database table is locked" in lower:
        message = "database is locked"
    elif "database or disk is full" in lower:
        message = "database or disk is full"
    elif "readonly" in lower or "read-only" in lower:
        message = "attempt to write a readonly database"
    return {"public_type": name, "sqlite_code": "", "message": message}


def result_dict(result: ScenarioResult) -> dict[str, Any]:
    value = asdict(result)
    value.pop("required_invariants", None)
    value.pop("required_fields", None)
    value.pop("fresh_process_required", None)
    if value["status"] == "skipped":
        value["skip"] = {"reason": value.pop("skip_reason") or "unspecified"}
    else:
        value.pop("skip_reason", None)
    return value


def make_report(profile: str, results: list[ScenarioResult]) -> dict[str, Any]:
    statuses = [r.status for r in results]
    return {
        "schema_version": SCHEMA_VERSION,
        "profile": profile,
        "platform": {
            "system": platform.system().lower(),
            "python": sys.version.split()[0],
        },
        "localqueue": {"package": "localqueue", "harness": "operational-chaos"},
        "summary": {
            "total": len(results),
            "passed": statuses.count("passed"),
            "failed": statuses.count("failed"),
            "skipped": statuses.count("skipped"),
        },
        "scenarios": [result_dict(r) for r in results],
        "passed": bool(results) and all(r.status == "passed" for r in results),
    }


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
