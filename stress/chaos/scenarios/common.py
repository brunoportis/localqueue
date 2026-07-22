from __future__ import annotations

import json
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..model import Counts
from ..process import run_process
from ..sqlite import database_path

VALIDATOR_CODE = r"""
import json
import sqlite3
import sys
from localqueue import Empty, SimpleQueue

queue_dir = sys.argv[1]
consume = sys.argv[2] == "consume"
queue = SimpleQueue(queue_dir)
stats_before = queue.stats()
payload = None
if consume and stats_before["ready"]:
    job = queue.get(block=False)
    payload = job.data
    queue.ack(job)
stats_after = queue.stats()
queue.close()
with sqlite3.connect(queue_dir + "/localqueue.db") as connection:
    integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
print(json.dumps({
    "stats_before": stats_before,
    "stats_after": stats_after,
    "payload": payload,
    "integrity_check": integrity,
}))
"""

OPERATION_CODE = r"""
import json
import sys
from localqueue import SimpleQueue

queue_dir, operation, payload, fsync = sys.argv[1:5]
try:
    queue = SimpleQueue(queue_dir, fsync=fsync == "true")
    confirmed = False
    if operation == "put":
        queue.put(json.loads(payload))
        confirmed = True
        operation_result = None
    elif operation == "purge":
        operation_result = queue.purge(int(json.loads(payload)))
        confirmed = True
    else:
        raise ValueError(operation)
    stats = queue.stats()
    queue.close()
    print(json.dumps({
        "ok": True,
        "confirmed": confirmed,
        "operation_result": operation_result,
        "stats": stats,
    }))
except BaseException as error:
    print(json.dumps({
        "ok": False,
        "confirmed": False,
        "error": {
            "public_type": type(error).__name__,
            "sqlite_code": str(getattr(error, "sqlite_errorname", "") or ""),
            "message": " ".join(str(error).split()),
        },
    }))
"""


@dataclass
class ScenarioContext:
    root: Path
    name: str

    def __post_init__(self) -> None:
        self.path = self.root / self.name
        if self.path.exists():
            shutil.rmtree(self.path)
        self.queue_dir = self.path / "queue"
        self.path.mkdir(parents=True)

    @property
    def db_path(self) -> Path:
        return database_path(self.queue_dir)

    def artifacts(self) -> list[str]:
        return [
            str(path.relative_to(self.root))
            for path in sorted(self.path.rglob("*"))
            if path.is_file()
        ]


def parse_json_output(stdout: str) -> dict[str, Any]:
    lines = [line for line in stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError("child produced no JSON output")
    return dict(json.loads(lines[-1]))


def run_queue_operation(
    context: ScenarioContext,
    operation: str,
    payload: Any,
    *,
    queue_dir: Path | None = None,
    fsync: bool = False,
    timeout: float = 10.0,
    label: str = "operation",
) -> dict[str, Any]:
    target = queue_dir or context.queue_dir
    completed = run_process(
        [
            sys.executable,
            "-c",
            OPERATION_CODE,
            str(target),
            operation,
            json.dumps(payload),
            str(fsync).lower(),
        ],
        timeout=timeout,
    )
    (context.path / f"{label}.stdout.txt").write_text(
        completed.stdout, encoding="utf-8"
    )
    (context.path / f"{label}.stderr.txt").write_text(
        completed.stderr, encoding="utf-8"
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"queue operation child failed with {completed.returncode}: {completed.stderr}"
        )
    return parse_json_output(completed.stdout)


def validate_queue(
    context: ScenarioContext,
    *,
    queue_dir: Path | None = None,
    consume: bool = False,
    label: str = "validator",
) -> dict[str, Any]:
    target = queue_dir or context.queue_dir
    completed = run_process(
        [
            sys.executable,
            "-c",
            VALIDATOR_CODE,
            str(target),
            "consume" if consume else "stats",
        ],
        timeout=10.0,
    )
    (context.path / f"{label}.stdout.txt").write_text(
        completed.stdout, encoding="utf-8"
    )
    (context.path / f"{label}.stderr.txt").write_text(
        completed.stderr, encoding="utf-8"
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"fresh validator failed with {completed.returncode}: {completed.stderr}"
        )
    return parse_json_output(completed.stdout)


def public_error(result: dict[str, Any]) -> dict[str, Any] | None:
    value = result.get("error")
    return dict(value) if isinstance(value, dict) else None


def counts(value: Any) -> Counts:
    return {key: int(value[key]) for key in ("ready", "processing", "acked", "failed")}
