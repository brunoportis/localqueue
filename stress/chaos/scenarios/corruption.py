from __future__ import annotations

import sqlite3
from pathlib import Path

from localqueue import SimpleQueue

from ..model import ScenarioResult
from ..sqlite import sqlite_error_fields
from .common import ScenarioContext


def evaluate_open(
    result: ScenarioResult, label: str, error: BaseException | None
) -> None:
    result.invariant(
        f"{label}_explicit_error",
        error is not None,
        f"SimpleQueue {'rejected' if error else 'accepted'} {label} input",
    )


def _attempt(queue_dir: Path) -> BaseException | None:
    try:
        queue = SimpleQueue(str(queue_dir))
    except BaseException as error:
        return error
    queue.close()
    return None


def run(_: str, artifacts_dir: Path) -> ScenarioResult:
    result = ScenarioResult(
        "corruption",
        expected_outcome="SimpleQueue rejects malformed and incompatible databases without silent reset",
        required_invariants=frozenset(
            {
                "malformed_explicit_error",
                "malformed_preserved",
                "incompatible_explicit_error",
                "incompatible_preserved",
            }
        ),
    )
    context = ScenarioContext(artifacts_dir, result.name)
    try:
        malformed_dir = context.path / "malformed"
        malformed_dir.mkdir()
        malformed_db = malformed_dir / "localqueue.db"
        original = b"not a sqlite database\n"
        malformed_db.write_bytes(original)
        malformed_error = _attempt(malformed_dir)
        evaluate_open(result, "malformed", malformed_error)
        result.error = sqlite_error_fields(malformed_error) if malformed_error else None
        result.invariant(
            "malformed_preserved",
            malformed_db.read_bytes() == original,
            "malformed file bytes remain unchanged",
        )

        incompatible_dir = context.path / "incompatible"
        incompatible_dir.mkdir()
        incompatible_db = incompatible_dir / "localqueue.db"
        with sqlite3.connect(incompatible_db) as connection:
            connection.execute("CREATE TABLE messages (unexpected TEXT)")
            connection.commit()
        incompatible_error = _attempt(incompatible_dir)
        evaluate_open(result, "incompatible", incompatible_error)
        with sqlite3.connect(incompatible_db) as connection:
            columns = [
                row[1] for row in connection.execute("PRAGMA table_info(messages)")
            ]
        result.invariant(
            "incompatible_preserved",
            columns == ["unexpected"],
            "SimpleQueue did not replace the incompatible messages table",
        )
        result.status = "passed"
    except BaseException as error:
        result.error = result.error or sqlite_error_fields(error)
    result.artifacts = context.artifacts()
    return result.finish()
