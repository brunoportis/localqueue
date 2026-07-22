from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from localqueue import SimpleQueue

from ..model import ScenarioResult
from .common import (
    ScenarioContext,
    counts,
    public_error,
    run_queue_operation,
    validate_queue,
)


def evaluate(
    result: ScenarioResult, existing: dict[str, Any], creation: dict[str, Any]
) -> None:
    existing_rejected = not bool(existing.get("ok"))
    creation_rejected = not bool(creation.get("ok"))
    result.operation_confirmed_to_caller = bool(existing.get("confirmed"))
    result.error = public_error(existing) or public_error(creation)
    result.invariant(
        "existing_write_rejected",
        existing_rejected,
        "SimpleQueue.put on the non-writable existing database returned an error",
    )
    result.invariant(
        "new_database_rejected",
        creation_rejected,
        "SimpleQueue could not create a database in the non-writable directory",
    )


def run(_: str, artifacts_dir: Path) -> ScenarioResult:
    result = ScenarioResult(
        "readonly",
        expected_outcome="SimpleQueue reports real filesystem write failures and preserves known state",
        required_invariants=frozenset(
            {
                "existing_write_rejected",
                "new_database_rejected",
                "known_state_preserved",
                "integrity_ok",
            }
        ),
        required_fields=frozenset(
            {
                "operation_confirmed_to_caller",
                "counts_before",
                "counts_after_failure",
                "counts_after_recovery",
                "integrity_check",
            }
        ),
        fresh_process_required=True,
    )
    context = ScenarioContext(artifacts_dir, result.name)
    if os.name != "posix" or getattr(os, "geteuid", lambda: 1)() == 0:
        result.status = "skipped"
        result.skip_reason = "requires non-root POSIX filesystem permission enforcement"
        return result
    try:
        queue = SimpleQueue(str(context.queue_dir))
        queue.put({"known": True})
        result.counts_before = counts(queue.stats())
        queue.close()
        file_mode = context.db_path.stat().st_mode
        dir_mode = context.queue_dir.stat().st_mode
        os.chmod(context.db_path, file_mode & ~0o222)
        os.chmod(context.queue_dir, dir_mode & ~0o222)
        try:
            existing = run_queue_operation(
                context, "put", {"unexpected": True}, label="existing"
            )
        finally:
            os.chmod(context.db_path, file_mode)
            os.chmod(context.queue_dir, dir_mode)

        new_dir = context.path / "new-readonly"
        new_dir.mkdir()
        new_mode = new_dir.stat().st_mode
        os.chmod(new_dir, new_mode & ~0o222)
        try:
            creation = run_queue_operation(
                context,
                "put",
                {"new": True},
                queue_dir=new_dir / "queue",
                label="new",
            )
        finally:
            os.chmod(new_dir, new_mode)
        evaluate(result, existing, creation)
        result.counts_after_failure = (
            result.counts_before if not existing["ok"] else counts(existing["stats"])
        )
        validation = validate_queue(context, consume=True)
        result.fresh_process_validated = True
        result.counts_after_recovery = counts(validation["stats_before"])
        result.integrity_check = str(validation["integrity_check"])
        result.invariant(
            "known_state_preserved",
            validation["payload"] == {"known": True}
            and result.counts_after_recovery["ready"] == 1,
            "fresh SimpleQueue process read the original payload",
        )
        result.invariant(
            "integrity_ok", result.integrity_check == "ok", result.integrity_check
        )
        result.status = "passed"
    except BaseException as error:
        result.error = result.error or {
            "public_type": type(error).__name__,
            "sqlite_code": "",
            "message": str(error),
        }
    result.artifacts = context.artifacts()
    return result.finish()
