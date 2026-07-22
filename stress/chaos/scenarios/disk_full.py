from __future__ import annotations

import sqlite3
from pathlib import Path

from localqueue import SimpleQueue

from ..model import ScenarioResult
from ..sqlite import product_sqlite_settings, sqlite_error_fields
from .common import ScenarioContext, counts, run_queue_operation, validate_queue


def run(_: str, artifacts_dir: Path) -> ScenarioResult:
    result = ScenarioResult(
        "disk-full",
        expected_outcome="SimpleQueue.put reports disk-full without a partial message and retry succeeds",
        required_invariants=frozenset(
            {
                "full_error_observed",
                "no_partial_transition",
                "integrity_ok",
                "retry_visible",
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
    try:
        queue = SimpleQueue(str(context.queue_dir))
        result.counts_before = counts(queue.stats())
        with sqlite3.connect(context.db_path) as external:
            pages = int(external.execute("PRAGMA page_count").fetchone()[0])
        set_page_limit = getattr(queue._native, "_test_set_max_page_count")
        applied_limit = int(set_page_limit(pages))
        result.pragmas = product_sqlite_settings(queue)
        result.pragmas["max_page_count"] = applied_limit
        try:
            queue.put({"payload": "x" * 1_000_000})
        except BaseException as error:
            result.error = sqlite_error_fields(error)
            result.operation_confirmed_to_caller = False
            observed = "full" in str(error).lower()
        else:
            result.operation_confirmed_to_caller = True
            observed = False
        result.counts_after_failure = counts(queue.stats())
        set_page_limit(2_147_483_646)
        queue.close()
        result.invariant(
            "full_error_observed",
            observed,
            "public SimpleQueue exception contains the disk-full outcome",
        )
        result.invariant(
            "no_partial_transition",
            result.counts_after_failure == result.counts_before,
            "failed put did not alter public stats",
        )
        validation = validate_queue(context)
        result.fresh_process_validated = True
        result.counts_after_recovery = counts(validation["stats_before"])
        result.integrity_check = str(validation["integrity_check"])
        result.invariant(
            "integrity_ok", result.integrity_check == "ok", result.integrity_check
        )
        retry = run_queue_operation(context, "put", {"payload": "retry"}, label="retry")
        result.retry_attempted = True
        result.retry_succeeded = bool(retry["ok"])
        result.retry_safe = result.retry_succeeded
        retry_validation = validate_queue(context, label="retry-validator")
        result.invariant(
            "retry_visible",
            counts(retry_validation["stats_before"])["ready"] == 1,
            "retry through SimpleQueue is visible after another reopen",
        )
        result.status = "passed"
    except BaseException as error:
        result.error = result.error or sqlite_error_fields(error)
    result.artifacts = context.artifacts()
    return result.finish()
