from __future__ import annotations

from pathlib import Path

from localqueue import DurabilityMode, SimpleQueue

from ..model import ScenarioResult
from ..sqlite import product_sqlite_settings
from .common import ScenarioContext, counts, validate_queue


def run_mode(
    _: str, artifacts_dir: Path, *, durability: DurabilityMode
) -> ScenarioResult:
    is_durable = durability is DurabilityMode.DURABLE
    mode = "full" if is_durable else "normal"
    expected_synchronous = 2 if is_durable else 1
    result = ScenarioResult(
        f"synchronous-{mode}",
        durability_mode=mode,
        expected_outcome=(
            f"SimpleQueue(durability=DurabilityMode.{durability.name}) executes "
            f"put/get/ack with SQLite {mode.upper()}"
        ),
        required_invariants=frozenset(
            {
                "product_pragmas",
                "public_lifecycle",
                "confirmed_preserved",
                "integrity_ok",
            }
        ),
        required_fields=frozenset(
            {
                "operation_confirmed_to_caller",
                "counts_before",
                "counts_after_recovery",
                "integrity_check",
            }
        ),
        fresh_process_required=True,
    )
    context = ScenarioContext(artifacts_dir, result.name)
    try:
        queue = SimpleQueue(str(context.queue_dir), durability=durability)
        result.counts_before = counts(queue.stats())
        queue.put({"mode": mode})
        result.operation_confirmed_to_caller = True
        job = queue.get(block=False)
        payload_ok = job.data == {"mode": mode}
        queue.ack(job)
        lifecycle_counts = counts(queue.stats())
        result.pragmas = product_sqlite_settings(queue)
        queue.close()
        result.invariant(
            "product_pragmas",
            result.pragmas["journal_mode"] == "wal"
            and result.pragmas["synchronous"] == expected_synchronous,
            "pragmas observed on the connection configured by SimpleQueue",
        )
        result.invariant(
            "public_lifecycle",
            payload_ok and lifecycle_counts["acked"] == 1,
            "put/get/ack completed through the public API",
        )
        validation = validate_queue(context)
        result.fresh_process_validated = True
        result.counts_after_recovery = counts(validation["stats_before"])
        result.integrity_check = str(validation["integrity_check"])
        result.invariant(
            "confirmed_preserved",
            result.counts_after_recovery["acked"] == 1,
            "fresh SimpleQueue process observed the confirmed ack",
        )
        result.invariant(
            "integrity_ok", result.integrity_check == "ok", result.integrity_check
        )
        result.status = "passed"
    except BaseException as error:
        result.error = {
            "public_type": type(error).__name__,
            "sqlite_code": "",
            "message": str(error),
        }
    result.artifacts = context.artifacts()
    return result.finish()


def normal(profile: str, artifacts_dir: Path) -> ScenarioResult:
    return run_mode(profile, artifacts_dir, durability=DurabilityMode.RELAXED)


def full(profile: str, artifacts_dir: Path) -> ScenarioResult:
    return run_mode(profile, artifacts_dir, durability=DurabilityMode.DURABLE)
