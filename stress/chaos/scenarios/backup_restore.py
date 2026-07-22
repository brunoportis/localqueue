from __future__ import annotations

import sqlite3
from pathlib import Path

from localqueue import SimpleQueue

from ..model import ScenarioResult
from .common import ScenarioContext, counts, validate_queue


def run(_: str, artifacts_dir: Path) -> ScenarioResult:
    result = ScenarioResult(
        "backup-restore",
        expected_outcome="internal sqlite3 backup restores the SimpleQueue backup point independently",
        required_invariants=frozenset(
            {
                "backup_point_counts",
                "payload_restored",
                "integrity_ok",
                "no_external_wal_required",
            }
        ),
        required_fields=frozenset(
            {"counts_before", "counts_after_recovery", "integrity_check"}
        ),
        fresh_process_required=True,
    )
    context = ScenarioContext(artifacts_dir, result.name)
    try:
        source = SimpleQueue(str(context.queue_dir))
        source.put({"point": "backup"})
        result.counts_before = counts(source.stats())
        restored_dir = context.path / "restored"
        restored_dir.mkdir()
        restored_db = restored_dir / "localqueue.db"
        with (
            sqlite3.connect(context.db_path) as source_db,
            sqlite3.connect(restored_db) as target_db,
        ):
            source_db.backup(target_db)
        source.put({"point": "after-backup"})
        source.close()
        no_sidecars = (
            not Path(f"{restored_db}-wal").exists()
            and not Path(f"{restored_db}-shm").exists()
        )
        validation = validate_queue(
            context, queue_dir=restored_dir, consume=True, label="restore-validator"
        )
        result.fresh_process_validated = True
        result.counts_after_recovery = counts(validation["stats_before"])
        result.integrity_check = str(validation["integrity_check"])
        result.invariant(
            "backup_point_counts",
            result.counts_after_recovery == result.counts_before,
            "restored SimpleQueue stats match the backup point",
        )
        result.invariant(
            "payload_restored",
            validation["payload"] == {"point": "backup"},
            "fresh SimpleQueue process consumed the backup-point payload",
        )
        result.invariant(
            "integrity_ok", result.integrity_check == "ok", result.integrity_check
        )
        result.invariant(
            "no_external_wal_required",
            no_sidecars,
            "restored copy opened without origin WAL/SHM files",
        )
        result.limitations.append(
            "internal fixture only; public backup API belongs to issue #22"
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
