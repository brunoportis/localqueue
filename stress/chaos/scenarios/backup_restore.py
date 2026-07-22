from __future__ import annotations

import shutil
from pathlib import Path

from localqueue import SimpleQueue

from ..model import ScenarioResult
from .common import ScenarioContext, counts, validate_queue


def run(_: str, artifacts_dir: Path) -> ScenarioResult:
    result = ScenarioResult(
        "backup-restore",
        expected_outcome="public online backup restores its verified point independently",
        required_invariants=frozenset(
            {
                "backup_point_counts",
                "payload_restored",
                "integrity_ok",
                "no_external_wal_required",
                "public_backup_verified",
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
        source.put({"point": "backup"}, job_id="backup-point")
        result.counts_before = counts(source.stats())
        restored_dir = context.path / "restored"

        backup = source.backup(restored_dir)
        result.pragmas["backup_result"] = backup.to_dict()
        source.put({"point": "after-backup"})
        source.close()

        restored_db = restored_dir / "localqueue.db"
        no_restored_sidecars = all(
            not Path(f"{restored_db}{suffix}").exists() for suffix in ("-wal", "-shm")
        )
        shutil.rmtree(context.queue_dir)
        origin_removed = not context.queue_dir.exists()

        validation = validate_queue(
            context, queue_dir=restored_dir, consume=True, label="restore-validator"
        )
        result.fresh_process_validated = True
        result.counts_after_recovery = counts(validation["stats_before"])
        result.integrity_check = str(validation["integrity_check"])
        result.invariant(
            "backup_point_counts",
            result.counts_after_recovery == result.counts_before,
            "restored SimpleQueue stats match the public backup point",
        )
        result.invariant(
            "payload_restored",
            validation["payload"] == {"point": "backup"},
            "fresh SimpleQueue process consumed the backup-point payload",
        )
        result.invariant(
            "integrity_ok",
            result.integrity_check == "ok"
            and validation["integrity_result"]["mode"] == "full",
            result.integrity_check,
        )
        result.invariant(
            "no_external_wal_required",
            no_restored_sidecars and origin_removed,
            "origin directory was removed before opening a standalone backup",
        )
        result.invariant(
            "public_backup_verified",
            backup.verified
            and backup.verification_mode == "full"
            and backup.verification_messages == ("ok",),
            "BackupResult records successful full verification and its messages",
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
