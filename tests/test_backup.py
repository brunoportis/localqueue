from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import FrozenInstanceError, fields
from pathlib import Path

import pytest
from localqueue import (
    BackupResult,
    DeliveryPolicy,
    DurabilityMode,
    Empty,
    LocalQueueError,
    SimpleQueue,
)


def inspect_backup(directory: Path) -> tuple[str, int]:
    database = directory / "localqueue.db"
    with sqlite3.connect(
        f"file:{database}?mode=ro&immutable=1", uri=True
    ) as connection:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        count = connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    return integrity, count


@pytest.mark.parametrize("durability", [DurabilityMode.RELAXED, DurabilityMode.DURABLE])
def test_backup_creates_exclusive_typed_snapshot(
    tmp_path: Path, durability: DurabilityMode
) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"), durability=durability)
    queue.put({"task": "one"})
    queue.put({"task": "two"})
    parent = tmp_path / "exports with spaces-å"
    parent.mkdir()
    destination = parent / "snapshot 東京"

    result = queue.backup(destination)

    database = destination / "localqueue.db"
    assert isinstance(result, BackupResult)
    assert result.schema_version == 1
    assert result.destination == str(destination)
    assert result.database_path == str(database)
    assert result.elapsed_seconds >= 0
    assert result.pages_copied == result.page_count
    assert result.page_count > 0
    assert result.database_size_bytes == database.stat().st_size
    assert result.verified is True
    assert result.verification_mode == "full"
    assert result.verification_messages == ("ok",)
    assert inspect_backup(destination) == ("ok", 2)
    assert sorted(path.name for path in destination.iterdir()) == ["localqueue.db"]

    serialized = result.to_dict()
    assert set(serialized) == {field.name for field in fields(BackupResult)}
    tuple_fields = {"verification_messages"}
    for field in fields(BackupResult):
        value = getattr(result, field.name)
        expected = list(value) if field.name in tuple_fields else value
        assert serialized[field.name] == expected
    json.dumps(serialized)
    with pytest.raises(FrozenInstanceError):
        result.verified = False  # type: ignore[misc]
    queue.close()


def test_backup_opens_with_simplequeue_from_a_fresh_process(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    queue.put({"task": "fresh process"})
    destination = tmp_path / "snapshot"
    queue.backup(destination)
    queue.close()

    script = """
import json, sys
from localqueue import DeliveryPolicy, SimpleQueue
queue = SimpleQueue(sys.argv[1])
integrity = queue.check_integrity(mode="full")
job = queue.get(block=False)
print(json.dumps({"integrity": integrity.to_dict(), "payload": job.data}))
queue.close()
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(destination)],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )

    observed = json.loads(completed.stdout)
    assert observed["integrity"]["ok"] is True
    assert observed["integrity"]["mode"] == "full"
    assert observed["payload"] == {"task": "fresh process"}


def test_backup_preserves_complete_queue_state_without_origin_sidecars(
    tmp_path: Path,
) -> None:
    source_directory = tmp_path / "source"
    alpha = SimpleQueue(
        str(source_directory), name="alpha", delivery=DeliveryPolicy(lease_seconds=300)
    )
    beta = SimpleQueue(str(source_directory), name="beta")

    processing_id = alpha.put({"state": "processing"})
    processing = alpha.get(block=False)
    assert processing.id == processing_id
    ready_id = alpha.put({"state": "ready"}, job_id="dedupe-ready")
    assert alpha.put({"ignored": True}, job_id="dedupe-ready") == ready_id

    acked_id = beta.put({"state": "acked"})
    acked = beta.get(block=False)
    assert acked.id == acked_id
    beta.ack(acked)
    failed_id = beta.put({"state": "failed"})
    failed = beta.get(block=False)
    assert failed.id == failed_id
    beta.fail(failed, "controlled failure")

    destination = tmp_path / "snapshot"
    result = alpha.backup(destination)
    database = destination / "localqueue.db"

    with sqlite3.connect(
        f"file:{database}?mode=ro&immutable=1", uri=True
    ) as connection:
        connection.row_factory = sqlite3.Row
        rows = {
            row["id"]: row
            for row in connection.execute(
                "SELECT id, queue, payload, status, attempts, lease_until, receipt, "
                "last_error, failure_reason, job_id FROM messages"
            )
        }
        indexes = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            )
        }

    assert json.loads(rows[processing_id]["payload"]) == {"state": "processing"}
    assert rows[processing_id]["queue"] == "alpha"
    assert rows[processing_id]["status"] == 1
    assert rows[processing_id]["attempts"] == 1
    assert rows[processing_id]["lease_until"] is not None
    assert rows[processing_id]["receipt"] == processing.receipt
    assert json.loads(rows[ready_id]["payload"]) == {"state": "ready"}
    assert rows[ready_id]["status"] == 0
    assert rows[ready_id]["attempts"] == 0
    assert rows[ready_id]["job_id"] == "dedupe-ready"
    assert rows[acked_id]["status"] == 2
    assert rows[acked_id]["attempts"] == 1
    assert rows[acked_id]["receipt"] is None
    assert json.loads(rows[acked_id]["payload"]) == {"state": "acked"}
    assert rows[failed_id]["status"] == 3
    assert rows[failed_id]["attempts"] == 1
    assert rows[failed_id]["last_error"] == "controlled failure"
    assert rows[failed_id]["failure_reason"] == "explicit_permanent_failure"
    assert json.loads(rows[failed_id]["payload"]) == {"state": "failed"}
    assert {"idx_messages_job_id", "idx_messages_queue_status"} <= indexes
    assert result.verification_messages == ("ok",)

    alpha.close()
    beta.close()
    shutil.rmtree(source_directory)
    assert not source_directory.exists()

    script = """
import json, sys
from localqueue import DeliveryPolicy, SimpleQueue
alpha = SimpleQueue(sys.argv[1], name="alpha")
beta = SimpleQueue(sys.argv[1], name="beta")
integrity = alpha.check_integrity(mode="full")
deduplicated_id = alpha.put({"must": "not replace"}, job_id="dedupe-ready")
ready = alpha.get(block=False)
print(json.dumps({
    "integrity": integrity.to_dict(),
    "alpha": alpha.stats(),
    "beta": beta.stats(),
    "deduplicated_id": deduplicated_id,
    "ready_payload": ready.data,
}))
alpha.close()
beta.close()
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(destination)],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )
    observed = json.loads(completed.stdout)
    assert observed["integrity"]["ok"] is True
    assert observed["integrity"]["mode"] == "full"
    assert observed["alpha"] == {
        "ready": 0,
        "processing": 2,
        "acked": 0,
        "failed": 0,
    }
    assert observed["beta"] == {
        "ready": 0,
        "processing": 0,
        "acked": 1,
        "failed": 1,
    }
    assert observed["deduplicated_id"] == ready_id
    assert observed["ready_payload"] == {"state": "ready"}


@pytest.mark.parametrize("existing_kind", ["directory", "file"])
def test_backup_never_changes_an_existing_destination(
    tmp_path: Path, existing_kind: str
) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    queue.put({"task": "replacement"})
    destination = tmp_path / "snapshot"
    if existing_kind == "directory":
        destination.mkdir()
        marker = destination / "keep.txt"
        marker.write_bytes(b"keep existing bytes")
    else:
        destination.write_bytes(b"keep existing bytes")

    with pytest.raises(FileExistsError):
        queue.backup(destination)

    if existing_kind == "directory":
        assert marker.read_bytes() == b"keep existing bytes"
        assert not (destination / "localqueue.db").exists()
    else:
        assert destination.read_bytes() == b"keep existing bytes"
    queue.close()


def test_two_backups_racing_for_one_directory_have_exactly_one_winner(
    tmp_path: Path,
) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    queue.put({"race": True})
    destination = tmp_path / "snapshot"
    barrier = threading.Barrier(3)
    results: list[BackupResult] = []
    errors: list[BaseException] = []

    def backup() -> None:
        barrier.wait()
        try:
            results.append(queue.backup(destination))
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=backup) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=2)
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert len(results) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], FileExistsError)
    assert inspect_backup(destination) == ("ok", 1)
    assert sorted(path.name for path in destination.iterdir()) == ["localqueue.db"]
    queue.close()


@pytest.mark.parametrize(
    "destination_kind", ["missing-parent", "source", "inside-source"]
)
def test_backup_rejects_invalid_destinations(
    tmp_path: Path, destination_kind: str
) -> None:
    queue_directory = tmp_path / "queue"
    queue = SimpleQueue(str(queue_directory))
    if destination_kind == "missing-parent":
        destination = tmp_path / "missing" / "snapshot"
    elif destination_kind == "source":
        destination = queue_directory
    else:
        destination = queue_directory / "nested-backup"

    with pytest.raises(ValueError):
        queue.backup(destination)

    queue.put({"source": "unchanged"})
    assert queue.stats()["ready"] == 1
    queue.close()


def test_relative_unicode_destination_preserves_public_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = tmp_path / "espaço 東京"
    workspace.mkdir()
    monkeypatch.chdir(workspace)
    queue = SimpleQueue("origem relativa")
    Path("backups").mkdir()
    destination = Path("backups") / "snapshot diário"

    result = queue.backup(destination)

    assert result.destination == os.fspath(destination)
    assert result.database_path == os.fspath(destination / "localqueue.db")
    assert inspect_backup(destination) == ("ok", 0)
    queue.close()


def test_backup_rejects_closed_queue(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    queue.close()

    with pytest.raises(LocalQueueError, match="^queue is closed$"):
        queue.backup(tmp_path / "snapshot")


def test_concurrent_snapshot_never_contains_a_partial_put_many_batch(
    tmp_path: Path,
) -> None:
    queue_directory = tmp_path / "queue"
    queue = SimpleQueue(str(queue_directory))
    batch_size = 8
    queue.put_many(
        [{"batch_id": "seed", "position": index} for index in range(batch_size)]
    )
    producer_started = threading.Event()
    stop = threading.Event()
    errors: list[BaseException] = []

    def produce() -> None:
        worker = SimpleQueue(str(queue_directory))
        try:
            for batch in range(300):
                if stop.is_set():
                    break
                worker.put_many(
                    [
                        {"batch_id": f"batch-{batch}", "position": position}
                        for position in range(batch_size)
                    ]
                )
                producer_started.set()
        except BaseException as error:
            errors.append(error)
        finally:
            worker.close()

    def consume() -> None:
        worker = SimpleQueue(str(queue_directory))
        try:
            while not stop.is_set():
                try:
                    worker.ack(worker.get(block=False))
                except Empty:
                    time.sleep(0.001)
        except BaseException as error:
            errors.append(error)
        finally:
            worker.close()

    threads = [threading.Thread(target=produce), threading.Thread(target=consume)]
    for thread in threads:
        thread.start()
    destination = tmp_path / "snapshot"
    try:
        assert producer_started.wait(timeout=5)
        queue.backup(destination)
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=10)
        queue.close()

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    with sqlite3.connect(
        f"file:{destination / 'localqueue.db'}?mode=ro&immutable=1", uri=True
    ) as connection:
        payloads = [
            json.loads(payload)
            for (payload,) in connection.execute("SELECT payload FROM messages")
        ]
    cardinalities: dict[str, int] = {}
    for payload in payloads:
        batch_id = str(payload["batch_id"])
        cardinalities[batch_id] = cardinalities.get(batch_id, 0) + 1
    assert cardinalities["seed"] == batch_size
    assert set(cardinalities.values()) == {batch_size}
    assert inspect_backup(destination)[0] == "ok"


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission semantics")
def test_backup_reports_destination_permission_error(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    destination_parent = tmp_path / "read-only"
    destination_parent.mkdir()
    destination_parent.chmod(0o500)
    destination = destination_parent / "snapshot"
    try:
        if os.access(destination_parent, os.W_OK):
            pytest.skip("current user can bypass directory permissions")
        with pytest.raises(LocalQueueError):
            queue.backup(destination)
        assert not destination.exists()
    finally:
        destination_parent.chmod(0o700)
        queue.close()


def test_disk_full_cleans_staged_directory_and_allows_retry(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    native = queue._get_native()
    hook = getattr(native, "_test_set_backup_max_page_count", None)
    if hook is None:
        pytest.skip("requires the explicit __crash_test extension build")
    queue.put({"blob": "x" * 200_000})
    failed_destination = tmp_path / "disk-full"
    hook(1)

    with pytest.raises(LocalQueueError, match="database or disk is full"):
        queue.backup(failed_destination)

    assert queue.stats()["ready"] == 1
    assert not failed_destination.exists()
    retry = queue.backup(tmp_path / "retry")
    assert retry.verified is True
    assert inspect_backup(tmp_path / "retry") == ("ok", 1)
    queue.close()
