from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import threading
from dataclasses import FrozenInstanceError, fields
from pathlib import Path

import pytest
from localqueue import BackupResult, LocalQueueError, SimpleQueue


def inspect_backup(path: Path) -> tuple[str, int]:
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        count = connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    return integrity, count


def test_backup_creates_independently_openable_typed_snapshot(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    queue.put({"task": "one"})
    queue.put({"task": "two"})
    destination = tmp_path / "exports with spaces-å" / "snapshot.db"
    destination.parent.mkdir()

    result = queue.backup(destination)

    assert isinstance(result, BackupResult)
    assert result.destination == str(destination)
    assert result.overwritten is False
    assert result.elapsed_seconds >= 0
    assert result.database_size_bytes == destination.stat().st_size
    assert inspect_backup(destination) == ("ok", 2)
    serialized = result.to_dict()
    assert set(serialized) == {field.name for field in fields(BackupResult)}
    assert all(
        serialized[field.name] == getattr(result, field.name)
        for field in fields(BackupResult)
    )
    json.dumps(serialized)
    with pytest.raises(FrozenInstanceError):
        result.overwritten = True  # type: ignore[misc]
    queue.close()


def test_backup_opens_from_a_fresh_process(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    queue.put({"task": "fresh process"})
    destination = tmp_path / "snapshot.db"
    queue.backup(destination)

    script = (
        "import sqlite3, sys; "
        "c=sqlite3.connect(f'file:{sys.argv[1]}?mode=ro', uri=True); "
        "print(c.execute('PRAGMA integrity_check').fetchone()[0]); "
        "print(c.execute('SELECT COUNT(*) FROM messages').fetchone()[0])"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script, str(destination)],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )

    assert completed.stdout.splitlines() == ["ok", "1"]
    queue.close()


def test_backup_collision_is_explicit_and_overwrite_is_opt_in(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    queue.put({"task": "replacement"})
    destination = tmp_path / "snapshot.db"
    destination.write_bytes(b"keep existing bytes")

    with pytest.raises(FileExistsError):
        queue.backup(destination)
    assert destination.read_bytes() == b"keep existing bytes"

    result = queue.backup(destination, overwrite=True)

    assert result.overwritten is True
    assert inspect_backup(destination) == ("ok", 1)
    queue.close()


@pytest.mark.parametrize(
    "destination_kind", ["missing-parent", "directory", "source", "source-alias"]
)
def test_backup_rejects_invalid_destinations(
    tmp_path: Path, destination_kind: str
) -> None:
    queue_directory = tmp_path / "queue"
    queue = SimpleQueue(str(queue_directory))
    if destination_kind == "missing-parent":
        destination = tmp_path / "missing" / "snapshot.db"
    elif destination_kind == "directory":
        destination = tmp_path / "destination"
        destination.mkdir()
    elif destination_kind == "source":
        destination = queue_directory / "localqueue.db"
    else:
        destination = queue_directory / ".." / "queue" / "localqueue.db"

    with pytest.raises(ValueError):
        queue.backup(destination, overwrite=True)

    queue.put({"source": "unchanged"})
    assert queue.stats()["ready"] == 1
    queue.close()


def test_backup_rejects_closed_queue(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    queue.close()

    with pytest.raises(LocalQueueError, match="^queue is closed$"):
        queue.backup(tmp_path / "snapshot.db")


def test_backup_remains_valid_with_active_producer_and_consumer(tmp_path: Path) -> None:
    queue_directory = tmp_path / "queue"
    queue = SimpleQueue(str(queue_directory))
    queue.put({"seed": True})
    producer_started = threading.Event()
    stop_producer = threading.Event()

    def produce_and_consume() -> None:
        worker = SimpleQueue(str(queue_directory))
        producer_started.set()
        index = 0
        while not stop_producer.is_set():
            worker.put({"concurrent": index})
            job = worker.get(block=False)
            worker.ack(job)
            index += 1
        worker.close()

    thread = threading.Thread(target=produce_and_consume)
    thread.start()
    assert producer_started.wait(timeout=2)
    destination = tmp_path / "snapshot.db"
    try:
        queue.backup(destination)
    finally:
        stop_producer.set()
        thread.join(timeout=5)

    assert not thread.is_alive()
    integrity, count = inspect_backup(destination)
    assert integrity == "ok"
    assert count >= 1
    queue.close()


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission semantics")
def test_backup_reports_destination_permission_error(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    destination_directory = tmp_path / "read-only"
    destination_directory.mkdir()
    destination_directory.chmod(0o500)
    destination = destination_directory / "snapshot.db"
    try:
        if os.access(destination_directory, os.W_OK):
            pytest.skip("current user can bypass directory permissions")
        with pytest.raises(LocalQueueError):
            queue.backup(destination)
    finally:
        destination_directory.chmod(0o700)
        queue.close()
