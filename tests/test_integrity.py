from __future__ import annotations

import json
import sqlite3
from dataclasses import FrozenInstanceError, fields
from pathlib import Path

import pytest
from localqueue import IntegrityCheckResult, LocalQueueError, SimpleQueue


def test_full_integrity_check_returns_typed_json_result(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    queue.put({"task": "verify"})

    result = queue.check_integrity()

    assert isinstance(result, IntegrityCheckResult)
    assert result.ok is True
    assert result.messages == ("ok",)
    assert result.check_mode == "full"
    assert result.elapsed_seconds >= 0
    serialized = result.to_dict()
    assert set(serialized) == {field.name for field in fields(IntegrityCheckResult)}
    assert serialized["messages"] == ["ok"]
    json.dumps(serialized)
    with pytest.raises(FrozenInstanceError):
        result.ok = False  # type: ignore[misc]
    queue.close()


def test_quick_integrity_check_reports_its_mode(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))

    result = queue.check_integrity(mode="quick")

    assert result.ok is True
    assert result.messages == ("ok",)
    assert result.check_mode == "quick"
    queue.close()


def test_integrity_check_preserves_sqlite_failure_messages(tmp_path: Path) -> None:
    queue_directory = tmp_path / "queue"
    queue = SimpleQueue(str(queue_directory))
    queue.close()

    database = queue_directory / "localqueue.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE invalid_data(value INTEGER CHECK(value > 0))")
        connection.execute("PRAGMA ignore_check_constraints=ON")
        connection.execute("INSERT INTO invalid_data VALUES (-1)")

    reopened = SimpleQueue(str(queue_directory))
    result = reopened.check_integrity()

    assert result.ok is False
    assert any("CHECK constraint failed" in message for message in result.messages)
    reopened.close()


def test_integrity_check_rejects_unknown_mode(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))

    with pytest.raises(ValueError, match="mode"):
        queue.check_integrity(mode="partial")  # type: ignore[arg-type]

    queue.close()


def test_integrity_check_rejects_closed_queue(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    queue.close()

    with pytest.raises(LocalQueueError, match="^queue is closed$"):
        queue.check_integrity()
