from __future__ import annotations

import json
import sqlite3
from dataclasses import FrozenInstanceError, fields
from pathlib import Path

import pytest
from localqueue import IntegrityCheckResult, LocalQueueError, SimpleQueue


def test_full_integrity_check_returns_complete_typed_json_result(
    tmp_path: Path,
) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    queue.put({"task": "verify"})

    result = queue.check_integrity()

    assert isinstance(result, IntegrityCheckResult)
    assert result.schema_version == 1
    assert result.mode == "full"
    assert result.max_errors == 100
    assert result.ok is True
    assert result.messages == ("ok",)
    assert result.elapsed_seconds >= 0

    serialized = result.to_dict()
    assert set(serialized) == {field.name for field in fields(IntegrityCheckResult)}
    for field in fields(IntegrityCheckResult):
        expected = (
            list(getattr(result, field.name))
            if field.name == "messages"
            else getattr(result, field.name)
        )
        assert serialized[field.name] == expected
    json.dumps(serialized)
    with pytest.raises(FrozenInstanceError):
        result.ok = False  # type: ignore[misc]
    queue.close()


def test_quick_integrity_check_reports_arguments(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))

    result = queue.check_integrity(mode="quick", max_errors=7)

    assert result.ok is True
    assert result.messages == ("ok",)
    assert result.mode == "quick"
    assert result.max_errors == 7
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


@pytest.mark.parametrize("max_errors", [0, 1001, -1])
def test_integrity_check_rejects_max_errors_outside_public_interval(
    tmp_path: Path, max_errors: int
) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))

    with pytest.raises(ValueError, match="between 1 and 1000"):
        queue.check_integrity(max_errors=max_errors)

    queue.close()


def test_integrity_check_rejects_non_integer_max_errors(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))

    with pytest.raises(TypeError, match="integer"):
        queue.check_integrity(max_errors=True)

    queue.close()


def test_integrity_check_rejects_unknown_mode(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))

    with pytest.raises(ValueError, match="mode"):
        queue.check_integrity(mode="partial")  # type: ignore[arg-type]

    queue.close()


def test_integrity_arguments_are_keyword_only(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))

    with pytest.raises(TypeError):
        queue.check_integrity("quick")  # type: ignore[misc]

    queue.close()


def test_integrity_check_rejects_closed_queue(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "queue"))
    queue.close()

    with pytest.raises(LocalQueueError, match="^queue is closed$"):
        queue.check_integrity()
