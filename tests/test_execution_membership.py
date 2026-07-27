from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from localqueue import LeaseExpired, SimpleQueue


def native_queue(path: Path, name: str = "parent"):
    queue = SimpleQueue(str(path), name=name)
    return queue, queue._get_native()


def entries(*items: tuple[str, bytes, str | None, str | None, str | None]):
    return list(items)


def create_execution(native, execution_id: str) -> None:
    native._execution_create(execution_id, "bus", "source", "checkpoint")


def test_execution_creation_inspection_and_duplicate_is_not_an_upsert(
    tmp_path: Path,
) -> None:
    queue, native = native_queue(tmp_path / "queue")
    try:
        create_execution(native, "run-1")
        assert native._execution_inspect("run-1")[1:5] == (
            "bus",
            "source",
            "checkpoint",
            False,
        )
        assert native._execution_mark_source_completed("run-1") is True
        assert native._execution_mark_source_completed("run-1") is False
        with pytest.raises(Exception):
            native._execution_create("run-1", "other", "other", None)
        assert native._execution_inspect("run-1")[1:5] == (
            "bus",
            "source",
            "checkpoint",
            True,
        )
    finally:
        queue.close()


def test_root_membership_includes_new_and_deduplicated_deliveries(
    tmp_path: Path,
) -> None:
    queue, native = native_queue(tmp_path / "queue")
    try:
        create_execution(native, "one")
        create_execution(native, "two")
        batch = entries(("parent", b"payload", "job", "key", "fingerprint"))
        assert native._enqueue_batch_with_identity_and_checkpoint_and_execution(
            batch, None, None, "one"
        )[0] == [(1, True)]
        assert native._enqueue_batch_with_identity_and_checkpoint_and_execution(
            batch, None, None, "two"
        )[0] == [(1, False)]
        # The composite primary key makes re-adding membership idempotent.
        native._enqueue_batch_with_identity_and_checkpoint_and_execution(
            batch, None, None, "two"
        )
        assert native._execution_delivery_states("one") == (1, 1, 0, 0, 0)
        assert native._execution_delivery_states("two") == (1, 1, 0, 0, 0)
    finally:
        queue.close()


def test_checkpoint_conflict_rolls_back_root_membership(tmp_path: Path) -> None:
    queue, native = native_queue(tmp_path / "queue")
    try:
        create_execution(native, "run")
        checkpoint = ("bus", "checkpoint", "wrong", 9, "cursor", None, 1)
        with pytest.raises(Exception):
            native._enqueue_batch_with_identity_and_checkpoint_and_execution(
                entries(("parent", b"payload", None, None, None)),
                None,
                checkpoint,
                "run",
            )
        assert native._execution_delivery_states("run") == (0, 0, 0, 0, 0)
    finally:
        queue.close()


def test_ack_fanout_propagates_all_parent_memberships_and_state(tmp_path: Path) -> None:
    queue, native = native_queue(tmp_path / "queue")
    try:
        for execution_id in ("one", "two"):
            create_execution(native, execution_id)
        root = entries(("parent", b"root", "root", "root", "root-fingerprint"))
        for execution_id in ("one", "two"):
            native._enqueue_batch_with_identity_and_checkpoint_and_execution(
                root, None, None, execution_id
            )
        lease = native.get(60_000)
        assert lease is not None
        child = [("child", "child", "child", "child-fingerprint")]
        assert (
            native._ack_and_fanout_with_identity(
                lease.id, lease.receipt, b"child", child
            )[0][1]
            is True
        )
        for execution_id in ("one", "two"):
            assert native._execution_delivery_states(execution_id) == (2, 1, 0, 1, 0)
    finally:
        queue.close()


def test_ack_fanout_deduplicated_child_and_lease_error_are_atomic(
    tmp_path: Path,
) -> None:
    queue, native = native_queue(tmp_path / "queue")
    try:
        create_execution(native, "run")
        # Pre-existing child has no membership until parent propagation joins it.
        native._enqueue_batch_with_identity(
            entries(("child", b"child", "child", "key", "fp")), None
        )
        native._enqueue_batch_with_identity_and_checkpoint_and_execution(
            entries(("parent", b"root", None, None, None)), None, None, "run"
        )
        lease = native.get(60_000)
        assert lease is not None
        child = [("child", "child", "key", "fp")]
        assert native._ack_and_fanout_with_identity(
            lease.id, lease.receipt, b"child", child
        ) == [(1, False)]
        assert native._execution_delivery_states("run") == (2, 1, 0, 1, 0)
        with pytest.raises(LeaseExpired):
            native._ack_and_fanout_with_identity(lease.id, lease.receipt, b"x", child)
        assert native._execution_delivery_states("run") == (2, 1, 0, 1, 0)
    finally:
        queue.close()


def test_membership_survives_retry_and_failure_and_foreign_keys_are_clean(
    tmp_path: Path,
) -> None:
    queue, native = native_queue(tmp_path / "queue")
    try:
        create_execution(native, "run")
        native._enqueue_batch_with_identity_and_checkpoint_and_execution(
            entries(("parent", b"root", None, None, None)), None, None, "run"
        )
        lease = native.get(60_000)
        assert lease is not None
        native.nack(lease.id, lease.receipt)
        assert native._execution_delivery_states("run") == (1, 1, 0, 0, 0)
        lease = native.get(60_000)
        assert lease is not None
        native.fail(lease.id, lease.receipt, "permanent")
        assert native._execution_delivery_states("run") == (1, 0, 0, 0, 1)
        with sqlite3.connect(tmp_path / "queue" / "localqueue.db") as connection:
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        queue.close()


def test_deduplication_conflict_rolls_back_ack_fanout_and_membership(
    tmp_path: Path,
) -> None:
    queue, native = native_queue(tmp_path / "queue")
    try:
        create_execution(native, "run")
        native._enqueue_batch_with_identity(
            entries(("child", b"child", "child", "key", "fingerprint")), None
        )
        native._enqueue_batch_with_identity_and_checkpoint_and_execution(
            entries(("parent", b"root", None, None, None)), None, None, "run"
        )
        lease = native.get(60_000)
        assert lease is not None
        with pytest.raises(Exception):
            native._ack_and_fanout_with_identity(
                lease.id,
                lease.receipt,
                b"conflicting",
                [("child", "child", "different-key", "different-fingerprint")],
            )
        # Parent remains leased and the pre-existing child never joins the execution.
        assert native._execution_delivery_states("run") == (1, 0, 1, 0, 0)
    finally:
        queue.close()


def test_backup_preserves_execution_records_and_memberships(tmp_path: Path) -> None:
    queue, native = native_queue(tmp_path / "queue")
    try:
        create_execution(native, "run")
        native._enqueue_batch_with_identity_and_checkpoint_and_execution(
            entries(("parent", b"root", None, None, None)), None, None, "run"
        )
        destination = tmp_path / "backup"
        queue.backup(destination)
        with sqlite3.connect(destination / "localqueue.db") as connection:
            assert connection.execute(
                "SELECT execution_id, source_completed FROM event_bus_executions"
            ).fetchall() == [("run", 0)]
            assert connection.execute(
                "SELECT execution_id, message_id FROM event_bus_execution_deliveries"
            ).fetchall() == [("run", 1)]
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        queue.close()
