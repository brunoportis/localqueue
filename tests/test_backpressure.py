from __future__ import annotations

import inspect
import json
import multiprocessing
import sqlite3
import threading
import time
from dataclasses import fields
from pathlib import Path
from typing import Any

import pytest
from localqueue import (
    DeliveryPolicy,
    EnqueueItem,
    Full,
    LocalQueueError,
    QueueDiagnostics,
    SimpleQueue,
)


def _pending(queue: SimpleQueue) -> int:
    stats = queue.stats()
    return stats["ready"] + stats["processing"]


def _expire_lease(path: Path, message_id: int) -> None:
    with sqlite3.connect(path / "localqueue.db") as connection:
        connection.execute(
            "UPDATE messages SET lease_until = ? WHERE id = ?",
            (int(time.time() * 1000) - 1_000, message_id),
        )


def _nonblocking_producer(
    path: str,
    name: str,
    limit: int,
    start: Any,
    results: Any,
) -> None:
    queue = SimpleQueue(path, name=name, max_pending_jobs=limit)
    try:
        start.wait()
        try:
            message_id = queue.put(
                {"pid": multiprocessing.current_process().pid}, block=False
            )
        except Full:
            results.put(("full", None))
        except BaseException as error:
            results.put(("error", repr(error)))
        else:
            results.put(("ok", message_id))
    finally:
        queue.close()


def _batch_producer(
    path: str,
    name: str,
    limit: int,
    batch_size: int,
    producer: int,
    start: Any,
    results: Any,
) -> None:
    queue = SimpleQueue(path, name=name, max_pending_jobs=limit)
    try:
        start.wait()
        try:
            ids = queue.put_many(
                [
                    {"producer": producer, "position": position}
                    for position in range(batch_size)
                ],
                block=False,
            )
        except Full:
            results.put(("full", []))
        except BaseException as error:
            results.put(("error", repr(error)))
        else:
            results.put(("ok", ids))
    finally:
        queue.close()


def _blocking_producer(
    path: str,
    limit: int,
    ready: Any,
    results: Any,
    timeout: float | None,
) -> None:
    queue = SimpleQueue(path, max_pending_jobs=limit)
    try:
        ready.set()
        started = time.monotonic()
        try:
            message_id = queue.put({"blocked": True}, timeout=timeout)
        except Full:
            results.put(("full", time.monotonic() - started))
        except BaseException as error:
            results.put(("error", repr(error)))
        else:
            results.put(("ok", message_id))
    finally:
        queue.close()


def _impossible_batch_producer(path: str, ready: Any, results: Any) -> None:
    queue = SimpleQueue(path, max_pending_jobs=2)
    try:
        ready.set()
        started = time.monotonic()
        try:
            queue.put_many([{"index": 0}, {"index": 1}, {"index": 2}])
        except Full as error:
            results.put(("full", str(error), time.monotonic() - started))
        except BaseException as error:
            results.put(("error", repr(error), time.monotonic() - started))
        else:
            results.put(("ok", "", time.monotonic() - started))
    finally:
        queue.close()


def _hold_writer_lock(path: str, ready: Any, release: Any) -> None:
    database = Path(path) / "localqueue.db"
    connection = sqlite3.connect(database, timeout=0)
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "INSERT INTO messages (queue, payload, status, attempts, max_attempts, "
            "available_at, created_at, updated_at) "
            "VALUES ('lock-holder', x'00', 2, 0, 1, 0, 0, 0)"
        )
        ready.set()
        release.wait(timeout=5)
    finally:
        connection.rollback()
        connection.close()


def test_default_is_unlimited_and_existing_put_signature_is_preserved(
    tmp_path: Path,
) -> None:
    queue = SimpleQueue(str(tmp_path))
    ids = [queue.put({"index": index}) for index in range(100)]
    positional = queue.put({"deduplicated": 1}, "positional-job-id")

    assert len(set(ids)) == 100
    assert queue.put({"deduplicated": 2}, "positional-job-id") == positional
    assert queue.diagnostics().max_pending_jobs is None
    queue.close()


@pytest.mark.parametrize("limit", [0, -1, -100])
def test_non_positive_limit_is_invalid(tmp_path: Path, limit: int) -> None:
    with pytest.raises(ValueError, match="max_pending_jobs.*positive"):
        SimpleQueue(str(tmp_path), max_pending_jobs=limit)


@pytest.mark.parametrize("limit", [True, False, 1.0, 2.0, "1"])
def test_non_integer_limit_is_invalid(tmp_path: Path, limit: object) -> None:
    with pytest.raises(TypeError, match="max_pending_jobs.*integer"):
        SimpleQueue(str(tmp_path), max_pending_jobs=limit)  # type: ignore[arg-type]


def test_positive_limit_and_nonblocking_put(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=2)
    queue.put({"index": 1})
    queue.put({"index": 2})

    with pytest.raises(Full, match="^queue is full$"):
        queue.put({"index": 3}, block=False)

    assert _pending(queue) == 2
    queue.close()


def test_nonblocking_put_many_is_atomic_at_capacity(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=3)
    seed_id = queue.put({"seed": True})
    before = queue.stats()

    with pytest.raises(Full, match="^queue is full$"):
        queue.put_many([{"index": 1}, {"index": 2}, {"index": 3}], block=False)

    assert queue.stats() == before
    accepted = queue.put_many([{"index": 1}, {"index": 2}], block=False)
    assert accepted[0] == seed_id + 1
    assert _pending(queue) == 3
    queue.close()


def test_empty_batch_stays_a_noop_when_full(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=1)
    queue.put({"seed": True})

    assert queue.put_many([], block=False) == []
    assert queue.put_many([], timeout=0) == []
    assert _pending(queue) == 1
    queue.close()


def test_existing_duplicate_is_allowed_when_full(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=1)
    existing = queue.put({"original": True}, job_id="same")

    assert queue.put({"ignored": True}, job_id="same", block=False) == existing
    assert _pending(queue) == 1
    queue.close()


def test_duplicate_inside_batch_consumes_one_slot(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=1)

    ids = queue.put_many(
        [
            EnqueueItem({"value": "first"}, job_id="same"),
            EnqueueItem({"value": "ignored"}, job_id="same"),
        ],
        block=False,
    )

    assert ids[0] == ids[1]
    assert _pending(queue) == 1
    queue.close()


def test_mixed_batch_counts_only_genuinely_new_rows(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=3)
    first = queue.put({"existing": 1}, job_id="existing-1")
    second = queue.put({"existing": 2}, job_id="existing-2")

    ids = queue.put_many(
        [
            EnqueueItem({"ignored": 1}, job_id="existing-1"),
            EnqueueItem({"ignored": 2}, job_id="existing-2"),
            EnqueueItem({"new": True}, job_id="new"),
        ],
        block=False,
    )

    assert ids[:2] == [first, second]
    assert len(set(ids)) == 3
    assert _pending(queue) == 3
    queue.close()


def test_capacity_is_independent_per_logical_queue(tmp_path: Path) -> None:
    alpha = SimpleQueue(str(tmp_path), name="alpha", max_pending_jobs=1)
    beta = SimpleQueue(str(tmp_path), name="beta", max_pending_jobs=1)
    alpha.put({"queue": "alpha"})
    beta.put({"queue": "beta"})

    with pytest.raises(Full):
        alpha.put({"extra": True}, block=False)
    with pytest.raises(Full):
        beta.put({"extra": True}, block=False)

    assert _pending(alpha) == _pending(beta) == 1
    alpha.close()
    beta.close()


def test_full_is_public_and_put_options_are_keyword_only() -> None:
    from localqueue import Full as PublicFull

    assert PublicFull is Full
    assert issubclass(Full, LocalQueueError)
    put = inspect.signature(SimpleQueue.put)
    put_many = inspect.signature(SimpleQueue.put_many)
    assert put.parameters["job_id"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert put.parameters["block"].kind is inspect.Parameter.KEYWORD_ONLY
    assert put.parameters["timeout"].kind is inspect.Parameter.KEYWORD_ONLY
    assert put_many.parameters["block"].kind is inspect.Parameter.KEYWORD_ONLY
    assert put_many.parameters["timeout"].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.parametrize("method", ["put", "put_many"])
def test_negative_timeout_is_invalid(tmp_path: Path, method: str) -> None:
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=1)
    argument = {"value": 1} if method == "put" else [{"value": 1}]

    with pytest.raises(ValueError, match="timeout.*non-negative"):
        getattr(queue, method)(argument, timeout=-0.001)

    queue.close()


@pytest.mark.parametrize("method", ["put", "put_many"])
def test_invalid_timeout_is_checked_before_serialization(
    tmp_path: Path, method: str
) -> None:
    class RecordingSerializer:
        def __init__(self) -> None:
            self.dumps_calls = 0

        def dumps(self, obj: object) -> bytes:
            self.dumps_calls += 1
            return json.dumps(obj).encode()

        def loads(self, data: bytes) -> object:
            return json.loads(data)

    serializer = RecordingSerializer()
    queue = SimpleQueue(str(tmp_path), serializer=serializer)
    argument = {"value": 1} if method == "put" else [{"value": 1}]

    with pytest.raises(ValueError, match="timeout.*non-negative"):
        getattr(queue, method)(argument, timeout=-0.001)

    assert serializer.dumps_calls == 0
    queue.close()


def test_timeout_zero_makes_one_immediate_attempt(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=1)
    queue.put({"seed": True})
    started = time.monotonic()

    with pytest.raises(Full, match="^queue is full$"):
        queue.put({"blocked": True}, timeout=0)

    assert time.monotonic() - started < 0.5
    queue.close()


def test_finite_timeout_uses_one_total_monotonic_deadline(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=1)
    queue.put({"seed": True})
    started = time.monotonic()

    with pytest.raises(Full, match="^queue is full$"):
        queue.put_many([{"blocked": True}], timeout=0.12)

    elapsed = time.monotonic() - started
    assert 0.08 <= elapsed < 0.75
    queue.close()


def test_waiting_put_serializes_only_once(tmp_path: Path) -> None:
    class CountingSerializer:
        def __init__(self) -> None:
            self.dumps_calls = 0

        def dumps(self, obj: object) -> bytes:
            self.dumps_calls += 1
            return json.dumps(obj).encode()

        def loads(self, data: bytes) -> object:
            return json.loads(data)

    serializer = CountingSerializer()
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=1, serializer=serializer)
    queue.put({"seed": True})

    with pytest.raises(Full):
        queue.put({"blocked": True}, timeout=0.05)

    assert serializer.dumps_calls == 2
    queue.close()


def test_waiting_put_many_serializes_each_item_only_once(tmp_path: Path) -> None:
    class CountingSerializer:
        def __init__(self) -> None:
            self.dumps_calls = 0

        def dumps(self, obj: object) -> bytes:
            self.dumps_calls += 1
            return json.dumps(obj).encode()

        def loads(self, data: bytes) -> object:
            return json.loads(data)

    serializer = CountingSerializer()
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=1, serializer=serializer)
    queue.put({"seed": True})

    with pytest.raises(Full):
        queue.put_many([{"blocked": 1}, {"blocked": 2}], timeout=0.05)

    assert serializer.dumps_calls == 3
    queue.close()


def test_short_timeout_caps_sqlite_busy_wait_without_turning_busy_into_full(
    tmp_path: Path,
) -> None:
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=1)
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    blocker = context.Process(
        target=_hold_writer_lock, args=(str(tmp_path), ready, release)
    )
    blocker.start()
    assert ready.wait(timeout=5)
    started = time.monotonic()
    try:
        with pytest.raises(LocalQueueError) as raised:
            queue.put({"blocked-by-writer": True}, timeout=0.08)
        elapsed = time.monotonic() - started
    finally:
        release.set()
        blocker.join(timeout=2)
        if blocker.is_alive():
            blocker.terminate()
            blocker.join(timeout=2)
    assert blocker.exitcode == 0
    assert not isinstance(raised.value, Full)
    assert "locked" in str(raised.value) or "busy" in str(raised.value)
    assert elapsed < 0.6
    assert queue.diagnostics().busy_timeout_ms == 5_000
    queue.close()


def test_indefinite_wait_is_released_by_ack(tmp_path: Path) -> None:
    queue = SimpleQueue(
        str(tmp_path), max_pending_jobs=1, delivery=DeliveryPolicy(lease_seconds=30)
    )
    queue.put({"seed": True})
    job = queue.get_nowait()
    result: list[int] = []
    errors: list[BaseException] = []

    def produce() -> None:
        try:
            result.append(queue.put({"after": "ack"}))
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=produce)
    thread.start()
    try:
        time.sleep(0.04)
        assert thread.is_alive()
        queue.ack(job)
        thread.join(timeout=2)
    finally:
        if thread.is_alive():
            queue.close()
            thread.join(timeout=2)

    assert not thread.is_alive()
    assert errors == []
    assert len(result) == 1
    assert _pending(queue) == 1
    queue.close()


def test_wait_is_released_by_fail(tmp_path: Path) -> None:
    queue = SimpleQueue(
        str(tmp_path), max_pending_jobs=1, delivery=DeliveryPolicy(lease_seconds=30)
    )
    queue.put({"seed": True})
    job = queue.get_nowait()
    completed = threading.Event()

    def produce() -> None:
        queue.put({"after": "fail"})
        completed.set()

    thread = threading.Thread(target=produce)
    thread.start()
    try:
        time.sleep(0.04)
        queue.fail(job)
        assert completed.wait(timeout=2)
        thread.join(timeout=2)
    finally:
        if thread.is_alive():
            queue.close()
            thread.join(timeout=2)

    assert _pending(queue) == 1
    queue.close()


def test_close_interrupts_waiting_producer(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=1)
    queue.put({"seed": True})
    errors: list[BaseException] = []

    def produce() -> None:
        try:
            queue.put({"blocked": True})
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=produce)
    thread.start()
    time.sleep(0.04)
    started = time.monotonic()
    queue.close()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert time.monotonic() - started < 0.75
    assert len(errors) == 1
    assert isinstance(errors[0], LocalQueueError)
    assert str(errors[0]) == "queue is closed"


def test_ack_and_fail_release_slots_but_nack_to_ready_does_not(tmp_path: Path) -> None:
    queue = SimpleQueue(
        str(tmp_path),
        max_pending_jobs=1,
        delivery=DeliveryPolicy(lease_seconds=30, max_retries=1),
    )
    queue.put({"seed": True})
    job = queue.get_nowait()
    queue.nack(job)
    with pytest.raises(Full):
        queue.put({"blocked": "nack"}, block=False)

    job = queue.get_nowait()
    queue.ack(job)
    queue.put({"after": "ack"}, block=False)
    job = queue.get_nowait()
    queue.fail(job)
    queue.put({"after": "fail"}, block=False)
    assert _pending(queue) == 1
    queue.close()


def test_exhausted_nack_releases_a_slot(tmp_path: Path) -> None:
    queue = SimpleQueue(
        str(tmp_path),
        max_pending_jobs=1,
        delivery=DeliveryPolicy(lease_seconds=30, max_retries=0),
    )
    queue.put({"seed": True})
    queue.nack(queue.get_nowait())

    assert queue.stats()["failed"] == 1
    queue.put({"after": "failed"}, block=False)
    assert _pending(queue) == 1
    queue.close()


def test_expired_lease_counts_and_reclaim_to_ready_preserves_usage(
    tmp_path: Path,
) -> None:
    queue = SimpleQueue(
        str(tmp_path),
        max_pending_jobs=1,
        delivery=DeliveryPolicy(lease_seconds=30, max_retries=1),
    )
    queue.put({"seed": True})
    job = queue.get_nowait()
    _expire_lease(tmp_path, job.id)

    with pytest.raises(Full):
        queue.put({"blocked": "expired"}, block=False)
    assert queue.reclaim_expired_leases() == 1
    with pytest.raises(Full):
        queue.put({"blocked": "reclaimed"}, block=False)

    assert _pending(queue) == 1
    queue.close()


def test_reclaim_to_failed_releases_usage(tmp_path: Path) -> None:
    queue = SimpleQueue(
        str(tmp_path),
        max_pending_jobs=1,
        delivery=DeliveryPolicy(lease_seconds=30, max_retries=0),
    )
    queue.put({"seed": True})
    job = queue.get_nowait()
    _expire_lease(tmp_path, job.id)

    assert queue.reclaim_expired_leases() == 1
    assert queue.stats()["failed"] == 1
    queue.put({"after": "failed"}, block=False)
    assert _pending(queue) == 1
    queue.close()


def test_delayed_job_counts_toward_capacity(tmp_path: Path) -> None:
    queue = SimpleQueue(
        str(tmp_path), max_pending_jobs=1, delivery=DeliveryPolicy(lease_seconds=30)
    )
    queue.put({"seed": True})
    queue.nack(queue.get_nowait(), delay=60)

    with pytest.raises(Full):
        queue.put({"blocked": True}, block=False)

    assert _pending(queue) == 1
    queue.close()


def test_retry_failed_consumes_capacity_and_preserves_not_found(tmp_path: Path) -> None:
    queue = SimpleQueue(
        str(tmp_path / "with-slot"),
        max_pending_jobs=1,
        delivery=DeliveryPolicy(lease_seconds=30),
    )
    failed_id = queue.put({"failed": True})
    queue.fail(queue.get_nowait())
    queue.retry_failed(failed_id)
    assert _pending(queue) == 1
    queue.close()

    unlimited = SimpleQueue(
        str(tmp_path / "full"), delivery=DeliveryPolicy(lease_seconds=30)
    )
    other = unlimited.put({"terminal": True})
    unlimited.fail(unlimited.get_nowait())
    unlimited.put({"pending": True})
    unlimited.close()
    full = SimpleQueue(str(tmp_path / "full"), max_pending_jobs=1)
    with pytest.raises(Full):
        full.retry_failed(other)
    with pytest.raises(LocalQueueError, match="^job not found$"):
        full.retry_failed(999_999)
    full.close()


def test_purge_terminal_rows_does_not_change_pending(tmp_path: Path) -> None:
    queue = SimpleQueue(
        str(tmp_path), max_pending_jobs=2, delivery=DeliveryPolicy(lease_seconds=30)
    )
    queue.put({"pending": True})
    queue.put({"acked": True})
    queue.ack(queue.get_nowait())
    before = _pending(queue)

    queue.purge(0)

    assert _pending(queue) == before == 1
    queue.close()


def test_existing_queue_can_open_above_new_limit(tmp_path: Path) -> None:
    unlimited = SimpleQueue(str(tmp_path))
    unlimited.put_many([{"index": index} for index in range(3)])
    unlimited.close()
    limited = SimpleQueue(str(tmp_path), max_pending_jobs=2)

    report = limited.diagnostics()
    assert report.pending_jobs == 3
    assert report.available_slots == 0
    with pytest.raises(Full):
        limited.put({"blocked": True}, block=False)
    limited.ack(limited.get_nowait())
    limited.ack(limited.get_nowait())
    limited.put({"allowed": True}, block=False)
    assert _pending(limited) == 2
    limited.close()


def test_duplicates_remain_available_when_queue_is_above_limit(tmp_path: Path) -> None:
    unlimited = SimpleQueue(str(tmp_path))
    existing_ids = [
        unlimited.put({"index": index}, job_id=f"existing-{index}")
        for index in range(3)
    ]
    unlimited.close()
    limited = SimpleQueue(str(tmp_path), max_pending_jobs=2)

    assert (
        limited.put({"ignored": True}, job_id="existing-0", block=False)
        == existing_ids[0]
    )
    assert limited.put_many(
        [
            EnqueueItem({"ignored": 1}, job_id="existing-1"),
            EnqueueItem({"ignored": 2}, job_id="existing-2"),
            EnqueueItem({"ignored": 3}, job_id="existing-1"),
        ],
        block=False,
    ) == [existing_ids[1], existing_ids[2], existing_ids[1]]
    assert limited.diagnostics().pending_jobs == 3
    with pytest.raises(Full, match="^queue is full$"):
        limited.put({"new": True}, job_id="new", block=False)
    assert limited.diagnostics().pending_jobs == 3
    limited.close()


def test_duplicates_in_all_states_never_consume_capacity(tmp_path: Path) -> None:
    queue = SimpleQueue(
        str(tmp_path), max_pending_jobs=4, delivery=DeliveryPolicy(lease_seconds=30)
    )
    ids: dict[str, int] = {}
    ids["processing"] = queue.put({"state": "processing"}, job_id="processing")
    processing = queue.get_nowait()
    ids["acked"] = queue.put({"state": "acked"}, job_id="acked")
    acked = queue.get_nowait()
    queue.ack(acked)
    ids["failed"] = queue.put({"state": "failed"}, job_id="failed")
    failed = queue.get_nowait()
    queue.fail(failed)
    ids["ready"] = queue.put({"state": "ready"}, job_id="ready")

    assert processing.id == ids["processing"]
    queue.close()
    queue = SimpleQueue(
        str(tmp_path), max_pending_jobs=1, delivery=DeliveryPolicy(lease_seconds=30)
    )

    before = queue.diagnostics().pending_jobs
    returned = queue.put_many(
        [EnqueueItem({"duplicate": state}, job_id=state) for state in ids],
        block=False,
    )

    assert returned == [ids[state] for state in ids]
    assert queue.diagnostics().pending_jobs == before == 2
    queue.close()


def test_impossible_batches_fail_immediately_without_writes(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=2)
    database = tmp_path / "localqueue.db"

    with sqlite3.connect(database) as connection:
        sequence_before = connection.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'messages'"
        ).fetchone()
    started = time.monotonic()
    with pytest.raises(Full, match="^queue is full$") as impossible:
        queue.put_many([{"index": 0}, {"index": 1}, {"index": 2}])
    assert time.monotonic() - started < 0.5
    assert type(impossible.value) is Full
    finite_started = time.monotonic()
    with pytest.raises(Full, match="^queue is full$"):
        queue.put_many(
            [
                EnqueueItem({"index": 0}, job_id="new-0"),
                EnqueueItem({"index": 1}, job_id="new-1"),
                EnqueueItem({"index": 2}, job_id="new-2"),
            ],
            timeout=5.0,
        )
    assert time.monotonic() - finite_started < 0.5
    with sqlite3.connect(database) as connection:
        sequence_after = connection.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'messages'"
        ).fetchone()

    assert queue.stats() == {"ready": 0, "processing": 0, "acked": 0, "failed": 0}
    assert sequence_after == sequence_before
    queue.close()


def test_oversized_raw_batch_can_fit_after_deduplication(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=2)
    existing_id = queue.put({"original": True}, job_id="existing")

    ids = queue.put_many(
        [
            EnqueueItem({"duplicate": 1}, job_id="existing"),
            EnqueueItem({"duplicate": 2}, job_id="existing"),
            EnqueueItem({"new": True}, job_id="new"),
        ]
    )
    duplicate_ids = queue.put_many(
        [EnqueueItem({"duplicate": index}, job_id="existing") for index in range(4)]
    )

    assert ids[0] == ids[1] == existing_id
    assert ids[2] != existing_id
    assert duplicate_ids == [existing_id] * 4
    assert queue.diagnostics().pending_jobs == 2
    queue.close()


def test_impossible_batch_does_not_leave_child_process_blocked(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    path = str(tmp_path / "impossible-process")
    ready = context.Event()
    results = context.Queue()
    process = context.Process(
        target=_impossible_batch_producer, args=(path, ready, results)
    )
    try:
        process.start()
        assert ready.wait(timeout=5)
        process.join(timeout=2)
        result = results.get(timeout=2)
    finally:
        if process.is_alive():
            process.terminate()
        process.join(timeout=2)

    assert process.exitcode == 0
    assert result[0:2] == ("full", "queue is full")
    assert result[2] < 0.9


@pytest.mark.parametrize("limit", [None, 3])
def test_diagnostics_schema_v2_matches_dataclass(
    tmp_path: Path, limit: int | None
) -> None:
    queue = SimpleQueue(str(tmp_path / str(limit)), max_pending_jobs=limit)
    queue.put({"ready": True})
    queue.put({"processing": True})
    queue.get_nowait()

    report = queue.diagnostics()

    assert isinstance(report, QueueDiagnostics)
    assert report.schema_version == 2
    assert report.max_pending_jobs == limit
    assert report.pending_jobs == 2
    assert report.available_slots == (None if limit is None else 1)
    serialized = report.to_dict()
    assert set(serialized) == {field.name for field in fields(QueueDiagnostics)}
    assert serialized["max_pending_jobs"] == limit
    assert serialized["pending_jobs"] == 2
    assert serialized["available_slots"] == (None if limit is None else 1)
    queue.close()


def test_many_processes_never_oversubscribe(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    path = str(tmp_path / "multiprocess")
    limit = 10
    producer_count = 24
    start = context.Event()
    results = context.Queue()
    processes = [
        context.Process(
            target=_nonblocking_producer,
            args=(path, "shared", limit, start, results),
        )
        for _ in range(producer_count)
    ]
    try:
        for process in processes:
            process.start()
        start.set()
        for process in processes:
            process.join(timeout=15)
        observed = [results.get(timeout=2) for _ in processes]
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=2)

    assert all(process.exitcode == 0 for process in processes)
    assert sum(status == "ok" for status, _ in observed) == limit
    assert sum(status == "full" for status, _ in observed) == producer_count - limit
    assert not [item for item in observed if item[0] == "error"]
    queue = SimpleQueue(path, name="shared", max_pending_jobs=limit)
    assert _pending(queue) == limit
    assert queue.check_integrity().ok is True
    queue.close()


def test_concurrent_batches_are_all_or_nothing(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    path = str(tmp_path / "batches")
    limit = 10
    batch_size = 3
    producer_count = 8
    start = context.Event()
    results = context.Queue()
    processes = [
        context.Process(
            target=_batch_producer,
            args=(path, "shared", limit, batch_size, index, start, results),
        )
        for index in range(producer_count)
    ]
    try:
        for process in processes:
            process.start()
        start.set()
        for process in processes:
            process.join(timeout=15)
        observed = [results.get(timeout=2) for _ in processes]
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=2)

    assert all(process.exitcode == 0 for process in processes)
    successes = [ids for status, ids in observed if status == "ok"]
    assert all(len(ids) == batch_size for ids in successes)
    assert len(successes) == limit // batch_size
    assert not [item for item in observed if item[0] == "error"]
    queue = SimpleQueue(path, name="shared", max_pending_jobs=limit)
    assert _pending(queue) == len(successes) * batch_size
    queue.close()


def test_process_waits_for_capacity_released_by_ack(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    path = str(tmp_path / "wait")
    queue = SimpleQueue(
        path, max_pending_jobs=1, delivery=DeliveryPolicy(lease_seconds=30)
    )
    queue.put({"seed": True})
    job = queue.get_nowait()
    ready = context.Event()
    results = context.Queue()
    process = context.Process(
        target=_blocking_producer, args=(path, 1, ready, results, 3.0)
    )
    try:
        process.start()
        assert ready.wait(timeout=5)
        time.sleep(0.05)
        assert process.is_alive()
        queue.ack(job)
        process.join(timeout=5)
        result = results.get(timeout=2)
    finally:
        if process.is_alive():
            process.terminate()
        process.join(timeout=2)
        queue.close()

    assert process.exitcode == 0
    assert result[0] == "ok"


def test_process_timeout_finishes_without_sqlite_busy_timeout_overrun(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    path = str(tmp_path / "timeout")
    queue = SimpleQueue(path, max_pending_jobs=1)
    queue.put({"seed": True})
    ready = context.Event()
    results = context.Queue()
    process = context.Process(
        target=_blocking_producer, args=(path, 1, ready, results, 0.15)
    )
    try:
        process.start()
        assert ready.wait(timeout=5)
        process.join(timeout=2)
        result = results.get(timeout=2)
    finally:
        if process.is_alive():
            process.terminate()
        process.join(timeout=2)
        queue.close()

    assert process.exitcode == 0
    assert result[0] == "full"
    assert 0.08 <= result[1] < 0.9
