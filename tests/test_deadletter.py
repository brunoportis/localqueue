import asyncio
import json
import multiprocessing
import sqlite3
from dataclasses import FrozenInstanceError

import pytest
from localqueue import (
    DeliveryPolicy,
    FailedMessage,
    FailureReason,
    Full,
    LocalQueueError,
    SimpleQueue,
    Worker,
)
from localqueue.bus import BaseEvent, BusTopology, EventBus, FailedDelivery


class BytesSerializer:
    def dumps(self, obj, /):
        if obj is None:
            return b"none"
        return str(obj).encode()

    def loads(self, data, /):
        if data == b"none":
            return None
        return data.decode()


def _open_queue_at_barrier(path: str, barrier) -> None:
    barrier.wait()
    queue = SimpleQueue(path)
    queue.close()


def _retry_at_barrier(path: str, message_id: int, barrier, results) -> None:
    queue = SimpleQueue(path)
    barrier.wait()
    try:
        queue.retry_failed(message_id)
    except LocalQueueError as error:
        results.put(("error", str(error)))
    else:
        results.put(("success", None))
    finally:
        queue.close()


def _failed(queue, value="payload"):
    message_id = queue.put(value)
    job = queue.get(block=False)
    queue.fail(job, "human diagnostic")
    return message_id


def test_failed_message_is_typed_immutable_and_preserves_none(tmp_path):
    queue = SimpleQueue(str(tmp_path), serializer=BytesSerializer())
    message_id = _failed(queue, None)

    record = queue.list_failed()[0]

    assert isinstance(record, FailedMessage)
    assert record.id == message_id
    assert record.data is None
    assert record.decoded is True
    assert record.decode_error is None
    assert record.raw_payload == b"none"
    assert record.reason is FailureReason.EXPLICIT_PERMANENT_FAILURE
    assert record.last_error == "human diagnostic"
    assert record.created_at <= record.updated_at
    with pytest.raises(FrozenInstanceError):
        record.attempts = 0
    queue.close()


def test_failed_decode_is_isolated_and_raw_bytes_are_exact(tmp_path):
    queue = SimpleQueue(str(tmp_path), serializer=BytesSerializer())
    first = _failed(queue, "first")
    corrupt = _failed(queue, "corrupt")
    third = _failed(queue, "third")
    with sqlite3.connect(tmp_path / "localqueue.db") as connection:
        connection.execute(
            "UPDATE messages SET payload = ? WHERE id = ?", (b"\xff\x00", corrupt)
        )

    records = queue.list_failed()

    assert [record.id for record in records] == [first, corrupt, third]
    assert [record.data for record in records] == ["first", None, "third"]
    assert records[1].decoded is False
    assert records[1].raw_payload == b"\xff\x00"
    assert records[1].decode_error.startswith("UnicodeDecodeError:")
    queue.close()


@pytest.mark.parametrize(
    ("kwargs", "error", "message"),
    [
        ({"limit": True}, TypeError, "'limit' must be an integer"),
        ({"limit": 1.0}, TypeError, "'limit' must be an integer"),
        ({"offset": False}, TypeError, "'offset' must be an integer"),
        ({"offset": "1"}, TypeError, "'offset' must be an integer"),
        ({"limit": -1}, ValueError, "'limit' must be non-negative"),
        ({"offset": -1}, ValueError, "'offset' must be non-negative"),
    ],
)
def test_failed_pagination_validation(tmp_path, kwargs, error, message):
    queue = SimpleQueue(str(tmp_path))
    with pytest.raises(error, match=message):
        queue.list_failed(**kwargs)
    queue.close()


def test_failed_pagination_is_ordered_and_deterministic(tmp_path):
    queue = SimpleQueue(str(tmp_path))
    ids = [_failed(queue, index) for index in range(5)]

    assert queue.list_failed(limit=0) == []
    assert [item.id for item in queue.list_failed(limit=2)] == ids[:2]
    assert [item.id for item in queue.list_failed(limit=2, offset=2)] == ids[2:4]
    assert [item.id for item in queue.list_failed(offset=4)] == ids[4:]
    queue.close()


def test_reason_null_and_future_values_are_legacy_unknown(tmp_path):
    queue = SimpleQueue(str(tmp_path))
    first = _failed(queue, "old")
    second = _failed(queue, "future")
    with sqlite3.connect(tmp_path / "localqueue.db") as connection:
        connection.execute(
            "UPDATE messages SET failure_reason = NULL WHERE id = ?", (first,)
        )
        connection.execute(
            "UPDATE messages SET failure_reason = 'future_reason_v99' WHERE id = ?",
            (second,),
        )

    assert [item.reason for item in queue.list_failed()] == [
        FailureReason.LEGACY_UNKNOWN,
        FailureReason.LEGACY_UNKNOWN,
    ]
    queue.close()


def test_worker_persists_permanent_and_exhausted_reasons(tmp_path):
    class Permanent(Exception):
        pass

    permanent = SimpleQueue(str(tmp_path / "permanent"))
    permanent.put("value")
    Worker(
        permanent,
        lambda job: (_ for _ in ()).throw(Permanent("invalid customer")),
        permanent_errors=(Permanent,),
    ).run_once()
    record = permanent.list_failed()[0]
    assert record.reason is FailureReason.PERMANENT_HANDLER_ERROR
    assert record.last_error == "Permanent: invalid customer"
    permanent.close()

    exhausted = SimpleQueue(
        str(tmp_path / "exhausted"),
        delivery=DeliveryPolicy(max_retries=0),
    )
    exhausted.put("value")
    Worker(
        exhausted, lambda job: (_ for _ in ()).throw(RuntimeError("again"))
    ).run_once()
    assert exhausted.list_failed()[0].reason is FailureReason.RETRIES_EXHAUSTED
    exhausted.close()


def test_final_lease_expiration_is_retries_exhausted(tmp_path):
    queue = SimpleQueue(
        str(tmp_path),
        delivery=DeliveryPolicy(lease_seconds=0.001, max_retries=0),
    )
    queue.put("value")
    queue.get(block=False)

    queue._get_native().reclaim_expired(2**62)

    assert queue.list_failed()[0].reason is FailureReason.RETRIES_EXHAUSTED
    queue.close()


def test_retry_preserves_identity_bytes_and_creation_metadata(tmp_path):
    queue = SimpleQueue(str(tmp_path), serializer=BytesSerializer())
    message_id = queue.put("exact", job_id="replay-job")
    job = queue.get(block=False)
    assert job is not None
    queue.fail(job, last_error="failed")
    before = queue.list_failed()[0]

    queue.retry_failed(message_id)

    with sqlite3.connect(tmp_path / "localqueue.db") as connection:
        row = connection.execute(
            "SELECT id, payload, attempts, last_error, failure_reason, job_id, "
            "created_at, updated_at FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()
    assert row[:7] == (
        message_id,
        b"exact",
        0,
        None,
        None,
        "replay-job",
        int(before.created_at * 1000),
    )
    assert row[7] >= int(before.updated_at * 1000)
    with pytest.raises(LocalQueueError, match="job not found"):
        queue.retry_failed(message_id)
    queue.close()


def test_retry_validates_id_and_respects_capacity(tmp_path):
    queue = SimpleQueue(str(tmp_path), max_pending_jobs=1)
    failed_id = _failed(queue, "failed")
    queue.put("pending")
    with pytest.raises(Full):
        queue.retry_failed(failed_id)
    for invalid in ("1", True):
        with pytest.raises(TypeError, match="'message_id' must be an integer"):
            queue.retry_failed(invalid)
    assert queue.list_failed()[0].id == failed_id
    queue.close()


def test_two_processes_retry_exactly_once(tmp_path):
    queue = SimpleQueue(str(tmp_path))
    failed_id = _failed(queue)
    queue.close()
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(3)
    results = context.Queue()
    processes = [
        context.Process(
            target=_retry_at_barrier,
            args=(str(tmp_path), failed_id, barrier, results),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    barrier.wait()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0

    outcomes = sorted(results.get(timeout=2) for _ in processes)
    assert outcomes == [("error", "job not found"), ("success", None)]
    reopened = SimpleQueue(str(tmp_path))
    assert reopened.stats() == {
        "ready": 1,
        "processing": 0,
        "acked": 0,
        "failed": 0,
    }
    assert reopened.get(block=False).attempts == 0
    reopened.close()


def test_old_schema_migrates_without_rewriting_rows_and_concurrent_open_is_safe(
    tmp_path,
):
    database = tmp_path / "localqueue.db"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT, queue TEXT NOT NULL,
                payload BLOB NOT NULL, status INTEGER NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL,
                available_at INTEGER NOT NULL, lease_until INTEGER, receipt TEXT,
                last_error TEXT, job_id TEXT, created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            INSERT INTO messages VALUES
                (1, 'default', X'226F6C6422', 3, 2, 3, 10, NULL, NULL,
                 'legacy error', 'legacy-job', 11, 12);
            """
        )
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(3)
    processes = [
        context.Process(target=_open_queue_at_barrier, args=(str(tmp_path), barrier))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    barrier.wait()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0

    queue = SimpleQueue(str(tmp_path))
    record = queue.list_failed()[0]
    assert record.data == "old"
    assert record.raw_payload == b'"old"'
    assert record.reason is FailureReason.LEGACY_UNKNOWN
    assert (
        record.attempts,
        record.last_error,
        record.created_at,
        record.updated_at,
    ) == (
        2,
        "legacy error",
        0.011,
        0.012,
    )
    queue.close()


class PaymentCreated(BaseEvent):
    amount: int


class SelectiveEnvelopeSerializer:
    def dumps(self, obj: dict[str, object], /) -> bytes:
        return json.dumps(obj).encode()

    def loads(self, data: bytes, /) -> object:
        if data == b"broken":
            raise ValueError("cannot inspect envelope")
        return json.loads(data)


def _run(awaitable):
    return asyncio.run(awaitable)


def test_subscription_inspection_classifies_and_reconstructs_event(tmp_path):
    class Permanent(Exception):
        pass

    bus = EventBus(
        str(tmp_path),
        topology=BusTopology({"payments": [PaymentCreated]}),
        delivery=DeliveryPolicy(max_retries=0),
    )
    bus.on(
        PaymentCreated,
        lambda event: (_ for _ in ()).throw(Permanent("declined")),
        subscription="payments",
        permanent_errors=(Permanent,),
    )
    receipt = bus.dispatch(PaymentCreated(amount=10))
    _run(bus.run_subscription("payments", idle_timeout=0.2))

    delivery = bus.subscription("payments").list_failed()[0]

    assert isinstance(delivery, FailedDelivery)
    assert delivery.id == receipt.message_ids[0]
    assert delivery.subscription == "payments"
    assert isinstance(delivery.event, PaymentCreated)
    assert delivery.event_type == "PaymentCreated"
    assert delivery.reason is FailureReason.PERMANENT_HANDLER_ERROR
    assert delivery.inspection_error is None
    assert delivery.raw_payload
    bus.close()


def test_subscription_unknown_event_retains_type_and_raw_payload(tmp_path):
    bus = EventBus(
        str(tmp_path),
        topology=BusTopology({"payments": ["*"]}),
        delivery=DeliveryPolicy(max_retries=0),
    )
    bus.on("*", lambda event: None, subscription="payments")
    queue = bus._open_subscription_queue("payments")
    raw = json.dumps(
        {
            "event_id": "11111111-1111-1111-1111-111111111111",
            "event_created_at": "2026-01-01T00:00:00+00:00",
            "event_type": "FuturePayment",
            "payload": {},
        }
    ).encode()
    message_id = queue._get_native().put(raw)
    queue.close()
    _run(bus.run_subscription("payments", idle_timeout=0.2))

    delivery = bus.subscription("payments").list_failed()[0]
    assert delivery.id == message_id
    assert delivery.event is None
    assert delivery.event_type == "FuturePayment"
    assert delivery.raw_payload == raw
    assert delivery.reason is FailureReason.UNKNOWN_EVENT_TYPE
    assert delivery.inspection_error == "unknown event: 'FuturePayment'"
    bus.close()


def test_subscription_inspects_malformed_and_serializer_failure_independently(
    tmp_path,
):
    bus = EventBus(
        str(tmp_path),
        topology=BusTopology({"payments": ["*"]}),
        delivery=DeliveryPolicy(max_retries=0),
        serializer=SelectiveEnvelopeSerializer(),
    )
    bus.on("*", lambda event: None, subscription="payments")
    queue = bus._open_subscription_queue("payments")
    native_id = queue._get_native().put(b"broken")
    lease = queue._get_native().get(30_000)
    assert lease.id == native_id
    queue._get_native().fail(
        lease.id,
        lease.receipt,
        "stored diagnostic",
        "invalid_envelope",
    )
    queue.put(["malformed"])
    queue.close()
    _run(bus.run_subscription("payments", idle_timeout=0.2))

    undecodable, malformed = bus.subscription("payments").list_failed()

    assert malformed.event_type is None
    assert malformed.inspection_error.startswith("malformed envelope:")
    assert undecodable.event_type is None
    assert undecodable.raw_payload == b"broken"
    assert undecodable.inspection_error == ("ValueError: cannot inspect envelope")
    assert undecodable.reason is FailureReason.INVALID_ENVELOPE
    bus.close()


def test_subscription_retry_is_isolated_and_works_after_bus_close(tmp_path):
    class Permanent(Exception):
        pass

    bus = EventBus(
        str(tmp_path),
        topology=BusTopology({"a": [PaymentCreated], "b": [PaymentCreated]}),
        delivery=DeliveryPolicy(max_retries=0),
    )
    for name in ("a", "b"):
        bus.on(
            PaymentCreated,
            lambda event: (_ for _ in ()).throw(Permanent("stop")),
            subscription=name,
            permanent_errors=(Permanent,),
        )
    receipt = bus.dispatch(PaymentCreated(amount=20))
    _run(bus.run(idle_timeout=0.2))
    failed_a = bus.subscription("a").list_failed()[0]
    failed_b = bus.subscription("b").list_failed()[0]
    bus.close()

    bus.subscription("a").retry_failed(failed_a.id)

    assert bus.subscription("a").list_failed() == []
    assert bus.subscription("b").list_failed()[0].id == failed_b.id
    with pytest.raises(LocalQueueError, match="job not found"):
        bus.subscription("a").retry_failed(failed_b.id)
    assert set(receipt.message_ids) == {failed_a.id, failed_b.id}


def test_subscription_closes_internal_queue_when_listing_fails(tmp_path, monkeypatch):
    bus = EventBus(
        str(tmp_path),
        topology=BusTopology({"payments": [PaymentCreated]}),
    )

    class ExplodingQueue:
        closed = False

        def list_failed(self, *, limit, offset):
            raise RuntimeError("inspection unavailable")

        def close(self):
            self.closed = True

    queue = ExplodingQueue()
    monkeypatch.setattr(bus, "_open_subscription_queue", lambda subscription: queue)

    with pytest.raises(RuntimeError, match="inspection unavailable"):
        bus.subscription("payments").list_failed()

    assert queue.closed
    bus.close()
