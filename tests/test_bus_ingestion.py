from __future__ import annotations

import asyncio
import json
import math
import sqlite3
import threading
from pathlib import Path

import pytest
from localqueue import DeduplicationConflict, DeliveryPolicy
from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    NoSubscribers,
    event,
)


class Ping(BaseEvent):
    seq: int


class Pong(BaseEvent):
    seq: int


@event(identity="key")
class Keyed(BaseEvent):
    key: str
    value: str


class CountingSerializer:
    def __init__(self) -> None:
        self.dumps_calls = 0

    def dumps(self, obj: dict[str, object], /) -> bytes:
        self.dumps_calls += 1
        return json.dumps(obj).encode()

    def loads(self, data: bytes, /) -> object:
        return json.loads(data)


class BatchSpy:
    """Proxy for NativeQueue recording every _enqueue_batch_with_identity call."""

    def __init__(self, native) -> None:
        self._native = native
        self.calls: list[tuple[list, object]] = []

    def _enqueue_batch_with_identity(self, entries, capacity):
        self.calls.append((list(entries), capacity))
        return self._native._enqueue_batch_with_identity(entries, capacity)

    def close(self):
        return self._native.close()


def make_bus(path, topology=None, **kwargs) -> EventBus:
    return EventBus(
        str(path),
        name="test",
        topology=BusTopology(topology if topology is not None else {"s1": ["*"]}),
        delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=1),
        **kwargs,
    )


def run(coro):
    return asyncio.run(coro)


def queue_payloads(path: Path, queue: str) -> list[dict]:
    connection = sqlite3.connect(path / "localqueue.db")
    try:
        rows = connection.execute(
            "SELECT payload FROM messages WHERE queue = ? ORDER BY id", (queue,)
        ).fetchall()
    finally:
        connection.close()
    return [json.loads(row[0]) for row in rows]


def queue_seqs(path: Path, queue: str) -> list[int]:
    return [envelope["payload"]["seq"] for envelope in queue_payloads(path, queue)]


S1 = "__bus__:test:s1"
S2 = "__bus__:test:s2"


class RecordingIterable:
    """Records whether iteration ever started."""

    def __init__(self, items) -> None:
        self.items = items
        self.started = False

    def __iter__(self):
        self.started = True
        return iter(self.items)


class TestSources:
    def test_empty_list(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            result = run(bus.ingest([]))
            assert result.items_read == 0
            assert result.events_dispatched == 0
            assert result.events_unrouted == 0
            assert result.deliveries_inserted == 0
            assert result.deliveries_deduplicated == 0
            assert result.deliveries_total == 0
            assert result.batches_committed == 0
            assert math.isfinite(result.elapsed_seconds)
            assert result.elapsed_seconds >= 0
        finally:
            bus.close()

    def test_list(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            result = run(bus.ingest([Ping(seq=1), Ping(seq=2)]))
            assert result.items_read == 2
            assert result.events_dispatched == 2
            assert result.deliveries_inserted == 2
            assert queue_seqs(tmp_path / "bus", S1) == [1, 2]
        finally:
            bus.close()

    def test_tuple(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            result = run(bus.ingest((Ping(seq=1), Ping(seq=2))))
            assert result.items_read == 2
            assert queue_seqs(tmp_path / "bus", S1) == [1, 2]
        finally:
            bus.close()

    def test_generator(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            result = run(bus.ingest(Ping(seq=i) for i in range(3)))
            assert result.items_read == 3
            assert queue_seqs(tmp_path / "bus", S1) == [0, 1, 2]
        finally:
            bus.close()

    def test_async_generator(self, tmp_path):
        bus = make_bus(tmp_path / "bus")

        async def agen():
            for i in range(3):
                yield Ping(seq=i)

        try:
            result = run(bus.ingest(agen()))
            assert result.items_read == 3
            assert queue_seqs(tmp_path / "bus", S1) == [0, 1, 2]
        finally:
            bus.close()

    def test_iterable_object(self, tmp_path):
        bus = make_bus(tmp_path / "bus")

        class CustomIterable:
            def __iter__(self):
                return iter([Ping(seq=1), Ping(seq=2)])

        try:
            result = run(bus.ingest(CustomIterable()))
            assert result.items_read == 2
            assert queue_seqs(tmp_path / "bus", S1) == [1, 2]
        finally:
            bus.close()

    def test_classic_sequence_protocol_source(self, tmp_path):
        bus = make_bus(tmp_path / "bus")

        class ClassicSequence:
            def __getitem__(self, index):
                if index >= 2:
                    raise IndexError
                return Ping(seq=index + 1)

        try:
            result = run(bus.ingest(ClassicSequence()))
            assert result.items_read == 2
            assert queue_seqs(tmp_path / "bus", S1) == [1, 2]
        finally:
            bus.close()

    def test_async_iterable_object(self, tmp_path):
        bus = make_bus(tmp_path / "bus")

        class CustomAsyncIterable:
            def __aiter__(self):
                async def agen():
                    yield Ping(seq=1)
                    yield Ping(seq=2)

                return agen()

        try:
            result = run(bus.ingest(CustomAsyncIterable()))
            assert result.items_read == 2
            assert queue_seqs(tmp_path / "bus", S1) == [1, 2]
        finally:
            bus.close()

    def test_object_with_both_iter_and_aiter_prefers_async(self, tmp_path):
        bus = make_bus(tmp_path / "bus")

        class Both:
            def __init__(self) -> None:
                self.used = None

            def __iter__(self):
                self.used = "sync"
                return iter([Ping(seq=1)])

            def __aiter__(self):
                self.used = "async"

                async def agen():
                    yield Ping(seq=1)

                return agen()

        source = Both()
        try:
            result = run(bus.ingest(source))
            assert result.items_read == 1
            assert source.used == "async"
        finally:
            bus.close()

    def test_invalid_source_raises_typeerror(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            with pytest.raises(TypeError, match="Iterable or AsyncIterable"):
                run(bus.ingest(42))
        finally:
            bus.close()

    @pytest.mark.parametrize("batch_size", [0, -1])
    def test_invalid_batch_size_value_not_consumed(self, tmp_path, batch_size):
        bus = make_bus(tmp_path / "bus")
        source = RecordingIterable([Ping(seq=1)])
        try:
            with pytest.raises(ValueError, match="batch_size"):
                run(bus.ingest(source, batch_size=batch_size))
            assert not source.started
        finally:
            bus.close()

    @pytest.mark.parametrize("batch_size", [True, "10"])
    def test_invalid_batch_size_type_not_consumed(self, tmp_path, batch_size):
        bus = make_bus(tmp_path / "bus")
        source = RecordingIterable([Ping(seq=1)])
        try:
            with pytest.raises(TypeError, match="batch_size"):
                run(bus.ingest(source, batch_size=batch_size))
            assert not source.started
        finally:
            bus.close()

    def test_invalid_max_pending_zero_not_consumed(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        source = RecordingIterable([Ping(seq=1)])
        try:
            with pytest.raises(ValueError, match="max_pending"):
                run(bus.ingest(source, max_pending=0))
            assert not source.started
        finally:
            bus.close()

    def test_invalid_max_pending_bool_not_consumed(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        source = RecordingIterable([Ping(seq=1)])
        try:
            with pytest.raises(TypeError, match="max_pending"):
                run(bus.ingest(source, max_pending=True))
            assert not source.started
        finally:
            bus.close()

    def test_invalid_max_pending_above_bound_not_consumed(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        source = RecordingIterable([Ping(seq=1)])
        try:
            with pytest.raises(ValueError, match="max_pending"):
                run(bus.ingest(source, max_pending=2**63))
            assert not source.started
        finally:
            bus.close()

    def test_invalid_transform_not_consumed(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        source = RecordingIterable([Ping(seq=1)])
        try:
            with pytest.raises(TypeError, match="transform"):
                run(bus.ingest(source, transform="x"))
            assert not source.started
        finally:
            bus.close()

    def test_source_reads_ahead_no_more_than_batch_size(self, tmp_path, monkeypatch):
        bus = make_bus(tmp_path / "bus", {"s1": ["*"]})
        tracker_state = {"count": 0}

        class Tracker:
            def __iter__(self):
                for _ in range(10):
                    tracker_state["count"] += 1
                    yield Ping(seq=tracker_state["count"])

        native = bus._native_queue
        consumed_at_calls: list[int] = []

        class Spy:
            def _enqueue_batch_with_identity(self, entries, capacity):
                consumed_at_calls.append(tracker_state["count"])
                return native._enqueue_batch_with_identity(entries, capacity)

            def close(self):
                return native.close()

        monkeypatch.setattr(bus, "_native_queue", Spy())
        try:
            run(bus.ingest(Tracker(), batch_size=3))
            assert consumed_at_calls[0] <= 3
            assert consumed_at_calls == [3, 6, 9, 10]
        finally:
            bus.close()

    def test_source_exception_propagates_unwrapped_and_keeps_prior_batches(
        self, tmp_path
    ):
        bus = make_bus(tmp_path / "bus", {"s1": ["*"]})

        class Boom(Exception):
            pass

        def source():
            for i in range(1, 4):
                yield Ping(seq=i)
            raise Boom("source exploded")

        try:
            with pytest.raises(Boom, match="source exploded"):
                run(bus.ingest(source(), batch_size=2))
            # First batch committed; the incomplete second batch did not.
            assert queue_seqs(tmp_path / "bus", S1) == [1, 2]
        finally:
            bus.close()

    def test_async_source_cancellation_propagates(self, tmp_path):
        bus = make_bus(tmp_path / "bus", {"s1": ["*"]})
        never = asyncio.Event()

        async def agen():
            yield Ping(seq=1)
            await never.wait()
            yield Ping(seq=2)

        async def main():
            task = asyncio.create_task(bus.ingest(agen(), batch_size=10))
            await asyncio.sleep(0.05)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        try:
            run(main())
            # The first item was consumed but never committed.
            assert queue_seqs(tmp_path / "bus", S1) == []
        finally:
            bus.close()

    def test_cancellation_waits_for_in_flight_native_commit(
        self, tmp_path, monkeypatch
    ):
        bus = make_bus(tmp_path / "bus", {"s1": ["*"]})
        native = bus._native_queue
        transaction_started = threading.Event()
        release_transaction = threading.Event()

        class PausedCommit:
            def _enqueue_batch_with_identity(self, entries, capacity):
                transaction_started.set()
                assert release_transaction.wait(timeout=5)
                return native._enqueue_batch_with_identity(entries, capacity)

            def close(self):
                return native.close()

        monkeypatch.setattr(bus, "_native_queue", PausedCommit())

        async def main():
            task = asyncio.create_task(bus.ingest([Ping(seq=1)], batch_size=1))
            assert await asyncio.to_thread(transaction_started.wait, 5)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            release_transaction.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 5)

        try:
            run(main())
            assert queue_seqs(tmp_path / "bus", S1) == [1]
        finally:
            release_transaction.set()
            bus.close()


class TestTransform:
    def test_sync_transform(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            result = run(bus.ingest([1, 2, 3], transform=lambda i: Ping(seq=i)))
            assert result.items_read == 3
            assert result.events_dispatched == 3
            assert queue_seqs(tmp_path / "bus", S1) == [1, 2, 3]
        finally:
            bus.close()

    def test_async_transform(self, tmp_path):
        bus = make_bus(tmp_path / "bus")

        async def transform(i):
            await asyncio.sleep(0)
            return Ping(seq=i * 10)

        try:
            result = run(bus.ingest([1, 2], transform=transform))
            assert result.items_read == 2
            assert queue_seqs(tmp_path / "bus", S1) == [10, 20]
        finally:
            bus.close()

    def test_transform_returning_non_event_raises_with_type_and_index(self, tmp_path):
        bus = make_bus(tmp_path / "bus")

        def transform(i):
            if i == 42:
                return "not an event"
            return Ping(seq=i)

        try:
            with pytest.raises(
                TypeError, match="transform returned str for source item 42"
            ):
                run(bus.ingest(range(1, 43), transform=transform))
        finally:
            bus.close()

    def test_transform_exception_propagates_as_same_object(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        sentinel = RuntimeError("transform exploded")

        def transform(i):
            raise sentinel

        caught = []
        try:
            try:
                run(bus.ingest([1], transform=transform))
            except RuntimeError as error:
                caught.append(error)
            assert caught == [sentinel]
        finally:
            bus.close()

    def test_transform_called_exactly_once_per_item(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        calls = []

        def transform(i):
            calls.append(i)
            return Ping(seq=i)

        try:
            run(bus.ingest([1, 2, 3], transform=transform, batch_size=2))
            assert calls == [1, 2, 3]
        finally:
            bus.close()

    def test_without_transform_non_event_raises_with_index(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            with pytest.raises(
                TypeError, match="source item 2 is a str; expected BaseEvent"
            ):
                run(bus.ingest([Ping(seq=1), "oops", Ping(seq=3)]))
        finally:
            bus.close()

    def test_transformed_event_order_preserved(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            run(bus.ingest([3, 1, 2], transform=lambda i: Ping(seq=i), batch_size=2))
            assert queue_seqs(tmp_path / "bus", S1) == [3, 1, 2]
        finally:
            bus.close()


class TestBatchDispatch:
    def test_one_native_call_per_commit(self, tmp_path, monkeypatch):
        bus = make_bus(tmp_path / "bus", {"s1": ["*"]})
        spy = BatchSpy(bus._native_queue)
        monkeypatch.setattr(bus, "_native_queue", spy)
        try:
            run(bus.ingest([Ping(seq=i) for i in range(5)], batch_size=5))
            assert len(spy.calls) == 1
            assert len(spy.calls[0][0]) == 5
        finally:
            bus.close()

    def test_ceil_commits_and_final_partial_batch(self, tmp_path, monkeypatch):
        bus = make_bus(tmp_path / "bus", {"s1": ["*"]})
        spy = BatchSpy(bus._native_queue)
        monkeypatch.setattr(bus, "_native_queue", spy)
        try:
            result = run(bus.ingest([Ping(seq=i) for i in range(7)], batch_size=5))
            assert [len(call[0]) for call in spy.calls] == [5, 2]
            assert result.batches_committed == 2
            assert queue_seqs(tmp_path / "bus", S1) == list(range(7))
        finally:
            bus.close()

    def test_mixed_event_types_route_to_their_subscriptions(self, tmp_path):
        bus = make_bus(
            tmp_path / "bus",
            {"pings": [Ping], "pongs": [Pong], "all": ["*"]},
        )
        try:
            result = run(bus.ingest([Ping(seq=1), Pong(seq=2), Ping(seq=3)]))
            assert result.events_dispatched == 3
            # Ping -> pings + all; Pong -> pongs + all
            assert result.deliveries_inserted == 6
            pings = queue_seqs(tmp_path / "bus", "__bus__:test:pings")
            pongs = queue_seqs(tmp_path / "bus", "__bus__:test:pongs")
            everything = queue_payloads(tmp_path / "bus", "__bus__:test:all")
            assert pings == [1, 3]
            assert pongs == [2]
            assert [e["event_type"] for e in everything] == ["Ping", "Pong", "Ping"]
        finally:
            bus.close()

    def test_fanout_to_multiple_subscriptions(self, tmp_path):
        bus = make_bus(tmp_path / "bus", {"s1": [Ping], "s2": [Ping]})
        try:
            result = run(bus.ingest([Ping(seq=1)]))
            assert result.events_dispatched == 1
            assert result.deliveries_inserted == 2
            assert result.deliveries_total == 2
            assert queue_seqs(tmp_path / "bus", S1) == [1]
            assert queue_seqs(tmp_path / "bus", S2) == [1]
        finally:
            bus.close()

    def test_per_queue_order_preserved_across_types(self, tmp_path):
        bus = make_bus(tmp_path / "bus", {"all": ["*"]})
        try:
            run(bus.ingest([Ping(seq=1), Pong(seq=1), Ping(seq=2)]))
            envelopes = queue_payloads(tmp_path / "bus", "__bus__:test:all")
            assert [(e["event_type"], e["payload"]["seq"]) for e in envelopes] == [
                ("Ping", 1),
                ("Pong", 1),
                ("Ping", 2),
            ]
        finally:
            bus.close()

    def test_whole_batch_rolls_back_on_deduplication_conflict(self, tmp_path):
        bus = make_bus(tmp_path / "bus", {"s1": [Keyed], "s2": [Keyed]})
        try:
            with pytest.raises(DeduplicationConflict):
                run(
                    bus.ingest(
                        [
                            Keyed(key="1", value="a"),
                            Keyed(key="1", value="b"),
                        ],
                        batch_size=2,
                    )
                )
            assert queue_payloads(tmp_path / "bus", S1) == []
            assert queue_payloads(tmp_path / "bus", S2) == []
        finally:
            bus.close()

    def test_serialization_once_per_event(self, tmp_path):
        serializer = CountingSerializer()
        bus = make_bus(tmp_path / "bus", {"s1": ["*"]}, serializer=serializer)
        try:
            run(bus.ingest([Ping(seq=i) for i in range(5)], batch_size=2))
            assert serializer.dumps_calls == 5
        finally:
            bus.close()

    def test_registry_reconstructs_ingested_types_on_fresh_bus(self, tmp_path):
        path = tmp_path / "bus"
        bus = make_bus(path, {"s1": ["*"]})
        run(bus.ingest([Ping(seq=1), Pong(seq=2)]))
        bus.close()

        fresh = make_bus(path, {"s1": ["*"]})
        seen = []
        try:
            fresh.on(Ping, lambda e: seen.append(("Ping", e.seq)), subscription="s1")
            fresh.on(Pong, lambda e: seen.append(("Pong", e.seq)), subscription="s1")
            run(fresh.run(idle_timeout=0.3))
            assert seen == [("Ping", 1), ("Pong", 2)]
        finally:
            fresh.close()

    def test_dispatch_and_dispatch_async_receipts_unchanged_for_duplicates(
        self, tmp_path
    ):
        bus = make_bus(tmp_path / "bus", {"s1": [Keyed]})
        try:
            first = bus.dispatch(Keyed(key="1", value="a"))
            duplicate = bus.dispatch(Keyed(key="1", value="a"))
            assert first.message_ids == duplicate.message_ids
            assert first.inserted == (True,)
            assert duplicate.inserted == (False,)
            async_duplicate = run(bus.dispatch_async(Keyed(key="1", value="a")))
            assert async_duplicate.message_ids == first.message_ids
            assert async_duplicate.inserted == (False,)
        finally:
            bus.close()


class TestNoSubscribers:
    def test_require_subscribers_raises_naming_event_type(self, tmp_path):
        bus = make_bus(tmp_path / "bus", {"s1": [Ping]})
        try:
            with pytest.raises(NoSubscribers, match="no subscription for 'Pong'"):
                run(bus.ingest([Ping(seq=1), Pong(seq=1)], batch_size=2))
            # Nothing of the incomplete group was persisted.
            assert queue_payloads(tmp_path / "bus", S1) == []
        finally:
            bus.close()

    def test_require_subscribers_keeps_earlier_committed_batches(self, tmp_path):
        bus = make_bus(tmp_path / "bus", {"s1": [Ping]})
        try:
            with pytest.raises(NoSubscribers):
                run(bus.ingest([Ping(seq=1), Pong(seq=1)], batch_size=1))
            assert queue_seqs(tmp_path / "bus", S1) == [1]
        finally:
            bus.close()

    def test_all_unrouted_group_skips_native_call(self, tmp_path, monkeypatch):
        bus = make_bus(tmp_path / "bus", {}, require_subscribers=False)
        spy = BatchSpy(bus._native_queue)
        monkeypatch.setattr(bus, "_native_queue", spy)
        try:
            result = run(bus.ingest([Ping(seq=1), Ping(seq=2)]))
            assert result.items_read == 2
            assert result.events_dispatched == 0
            assert result.events_unrouted == 2
            assert result.batches_committed == 0
            assert spy.calls == []
        finally:
            bus.close()

    def test_mixed_group_persists_only_routed_events(self, tmp_path, monkeypatch):
        bus = make_bus(tmp_path / "bus", {"s1": [Ping]}, require_subscribers=False)
        spy = BatchSpy(bus._native_queue)
        monkeypatch.setattr(bus, "_native_queue", spy)
        try:
            result = run(bus.ingest([Ping(seq=1), Pong(seq=2)], batch_size=2))
            assert result.events_dispatched == 1
            assert result.events_unrouted == 1
            assert result.deliveries_inserted == 1
            assert len(spy.calls) == 1
            assert len(spy.calls[0][0]) == 1
            assert queue_seqs(tmp_path / "bus", S1) == [1]
        finally:
            bus.close()


class TestIdentity:
    def test_same_identity_same_payload_twice_in_one_batch(self, tmp_path):
        bus = make_bus(tmp_path / "bus", {"s1": [Keyed]})
        try:
            result = run(
                bus.ingest(
                    [Keyed(key="1", value="a"), Keyed(key="1", value="a")],
                    batch_size=2,
                )
            )
            assert result.deliveries_inserted == 1
            assert result.deliveries_deduplicated == 1
            connection = sqlite3.connect(tmp_path / "bus" / "localqueue.db")
            try:
                count = connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE queue = ?", (S1,)
                ).fetchone()[0]
            finally:
                connection.close()
            assert count == 1
        finally:
            bus.close()

    def test_same_identity_as_persisted_row_is_deduplicated(self, tmp_path):
        bus = make_bus(tmp_path / "bus", {"s1": [Keyed]})
        try:
            bus.dispatch(Keyed(key="1", value="a"))
            result = run(bus.ingest([Keyed(key="1", value="a")]))
            assert result.deliveries_inserted == 0
            assert result.deliveries_deduplicated == 1
        finally:
            bus.close()

    def test_same_identity_different_payload_vs_storage_conflicts(self, tmp_path):
        bus = make_bus(tmp_path / "bus", {"s1": [Keyed]})
        try:
            bus.dispatch(Keyed(key="1", value="a"))
            with pytest.raises(DeduplicationConflict):
                run(bus.ingest([Keyed(key="1", value="b")]))
        finally:
            bus.close()

    def test_dedup_is_per_subscription_on_fanout(self, tmp_path):
        path = tmp_path / "bus"
        first_bus = make_bus(path, {"s1": [Keyed]})
        first_bus.dispatch(Keyed(key="1", value="a"))
        first_bus.close()

        bus = make_bus(path, {"s1": [Keyed], "s2": [Keyed]})
        try:
            result = run(bus.ingest([Keyed(key="1", value="a")]))
            assert result.deliveries_inserted == 1
            assert result.deliveries_deduplicated == 1
        finally:
            bus.close()

    def test_events_without_identity_are_separate_occurrences(self, tmp_path):
        bus = make_bus(tmp_path / "bus", {"s1": [Ping]})
        try:
            result = run(bus.ingest([Ping(seq=1), Ping(seq=1)]))
            assert result.deliveries_inserted == 2
            assert result.deliveries_deduplicated == 0
        finally:
            bus.close()


class TestResult:
    def test_counters_for_mixed_run(self, tmp_path):
        bus = make_bus(
            tmp_path / "bus",
            {"s1": [Ping], "s2": [Ping], "keyed": [Keyed]},
            require_subscribers=False,
        )
        try:
            result = run(
                bus.ingest(
                    [
                        Ping(seq=1),
                        Keyed(key="1", value="a"),
                        Keyed(key="1", value="a"),
                        Pong(seq=9),
                    ],
                    batch_size=2,
                )
            )
            assert result.items_read == 4
            # Ping fans out to s1+s2; Keyed duplicates dedup on "keyed";
            # Pong has no route and is unrouted.
            assert result.events_dispatched == 3
            assert result.events_unrouted == 1
            assert result.deliveries_inserted == 3
            assert result.deliveries_deduplicated == 1
            assert result.deliveries_total == 4
            assert result.batches_committed == 2
            assert math.isfinite(result.elapsed_seconds)
            assert result.elapsed_seconds >= 0
        finally:
            bus.close()

    def test_elapsed_is_finite_on_empty_source(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            result = run(bus.ingest([]))
            assert result.elapsed_seconds >= 0
            assert math.isfinite(result.elapsed_seconds)
        finally:
            bus.close()
