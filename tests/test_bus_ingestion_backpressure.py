from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import sqlite3
from pathlib import Path

import pytest
from localqueue import DeliveryPolicy, Empty, Full
from localqueue import localqueue as native_module
from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    event,
)


class Tick(BaseEvent):
    seq: int


@event(identity="key")
class KeyedBp(BaseEvent):
    key: str
    value: str


S1 = "__bus__:test:s1"


def make_bus(path, topology=None, **kwargs) -> EventBus:
    # Long leases: tests ack explicitly and must not see lease expiry.
    return EventBus(
        str(path),
        name="test",
        topology=BusTopology(topology if topology is not None else {"s1": ["*"]}),
        delivery=DeliveryPolicy(lease_seconds=30.0, max_retries=1),
        **kwargs,
    )


def run(coro):
    return asyncio.run(coro)


def queue_seqs(path: Path, queue: str) -> list[int]:
    connection = sqlite3.connect(path / "localqueue.db")
    try:
        rows = connection.execute(
            "SELECT payload FROM messages WHERE queue = ? ORDER BY id", (queue,)
        ).fetchall()
    finally:
        connection.close()
    return [json.loads(row[0])["payload"]["seq"] for row in rows]


def pending(queue) -> int:
    stats = queue.stats()
    return stats["ready"] + stats["processing"]


async def drain_until(queue, done) -> None:
    """Ack every deliverable row until ``done()`` is true."""
    while not done():
        try:
            queue.ack(queue.get_nowait())
        except Empty:
            await asyncio.sleep(0.001)


class BatchSpy:
    def __init__(self, native) -> None:
        self._native = native
        self.calls: list[list] = []

    def _enqueue_batch_with_identity(self, entries, capacity):
        self.calls.append(list(entries))
        return self._native._enqueue_batch_with_identity(entries, capacity)

    def close(self):
        return self._native.close()


class CountingSerializer:
    def __init__(self) -> None:
        self.dumps_calls = 0

    def dumps(self, obj: dict[str, object], /) -> bytes:
        self.dumps_calls += 1
        return json.dumps(obj).encode()

    def loads(self, data: bytes, /) -> object:
        return json.loads(data)


class TestCapacitySemantics:
    def test_exact_fit_commits_immediately(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            result = run(bus.ingest([Tick(seq=1), Tick(seq=2)], max_pending=2))
            assert result.deliveries_inserted == 2
            assert result.batches_committed == 1
        finally:
            bus.close()

    def test_max_pending_none_path(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            result = run(
                bus.ingest([Tick(seq=i) for i in range(100)], max_pending=None)
            )
            assert result.deliveries_inserted == 100
        finally:
            bus.close()

    def test_ready_rows_count_against_max_pending(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            bus.dispatch(Tick(seq=0))
            task_done = []

            async def main():
                task = asyncio.create_task(bus.ingest([Tick(seq=1)], max_pending=1))
                await asyncio.sleep(0.05)
                task_done.append(task.done())
                queue = bus._open_subscription_queue("s1")
                try:
                    queue.ack(queue.get_nowait())
                finally:
                    queue.close()
                return await asyncio.wait_for(task, 5)

            result = run(main())
            assert task_done == [False]
            assert result.deliveries_inserted == 1
        finally:
            bus.close()

    def test_leased_rows_count_against_max_pending(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            bus.dispatch(Tick(seq=0))
            queue = bus._open_subscription_queue("s1")
            try:
                job = queue.get_nowait()  # LEASED, not acked
                observed = []

                async def main():
                    task = asyncio.create_task(bus.ingest([Tick(seq=1)], max_pending=1))
                    await asyncio.sleep(0.05)
                    observed.append(task.done())
                    queue.ack(job)
                    return await asyncio.wait_for(task, 5)

                result = run(main())
                assert observed == [False]
                assert result.deliveries_inserted == 1
            finally:
                queue.close()
        finally:
            bus.close()

    def test_acked_rows_do_not_count(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            bus.dispatch(Tick(seq=0))
            queue = bus._open_subscription_queue("s1")
            try:
                queue.ack(queue.get_nowait())
            finally:
                queue.close()
            result = run(bus.ingest([Tick(seq=1)], max_pending=1))
            assert result.deliveries_inserted == 1
        finally:
            bus.close()

    def test_failed_rows_do_not_count(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            bus.dispatch(Tick(seq=0))
            queue = bus._open_subscription_queue("s1")
            try:
                queue.fail(queue.get_nowait())
            finally:
                queue.close()
            result = run(bus.ingest([Tick(seq=1)], max_pending=1))
            assert result.deliveries_inserted == 1
        finally:
            bus.close()

    def test_plain_dispatch_stays_unlimited_after_ingest_limit(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            run(bus.ingest([Tick(seq=1), Tick(seq=2)], max_pending=2))
            for i in range(3, 6):
                bus.dispatch(Tick(seq=i))
            assert queue_seqs(tmp_path / "bus", S1) == [1, 2, 3, 4, 5]
        finally:
            bus.close()


class TestBackpressureWait:
    def test_waiting_does_not_block_other_tasks(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            bus.dispatch(Tick(seq=0))

            async def main():
                ticks = 0
                stop = asyncio.Event()

                async def ticker():
                    nonlocal ticks
                    while not stop.is_set():
                        ticks += 1
                        await asyncio.sleep(0.005)

                tick_task = asyncio.create_task(ticker())
                task = asyncio.create_task(bus.ingest([Tick(seq=1)], max_pending=1))
                await asyncio.sleep(0.1)
                assert not task.done()
                stop.set()
                await tick_task
                queue = bus._open_subscription_queue("s1")
                try:
                    queue.ack(queue.get_nowait())
                finally:
                    queue.close()
                result = await asyncio.wait_for(task, 5)
                return ticks, result

            ticks, result = run(main())
            assert ticks > 2
            assert result.deliveries_inserted == 1
        finally:
            bus.close()

    def test_cancellation_during_wait_propagates(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            bus.dispatch(Tick(seq=0))

            async def main():
                task = asyncio.create_task(bus.ingest([Tick(seq=1)], max_pending=1))
                await asyncio.sleep(0.05)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert task.cancelled()

            run(main())
        finally:
            bus.close()

    def test_closing_bus_during_wait_raises_runtime_error(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        bus.dispatch(Tick(seq=0))

        async def main():
            task = asyncio.create_task(bus.ingest([Tick(seq=1)], max_pending=1))
            await asyncio.sleep(0.05)
            bus.close()
            with pytest.raises(RuntimeError, match="closed"):
                await task

        run(main())

    def test_source_does_not_advance_while_backpressured(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            bus.dispatch(Tick(seq=0))
            tracker_state = {"count": 0}

            def source():
                for i in range(1, 4):
                    tracker_state["count"] += 1
                    yield Tick(seq=i)

            async def main():
                task = asyncio.create_task(
                    bus.ingest(source(), batch_size=1, max_pending=1)
                )
                await asyncio.sleep(0.1)
                observed = tracker_state["count"]
                queue = bus._open_subscription_queue("s1")
                try:
                    while not task.done():
                        try:
                            queue.ack(queue.get_nowait())
                        except Empty:
                            await asyncio.sleep(0.005)
                finally:
                    queue.close()
                return observed, await asyncio.wait_for(task, 5)

            observed, result = run(main())
            # Only the in-flight item was consumed while the queue was full.
            assert observed == 1
            assert result.items_read == 3
            assert tracker_state["count"] == 3
        finally:
            bus.close()

    def test_transform_and_serialization_not_repeated_during_wait(self, tmp_path):
        serializer = CountingSerializer()
        bus = make_bus(tmp_path / "bus", serializer=serializer)
        try:
            bus.dispatch(Tick(seq=0))  # 1 dumps call for the seed
            transform_calls = []

            def transform(i):
                transform_calls.append(i)
                return Tick(seq=i)

            async def main():
                task = asyncio.create_task(
                    bus.ingest([1, 2], transform=transform, batch_size=2, max_pending=1)
                )
                await asyncio.sleep(0.1)
                mid = (len(transform_calls), serializer.dumps_calls)
                queue = bus._open_subscription_queue("s1")
                try:
                    drainer = asyncio.create_task(drain_until(queue, task.done))
                    try:
                        result = await asyncio.wait_for(task, 5)
                    finally:
                        await drainer
                finally:
                    queue.close()
                return mid, result

            mid, result = run(main())
            assert mid == (2, 3)  # both items transformed+serialized once already
            assert transform_calls == [1, 2]
            assert serializer.dumps_calls == 3
            assert result.deliveries_inserted == 2
        finally:
            bus.close()

    def test_retried_batch_payloads_are_identical(self, tmp_path, monkeypatch):
        bus = make_bus(tmp_path / "bus")
        bus.dispatch(Tick(seq=0))
        spy = BatchSpy(bus._native_queue)
        monkeypatch.setattr(bus, "_native_queue", spy)
        try:

            async def main():
                task = asyncio.create_task(bus.ingest([Tick(seq=1)], max_pending=1))
                while len(spy.calls) < 2:
                    await asyncio.sleep(0.005)
                queue = bus._open_subscription_queue("s1")
                try:
                    queue.ack(queue.get_nowait())
                finally:
                    queue.close()
                return await asyncio.wait_for(task, 5)

            run(asyncio.wait_for(main(), 10))
            assert len(spy.calls) >= 2
            assert spy.calls[0] == spy.calls[1]
        finally:
            bus.close()


class TestSplits:
    def test_oversized_batch_splits_preserving_order(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        transform_calls = []

        def transform(i):
            transform_calls.append(i)
            return Tick(seq=i)

        async def main():
            task = asyncio.create_task(
                bus.ingest(range(10), transform=transform, batch_size=10, max_pending=4)
            )
            queue = bus._open_subscription_queue("s1")
            try:
                drainer = asyncio.create_task(drain_until(queue, task.done))
                try:
                    result = await asyncio.wait_for(task, 10)
                finally:
                    await drainer
            finally:
                queue.close()
            return result

        try:
            result = run(main())
            assert result.deliveries_inserted == 10
            assert result.batches_committed > 1
            assert queue_seqs(tmp_path / "bus", S1) == list(range(10))
            assert sorted(transform_calls) == list(range(10))
            assert len(transform_calls) == 10
        finally:
            bus.close()

    def test_unsplittable_single_event_raises_full(self, tmp_path, monkeypatch):
        bus = make_bus(tmp_path / "bus")
        native = bus._native_queue

        class AlwaysImpossible:
            def _enqueue_batch_with_identity(self, entries, capacity):
                raise native_module._FullImpossible("never fits")

            def close(self):
                return native.close()

        monkeypatch.setattr(bus, "_native_queue", AlwaysImpossible())
        try:
            with pytest.raises(Full, match="single event exceeds 'max_pending'"):
                run(bus.ingest([Tick(seq=1)], batch_size=1, max_pending=1))
        finally:
            bus.close()

    def test_split_halves_commit_via_stubbed_impossible(self, tmp_path, monkeypatch):
        bus = make_bus(tmp_path / "bus")
        native = bus._native_queue

        class ImpossibleAboveOne:
            def _enqueue_batch_with_identity(self, entries, capacity):
                if len(entries) > 1:
                    raise native_module._FullImpossible("too many")
                return native._enqueue_batch_with_identity(entries, capacity)

            def close(self):
                return native.close()

        monkeypatch.setattr(bus, "_native_queue", ImpossibleAboveOne())
        try:
            result = run(
                bus.ingest([Tick(seq=1), Tick(seq=2)], batch_size=2, max_pending=10)
            )
            assert result.deliveries_inserted == 2
            assert result.batches_committed == 2
            assert queue_seqs(tmp_path / "bus", S1) == [1, 2]
        finally:
            bus.close()

    def test_batch_splits_down_to_single_events(self, tmp_path):
        bus = make_bus(tmp_path / "bus")

        async def main():
            task = asyncio.create_task(
                bus.ingest([Tick(seq=1), Tick(seq=2)], batch_size=2, max_pending=1)
            )
            queue = bus._open_subscription_queue("s1")
            try:
                drainer = asyncio.create_task(drain_until(queue, task.done))
                try:
                    result = await asyncio.wait_for(task, 10)
                finally:
                    await drainer
            finally:
                queue.close()
            return result

        try:
            result = run(main())
            assert result.deliveries_inserted == 2
            assert result.batches_committed == 2
            assert queue_seqs(tmp_path / "bus", S1) == [1, 2]
        finally:
            bus.close()


class TestIdentityCapacity:
    def test_fully_deduplicated_batch_passes_with_full_queue(self, tmp_path):
        bus = make_bus(tmp_path / "bus", {"s1": [KeyedBp]})
        try:
            bus.dispatch(KeyedBp(key="1", value="a"))
            result = run(
                bus.ingest(
                    [KeyedBp(key="1", value="a") for _ in range(50)],
                    batch_size=50,
                    max_pending=1,
                )
            )
            assert result.deliveries_inserted == 0
            assert result.deliveries_deduplicated == 50
            assert result.batches_committed == 1
        finally:
            bus.close()

    def test_thousand_same_identity_events_consume_one_slot(self, tmp_path):
        bus = make_bus(tmp_path / "bus", {"s1": [KeyedBp]})
        try:
            result = run(
                bus.ingest(
                    (KeyedBp(key="1", value="a") for _ in range(1000)),
                    batch_size=1000,
                    max_pending=1,
                )
            )
            assert result.deliveries_inserted == 1
            assert result.deliveries_deduplicated == 999
        finally:
            bus.close()

    def test_fanout_requires_capacity_in_all_subscriptions(self, tmp_path):
        path = tmp_path / "bus"
        seed_bus = make_bus(path, {"s1": [Tick]})
        seed_bus.dispatch(Tick(seq=0))
        seed_bus.close()

        bus = make_bus(path, {"s1": [Tick], "s2": [Tick]})
        try:
            s2_stats_during_wait = []

            async def main():
                task = asyncio.create_task(bus.ingest([Tick(seq=1)], max_pending=1))
                await asyncio.sleep(0.1)
                s2 = bus._open_subscription_queue("s2")
                try:
                    s2_stats_during_wait.append(s2.stats())
                finally:
                    s2.close()
                queue = bus._open_subscription_queue("s1")
                try:
                    queue.ack(queue.get_nowait())
                finally:
                    queue.close()
                return await asyncio.wait_for(task, 5)

            result = run(main())
            assert s2_stats_during_wait == [
                {"ready": 0, "processing": 0, "acked": 0, "failed": 0}
            ]
            assert result.deliveries_inserted == 2
            assert queue_seqs(path, S1) == [0, 1]
            assert queue_seqs(path, "__bus__:test:s2") == [1]
        finally:
            bus.close()


class TestConcurrency:
    def test_two_concurrent_ingesters_respect_max_pending(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            max_seen = 0

            async def main():
                nonlocal max_seen
                first = asyncio.create_task(
                    bus.ingest(
                        (Tick(seq=i) for i in range(5)), batch_size=5, max_pending=5
                    )
                )
                second = asyncio.create_task(
                    bus.ingest(
                        (Tick(seq=i) for i in range(5, 10)),
                        batch_size=5,
                        max_pending=5,
                    )
                )
                queue = bus._open_subscription_queue("s1")
                try:
                    while not (first.done() and second.done()):
                        max_seen = max(max_seen, pending(queue))
                        try:
                            queue.ack(queue.get_nowait())
                        except Empty:
                            await asyncio.sleep(0.001)
                    max_seen = max(max_seen, pending(queue))
                finally:
                    queue.close()
                return await asyncio.gather(first, second)

            results = run(asyncio.wait_for(main(), 15))
            assert sum(r.deliveries_inserted for r in results) == 10
            assert max_seen <= 5
        finally:
            bus.close()

    def test_two_buses_same_path_respect_max_pending(self, tmp_path):
        path = tmp_path / "bus"
        bus_a = make_bus(path)
        bus_b = make_bus(path)
        max_seen = 0
        try:

            async def main():
                nonlocal max_seen
                first = asyncio.create_task(
                    bus_a.ingest(
                        (Tick(seq=i) for i in range(4)), batch_size=4, max_pending=3
                    )
                )
                second = asyncio.create_task(
                    bus_b.ingest(
                        (Tick(seq=i) for i in range(10, 14)),
                        batch_size=4,
                        max_pending=3,
                    )
                )
                queue = bus_a._open_subscription_queue("s1")
                try:
                    while not (first.done() and second.done()):
                        max_seen = max(max_seen, pending(queue))
                        try:
                            queue.ack(queue.get_nowait())
                        except Empty:
                            await asyncio.sleep(0.001)
                    max_seen = max(max_seen, pending(queue))
                finally:
                    queue.close()
                return await asyncio.gather(first, second)

            results = run(asyncio.wait_for(main(), 15))
            assert sum(r.deliveries_inserted for r in results) == 8
            assert max_seen <= 3
        finally:
            bus_a.close()
            bus_b.close()

    def test_infinite_source_is_never_materialized(self, tmp_path):
        bus = make_bus(tmp_path / "bus", {"s1": [KeyedBp]})
        try:
            counter = itertools.count()

            def infinite():
                while True:
                    yield KeyedBp(key=str(next(counter)), value="v")

            async def main():
                task = asyncio.create_task(
                    bus.ingest(infinite(), batch_size=1, max_pending=2)
                )
                queue = bus._open_subscription_queue("s1")
                acked = 0
                try:
                    while acked < 5:
                        try:
                            queue.ack(queue.get_nowait())
                            acked += 1
                        except Empty:
                            await asyncio.sleep(0.001)
                finally:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                    queue.close()
                return acked

            acked = run(asyncio.wait_for(main(), 10))
            assert acked == 5
        finally:
            bus.close()
