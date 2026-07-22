"""Bounded in-process EventBus subscription concurrency."""

from __future__ import annotations

import asyncio

import pytest
from localqueue.bus import BaseEvent, BusTopology, EventBus


class WorkSubmitted(BaseEvent):
    sequence: int


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def bus(tmp_path):
    instance = EventBus(
        str(tmp_path / "bus"),
        name="test",
        topology=BusTopology({"email": [WorkSubmitted]}),
        lease_seconds=5.0,
    )
    yield instance
    instance.close()


class TestSubscriptionConcurrencyConfiguration:
    @pytest.mark.parametrize("value", [0, -1])
    def test_rejects_non_positive_concurrency(self, bus, value):
        with pytest.raises(ValueError, match="concurrency"):
            bus.subscription("email", concurrency=value)

    @pytest.mark.parametrize("value", [True, 1.5, "2", None])
    def test_rejects_non_integer_concurrency(self, bus, value):
        with pytest.raises(TypeError, match="concurrency"):
            bus.subscription("email", concurrency=value)

    def test_keeps_declared_concurrency_for_subsequent_binders(self, bus):
        configured = bus.subscription("email", concurrency=8)

        assert configured.concurrency == 8
        assert bus.subscription("email").concurrency == 8

    def test_rejects_conflicting_concurrency_for_same_subscription(self, bus):
        bus.subscription("email", concurrency=2)

        with pytest.raises(ValueError, match="already configured"):
            bus.subscription("email", concurrency=3)


class TestBoundedSubscriptionConcurrency:
    def test_each_concurrent_delivery_keeps_its_own_heartbeat(self, tmp_path):
        instance = EventBus(
            str(tmp_path / "bus"),
            name="test",
            topology=BusTopology({"email": [WorkSubmitted]}),
            lease_seconds=0.15,
        )
        subscription = instance.subscription("email", concurrency=2)
        completed: list[int] = []

        @subscription.handler(WorkSubmitted)
        async def handle(event):
            await asyncio.sleep(0.35)
            completed.append(event.sequence)

        instance.dispatch(WorkSubmitted(sequence=1))
        instance.dispatch(WorkSubmitted(sequence=2))
        try:
            run(instance.run_subscription("email", idle_timeout=0.1))
            queue = instance._open_subscription_queue("email")
            try:
                assert queue.stats()["acked"] == 2
            finally:
                queue.close()
        finally:
            instance.close()

        assert sorted(completed) == [1, 2]

    def test_never_runs_more_handlers_than_the_declared_bound(self, bus):
        subscription = bus.subscription("email", concurrency=2)
        active = 0
        peak = 0
        started = asyncio.Event()
        release = asyncio.Event()
        completed: list[int] = []

        @subscription.handler(WorkSubmitted)
        async def handle(event):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            if active == 2:
                started.set()
            await release.wait()
            completed.append(event.sequence)
            active -= 1

        for sequence in range(4):
            bus.dispatch(WorkSubmitted(sequence=sequence))

        async def consume():
            task = asyncio.create_task(bus.run_subscription("email", idle_timeout=0.1))
            await asyncio.wait_for(started.wait(), timeout=1.0)
            assert peak == 2
            release.set()
            await asyncio.wait_for(task, timeout=2.0)

        run(consume())

        assert peak == 2
        assert sorted(completed) == [0, 1, 2, 3]

    def test_concurrent_deliveries_do_not_promise_completion_order(self, bus):
        subscription = bus.subscription("email", concurrency=2)
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        completed: list[int] = []

        @subscription.handler(WorkSubmitted)
        async def handle(event):
            if event.sequence == 1:
                first_started.set()
                await release_first.wait()
            completed.append(event.sequence)

        bus.dispatch(WorkSubmitted(sequence=1))
        bus.dispatch(WorkSubmitted(sequence=2))

        async def consume():
            task = asyncio.create_task(bus.run_subscription("email", idle_timeout=0.1))
            await asyncio.wait_for(first_started.wait(), timeout=1.0)
            for _ in range(50):
                if completed == [2]:
                    break
                await asyncio.sleep(0.01)
            assert completed == [2]
            release_first.set()
            await asyncio.wait_for(task, timeout=2.0)

        run(consume())

        assert completed == [2, 1]

    def test_handler_failure_releases_a_slot_for_another_delivery(self, bus):
        subscription = bus.subscription("email", concurrency=2)
        release_slow = asyncio.Event()
        third_started = asyncio.Event()
        active = 0
        peak = 0

        @subscription.handler(WorkSubmitted, permanent_errors=(RuntimeError,))
        async def handle(event):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                if event.sequence == 1:
                    raise RuntimeError("transient")
                if event.sequence == 2:
                    await release_slow.wait()
                if event.sequence == 3:
                    third_started.set()
            finally:
                active -= 1

        for sequence in (1, 2, 3):
            bus.dispatch(WorkSubmitted(sequence=sequence))

        async def consume():
            task = asyncio.create_task(bus.run_subscription("email", idle_timeout=0.1))
            await asyncio.wait_for(third_started.wait(), timeout=1.0)
            assert peak == 2
            release_slow.set()
            await asyncio.wait_for(task, timeout=2.0)

        run(consume())

        assert peak == 2

    def test_cancellation_stops_new_claims_and_cancels_active_async_handlers(self, bus):
        subscription = bus.subscription("email", concurrency=1)
        handler_started = asyncio.Event()
        handler_cancelled = asyncio.Event()

        @subscription.handler(WorkSubmitted)
        async def handle(event):
            handler_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                handler_cancelled.set()
                raise

        bus.dispatch(WorkSubmitted(sequence=1))
        bus.dispatch(WorkSubmitted(sequence=2))

        async def consume_then_cancel():
            task = asyncio.create_task(bus.run_subscription("email"))
            await asyncio.wait_for(handler_started.wait(), timeout=1.0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        run(consume_then_cancel())

        assert handler_cancelled.is_set()
        queue = bus._open_subscription_queue("email")
        try:
            assert queue.stats()["ready"] == 1
        finally:
            queue.close()
