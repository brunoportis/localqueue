"""Async EventBus handler timeout semantics."""

from __future__ import annotations

import asyncio

import pytest
from localqueue.bus import BaseEvent, BusTopology, EventBus


class WorkSubmitted(BaseEvent):
    event_name = "timeout.work.submitted"

    sequence: int


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def bus(tmp_path):
    instance = EventBus(
        str(tmp_path / "bus"),
        name="test",
        topology=BusTopology({"email": [WorkSubmitted]}),
        lease_seconds=0.15,
        max_retries=0,
    )
    yield instance
    instance.close()


class TestHandlerTimeoutConfiguration:
    @pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
    def test_rejects_non_positive_or_unbounded_timeout(self, bus, timeout):
        with pytest.raises(ValueError, match="timeout"):
            bus.on(
                WorkSubmitted,
                lambda event: None,
                subscription="email",
                timeout=timeout,
            )

    @pytest.mark.parametrize("timeout", [True, "0.1", object()])
    def test_rejects_non_numeric_timeout(self, bus, timeout):
        with pytest.raises(TypeError, match="timeout"):
            bus.on(
                WorkSubmitted,
                lambda event: None,
                subscription="email",
                timeout=timeout,
            )

    def test_rejects_timeout_for_sync_handler(self, bus):
        with pytest.raises(TypeError, match="async handlers"):
            bus.on(
                WorkSubmitted,
                lambda event: None,
                subscription="email",
                timeout=0.1,
            )


class TestAsyncHandlerTimeout:
    def test_async_handler_completing_before_deadline_is_acked(self, bus):
        completed: list[int] = []

        @bus.on(WorkSubmitted, subscription="email", timeout=0.1)
        async def handle(event):
            await asyncio.sleep(0)
            completed.append(event.sequence)

        bus.dispatch(WorkSubmitted(sequence=1))
        run(bus.run_subscription("email", idle_timeout=0.05))

        queue = bus._open_subscription_queue("email")
        try:
            assert queue.stats()["acked"] == 1
            assert queue.stats()["failed"] == 0
        finally:
            queue.close()
        assert completed == [1]

    def test_timeout_cancels_handler_and_records_distinct_error(self, bus):
        cancelled = asyncio.Event()

        @bus.on(WorkSubmitted, subscription="email", timeout=0.02)
        async def handle(event):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        bus.dispatch(WorkSubmitted(sequence=1))
        run(bus.run_subscription("email", idle_timeout=0.05))

        queue = bus._open_subscription_queue("email")
        try:
            assert queue.stats()["acked"] == 0
            assert queue.stats()["failed"] == 1
            assert (
                "handler timeout after 0.02 seconds"
                in queue.list_failed()[0]["last_error"]
            )
        finally:
            queue.close()
        assert cancelled.is_set()

    def test_permanent_errors_still_fail_with_a_timeout_configured(self, bus):
        class PermanentError(Exception):
            pass

        @bus.on(
            WorkSubmitted,
            subscription="email",
            timeout=0.1,
            permanent_errors=(PermanentError,),
        )
        async def handle(event):
            raise PermanentError("do not retry")

        bus.dispatch(WorkSubmitted(sequence=1))
        run(bus.run_subscription("email", idle_timeout=0.05))

        queue = bus._open_subscription_queue("email")
        try:
            assert queue.stats()["failed"] == 1
            assert (
                "permanent failure: do not retry"
                in queue.list_failed()[0]["last_error"]
            )
        finally:
            queue.close()

    def test_external_cancellation_is_not_reported_as_timeout(self, bus):
        started = asyncio.Event()
        cancelled = asyncio.Event()

        @bus.on(WorkSubmitted, subscription="email", timeout=1.0)
        async def handle(event):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        bus.dispatch(WorkSubmitted(sequence=1))

        async def consume_then_cancel():
            task = asyncio.create_task(bus.run_subscription("email"))
            await asyncio.wait_for(started.wait(), timeout=1.0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        run(consume_then_cancel())

        queue = bus._open_subscription_queue("email")
        try:
            assert queue.stats()["acked"] == 0
            assert queue.stats()["failed"] == 0
        finally:
            queue.close()
        assert cancelled.is_set()

    def test_lease_loss_prevents_ack_when_handler_finishes_near_deadline(
        self, bus, monkeypatch
    ):
        from localqueue.bus import consumer

        lease_lost = asyncio.Event()
        heartbeat_cancelled = asyncio.Event()

        async def lose_lease(queue, job, interval, state):
            state["lease_lost"] = True
            lease_lost.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                heartbeat_cancelled.set()
                raise

        monkeypatch.setattr(consumer, "_heartbeat", lose_lease)

        @bus.on(WorkSubmitted, subscription="email", timeout=0.1)
        async def handle(event):
            await lease_lost.wait()
            await asyncio.sleep(0)

        bus.dispatch(WorkSubmitted(sequence=1))
        run(bus.run_subscription("email", idle_timeout=0.05))

        queue = bus._open_subscription_queue("email")
        try:
            assert queue.stats()["acked"] == 0
            assert queue.stats()["processing"] == 1
        finally:
            queue.close()
        assert heartbeat_cancelled.is_set()
