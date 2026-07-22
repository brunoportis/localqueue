"""Bounded in-process EventBus subscription concurrency."""

from __future__ import annotations

import asyncio
import threading

import pytest
from localqueue.bus import BaseEvent, BusTopology, EventBus


class WorkSubmitted(BaseEvent):
    sequence: int


def run(coro):
    return asyncio.run(coro)


def _concurrent_consumer_group_worker(path, processed, active, duplicates, peak, lock):
    """Consume with a per-process bound while recording aggregate activity."""
    bus = EventBus(
        path,
        name="group",
        topology=BusTopology({"email": [WorkSubmitted]}),
        lease_seconds=5.0,
    )

    def handle(event):
        event_id = str(event.event_id)
        with lock:
            if event_id in active:
                duplicates.append(event_id)
            active[event_id] = True
            peak.value = max(peak.value, len(active))
        import time

        time.sleep(0.05)
        with lock:
            processed.append(event_id)
            active.pop(event_id, None)

    bus.subscription("email", concurrency=2).handler(WorkSubmitted, handle)
    try:
        asyncio.run(bus.run_subscription("email", idle_timeout=1.0))
    finally:
        bus.close()


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

    @pytest.mark.parametrize("value", [True, 1.5, "2", object()])
    def test_rejects_non_integer_concurrency(self, bus, value):
        with pytest.raises(TypeError, match="concurrency"):
            bus.subscription("email", concurrency=value)

    def test_keeps_declared_concurrency_for_subsequent_binders(self, bus):
        configured = bus.subscription("email", concurrency=8)

        assert configured.concurrency == 8
        assert bus.subscription("email").concurrency == 8

    def test_old_binder_reflects_configuration_and_is_read_only(self, bus):
        old_binder = bus.subscription("email")
        bus.subscription("email", concurrency=8)
        new_binder = bus.subscription("email")

        assert old_binder.concurrency == 8
        assert new_binder.concurrency == 8
        with pytest.raises(AttributeError):
            old_binder.concurrency = 99

    def test_defaults_to_one_without_overwriting_configuration(self, bus):
        assert bus.subscription("email").concurrency == 1
        bus.subscription("email", concurrency=8)

        assert bus.subscription("email").concurrency == 8

    def test_repeating_explicit_value_is_idempotent(self, bus):
        bus.subscription("email", concurrency=2)

        assert bus.subscription("email", concurrency=2).concurrency == 2

    def test_rejects_conflicting_concurrency_for_same_subscription(self, bus):
        bus.subscription("email", concurrency=2)

        with pytest.raises(ValueError, match="already configured"):
            bus.subscription("email", concurrency=3)

    def test_compatibility_registration_preserves_existing_configuration(self, bus):
        bus.subscription("email", concurrency=8)
        bus.on(WorkSubmitted, lambda event: None, subscription="email")

        assert bus.subscription("email").concurrency == 8

    def test_rejects_configuration_after_consumer_has_started(self, bus):
        started = asyncio.Event()
        release = asyncio.Event()

        @bus.subscription("email").handler(WorkSubmitted)
        async def handle(event):
            started.set()
            await release.wait()

        bus.dispatch(WorkSubmitted(sequence=1))

        async def consume():
            task = asyncio.create_task(bus.run_subscription("email"))
            await asyncio.wait_for(started.wait(), timeout=1.0)
            with pytest.raises(RuntimeError, match="before run"):
                bus.subscription("email", concurrency=2)
            release.set()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        run(consume())


class TestBoundedSubscriptionConcurrency:
    def test_idle_timeout_waits_for_active_deliveries(self, bus):
        subscription = bus.subscription("email", concurrency=2)
        started = asyncio.Event()
        release = asyncio.Event()

        @subscription.handler(WorkSubmitted)
        async def handle(event):
            started.set()
            await release.wait()

        bus.dispatch(WorkSubmitted(sequence=1))

        async def consume():
            task = asyncio.create_task(bus.run_subscription("email", idle_timeout=0.05))
            await asyncio.wait_for(started.wait(), timeout=1.0)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), timeout=0.15)
            release.set()
            await asyncio.wait_for(task, timeout=1.0)

        run(consume())

    def test_sync_handlers_never_exceed_the_declared_bound(self, bus):
        subscription = bus.subscription("email", concurrency=2)
        entered = threading.Event()
        release = threading.Event()
        lock = threading.Lock()
        active = 0
        peak = 0

        @subscription.handler(WorkSubmitted)
        def handle(event):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
                if active == 2:
                    entered.set()
            release.wait(timeout=2.0)
            with lock:
                active -= 1

        for sequence in range(4):
            bus.dispatch(WorkSubmitted(sequence=sequence))

        async def consume():
            task = asyncio.create_task(bus.run_subscription("email", idle_timeout=0.1))
            assert await asyncio.to_thread(entered.wait, 1.0)
            release.set()
            await asyncio.wait_for(task, timeout=2.0)

        run(consume())

        assert peak == 2

    def test_full_slots_do_not_prefetch_more_deliveries(self, bus):
        subscription = bus.subscription("email", concurrency=3)
        entered = asyncio.Event()
        release = asyncio.Event()
        active = 0

        @subscription.handler(WorkSubmitted)
        async def handle(event):
            nonlocal active
            active += 1
            if active == 3:
                entered.set()
            await release.wait()
            active -= 1

        for sequence in range(6):
            bus.dispatch(WorkSubmitted(sequence=sequence))

        async def consume():
            task = asyncio.create_task(bus.run_subscription("email", idle_timeout=0.1))
            await asyncio.wait_for(entered.wait(), timeout=1.0)
            queue = bus._open_subscription_queue("email")
            try:
                stats = queue.stats()
                assert stats["processing"] == 3
                assert stats["ready"] == 3
            finally:
                queue.close()
            release.set()
            await asyncio.wait_for(task, timeout=2.0)

        run(consume())

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

    def test_cancellation_of_sync_handler_leaves_delivery_untransitioned(self, bus):
        subscription = bus.subscription("email", concurrency=1)
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        @subscription.handler(WorkSubmitted)
        def handle(event):
            started.set()
            release.wait(timeout=2.0)
            finished.set()

        bus.dispatch(WorkSubmitted(sequence=1))

        async def consume_then_cancel():
            task = asyncio.create_task(bus.run_subscription("email"))
            assert await asyncio.to_thread(started.wait, 1.0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
            assert await asyncio.to_thread(finished.wait, 1.0)

        run(consume_then_cancel())

        queue = bus._open_subscription_queue("email")
        try:
            stats = queue.stats()
            assert stats["acked"] == 0
            assert stats["failed"] == 0
            assert stats["processing"] == 1
        finally:
            queue.close()


class TestSingleSubscriptionRunner:
    def test_rejects_overlapping_run_subscription_calls(self, bus):
        started = asyncio.Event()
        release = asyncio.Event()

        @bus.subscription("email").handler(WorkSubmitted)
        async def handle(event):
            started.set()
            await release.wait()

        bus.dispatch(WorkSubmitted(sequence=1))

        async def consume():
            first = asyncio.create_task(bus.run_subscription("email"))
            await asyncio.wait_for(started.wait(), timeout=1.0)
            with pytest.raises(RuntimeError, match="already running"):
                await bus.run_subscription("email")
            release.set()
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first

        run(consume())

    def test_releases_runner_after_queue_open_failure(self, bus, monkeypatch):
        bus.on(WorkSubmitted, lambda event: None, subscription="email")
        original_open = bus._open_subscription_queue

        def fail_open(subscription):
            raise OSError("open failed")

        monkeypatch.setattr(bus, "_open_subscription_queue", fail_open)
        with pytest.raises(OSError, match="open failed"):
            run(bus.run_subscription("email"))
        assert bus._running_subscriptions == set()

        monkeypatch.setattr(bus, "_open_subscription_queue", original_open)
        bus.dispatch(WorkSubmitted(sequence=1))
        run(bus.run_subscription("email", idle_timeout=0.05))

    def test_releases_runner_after_unexpected_delivery_failure(self, bus):
        class Fatal(BaseException):
            pass

        @bus.subscription("email").handler(WorkSubmitted)
        async def handle(event):
            raise Fatal("fatal")

        bus.dispatch(WorkSubmitted(sequence=1))
        with pytest.raises(Fatal, match="fatal"):
            run(bus.run_subscription("email"))

        assert bus._running_subscriptions == set()

    def test_run_and_run_subscription_cannot_overlap_same_subscription(self, tmp_path):
        instance = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology(
                {"email": [WorkSubmitted], "billing": [WorkSubmitted]}
            ),
        )
        started = asyncio.Event()
        release = asyncio.Event()

        @instance.subscription("email").handler(WorkSubmitted)
        async def email_handler(event):
            started.set()
            await release.wait()

        @instance.subscription("billing").handler(WorkSubmitted)
        async def billing_handler(event):
            await release.wait()

        instance.dispatch(WorkSubmitted(sequence=1))

        async def consume():
            task = asyncio.create_task(instance.run())
            await asyncio.wait_for(started.wait(), timeout=1.0)
            with pytest.raises(RuntimeError, match="already running"):
                await instance.run_subscription("email")
            release.set()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        try:
            run(consume())
        finally:
            instance.close()

    def test_two_run_calls_cannot_overlap(self, tmp_path):
        instance = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology(
                {"email": [WorkSubmitted], "billing": [WorkSubmitted]}
            ),
        )
        started = asyncio.Event()
        release = asyncio.Event()

        @instance.subscription("email").handler(WorkSubmitted)
        async def email_handler(event):
            started.set()
            await release.wait()

        @instance.subscription("billing").handler(WorkSubmitted)
        async def billing_handler(event):
            await release.wait()

        instance.dispatch(WorkSubmitted(sequence=1))

        async def consume():
            first = asyncio.create_task(instance.run())
            await asyncio.wait_for(started.wait(), timeout=1.0)
            with pytest.raises(RuntimeError, match="already running"):
                await instance.run()
            release.set()
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first

        try:
            run(consume())
        finally:
            instance.close()


class TestConcurrentDeliveryFailures:
    def test_observes_every_completed_delivery_before_propagating(self, bus):
        class FirstFatal(BaseException):
            pass

        class SecondFatal(BaseException):
            pass

        release = asyncio.Event()
        entered = 0

        @bus.subscription("email", concurrency=2).handler(WorkSubmitted)
        async def handle(event):
            nonlocal entered
            entered += 1
            if entered == 2:
                release.set()
            await release.wait()
            await asyncio.sleep(0)
            if event.sequence == 1:
                raise FirstFatal("first")
            raise SecondFatal("second")

        bus.dispatch(WorkSubmitted(sequence=1))
        bus.dispatch(WorkSubmitted(sequence=2))

        with pytest.raises(FirstFatal, match="first"):
            run(bus.run_subscription("email"))
        assert bus._running_subscriptions == set()


class TestRunFailureCleanup:
    def test_run_cancels_sibling_subscription_after_unexpected_failure(self, tmp_path):
        class Fatal(BaseException):
            pass

        instance = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology(
                {"email": [WorkSubmitted], "billing": [WorkSubmitted]}
            ),
        )
        billing_started = asyncio.Event()
        billing_cancelled = asyncio.Event()

        @instance.subscription("billing").handler(WorkSubmitted)
        async def billing_handler(event):
            billing_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                billing_cancelled.set()
                raise

        @instance.subscription("email").handler(WorkSubmitted)
        async def email_handler(event):
            await billing_started.wait()
            raise Fatal("email failed")

        instance.dispatch(WorkSubmitted(sequence=1))
        instance.dispatch(WorkSubmitted(sequence=2))
        try:
            with pytest.raises(Fatal, match="email failed"):
                run(instance.run())
            assert billing_cancelled.is_set()
            queue = instance._open_subscription_queue("billing")
            try:
                assert queue.stats()["ready"] == 1
            finally:
                queue.close()
            assert instance._running_subscriptions == set()
        finally:
            instance.close()


class TestConcurrentConsumerGroup:
    def test_each_process_applies_its_own_bound(self, tmp_path):
        import multiprocessing

        path = str(tmp_path / "group")
        topology = BusTopology({"email": [WorkSubmitted]})
        producer = EventBus(path, name="group", topology=topology, lease_seconds=5.0)
        event_ids = []
        for sequence in range(16):
            event = WorkSubmitted(sequence=sequence)
            event_ids.append(str(event.event_id))
            producer.dispatch(event)
        producer.close()

        context = multiprocessing.get_context("spawn")
        with context.Manager() as manager:
            processed = manager.list()
            active = manager.dict()
            duplicates = manager.list()
            peak = manager.Value("i", 0)
            lock = manager.Lock()
            processes = [
                context.Process(
                    target=_concurrent_consumer_group_worker,
                    args=(path, processed, active, duplicates, peak, lock),
                )
                for _ in range(2)
            ]
            try:
                for process in processes:
                    process.start()
                for process in processes:
                    process.join(timeout=30)
                assert [process.exitcode for process in processes] == [0, 0]
            finally:
                for process in processes:
                    if process.is_alive():
                        process.terminate()
                    process.join(timeout=5)

            assert list(duplicates) == []
            assert sorted(processed) == sorted(event_ids)
            assert peak.value > 2

        verifier = EventBus(path, name="group", topology=topology)
        queue = verifier._open_subscription_queue("email")
        try:
            stats = queue.stats()
            assert stats["acked"] == len(event_ids)
            assert stats["processing"] == 0
        finally:
            queue.close()
            verifier.close()


class TestIndependentSubscriptions:
    def test_run_applies_each_subscription_bound_independently(self, tmp_path):
        instance = EventBus(
            str(tmp_path / "bus"),
            name="test",
            topology=BusTopology(
                {"email": [WorkSubmitted], "billing": [WorkSubmitted]}
            ),
        )
        email = instance.subscription("email", concurrency=3)
        billing = instance.subscription("billing", concurrency=1)
        email_active = 0
        email_peak = 0
        billing_active = 0
        billing_peak = 0
        email_started = asyncio.Event()
        billing_started = asyncio.Event()
        release = asyncio.Event()

        @email.handler(WorkSubmitted)
        async def send_email(event):
            nonlocal email_active, email_peak
            email_active += 1
            email_peak = max(email_peak, email_active)
            if email_active == 3:
                email_started.set()
            await release.wait()
            email_active -= 1

        @billing.handler(WorkSubmitted)
        async def charge(event):
            nonlocal billing_active, billing_peak
            billing_active += 1
            billing_peak = max(billing_peak, billing_active)
            billing_started.set()
            await release.wait()
            billing_active -= 1

        for sequence in range(3):
            instance.dispatch(WorkSubmitted(sequence=sequence))

        async def consume():
            task = asyncio.create_task(instance.run(idle_timeout=0.1))
            await asyncio.wait_for(email_started.wait(), timeout=1.0)
            await asyncio.wait_for(billing_started.wait(), timeout=1.0)
            assert email_peak == 3
            assert billing_peak == 1
            release.set()
            await asyncio.wait_for(task, timeout=2.0)

        try:
            run(consume())
        finally:
            instance.close()

        assert email_peak == 3
        assert billing_peak == 1


class TestConcurrentCausality:
    def test_preserves_causality_metadata_for_concurrent_deliveries(self, bus):
        subscription = bus.subscription("email", concurrency=2)
        received = []
        completed = asyncio.Event()

        @subscription.handler(WorkSubmitted)
        async def handle(event):
            received.append(event)
            if len(received) == 2:
                completed.set()

        root = WorkSubmitted(sequence=1)
        child = WorkSubmitted.from_parent(root, sequence=2)
        bus.dispatch(root)
        bus.dispatch(child)

        async def consume():
            task = asyncio.create_task(bus.run_subscription("email", idle_timeout=0.1))
            await asyncio.wait_for(completed.wait(), timeout=1.0)
            await asyncio.wait_for(task, timeout=1.0)

        run(consume())

        received_by_id = {event.event_id: event for event in received}
        assert received_by_id[root.event_id].correlation_id == root.correlation_id
        assert received_by_id[root.event_id].causation_id is None
        assert received_by_id[child.event_id].correlation_id == root.correlation_id
        assert received_by_id[child.event_id].causation_id == root.event_id
