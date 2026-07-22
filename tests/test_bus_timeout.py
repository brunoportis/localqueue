"""Async EventBus handler timeout semantics."""

from __future__ import annotations

import asyncio
import functools

import pytest
from localqueue.bus import BaseEvent, BusTopology, EventBus


class WorkSubmitted(BaseEvent):
    event_name = "timeout.work.submitted"

    sequence: int


def run(coro):
    return asyncio.run(coro)


def controlled_deadline(monkeypatch):
    """Replace wall-clock expiry with an explicitly released timer task."""
    from localqueue.bus import consumer

    started = asyncio.Event()
    release = asyncio.Event()
    cancelled = asyncio.Event()

    async def timer(timeout):
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    monkeypatch.setattr(consumer, "_deadline_timer", timer)
    return started, release, cancelled


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


class TestTimeoutPrecedenceAndCleanup:
    def test_user_timeouterror_is_a_transient_handler_failure(self, bus):
        @bus.on(WorkSubmitted, subscription="email", timeout=1.0)
        async def handle(event):
            raise TimeoutError("upstream timeout")

        bus.dispatch(WorkSubmitted(sequence=1))
        run(bus.run_subscription("email", idle_timeout=0.05))

        queue = bus._open_subscription_queue("email")
        try:
            assert queue.stats()["failed"] == 1
            assert queue.list_failed()[0]["last_error"] == "upstream timeout"
        finally:
            queue.close()

    def test_user_timeouterror_can_be_a_permanent_handler_failure(self, bus):
        @bus.on(
            WorkSubmitted,
            subscription="email",
            timeout=1.0,
            permanent_errors=(TimeoutError,),
        )
        async def handle(event):
            raise TimeoutError("upstream timeout")

        bus.dispatch(WorkSubmitted(sequence=1))
        run(bus.run_subscription("email", idle_timeout=0.05))

        queue = bus._open_subscription_queue("email")
        try:
            assert queue.stats()["failed"] == 1
            assert queue.list_failed()[0]["last_error"] == (
                "permanent failure: upstream timeout"
            )
        finally:
            queue.close()

    def test_internal_timeout_ignores_permanent_timeouterror(self, bus):
        cancelled = asyncio.Event()

        @bus.on(
            WorkSubmitted,
            subscription="email",
            timeout=0.01,
            permanent_errors=(TimeoutError,),
        )
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
            assert queue.list_failed()[0]["last_error"] == (
                "handler timeout after 0.01 seconds"
            )
        finally:
            queue.close()
        assert cancelled.is_set()

    def test_suppressed_cancellation_remains_an_internal_timeout(self, bus):
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()

        @bus.on(WorkSubmitted, subscription="email", timeout=0.01)
        async def handle(event):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cleanup_started.set()
                await release_cleanup.wait()
                return

        bus.dispatch(WorkSubmitted(sequence=1))

        async def consume():
            task = asyncio.create_task(bus.run_subscription("email", idle_timeout=0.05))
            await asyncio.wait_for(cleanup_started.wait(), timeout=1.0)
            release_cleanup.set()
            await task

        run(consume())

        queue = bus._open_subscription_queue("email")
        try:
            assert queue.stats()["acked"] == 0
            assert queue.stats()["failed"] == 1
            assert queue.list_failed()[0]["last_error"] == (
                "handler timeout after 0.01 seconds"
            )
        finally:
            queue.close()

    def test_cleanup_failure_does_not_replace_internal_timeout(self, bus, caplog):
        @bus.on(WorkSubmitted, subscription="email", timeout=0.01)
        async def handle(event):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise RuntimeError("cleanup failed")

        bus.dispatch(WorkSubmitted(sequence=1))
        run(bus.run_subscription("email", idle_timeout=0.05))

        queue = bus._open_subscription_queue("email")
        try:
            assert queue.stats()["failed"] == 1
            assert queue.list_failed()[0]["last_error"] == (
                "handler timeout after 0.01 seconds"
            )
        finally:
            queue.close()
        assert "cleanup" in caplog.text

    def test_async_callable_forms_are_accepted(self, bus):
        class CallableHandler:
            async def __call__(self, event):
                return None

        class MethodHandler:
            async def handle(self, event):
                return None

        method = MethodHandler()
        forms = [CallableHandler(), method.handle, functools.partial(method.handle)]
        for index, handler in enumerate(forms):
            instance = EventBus(
                str(bus.path / str(index)),
                name=f"form-{index}",
                topology=BusTopology({"email": [WorkSubmitted]}),
            )
            try:
                instance.on(WorkSubmitted, handler, subscription="email", timeout=1.0)
            finally:
                instance.close()


class TestControlledTimeoutLifecycle:
    def test_external_cancellation_during_timeout_cleanup_wins(
        self, bus, monkeypatch, caplog
    ):
        from localqueue.bus import consumer
        from localqueue.core import SimpleQueue

        timer_started, release_timer, _ = controlled_deadline(monkeypatch)
        handler_started = asyncio.Event()
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        handler_finished = asyncio.Event()
        heartbeat_started = asyncio.Event()
        heartbeat_cancelled = asyncio.Event()
        transitions = []

        async def heartbeat(queue, job, interval, state):
            heartbeat_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                heartbeat_cancelled.set()
                raise

        monkeypatch.setattr(consumer, "_heartbeat", heartbeat)
        for name in ("ack", "nack", "fail"):
            original = getattr(SimpleQueue, name)

            def transition(self, job, _original=original, _name=name, **kwargs):
                transitions.append(_name)
                return _original(self, job, **kwargs)

            monkeypatch.setattr(SimpleQueue, name, transition)

        @bus.on(WorkSubmitted, subscription="email", timeout=1.0)
        async def handle(event):
            handler_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cleanup_started.set()
                try:
                    await release_cleanup.wait()
                finally:
                    handler_finished.set()

        bus.dispatch(WorkSubmitted(sequence=1))

        async def consume_then_cancel_during_cleanup():
            task = asyncio.create_task(bus.run_subscription("email"))
            try:
                await asyncio.wait_for(handler_started.wait(), timeout=1.0)
                await asyncio.wait_for(timer_started.wait(), timeout=1.0)
                await asyncio.wait_for(heartbeat_started.wait(), timeout=1.0)
                release_timer.set()
                await asyncio.wait_for(cleanup_started.wait(), timeout=1.0)
                task.cancel()
                release_cleanup.set()
                with pytest.raises(asyncio.CancelledError):
                    await task
            finally:
                release_cleanup.set()
                if not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

            pending = [
                item
                for item in asyncio.all_tasks()
                if item is not asyncio.current_task() and not item.done()
            ]
            assert pending == []

        run(consume_then_cancel_during_cleanup())
        assert transitions == []
        assert handler_finished.is_set()
        assert heartbeat_cancelled.is_set()
        assert "handler timeout" not in caplog.text

    def test_direct_and_decorator_registration_keep_timeout_for_each_api(
        self, tmp_path
    ):
        async def handler(event):
            return None

        registrations = [
            lambda instance: instance.subscription("email").handler(
                WorkSubmitted, handler, timeout=1.0
            ),
            lambda instance: instance.subscription("email").handler(
                WorkSubmitted, timeout=1.0
            )(handler),
            lambda instance: instance.on(
                WorkSubmitted, handler, subscription="email", timeout=1.0
            ),
            lambda instance: instance.on(
                WorkSubmitted, subscription="email", timeout=1.0
            )(handler),
        ]
        for index, register in enumerate(registrations):
            instance = EventBus(
                str(tmp_path / str(index)),
                name=f"registration-{index}",
                topology=BusTopology({"email": [WorkSubmitted]}),
            )
            try:
                register(instance)
                assert (
                    instance._handlers[("email", "timeout.work.submitted")].timeout
                    == 1.0
                )
            finally:
                instance.close()

    def test_timeout_cleanup_keeps_heartbeat_until_nack(self, bus, monkeypatch):
        from localqueue.bus import consumer
        from localqueue.core import SimpleQueue

        timer_started, release_timer, _ = controlled_deadline(monkeypatch)
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        heartbeat_started = asyncio.Event()
        heartbeat_cancelled = asyncio.Event()
        nack_after_heartbeat = []
        original_nack = SimpleQueue.nack

        async def heartbeat(queue, job, interval, state):
            heartbeat_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                heartbeat_cancelled.set()
                raise

        def nack(self, job, **kwargs):
            nack_after_heartbeat.append(heartbeat_cancelled.is_set())
            return original_nack(self, job, **kwargs)

        monkeypatch.setattr(consumer, "_heartbeat", heartbeat)
        monkeypatch.setattr(SimpleQueue, "nack", nack)

        @bus.on(WorkSubmitted, subscription="email", timeout=1.0)
        async def handle(event):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cleanup_started.set()
                await release_cleanup.wait()

        bus.dispatch(WorkSubmitted(sequence=1))

        async def consume():
            task = asyncio.create_task(bus.run_subscription("email", idle_timeout=0.05))
            await asyncio.wait_for(timer_started.wait(), timeout=1.0)
            await asyncio.wait_for(heartbeat_started.wait(), timeout=1.0)
            release_timer.set()
            await asyncio.wait_for(cleanup_started.wait(), timeout=1.0)
            assert not heartbeat_cancelled.is_set()
            release_cleanup.set()
            await task

        run(consume())
        assert nack_after_heartbeat == [True]

    def test_external_cancellation_near_deadline_has_no_transition(
        self, bus, monkeypatch
    ):
        from localqueue.core import SimpleQueue

        timer_started, _release_timer, timer_cancelled = controlled_deadline(
            monkeypatch
        )
        handler_started = asyncio.Event()
        handler_cancelled = asyncio.Event()
        transitions = []

        for name in ("ack", "nack", "fail"):
            original = getattr(SimpleQueue, name)

            def transition(self, job, _original=original, _name=name, **kwargs):
                transitions.append(_name)
                return _original(self, job, **kwargs)

            monkeypatch.setattr(SimpleQueue, name, transition)

        @bus.on(WorkSubmitted, subscription="email", timeout=1.0)
        async def handle(event):
            handler_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                handler_cancelled.set()
                raise

        bus.dispatch(WorkSubmitted(sequence=1))

        async def consume_then_cancel():
            task = asyncio.create_task(bus.run_subscription("email"))
            await asyncio.wait_for(handler_started.wait(), timeout=1.0)
            await asyncio.wait_for(timer_started.wait(), timeout=1.0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        run(consume_then_cancel())
        assert transitions == []
        assert handler_cancelled.is_set()
        assert timer_cancelled.is_set()

    def test_first_attempt_times_out_and_second_attempt_succeeds(
        self, tmp_path, monkeypatch
    ):
        instance = EventBus(
            str(tmp_path / "retry"),
            name="retry",
            topology=BusTopology({"email": [WorkSubmitted]}),
            max_retries=1,
        )
        timer_started, release_timer, _ = controlled_deadline(monkeypatch)
        attempts = 0
        first_cancelled = asyncio.Event()

        @instance.on(WorkSubmitted, subscription="email", timeout=1.0)
        async def handle(event):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    first_cancelled.set()
                    raise

        instance.dispatch(WorkSubmitted(sequence=1))

        async def consume():
            task = asyncio.create_task(
                instance.run_subscription("email", idle_timeout=0.1)
            )
            await asyncio.wait_for(timer_started.wait(), timeout=1.0)
            release_timer.set()
            await task

        try:
            run(consume())
            queue = instance._open_subscription_queue("email")
            try:
                assert queue.stats()["acked"] == 1
                assert queue.stats()["failed"] == 0
            finally:
                queue.close()
        finally:
            instance.close()
        assert attempts == 2
        assert first_cancelled.is_set()

    def test_exact_and_wildcard_handlers_keep_individual_timeouts(
        self, tmp_path, monkeypatch
    ):
        class OtherWork(BaseEvent):
            event_name = "timeout.other.work"
            sequence: int

        instance = EventBus(
            str(tmp_path / "patterns"),
            name="patterns",
            topology=BusTopology({"email": [WorkSubmitted, OtherWork]}),
            max_retries=0,
        )
        timer_values = []
        release_timer = asyncio.Event()

        async def timer(timeout):
            timer_values.append(timeout)
            await release_timer.wait()

        monkeypatch.setattr("localqueue.bus.consumer._deadline_timer", timer)

        @instance.subscription("email", concurrency=2).handler(
            WorkSubmitted, timeout=1.0
        )
        async def exact(event):
            await asyncio.Event().wait()

        @instance.subscription("email").handler("*", timeout=2.0)
        async def wildcard(event):
            await asyncio.Event().wait()

        instance.dispatch(WorkSubmitted(sequence=1))
        instance.dispatch(OtherWork(sequence=2))

        async def consume():
            task = asyncio.create_task(
                instance.run_subscription("email", idle_timeout=0.05)
            )
            while sorted(timer_values) != [1.0, 2.0]:
                await asyncio.sleep(0)
            release_timer.set()
            await task

        try:
            run(consume())
        finally:
            instance.close()
        assert sorted(timer_values) == [1.0, 2.0]

    def test_timeout_waits_for_cleanup_before_freeing_only_its_slot(
        self, tmp_path, monkeypatch
    ):
        instance = EventBus(
            str(tmp_path / "concurrent"),
            name="concurrent",
            topology=BusTopology({"email": [WorkSubmitted]}),
            max_retries=0,
        )
        timer_started = asyncio.Event()
        release_timer = asyncio.Event()
        timer_calls = 0
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        second_started = asyncio.Event()
        release_second = asyncio.Event()
        third_started = asyncio.Event()
        nacked = asyncio.Event()

        async def timer(timeout):
            nonlocal timer_calls
            timer_calls += 1
            if timer_calls == 1:
                timer_started.set()
                await release_timer.wait()
            else:
                await asyncio.Future()

        from localqueue.core import SimpleQueue

        original_nack = SimpleQueue.nack

        def nack(self, job, **kwargs):
            nacked.set()
            return original_nack(self, job, **kwargs)

        monkeypatch.setattr("localqueue.bus.consumer._deadline_timer", timer)
        monkeypatch.setattr(SimpleQueue, "nack", nack)

        @instance.subscription("email", concurrency=2).handler(
            WorkSubmitted, timeout=1.0
        )
        async def handle(event):
            if event.sequence == 1:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cleanup_started.set()
                    await release_cleanup.wait()
            elif event.sequence == 2:
                second_started.set()
                await release_second.wait()
            else:
                third_started.set()

        instance.dispatch(WorkSubmitted(sequence=1))

        async def consume():
            task = asyncio.create_task(
                instance.run_subscription("email", idle_timeout=0.05)
            )
            await asyncio.wait_for(timer_started.wait(), timeout=1.0)
            instance.dispatch(WorkSubmitted(sequence=2))
            instance.dispatch(WorkSubmitted(sequence=3))
            await asyncio.wait_for(second_started.wait(), timeout=1.0)
            release_timer.set()
            await asyncio.wait_for(cleanup_started.wait(), timeout=1.0)
            assert not third_started.is_set()
            release_cleanup.set()
            await asyncio.wait_for(nacked.wait(), timeout=1.0)
            await asyncio.wait_for(third_started.wait(), timeout=1.0)
            release_second.set()
            await task

        try:
            run(consume())
        finally:
            instance.close()
