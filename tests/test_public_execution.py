from __future__ import annotations

import asyncio
from datetime import timezone
from uuid import uuid4

import pytest
from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    ExecutionFailed,
    ExecutionResult,
    Reject,
    Retry,
    RetryPolicy,
    SequenceSource,
    event,
)
from localqueue.bus.execution import _ExecutionSnapshot


@event(identity="key")
class ExecuteRoot(BaseEvent):
    event_name = "execute.root"

    key: str


@event(identity="key")
class ExecuteChild(BaseEvent):
    event_name = "execute.child"

    key: str


def test_execute_empty_source_returns_public_terminal_result(tmp_path) -> None:
    bus = EventBus(str(tmp_path))

    @bus.source(SequenceSource([], fingerprint="empty-v1"), checkpoint="empty")
    def empty(value: str) -> ExecuteRoot:
        return ExecuteRoot(key=value)

    try:
        result = asyncio.run(bus.execute(empty))
        assert isinstance(result, ExecutionResult)
        assert result.source_completed and result.completed and result.succeeded
        assert result.deliveries_total == 0
        assert result.completed_at.tzinfo is timezone.utc
        assert result.created_at.tzinfo is timezone.utc
        result.raise_for_failures()
        assert bus._native_queue is not None
    finally:
        bus.close()


def test_execute_starts_local_handlers_and_waits_for_descendants(tmp_path) -> None:
    bus = EventBus(str(tmp_path))
    handled: list[str] = []

    @bus.handler(ExecuteRoot)
    async def create_child(root: ExecuteRoot) -> ExecuteChild:
        handled.append(f"root:{root.key}")
        return ExecuteChild(key=root.key)

    @bus.handler(ExecuteChild)
    async def audit(child: ExecuteChild) -> None:
        handled.append(f"child:{child.key}")

    @bus.source(SequenceSource(["one"], fingerprint="one-v1"), checkpoint="one")
    def source(value: str) -> ExecuteRoot:
        return ExecuteRoot(key=value)

    try:
        result = asyncio.run(bus.execute(source, timeout=2))
        assert handled == ["root:one", "child:one"]
        assert result.deliveries_total == 2
        assert result.deliveries_acknowledged == 2
        assert result.deliveries_failed == 0
        assert bus._execute_runner_task is None
    finally:
        bus.close()


def test_execute_returns_terminal_failures_for_explicit_opt_in(tmp_path) -> None:
    bus = EventBus(str(tmp_path))

    @bus.handler(ExecuteRoot)
    async def reject(_event: ExecuteRoot) -> None:
        raise Reject("invalid")

    @bus.source(SequenceSource(["bad"], fingerprint="bad-v1"), checkpoint="bad")
    def source(value: str) -> ExecuteRoot:
        return ExecuteRoot(key=value)

    try:
        result = asyncio.run(bus.execute(source, timeout=2))
        assert result.completed and not result.succeeded
        assert result.deliveries_failed == 1
        with pytest.raises(ExecutionFailed) as raised:
            result.raise_for_failures()
        assert raised.value.result is result
        assert str(result.execution_id) in str(raised.value)
    finally:
        bus.close()


def test_subscription_config_is_shared_mutable_state_and_freezes(tmp_path) -> None:
    bus = EventBus(str(tmp_path), concurrency=3)

    @bus.handler(ExecuteRoot)
    async def handle(_event: ExecuteRoot) -> None:
        return None

    subscription = bus.subscription(ExecuteRoot.event_name)
    another_facade = bus.subscription(ExecuteRoot.event_name).config
    retry = RetryPolicy.fixed(max_attempts=2, delay=0)
    assert subscription.config.concurrency == 3
    assert subscription.config.frozen is False

    subscription.config.concurrency = 5
    subscription.config.retry = retry
    assert another_facade.concurrency == 5
    assert another_facade.retry is retry

    async def consume() -> None:
        runner = asyncio.create_task(bus.run())
        while not subscription.config.frozen:
            await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="configuration must be set before run"):
            subscription.config.concurrency = 2
        runner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runner

    try:
        asyncio.run(consume())
    finally:
        bus.close()


def test_concurrent_execute_calls_share_and_reference_count_managed_runner(
    tmp_path, monkeypatch
) -> None:
    bus = EventBus(str(tmp_path))

    @bus.handler(ExecuteRoot)
    async def handle(_event: ExecuteRoot) -> None:
        return None

    runner_started = asyncio.Event()
    runner_stopped = asyncio.Event()
    run_calls = 0

    async def fake_run(*, idle_timeout=None) -> None:
        nonlocal run_calls
        assert idle_timeout is None
        run_calls += 1
        runner_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            runner_stopped.set()

    class Handle:
        def __init__(self, done: asyncio.Event) -> None:
            self.done = done
            self.resumed = False

        async def run(self) -> _ExecutionSnapshot:
            await self.done.wait()
            now = 1_700_000_000_000
            return _ExecutionSnapshot(
                uuid4(),
                "source",
                "checkpoint",
                "v1",
                "generation",
                True,
                now,
                now,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                None,
                now,
                now,
            )

    first_done, second_done = asyncio.Event(), asyncio.Event()
    handles = iter((Handle(first_done), Handle(second_done)))

    async def fake_open(_source):
        return next(handles)

    monkeypatch.setattr(bus, "run", fake_run)
    monkeypatch.setattr(bus, "_open_execution", fake_open)

    async def scenario() -> None:
        first = asyncio.create_task(bus.execute(object()))  # type: ignore[arg-type]
        second = asyncio.create_task(bus.execute(object()))  # type: ignore[arg-type]
        await runner_started.wait()
        assert run_calls == 1
        assert bus._execute_runner_users == 2

        first_done.set()
        await first
        assert not runner_stopped.is_set()
        assert bus._execute_runner_users == 1

        second_done.set()
        await second
        await runner_stopped.wait()
        assert bus._execute_runner_task is None
        assert bus._execute_runner_users == 0

    try:
        asyncio.run(scenario())
    finally:
        bus.close()


def test_timeout_preserves_progress_and_same_execution_resumes(tmp_path) -> None:
    topology = BusTopology({"roots": [ExecuteRoot]})
    bus = EventBus(str(tmp_path), topology=topology)

    @bus.source(SequenceSource(["one"], fingerprint="timeout-v1"), checkpoint="timeout")
    def source(value: str) -> ExecuteRoot:
        return ExecuteRoot(key=value)

    async def time_out() -> None:
        with pytest.raises(TimeoutError):
            await bus.execute(source, timeout=0.1)

    try:
        asyncio.run(time_out())
        assert bus._execute_runner_task is None

        @bus.subscription("roots").handler(ExecuteRoot)
        async def handle(_event: ExecuteRoot) -> None:
            return None

        result = asyncio.run(bus.execute(source, timeout=2))
        assert result.resumed is True
        assert result.items_committed == 1
        assert result.deliveries_acknowledged == 1
        assert bus._native_queue is not None
    finally:
        bus.close()


def test_delayed_retry_blocks_execution_until_acknowledged(tmp_path) -> None:
    bus = EventBus(str(tmp_path))
    attempts = 0

    @bus.handler(ExecuteRoot)
    async def retry_once(_event: ExecuteRoot) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise Retry("later", after=0.15)

    @bus.source(SequenceSource(["one"], fingerprint="retry-v1"), checkpoint="retry")
    def source(value: str) -> ExecuteRoot:
        return ExecuteRoot(key=value)

    try:
        result = asyncio.run(bus.execute(source, timeout=2))
        assert attempts == 2
        assert result.deliveries_acknowledged == 1
        assert result.deliveries_ready == 0
    finally:
        bus.close()


def test_retry_failed_reopens_and_refinalizes_same_execution(tmp_path) -> None:
    bus = EventBus(str(tmp_path))
    should_fail = True

    @bus.handler(ExecuteRoot)
    async def maybe_reject(_event: ExecuteRoot) -> None:
        if should_fail:
            raise Reject("first attempt")

    @bus.source(SequenceSource(["one"], fingerprint="replay-v1"), checkpoint="replay")
    def source(value: str) -> ExecuteRoot:
        return ExecuteRoot(key=value)

    try:
        first = asyncio.run(bus.execute(source, timeout=2))
        failed = bus.subscription(ExecuteRoot.event_name).list_failed()
        assert first.deliveries_failed == len(failed) == 1

        should_fail = False
        bus.subscription(ExecuteRoot.event_name).retry_failed(failed[0].id)
        second = asyncio.run(bus.execute(source, timeout=2))

        assert second.execution_id == first.execution_id
        assert second.resumed is True
        assert second.deliveries_failed == 0
        assert second.deliveries_acknowledged == 1
    finally:
        bus.close()
