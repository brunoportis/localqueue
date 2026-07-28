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


def terminal_snapshot() -> _ExecutionSnapshot:
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


class ControlledHandle:
    resumed = False

    def __init__(self, done: asyncio.Event | None = None) -> None:
        self.done = done

    async def run(self) -> _ExecutionSnapshot:
        if self.done is not None:
            await self.done.wait()
        return terminal_snapshot()


async def wait_until(predicate) -> None:
    while not predicate():
        await asyncio.sleep(0)


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


def test_handler_and_config_reject_conflicts_in_both_directions(tmp_path) -> None:
    first_policy = RetryPolicy.fixed(max_attempts=2, delay=0)
    other_policy = RetryPolicy.fixed(max_attempts=3, delay=0)

    handler_first = EventBus(str(tmp_path / "handler-first"))

    @handler_first.handler(ExecuteRoot, concurrency=5, retry=first_policy)
    async def first_handler(_event: ExecuteRoot) -> None:
        return None

    first_config = handler_first.subscription(ExecuteRoot.event_name).config
    first_config.concurrency = 5
    first_config.retry = first_policy
    with pytest.raises(ValueError, match="concurrency=5"):
        first_config.concurrency = 6
    with pytest.raises(ValueError, match="conflicting retry policy"):
        first_config.retry = other_policy
    with pytest.raises(ValueError, match="conflicting retry policy"):
        first_config.retry = None

    topology = BusTopology({"shared": [ExecuteRoot, ExecuteChild]})
    config_first = EventBus(str(tmp_path / "config-first"), topology=topology)
    shared = config_first.subscription("shared")
    shared.config.concurrency = 5
    shared.config.retry = first_policy

    @config_first.handler(
        ExecuteRoot,
        subscription="shared",
        concurrency=5,
        retry=first_policy,
    )
    async def same_handler(_event: ExecuteRoot) -> None:
        return None

    with pytest.raises(ValueError, match="concurrency=5"):

        @config_first.handler(
            ExecuteChild,
            subscription="shared",
            concurrency=6,
        )
        async def conflicting_concurrency(_event: ExecuteChild) -> None:
            return None

    with pytest.raises(ValueError, match="conflicting retry policy"):

        @config_first.handler(
            ExecuteChild,
            subscription="shared",
            retry=other_policy,
        )
        async def conflicting_retry(_event: ExecuteChild) -> None:
            return None

    handler_first.close()
    config_first.close()


def test_subscription_binder_and_config_reject_conflicts_in_both_directions(
    tmp_path,
) -> None:
    binder_first = EventBus(
        str(tmp_path / "binder-first"),
        topology=BusTopology({"shared": [ExecuteRoot]}),
    )
    config = binder_first.subscription("shared", concurrency=4).config
    config.concurrency = 4
    with pytest.raises(ValueError, match="concurrency=4"):
        config.concurrency = 5

    config_first = EventBus(
        str(tmp_path / "config-first"),
        topology=BusTopology({"shared": [ExecuteRoot]}),
    )
    config_first.subscription("shared").config.concurrency = 4
    assert config_first.subscription("shared", concurrency=4).concurrency == 4
    with pytest.raises(ValueError, match="concurrency=4"):
        config_first.subscription("shared", concurrency=5)

    binder_first.close()
    config_first.close()


def test_frozen_subscription_rejects_equal_and_different_config_values(
    tmp_path,
) -> None:
    bus = EventBus(str(tmp_path))
    retry = RetryPolicy.fixed(max_attempts=2, delay=0)

    @bus.handler(ExecuteRoot, concurrency=3, retry=retry)
    async def handle(_event: ExecuteRoot) -> None:
        return None

    subscription = bus.subscription(ExecuteRoot.event_name)

    async def scenario() -> None:
        runner = asyncio.create_task(bus.run())
        await wait_until(lambda: subscription.config.frozen)
        for concurrency in (3, 4):
            with pytest.raises(RuntimeError, match="configuration must be set"):
                subscription.config.concurrency = concurrency
            with pytest.raises(RuntimeError, match="configuration must be set"):
                bus.subscription(ExecuteRoot.event_name, concurrency=concurrency)
        for policy in (
            retry,
            RetryPolicy.fixed(max_attempts=3, delay=0),
            None,
        ):
            with pytest.raises(RuntimeError, match="configuration must be set"):
                subscription.config.retry = policy
        runner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runner

    try:
        asyncio.run(scenario())
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

    first_done, second_done = asyncio.Event(), asyncio.Event()
    handles = iter((ControlledHandle(first_done), ControlledHandle(second_done)))

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


def test_new_acquire_waits_for_final_managed_runner_shutdown(
    tmp_path, monkeypatch
) -> None:
    bus = EventBus(str(tmp_path))

    @bus.handler(ExecuteRoot)
    async def handle(_event: ExecuteRoot) -> None:
        return None

    async def scenario() -> None:
        run_calls = 0
        runner_started = asyncio.Event()
        shutdown_started = asyncio.Event()
        allow_shutdown = asyncio.Event()

        async def controlled_run(*, idle_timeout=None) -> None:
            nonlocal run_calls
            assert idle_timeout is None
            run_calls += 1
            runner_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                shutdown_started.set()
                await allow_shutdown.wait()
                raise

        monkeypatch.setattr(bus, "run", controlled_run)
        first_runner = await bus._acquire_execute_runner()
        assert first_runner is not None
        await runner_started.wait()

        release = asyncio.create_task(bus._release_execute_runner(first_runner))
        await shutdown_started.wait()
        acquire = asyncio.create_task(bus._acquire_execute_runner())
        await asyncio.sleep(0)
        assert not acquire.done()
        assert bus._execute_runner_task is first_runner

        allow_shutdown.set()
        await release
        second_runner = await acquire
        assert second_runner is not None
        assert second_runner is not first_runner
        await wait_until(lambda: run_calls == 2)
        await bus._release_execute_runner(second_runner)

    try:
        asyncio.run(scenario())
    finally:
        bus.close()


def test_managed_runner_failure_reaches_all_concurrent_execute_callers(
    tmp_path, monkeypatch
) -> None:
    bus = EventBus(str(tmp_path))

    @bus.handler(ExecuteRoot)
    async def handle(_event: ExecuteRoot) -> None:
        return None

    async def scenario() -> None:
        fail = asyncio.Event()
        error = RuntimeError("runner failed")

        async def failing_run(*, idle_timeout=None) -> None:
            assert idle_timeout is None
            await fail.wait()
            raise error

        async def fake_open(_source) -> ControlledHandle:
            return ControlledHandle(asyncio.Event())

        monkeypatch.setattr(bus, "run", failing_run)
        monkeypatch.setattr(bus, "_open_execution", fake_open)
        first = asyncio.create_task(bus.execute(object()))  # type: ignore[arg-type]
        second = asyncio.create_task(bus.execute(object()))  # type: ignore[arg-type]
        await wait_until(lambda: bus._execute_runner_users == 2)
        fail.set()

        for execution in (first, second):
            with pytest.raises(RuntimeError, match="runner failed") as raised:
                await execution
            assert raised.value is error
        assert bus._execute_runner_task is None

    try:
        asyncio.run(scenario())
    finally:
        bus.close()


def test_cancelling_one_execute_keeps_shared_runner_for_other_user(
    tmp_path, monkeypatch
) -> None:
    bus = EventBus(str(tmp_path))

    @bus.handler(ExecuteRoot)
    async def handle(_event: ExecuteRoot) -> None:
        return None

    async def scenario() -> None:
        runner_stopped = asyncio.Event()

        async def controlled_run(*, idle_timeout=None) -> None:
            assert idle_timeout is None
            try:
                await asyncio.Event().wait()
            finally:
                runner_stopped.set()

        first_done, second_done = asyncio.Event(), asyncio.Event()
        handles = iter((ControlledHandle(first_done), ControlledHandle(second_done)))

        async def fake_open(_source) -> ControlledHandle:
            return next(handles)

        monkeypatch.setattr(bus, "run", controlled_run)
        monkeypatch.setattr(bus, "_open_execution", fake_open)
        first = asyncio.create_task(bus.execute(object()))  # type: ignore[arg-type]
        second = asyncio.create_task(bus.execute(object()))  # type: ignore[arg-type]
        await wait_until(lambda: bus._execute_runner_users == 2)

        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert bus._execute_runner_users == 1
        assert not runner_stopped.is_set()

        second_done.set()
        await second
        assert runner_stopped.is_set()
        assert bus._execute_runner_task is None

    try:
        asyncio.run(scenario())
    finally:
        bus.close()


def test_execute_does_not_cancel_already_active_external_runner(tmp_path) -> None:
    bus = EventBus(str(tmp_path))

    @bus.handler(ExecuteRoot)
    async def handle(_event: ExecuteRoot) -> None:
        return None

    @bus.source(SequenceSource([], fingerprint="external-v1"), checkpoint="external")
    def source(value: str) -> ExecuteRoot:
        return ExecuteRoot(key=value)

    async def scenario() -> None:
        runner = asyncio.create_task(bus.run())
        await wait_until(lambda: bus._run_active)
        result = await bus.execute(source)
        assert result.completed
        assert not runner.done()
        assert bus._execute_runner_task is None
        runner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runner

    try:
        asyncio.run(scenario())
    finally:
        bus.close()


def test_unrelated_ready_delivery_does_not_block_execute(tmp_path) -> None:
    topology = BusTopology(
        {
            "roots": [ExecuteRoot],
            "unrelated": [ExecuteChild],
        }
    )
    bus = EventBus(str(tmp_path), topology=topology)

    @bus.subscription("roots").handler(ExecuteRoot)
    async def handle(_event: ExecuteRoot) -> None:
        return None

    bus.dispatch(ExecuteChild(key="outside"))

    @bus.source(
        SequenceSource(["inside"], fingerprint="scoped-v1"), checkpoint="scoped"
    )
    def source(value: str) -> ExecuteRoot:
        return ExecuteRoot(key=value)

    try:
        result = asyncio.run(bus.execute(source, timeout=2))
        queue = bus._open_subscription_queue("unrelated")
        try:
            assert queue.stats()["ready"] == 1
        finally:
            queue.close()
        assert result.deliveries_total == 1
        assert result.deliveries_acknowledged == 1
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
