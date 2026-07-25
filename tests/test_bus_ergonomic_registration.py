import asyncio
import threading

import pytest
from localqueue import DeliveryPolicy, FailureReason
from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    HandlerContext,
    NoSubscribers,
    Reject,
    Retry,
)


class ErgonomicInput(BaseEvent):
    value: int


class ErgonomicOutput(BaseEvent):
    value: int


class InvalidSubscriptionEvent(BaseEvent):
    event_name = "contact/requested"


class WildcardEvent(BaseEvent):
    event_name = "*"


def run(coro):
    return asyncio.run(coro)


def test_topology_with_route_returns_an_immutable_extended_snapshot():
    original = BusTopology({"existing": [ErgonomicInput]})

    extended = original._with_route("ErgonomicOutput", ErgonomicOutput)
    repeated = extended._with_route("ErgonomicOutput", ErgonomicOutput)

    assert original.subscription_names == ("existing",)
    assert extended.subscription_names == ("ErgonomicOutput", "existing")
    assert extended.subscriptions_for("ErgonomicInput") == ("existing",)
    assert extended.subscriptions_for("ErgonomicOutput") == ("ErgonomicOutput",)
    assert repeated.subscription_names == extended.subscription_names
    assert repeated.subscriptions_for("ErgonomicOutput") == ("ErgonomicOutput",)


def test_topology_is_optional_and_empty_bus_still_requires_subscribers(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))
    try:
        assert bus.topology.subscription_names == ()
        with pytest.raises(NoSubscribers):
            bus.dispatch(ErgonomicInput(value=1))
    finally:
        bus.close()


def test_handler_declares_default_subscription_and_processes_event(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))
    handled = []

    @bus.handler(ErgonomicInput)
    def handle(event):
        handled.append(event.value)

    try:
        receipt = bus.dispatch(ErgonomicInput(value=1))
        run(bus.run(idle_timeout=0.05))

        assert receipt.subscriptions == ("ErgonomicInput",)
        assert bus.subscription("ErgonomicInput").concurrency == 1
        assert handled == [1]
    finally:
        bus.close()


def test_handler_direct_registration_preserves_callable(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))

    def handle(event):
        return None

    try:
        assert bus.handler(ErgonomicInput, handle) is handle
        assert bus.topology.routes("ErgonomicInput", "ErgonomicInput")
    finally:
        bus.close()


def test_handler_uses_bus_concurrency_default_and_explicit_override(tmp_path):
    bus = EventBus(str(tmp_path / "bus"), concurrency=3)

    @bus.handler(ErgonomicInput)
    def handle_input(event):
        return None

    @bus.handler(ErgonomicOutput, concurrency=7)
    async def handle_output(event):
        return None

    try:
        assert bus.subscription("ErgonomicInput").concurrency == 3
        assert bus.subscription("ErgonomicOutput").concurrency == 7
    finally:
        bus.close()


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (0, ValueError),
        (-1, ValueError),
        (True, TypeError),
        (1.5, TypeError),
        ("2", TypeError),
    ],
)
def test_concurrency_validation_is_shared_by_bus_and_handler(tmp_path, value, error):
    with pytest.raises(error, match="positive integer"):
        EventBus(str(tmp_path / "invalid-bus"), concurrency=value)

    bus = EventBus(str(tmp_path / "bus"))
    try:
        with pytest.raises(error, match="positive integer"):
            bus.handler(ErgonomicInput, concurrency=value)
        assert bus.topology.subscription_names == ()
    finally:
        bus.close()


@pytest.mark.parametrize("event", ["ErgonomicInput", "*", object])
def test_handler_rejects_non_event_classes_without_mutation(tmp_path, event):
    bus = EventBus(str(tmp_path / "bus"))
    try:
        with pytest.raises(TypeError, match=r"subclass of BaseEvent.*bus\.on"):
            bus.handler(event)
        assert bus.topology.subscription_names == ()
    finally:
        bus.close()


def test_handler_rejects_event_class_with_wildcard_name_without_mutation(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))
    try:
        with pytest.raises(ValueError, match="wildcard"):
            bus.handler(
                WildcardEvent,
                lambda event: None,
                subscription="catch-all",
            )
        assert bus.topology.subscription_names == ()
        assert bus._handlers == {}
    finally:
        bus.close()


def test_invalid_default_subscription_name_requires_explicit_override(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))
    try:
        with pytest.raises(ValueError, match="subscription=.*contact-requested"):
            bus.handler(InvalidSubscriptionEvent, lambda event: None)

        handler = bus.handler(
            InvalidSubscriptionEvent,
            lambda event: None,
            subscription="contact-requested",
        )
        assert callable(handler)
        assert bus.topology.routes("contact-requested", "contact/requested")
    finally:
        bus.close()


def test_failed_handler_registration_leaves_no_route_or_concurrency(tmp_path):
    bus = EventBus(str(tmp_path / "bus"), concurrency=3)

    try:
        with pytest.raises(TypeError, match="only supported for async"):

            @bus.handler(ErgonomicInput, concurrency=7, timeout=1)
            def invalid_sync_handler(event):
                return None

        with pytest.raises(TypeError, match="callable"):
            bus.handler(ErgonomicOutput, object(), concurrency=9)

        assert bus.topology.subscription_names == ()
        assert bus._subscription_concurrency == {}
        assert bus._handlers == {}
    finally:
        bus.close()


@pytest.mark.parametrize(
    ("kwargs", "error", "message"),
    [
        ({"subscription": ""}, ValueError, "invalid 'subscription'"),
        ({"permanent_errors": (object,)}, TypeError, "exception classes"),
        ({"timeout": True}, TypeError, "positive number"),
        ({"timeout": 0}, ValueError, "positive finite"),
    ],
)
def test_invalid_handler_configuration_leaves_no_partial_state(
    tmp_path, kwargs, error, message
):
    bus = EventBus(str(tmp_path / "bus"))
    try:
        with pytest.raises(error, match=message):
            bus.handler(ErgonomicInput, lambda event: None, **kwargs)
        assert bus.topology.subscription_names == ()
        assert bus._subscription_concurrency == {}
        assert bus._handlers == {}
    finally:
        bus.close()


def test_invalid_handler_signature_leaves_no_partial_state(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))

    def invalid(event, context, extra):
        return None

    try:
        with pytest.raises(TypeError, match=r"either \(event\) or \(event, context\)"):
            bus.handler(ErgonomicInput, invalid, concurrency=4)
        assert bus.topology.subscription_names == ()
        assert bus._subscription_concurrency == {}
        assert bus._handlers == {}
    finally:
        bus.close()


def test_explicit_topology_is_preserved_and_not_mutated(tmp_path):
    topology = BusTopology({"existing": [ErgonomicInput]})
    bus = EventBus(str(tmp_path / "bus"), topology=topology)

    @bus.handler(ErgonomicOutput)
    def handle(event):
        return None

    try:
        assert topology.subscription_names == ("existing",)
        assert bus.topology.subscription_names == ("ErgonomicOutput", "existing")
    finally:
        bus.close()


def test_existing_route_is_idempotent_when_registering_handler(tmp_path):
    topology = BusTopology({"worker": [ErgonomicInput]})
    bus = EventBus(str(tmp_path / "bus"), topology=topology)
    try:
        bus.handler(ErgonomicInput, lambda event: None, subscription="worker")
        assert bus.topology.subscription_names == ("worker",)
        assert bus.topology.subscriptions_for("ErgonomicInput") == ("worker",)
    finally:
        bus.close()


def test_async_handler_receives_context_and_acks(tmp_path):
    contexts = []
    handled = []

    def create_context(runtime):
        context = HandlerContext(runtime)
        contexts.append(context)
        return context

    bus = EventBus(str(tmp_path / "bus"), context_factory=create_context)

    @bus.handler(ErgonomicInput)
    async def handle(event, ctx):
        handled.append((event.value, ctx.handler_name))

    try:
        bus.dispatch(ErgonomicInput(value=4))
        run(bus.run(idle_timeout=0.05))

        assert handled == [(4, "handle")]
        assert len(contexts) == 1
        queue = bus._open_subscription_queue("ErgonomicInput")
        try:
            assert queue.stats()["acked"] == 1
        finally:
            queue.close()
    finally:
        bus.close()


def test_implicit_returned_event_chain_uses_transactional_routing(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))
    outputs = []

    @bus.handler(ErgonomicInput)
    async def handle_input(event):
        return ErgonomicOutput(value=event.value + 1)

    @bus.handler(ErgonomicOutput)
    async def handle_output(event):
        outputs.append(event.value)

    try:
        bus.dispatch(ErgonomicInput(value=1))
        run(bus.run(idle_timeout=0.1))

        assert outputs == [2]
        for subscription in ("ErgonomicInput", "ErgonomicOutput"):
            queue = bus._open_subscription_queue(subscription)
            try:
                assert queue.stats()["acked"] == 1
            finally:
                queue.close()
    finally:
        bus.close()


def test_duplicate_default_handler_fails_without_changing_topology(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))
    bus.handler(ErgonomicInput, lambda event: None)
    topology = bus.topology

    try:
        with pytest.raises(ValueError, match="already registered"):
            bus.handler(ErgonomicInput, lambda event: None)
        assert bus.topology is topology
        assert bus.topology.subscriptions_for("ErgonomicInput") == ("ErgonomicInput",)
    finally:
        bus.close()


def test_distinct_explicit_subscriptions_fan_out_same_event(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))
    handled = []

    bus.handler(
        ErgonomicInput,
        lambda event: handled.append(("project", event.value)),
        subscription="project",
    )
    bus.handler(
        ErgonomicInput,
        lambda event: handled.append(("notify", event.value)),
        subscription="notify",
    )

    try:
        receipt = bus.dispatch(ErgonomicInput(value=5))
        run(bus.run(idle_timeout=0.05))

        assert receipt.subscriptions == ("notify", "project")
        assert sorted(handled) == [("notify", 5), ("project", 5)]
    finally:
        bus.close()


def test_shared_subscription_selects_handler_by_event_type(tmp_path):
    bus = EventBus(str(tmp_path / "bus"), concurrency=2)
    handled = []

    bus.handler(
        ErgonomicInput,
        lambda event: handled.append(("input", event.value)),
        subscription="projector",
        concurrency=3,
    )
    bus.handler(
        ErgonomicOutput,
        lambda event: handled.append(("output", event.value)),
        subscription="projector",
        concurrency=3,
    )

    try:
        bus.dispatch(ErgonomicInput(value=1))
        bus.dispatch(ErgonomicOutput(value=2))
        run(bus.run(idle_timeout=0.05))

        assert sorted(handled) == [("input", 1), ("output", 2)]
        assert bus.topology.subscription_names == ("projector",)
        assert bus.subscription("projector").concurrency == 3
    finally:
        bus.close()


def test_conflicting_shared_concurrency_fails_without_partial_route(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))
    bus.handler(
        ErgonomicInput, lambda event: None, subscription="projector", concurrency=3
    )
    topology = bus.topology

    try:
        with pytest.raises(ValueError, match="already configured"):
            bus.handler(
                ErgonomicOutput,
                lambda event: None,
                subscription="projector",
                concurrency=4,
            )
        assert bus.topology is topology
        assert not bus.topology.routes("projector", "ErgonomicOutput")
        assert ("projector", "ErgonomicOutput") not in bus._handlers
        assert bus.subscription("projector").concurrency == 3
    finally:
        bus.close()


def test_running_subscription_rejects_new_handler_without_partial_route(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))
    started = asyncio.Event()
    release = asyncio.Event()

    @bus.handler(ErgonomicInput, subscription="worker")
    async def handle(event):
        started.set()
        await release.wait()

    bus.dispatch(ErgonomicInput(value=1))

    async def consume():
        task = asyncio.create_task(bus.run_subscription("worker"))
        await asyncio.wait_for(started.wait(), timeout=1)
        topology = bus.topology
        with pytest.raises(RuntimeError, match="before run"):
            bus.handler(ErgonomicOutput, lambda event: None, subscription="worker")
        assert bus.topology is topology
        assert not bus.topology.routes("worker", "ErgonomicOutput")
        release.set()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    try:
        run(consume())
    finally:
        bus.close()


def test_active_run_rejects_new_subscription_without_partial_configuration(tmp_path):
    bus = EventBus(str(tmp_path / "bus"), concurrency=2)
    started = asyncio.Event()
    release = asyncio.Event()

    @bus.handler(ErgonomicInput)
    async def handle(event):
        started.set()
        await release.wait()

    bus.dispatch(ErgonomicInput(value=1))

    async def consume():
        task = asyncio.create_task(bus.run(idle_timeout=0.05))
        await asyncio.wait_for(started.wait(), timeout=1)
        topology = bus.topology
        concurrency = dict(bus._subscription_concurrency)
        handlers = dict(bus._handlers)

        with pytest.raises(RuntimeError, match=r"before EventBus\.run starts"):
            bus.handler(
                ErgonomicOutput,
                lambda event: None,
                concurrency=7,
            )

        assert bus.topology is topology
        assert bus._subscription_concurrency == concurrency
        assert bus._handlers == handlers
        release.set()
        await asyncio.wait_for(task, timeout=1)

    try:
        run(consume())
    finally:
        bus.close()


def test_bus_default_concurrency_is_read_only(tmp_path):
    bus = EventBus(str(tmp_path / "bus"), concurrency=3)
    bus.handler(ErgonomicInput, lambda event: None)
    try:
        assert bus.concurrency == 3
        with pytest.raises(AttributeError):
            bus.concurrency = 0
        assert bus.concurrency == 3
        assert bus.subscription("ErgonomicInput").concurrency == 3
    finally:
        bus.close()


@pytest.mark.parametrize(("default", "override", "expected"), [(2, None, 2), (4, 3, 3)])
def test_handler_concurrency_bounds_active_deliveries(
    tmp_path, default, override, expected
):
    bus = EventBus(str(tmp_path / "bus"), concurrency=default)
    entered = threading.Event()
    release = threading.Event()
    lock = threading.Lock()
    active = 0
    peak = 0

    def handle(event):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if active == expected:
                entered.set()
        release.wait(timeout=2)
        with lock:
            active -= 1

    bus.handler(ErgonomicInput, handle, concurrency=override)
    for value in range(expected + 2):
        bus.dispatch(ErgonomicInput(value=value))

    async def consume():
        task = asyncio.create_task(bus.run(idle_timeout=0.05))
        assert await asyncio.to_thread(entered.wait, 1)
        release.set()
        await asyncio.wait_for(task, timeout=2)

    try:
        run(consume())
        assert peak == expected
    finally:
        bus.close()


def test_concurrency_is_not_a_global_limit_across_subscriptions(tmp_path):
    bus = EventBus(str(tmp_path / "bus"), concurrency=2)
    entered = threading.Event()
    release = threading.Event()
    lock = threading.Lock()
    active = 0
    peak = 0

    def handle(event):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if active == 4:
                entered.set()
        release.wait(timeout=2)
        with lock:
            active -= 1

    bus.handler(ErgonomicInput, handle)
    bus.handler(ErgonomicOutput, handle)
    for value in range(2):
        bus.dispatch(ErgonomicInput(value=value))
        bus.dispatch(ErgonomicOutput(value=value))

    async def consume():
        task = asyncio.create_task(bus.run(idle_timeout=0.05))
        assert await asyncio.to_thread(entered.wait, 1)
        release.set()
        await asyncio.wait_for(task, timeout=2)

    try:
        run(consume())
        assert peak == 4
    finally:
        bus.close()


def test_retry_reject_and_replay_are_inspected_by_implicit_subscription(tmp_path):
    bus = EventBus(
        str(tmp_path / "bus"),
        delivery=DeliveryPolicy(lease_seconds=1, max_retries=1),
    )
    replayed = set()
    attempts = []

    @bus.handler(ErgonomicInput, permanent_errors=(Retry,))
    def ergonomic_handler(event):
        attempts.append(event.value)
        if event.value in replayed:
            return None
        if event.value == 1:
            raise Retry("later")
        raise Reject("invalid", category="validation")

    try:
        bus.dispatch(ErgonomicInput(value=1))
        bus.dispatch(ErgonomicInput(value=2))
        run(bus.run(idle_timeout=0.1))

        failed = bus.subscription("ErgonomicInput").list_failed()
        assert {item.reason for item in failed} == {
            FailureReason.RETRIES_EXHAUSTED,
            FailureReason.REJECTED,
        }
        assert {item.subscription for item in failed} == {"ErgonomicInput"}
        assert "ergonomic_handler" not in bus.topology.subscription_names

        replayed.update({1, 2})
        for item in failed:
            bus.subscription("ErgonomicInput").retry_failed(item.id)
        run(bus.run(idle_timeout=0.1))

        queue = bus._open_subscription_queue("ErgonomicInput")
        try:
            assert queue.stats()["acked"] == 2
        finally:
            queue.close()
    finally:
        bus.close()
