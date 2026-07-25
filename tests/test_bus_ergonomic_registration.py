import asyncio

import pytest
from localqueue.bus import BaseEvent, BusTopology, EventBus, NoSubscribers


class Input(BaseEvent):
    value: int


class Output(BaseEvent):
    value: int


class InvalidSubscriptionEvent(BaseEvent):
    event_name = "contact/requested"


def run(coro):
    return asyncio.run(coro)


def test_topology_with_route_returns_an_immutable_extended_snapshot():
    original = BusTopology({"existing": [Input]})

    extended = original._with_route("Output", Output)
    repeated = extended._with_route("Output", Output)

    assert original.subscription_names == ("existing",)
    assert extended.subscription_names == ("Output", "existing")
    assert extended.subscriptions_for("Input") == ("existing",)
    assert extended.subscriptions_for("Output") == ("Output",)
    assert repeated.subscription_names == extended.subscription_names
    assert repeated.subscriptions_for("Output") == ("Output",)


def test_topology_is_optional_and_empty_bus_still_requires_subscribers(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))
    try:
        assert bus.topology.subscription_names == ()
        with pytest.raises(NoSubscribers):
            bus.dispatch(Input(value=1))
    finally:
        bus.close()


def test_handler_declares_default_subscription_and_processes_event(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))
    handled = []

    @bus.handler(Input)
    def handle(event):
        handled.append(event.value)

    try:
        receipt = bus.dispatch(Input(value=1))
        run(bus.run(idle_timeout=0.05))

        assert receipt.subscriptions == ("Input",)
        assert bus.subscription("Input").concurrency == 1
        assert handled == [1]
    finally:
        bus.close()


def test_handler_direct_registration_preserves_callable(tmp_path):
    bus = EventBus(str(tmp_path / "bus"))

    def handle(event):
        return None

    try:
        assert bus.handler(Input, handle) is handle
        assert bus.topology.routes("Input", "Input")
    finally:
        bus.close()


def test_handler_uses_bus_concurrency_default_and_explicit_override(tmp_path):
    bus = EventBus(str(tmp_path / "bus"), concurrency=3)

    @bus.handler(Input)
    def handle_input(event):
        return None

    @bus.handler(Output, concurrency=7)
    async def handle_output(event):
        return None

    try:
        assert bus.subscription("Input").concurrency == 3
        assert bus.subscription("Output").concurrency == 7
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
            bus.handler(Input, concurrency=value)
        assert bus.topology.subscription_names == ()
    finally:
        bus.close()


@pytest.mark.parametrize("event", ["Input", "*", object])
def test_handler_rejects_non_event_classes_without_mutation(tmp_path, event):
    bus = EventBus(str(tmp_path / "bus"))
    try:
        with pytest.raises(TypeError, match=r"subclass of BaseEvent.*bus\.on"):
            bus.handler(event)
        assert bus.topology.subscription_names == ()
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

            @bus.handler(Input, concurrency=7, timeout=1)
            def invalid_sync_handler(event):
                return None

        with pytest.raises(TypeError, match="callable"):
            bus.handler(Output, object(), concurrency=9)

        assert bus.topology.subscription_names == ()
        assert bus._subscription_concurrency == {}
        assert bus._handlers == {}
    finally:
        bus.close()


def test_explicit_topology_is_preserved_and_not_mutated(tmp_path):
    topology = BusTopology({"existing": [Input]})
    bus = EventBus(str(tmp_path / "bus"), topology=topology)

    @bus.handler(Output)
    def handle(event):
        return None

    try:
        assert topology.subscription_names == ("existing",)
        assert bus.topology.subscription_names == ("Output", "existing")
    finally:
        bus.close()


def test_existing_route_is_idempotent_when_registering_handler(tmp_path):
    topology = BusTopology({"worker": [Input]})
    bus = EventBus(str(tmp_path / "bus"), topology=topology)
    try:
        bus.handler(Input, lambda event: None, subscription="worker")
        assert bus.topology.subscription_names == ("worker",)
        assert bus.topology.subscriptions_for("Input") == ("worker",)
    finally:
        bus.close()
