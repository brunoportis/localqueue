import asyncio
from uuid import uuid4

import pytest
from localqueue import DeliveryPolicy, FailureReason
from localqueue.bus import BaseEvent, BusTopology, EventBus


class Input(BaseEvent):
    value: str


class Output(BaseEvent):
    value: str


def run(coro):
    return asyncio.run(coro)


def make_bus(path, *, require_subscribers=True):
    return EventBus(
        str(path),
        topology=BusTopology({"inputs": [Input], "a": [Output], "b": [Output]}),
        delivery=DeliveryPolicy(lease_seconds=1, max_retries=3),
        require_subscribers=require_subscribers,
    )


def stats(bus, subscription):
    queue = bus._open_subscription_queue(subscription)
    try:
        return queue.stats()
    finally:
        queue.close()


@pytest.mark.parametrize("asynchronous", [False, True])
def test_handler_returned_event_is_fanned_out_and_origin_is_acked(
    tmp_path, asynchronous
):
    bus = make_bus(tmp_path)
    returned = Output(value="derived")
    parent = Input(value="parent")

    if asynchronous:

        @bus.subscription("inputs").handler(Input)
        async def handle(event):
            return returned

    else:

        @bus.subscription("inputs").handler(Input)
        def handle(event):
            return returned

    bus.dispatch(parent)
    run(bus.run_subscription("inputs", idle_timeout=0.2))

    assert stats(bus, "inputs")["acked"] == 1
    assert stats(bus, "a")["ready"] == 1
    assert stats(bus, "b")["ready"] == 1
    queue = bus._open_subscription_queue("a")
    try:
        job = queue.get(False)
        envelope = job.data
        assert envelope["event_id"] == str(returned.event_id)
        assert envelope["correlation_id"] == str(parent.correlation_id)
        assert envelope["causation_id"] == str(parent.event_id)
    finally:
        queue.close()
    assert returned.correlation_id == returned.event_id
    assert returned.causation_id is None
    bus.close()


def test_sync_handler_returned_awaitable_is_awaited(tmp_path):
    bus = make_bus(tmp_path)

    @bus.subscription("inputs").handler(Input)
    def handle(event):
        async def result():
            return Output(value=event.value)

        return result()

    bus.dispatch(Input(value="awaitable"))
    run(bus.run_subscription("inputs", idle_timeout=0.2))

    assert stats(bus, "inputs")["acked"] == 1
    assert stats(bus, "a")["ready"] == 1
    bus.close()


def test_explicit_lineage_is_preserved(tmp_path):
    bus = make_bus(tmp_path)
    correlation_id = uuid4()
    causation_id = uuid4()
    returned = Output(
        value="explicit",
        correlation_id=correlation_id,
        causation_id=causation_id,
    )

    @bus.subscription("inputs").handler(Input)
    def handle(event):
        return returned

    bus.dispatch(Input(value="parent"))
    run(bus.run_subscription("inputs", idle_timeout=0.2))
    queue = bus._open_subscription_queue("a")
    try:
        envelope = queue.get(False).data
    finally:
        queue.close()
    assert envelope["correlation_id"] == str(correlation_id)
    assert envelope["causation_id"] == str(causation_id)
    bus.close()


def test_invalid_result_fails_immediately_without_stopping_consumer(tmp_path):
    bus = make_bus(tmp_path)
    seen = []

    @bus.subscription("inputs").handler(Input)
    def handle(event):
        seen.append(event.value)
        return "invalid" if event.value == "bad" else None

    bus.dispatch(Input(value="bad"))
    bus.dispatch(Input(value="good"))
    run(bus.run_subscription("inputs", idle_timeout=0.2))

    failed = bus.subscription("inputs").list_failed()
    assert seen == ["bad", "good"]
    assert failed[0].attempts == 1
    assert failed[0].reason is FailureReason.PERMANENT_HANDLER_ERROR
    assert "handle" in failed[0].last_error
    assert "str" in failed[0].last_error
    assert stats(bus, "inputs")["acked"] == 1
    assert stats(bus, "a")["ready"] == 0
    bus.close()


def test_returned_event_without_subscribers_obeys_policy(tmp_path):
    class Unrouted(BaseEvent):
        value: str

    for required in (True, False):
        bus = make_bus(tmp_path / str(required), require_subscribers=required)

        @bus.subscription("inputs").handler(Input)
        def handle(event):
            return Unrouted(value=event.value)

        bus.dispatch(Input(value="parent"))
        run(bus.run_subscription("inputs", idle_timeout=0.2))
        if required:
            failed = bus.subscription("inputs").list_failed()
            assert failed[0].attempts == 1
            assert failed[0].reason is FailureReason.PERMANENT_HANDLER_ERROR
            assert "Unrouted" in failed[0].last_error
        else:
            assert stats(bus, "inputs")["acked"] == 1
        bus.close()
