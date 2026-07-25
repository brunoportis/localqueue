import asyncio
import json
import threading
from uuid import uuid4

import pytest
from localqueue import DeliveryPolicy, FailureReason
from localqueue.bus import BaseEvent, BusTopology, EventBus, event


class Input(BaseEvent):
    value: str


class Output(BaseEvent):
    value: str


@event(identity="value")
class DurableOutput(BaseEvent):
    value: str
    detail: str


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


def test_returned_duplicate_is_success_and_origin_is_acked(tmp_path):
    bus = EventBus(
        str(tmp_path),
        topology=BusTopology({"inputs": [Input], "outputs": [DurableOutput]}),
    )
    bus.dispatch(DurableOutput(value="1", detail="same"))

    @bus.subscription("inputs").handler(Input)
    def handle(event):
        return DurableOutput(value="1", detail="same")

    bus.dispatch(Input(value="source"))
    run(bus.run_subscription("inputs", idle_timeout=0.2))

    assert stats(bus, "inputs")["acked"] == 1
    assert stats(bus, "outputs")["ready"] == 1
    bus.close()


def test_returned_identity_conflict_fails_permanently_without_partial_output(
    tmp_path,
):
    seed = EventBus(
        str(tmp_path),
        topology=BusTopology({"existing": [DurableOutput]}),
    )
    seed.dispatch(DurableOutput(value="1", detail="first"))
    seed.close()
    bus = EventBus(
        str(tmp_path),
        topology=BusTopology(
            {
                "inputs": [Input],
                "existing": [DurableOutput],
                "other": [DurableOutput],
            }
        ),
    )

    @bus.subscription("inputs").handler(Input)
    def handle(event):
        return DurableOutput(value="1", detail="different")

    bus.dispatch(Input(value="source"))
    run(bus.run_subscription("inputs", idle_timeout=0.2))

    failed = bus.subscription("inputs").list_failed()
    assert failed[0].attempts == 1
    assert failed[0].reason is FailureReason.PERMANENT_HANDLER_ERROR
    assert failed[0].failure_category == "deduplication_conflict"
    assert stats(bus, "other")["ready"] == 0
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


def test_timeout_discards_event_returned_during_cancellation_cleanup(tmp_path):
    bus = EventBus(
        str(tmp_path),
        topology=BusTopology({"inputs": [Input], "outputs": [Output]}),
        delivery=DeliveryPolicy(lease_seconds=1, max_retries=0),
    )

    @bus.subscription("inputs").handler(Input, timeout=0.01)
    async def handle(event):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return Output(value="too-late")

    bus.dispatch(Input(value="parent"))
    run(bus.run_subscription("inputs", idle_timeout=0.2))

    failed = bus.subscription("inputs").list_failed()
    assert failed[0].reason is FailureReason.HANDLER_TIMEOUT
    assert stats(bus, "outputs")["ready"] == 0
    bus.close()


def test_serialization_error_retries_without_output(tmp_path):
    class FailingOutputSerializer:
        def dumps(self, obj: dict[str, object], /) -> bytes:
            if obj["event_type"] == "Output":
                raise ValueError("cannot serialize Output")
            return json.dumps(obj).encode()

        def loads(self, data: bytes, /) -> object:
            return json.loads(data)

    bus = EventBus(
        str(tmp_path),
        topology=BusTopology({"inputs": [Input], "outputs": [Output]}),
        delivery=DeliveryPolicy(lease_seconds=1, max_retries=1),
        serializer=FailingOutputSerializer(),
    )

    @bus.subscription("inputs").handler(Input)
    def handle(event):
        return Output(value=event.value)

    bus.dispatch(Input(value="parent"))
    run(bus.run_subscription("inputs", idle_timeout=0.2))

    failed = bus.subscription("inputs").list_failed()
    assert failed[0].attempts == 2
    assert failed[0].reason is FailureReason.RETRIES_EXHAUSTED
    assert "cannot serialize Output" in failed[0].last_error
    assert stats(bus, "outputs")["ready"] == 0
    bus.close()


def test_existing_target_ids_are_reused_while_origin_is_acked(tmp_path):
    bus = make_bus(tmp_path)
    returned = Output(value="same-id")
    receipt = bus.dispatch(returned)

    @bus.subscription("inputs").handler(Input)
    def handle(event):
        return returned

    bus.dispatch(Input(value="parent"))
    run(bus.run_subscription("inputs", idle_timeout=0.2))

    assert stats(bus, "inputs")["acked"] == 1
    assert stats(bus, "a")["ready"] == 1
    assert stats(bus, "b")["ready"] == 1
    bus.close()

    reopened = make_bus(tmp_path)
    assert stats(reopened, "inputs")["acked"] == 1
    assert stats(reopened, "a")["ready"] == 1
    assert stats(reopened, "b")["ready"] == 1
    assert len(receipt.message_ids) == 2
    reopened.close()


def test_sync_serializer_does_not_block_the_event_loop(tmp_path):
    serializer_started = threading.Event()
    loop_progressed = threading.Event()
    serializer_observed_progress = []

    class BlockingOutputSerializer:
        def dumps(self, obj: dict[str, object], /) -> bytes:
            if obj["event_type"] == "Output":
                serializer_started.set()
                serializer_observed_progress.append(loop_progressed.wait(timeout=0.5))
            return json.dumps(obj).encode()

        def loads(self, data: bytes, /) -> object:
            return json.loads(data)

    bus = EventBus(
        str(tmp_path),
        topology=BusTopology({"inputs": [Input], "outputs": [Output]}),
        serializer=BlockingOutputSerializer(),
    )

    @bus.subscription("inputs").handler(Input)
    def handle(event):
        return Output(value=event.value)

    async def allow_loop_progress() -> None:
        await asyncio.to_thread(serializer_started.wait)
        await asyncio.sleep(0)
        loop_progressed.set()

    async def consume() -> None:
        progress_task = asyncio.create_task(allow_loop_progress())
        bus.dispatch(Input(value="parent"))
        await bus.run_subscription("inputs", idle_timeout=0.2)
        await progress_task

    run(consume())

    assert serializer_observed_progress == [True]
    assert stats(bus, "inputs")["acked"] == 1
    assert stats(bus, "outputs")["ready"] == 1
    bus.close()
