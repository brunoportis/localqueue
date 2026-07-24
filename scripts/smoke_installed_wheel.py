"""Exercise the public contract of an already installed localqueue wheel."""

from __future__ import annotations

import asyncio
import importlib.metadata
import os
import tempfile

import localqueue
from localqueue import (
    DeliveryPolicy,
    EnqueueItem,
    FailedMessage,
    FailureReason,
    SimpleQueue,
)
from localqueue import localqueue as native
from localqueue.bus import BaseEvent, BusTopology, EventBus, FailedDelivery


class SmokeEvent(BaseEvent):
    event_name = "release.smoke"
    value: str


class PermanentSmokeFailure(Exception):
    pass


def main() -> None:
    expected_version = os.environ.get("EXPECTED_LOCALQUEUE_VERSION")
    package_version = importlib.metadata.version("localqueue")
    assert "site-packages" in localqueue.__file__
    assert "site-packages" in native.__file__
    workspace = os.environ.get("GITHUB_WORKSPACE")
    if workspace:
        assert workspace not in localqueue.__file__
        assert workspace not in native.__file__
    assert package_version == native.__version__
    if expected_version:
        assert package_version == expected_version
    assert not any(name.startswith("_test_") for name in dir(native.NativeQueue))

    with tempfile.TemporaryDirectory() as directory:
        queue: SimpleQueue[dict[str, object]] = SimpleQueue(directory)
        queue.put_many(
            [
                EnqueueItem({"source": "wheel-smoke"}, job_id="first"),
                {"source": "wheel-smoke"},
            ]
        )
        first = queue.get(block=False)
        second = queue.get(block=False)
        assert first.data == {"source": "wheel-smoke"}
        assert second.data == {"source": "wheel-smoke"}
        queue.ack(first)
        queue.ack(second)
        assert queue.stats()["acked"] == 2

        queue.put({"source": "dead-letter"}, job_id="failed-wheel-smoke")
        failed_job = queue.get(block=False)
        queue.fail(failed_job, last_error="wheel smoke permanent failure")
        failed = queue.list_failed()
        assert len(failed) == 1
        failed_message = failed[0]
        assert isinstance(failed_message, FailedMessage)
        assert failed_message.reason is FailureReason.EXPLICIT_PERMANENT_FAILURE
        assert failed_message.decoded
        assert failed_message.data == {"source": "dead-letter"}
        assert failed_message.raw_payload
        assert failed_message.last_error == "wheel smoke permanent failure"
        queue.retry_failed(failed_message.id)
        replayed = queue.get(block=False)
        assert replayed.id == failed_message.id
        assert replayed.job_id == "failed-wheel-smoke"
        assert replayed.data == {"source": "dead-letter"}
        queue.ack(replayed)
        queue.close()

        topology = BusTopology({"receipt": [SmokeEvent]})
        bus = EventBus(
            directory,
            name="smoke",
            topology=topology,
            delivery=DeliveryPolicy(max_retries=0),
        )
        subscription = bus.subscription("receipt")

        @subscription.handler(
            SmokeEvent,
            permanent_errors=(PermanentSmokeFailure,),
        )
        def fail_delivery(event: SmokeEvent) -> None:
            raise PermanentSmokeFailure(event.value)

        receipt = bus.dispatch(SmokeEvent(value="ok"))
        assert receipt.subscriptions == ("receipt",)
        assert len(receipt.message_ids) == 1
        asyncio.run(bus.run_subscription("receipt", idle_timeout=0.2))

        failed_deliveries = subscription.list_failed()
        assert len(failed_deliveries) == 1
        delivery = failed_deliveries[0]
        assert isinstance(delivery, FailedDelivery)
        assert delivery.id == receipt.message_ids[0]
        assert delivery.reason is FailureReason.PERMANENT_HANDLER_ERROR
        assert isinstance(delivery.event, SmokeEvent)
        assert delivery.event.value == "ok"
        assert delivery.event_type == "release.smoke"
        assert delivery.inspection_error is None
        assert delivery.raw_payload
        subscription.retry_failed(delivery.id)
        assert subscription.list_failed() == []
        bus.close()


if __name__ == "__main__":
    main()
