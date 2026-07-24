"""Exercise the public contract of an already installed localqueue wheel."""

from __future__ import annotations

import importlib.metadata
import os
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path

import localqueue
from localqueue import FailedMessage, FailureReason, Full, SimpleQueue
from localqueue import localqueue as native
from localqueue.bus import BaseEvent, BusTopology, EventBus, FailedDelivery


class SmokeEvent(BaseEvent):
    event_name = "release.smoke"
    value: str


def _replay_identity(directory: str, message_id: int) -> tuple[str | None, int] | None:
    """Read replay invariants without retaining a SQLite handle on Windows."""
    with closing(sqlite3.connect(Path(directory) / "localqueue.db")) as connection:
        return connection.execute(
            "SELECT job_id, created_at FROM messages WHERE id = ?", (message_id,)
        ).fetchone()


def main() -> None:
    expected_version = os.environ.get("EXPECTED_LOCALQUEUE_VERSION")
    package_version = importlib.metadata.version("localqueue")
    assert "site-packages" in localqueue.__file__
    assert "site-packages" in native.__file__
    assert (Path(localqueue.__file__).parent / "py.typed").is_file()
    workspace = os.environ.get("GITHUB_WORKSPACE")
    if workspace:
        assert workspace not in localqueue.__file__
        assert workspace not in native.__file__
    assert package_version == native.__version__
    if expected_version:
        assert package_version == expected_version
    assert not any(name.startswith("_test_") for name in dir(native.NativeQueue))

    with tempfile.TemporaryDirectory() as directory:
        queue: SimpleQueue[str] = SimpleQueue(directory, max_pending_jobs=1)
        queue.put("ack")
        acknowledged = queue.get(block=False)
        assert acknowledged.data == "ack"
        queue.ack(acknowledged)

        queue.put("retry", job_id="wheel-smoke-retry")
        failed_job = queue.get(block=False)
        queue.fail(failed_job, last_error="deterministic smoke failure")
        failed = queue.list_failed()
        assert len(failed) == 1
        record = failed[0]
        assert isinstance(record, FailedMessage)
        assert record.reason is FailureReason.EXPLICIT_PERMANENT_FAILURE
        assert record.id == failed_job.id
        assert record.raw_payload == b'"retry"'
        assert record.decoded and record.data == "retry"

        queue.put("capacity-blocker")
        try:
            queue.retry_failed(record.id)
        except Full:
            pass
        else:
            raise AssertionError("retry must respect current queue capacity")
        blocker = queue.get(block=False)
        queue.ack(blocker)

        queue.retry_failed(record.id)
        replayed = queue.get(block=False)
        assert replayed.id == record.id
        assert replayed.data == "retry"
        identity = _replay_identity(directory, record.id)
        assert identity == ("wheel-smoke-retry", int(record.created_at * 1000))
        queue.ack(replayed)
        assert queue.stats()["acked"] == 3
        queue.close()

        topology = BusTopology({"receipt": [SmokeEvent]})
        bus = EventBus(directory, name="smoke", topology=topology)
        subscription = bus.subscription("receipt")

        class PermanentSmokeError(Exception):
            pass

        @subscription.handler(SmokeEvent, permanent_errors=(PermanentSmokeError,))
        def fail_delivery(event: SmokeEvent) -> None:
            raise PermanentSmokeError(event.value)

        receipt = bus.dispatch(SmokeEvent(value="retry"))
        assert receipt.subscriptions == ("receipt",)
        assert len(receipt.message_ids) == 1
        import asyncio

        asyncio.run(bus.run_subscription("receipt", idle_timeout=0.01))
        failed_deliveries = subscription.list_failed()
        assert len(failed_deliveries) == 1
        delivery = failed_deliveries[0]
        assert isinstance(delivery, FailedDelivery)
        assert delivery.id == receipt.message_ids[0]
        assert delivery.reason is FailureReason.PERMANENT_HANDLER_ERROR
        assert delivery.event_type == SmokeEvent.event_name
        assert isinstance(delivery.event, SmokeEvent)
        assert delivery.event.value == "retry"
        assert delivery.raw_payload
        subscription.retry_failed(delivery.id)
        bus.close()

        replayed_events: list[str] = []
        replay_bus = EventBus(directory, name="smoke", topology=topology)
        replay_subscription = replay_bus.subscription("receipt")

        @replay_subscription.handler(SmokeEvent)
        def handle_replayed_delivery(event: SmokeEvent) -> None:
            replayed_events.append(event.value)

        asyncio.run(replay_bus.run_subscription("receipt", idle_timeout=0.01))
        assert replayed_events == ["retry"]
        replay_bus.close()


if __name__ == "__main__":
    main()
