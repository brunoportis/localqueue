"""Exercise the public contract of an already installed localqueue wheel."""

from __future__ import annotations

import importlib.metadata
import os
import tempfile

import localqueue
from localqueue import EnqueueItem, SimpleQueue
from localqueue import localqueue as native
from localqueue.bus import BaseEvent, BusTopology, EventBus


class SmokeEvent(BaseEvent):
    event_name = "release.smoke"
    value: str


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
        queue = SimpleQueue(directory)
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
        queue.close()

        topology = BusTopology({"receipt": [SmokeEvent]})
        bus = EventBus(directory, name="smoke", topology=topology)
        receipt = bus.dispatch(SmokeEvent(value="ok"))
        assert receipt.subscriptions == ("receipt",)
        assert len(receipt.message_ids) == 1
        bus.close()


if __name__ == "__main__":
    main()
