"""Create a deterministic compatibility fixture using the installed release only."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
from pathlib import Path
from time import time

PAYLOAD = {
    "text": "olá, 世界",
    "number": 42,
    "enabled": True,
    "items": [1, "two"],
    "nested": {"key": "value"},
}


def module_paths() -> dict[str, str]:
    import localqueue
    from localqueue import localqueue as native

    return {
        "localqueue": str(Path(localqueue.__file__).resolve()),
        "native": str(Path(native.__file__).resolve()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wheel", required=True)
    parser.add_argument("--wheel-sha256", required=True)
    parser.add_argument("--event-bus", action="store_true")
    args = parser.parse_args()
    from localqueue import SimpleQueue

    root = args.output
    root.mkdir(parents=True, exist_ok=True)
    ids: dict[str, int] = {}
    with SimpleQueue(
        str(root), name="compat", lease_seconds=5.0, max_retries=1
    ) as queue:
        ids["processing"] = queue.put({"kind": "processing"})
        processing = queue.get()
        ids["processing"] = processing.id
        ids["acked"] = queue.put({"kind": "acked"})
        queue.ack(queue.get())
        ids["failed"] = queue.put({"kind": "failed"})
        queue.fail(queue.get(), last_error="fixture permanent failure")
        ids["failed_for_purge"] = queue.put({"kind": "failed for purge"})
        queue.fail(queue.get(), last_error="fixture purge failure")
        ids["ready"] = queue.put(PAYLOAD, job_id="compat-ready")
        ids["deduplicated"] = queue.put({"kind": "deduplicated"}, job_id="compat-dedup")
        duplicate_id = queue.put(
            {"kind": "deduplicated changed"}, job_id="compat-dedup"
        )

    capabilities = {
        "simple_queue": True,
        "event_bus": False,
        "batch_enqueue": hasattr(SimpleQueue, "put_many"),
    }
    event: dict[str, object] | None = None
    if args.event_bus:
        from localqueue.bus import BaseEvent, BusTopology, EventBus

        class HistoricalEvent(BaseEvent):
            event_name = "compat.historical"
            label: str

        bus = EventBus(
            str(root),
            name="compat-bus",
            topology=BusTopology({"events": [HistoricalEvent]}),
        )
        receipt = bus.dispatch(HistoricalEvent(label="from historical wheel"))
        bus.close()
        capabilities["event_bus"] = True
        event = {
            "event_id": str(receipt.event_id),
            "message_ids": list(receipt.message_ids),
            "payload": {"label": "from historical wheel"},
        }

    metadata = {
        "fixture_schema_version": 1,
        "historical_version": importlib.metadata.version("localqueue"),
        "wheel": args.wheel,
        "wheel_sha256": args.wheel_sha256,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "import_paths": module_paths(),
        "capabilities": capabilities,
        "ids": ids,
        "duplicate_id": duplicate_id,
        "payloads": {
            "ready": PAYLOAD,
            "processing": {"kind": "processing"},
            "delayed": {"kind": "delayed"},
            "failed": {"kind": "failed"},
        },
        "expected_counts": {"ready": 2, "processing": 1, "acked": 1, "failed": 2},
        "dead_letter_error": "fixture permanent failure",
        "processing_lease_deadline": time() + 5.0,
        "delayed_available_at": time() + 8.0,
        "event": event,
    }
    with SimpleQueue(
        str(root), name="compat-delayed", lease_seconds=5.0, max_retries=1
    ) as delayed_queue:
        ids["delayed"] = delayed_queue.put({"kind": "delayed"})
        delayed = delayed_queue.get()
        delayed_queue.nack(delayed, delay=8.0, last_error="fixture retry")
    (root / "fixture.json").write_text(
        json.dumps(metadata, sort_keys=True, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "fixture": str(root),
                "metadata_sha256": hashlib.sha256(
                    (root / "fixture.json").read_bytes()
                ).hexdigest(),
            }
        )
    )


if __name__ == "__main__":
    main()
