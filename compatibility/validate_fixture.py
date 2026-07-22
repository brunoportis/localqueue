"""Validate an old fixture through public APIs of the installed candidate wheel."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import sqlite3
import time
from pathlib import Path


def assertion(result: dict[str, object], name: str, condition: bool) -> None:
    result.setdefault("assertions", []).append({"name": name, "ok": condition})  # type: ignore[union-attr]
    if not condition:
        raise AssertionError(name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    from localqueue import Empty, SimpleQueue

    fixture = json.loads((args.fixture / "fixture.json").read_text(encoding="utf-8"))
    result: dict[str, object] = {
        "package_version": importlib.metadata.version("localqueue"),
        "assertions": [],
    }
    with SimpleQueue(
        str(args.fixture), name="compat", lease_seconds=0.25, max_retries=1
    ) as queue:
        assertion(result, "initial_integrity", queue.check_integrity().ok)
        initial = queue.stats()
        assertion(result, "initial_counts", initial == fixture["expected_counts"])
        failed = queue.list_failed()
        assertion(
            result, "failed_payload", failed[0]["data"] == fixture["payloads"]["failed"]
        )
        assertion(
            result,
            "failed_error",
            failed[0]["last_error"] == fixture["dead_letter_error"],
        )
        ready = queue.get()
        assertion(result, "ready_payload", ready.data == fixture["payloads"]["ready"])
        queue.ack(ready)
        deduplicated = queue.get()
        assertion(
            result,
            "deduplicated_payload",
            deduplicated.data == {"kind": "deduplicated"},
        )
        queue.ack(deduplicated)
        assertion(result, "processing_initial", queue.stats()["processing"] == 1)
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and queue.reclaim_expired_leases() == 0:
            time.sleep(0.05)
        assertion(result, "reclaimed_once", queue.stats()["processing"] == 0)
        reclaimed = queue.get()
        assertion(
            result,
            "processing_payload",
            reclaimed.data == fixture["payloads"]["processing"],
        )
        assertion(result, "processing_attempt", reclaimed.attempts == 1)
        queue.ack(reclaimed)
        try:
            queue.get_nowait()
            raise AssertionError("delayed_available_too_early")
        except Empty:
            assertion(result, "delayed_not_ready_early", True)
        remaining = max(0.0, fixture["delayed_available_at"] - time.time())
        time.sleep(remaining + 0.1)
        delayed = queue.get()
        assertion(
            result, "delayed_payload", delayed.data == fixture["payloads"]["delayed"]
        )
        assertion(result, "delayed_attempt", delayed.attempts == 1)
        queue.ack(delayed)
        queue.retry_failed(failed[0]["id"])
        retried = queue.get()
        assertion(
            result,
            "retry_failed_payload",
            retried.data == fixture["payloads"]["failed"],
        )
        queue.ack(retried)
        duplicate = queue.put({"kind": "ignored"}, job_id="compat-dedup")
        assertion(
            result, "deduplication_return", duplicate == fixture["ids"]["deduplicated"]
        )
        assertion(result, "purge_acked", queue.purge(0, include_failed=True) >= 4)
        assertion(result, "final_integrity", queue.check_integrity().ok)
        assertion(result, "no_processing", queue.stats()["processing"] == 0)
    with sqlite3.connect(args.fixture / "localqueue.db") as db:
        assertion(
            result,
            "sqlite_integrity",
            db.execute("PRAGMA integrity_check").fetchone()[0] == "ok",
        )

    if fixture["capabilities"]["event_bus"]:
        from localqueue.bus import BaseEvent, BusTopology, EventBus

        class CurrentEvent(BaseEvent):
            event_name = "compat.historical"
            label: str

        observed: list[CurrentEvent] = []
        bus = EventBus(
            str(args.fixture),
            name="compat-bus",
            topology=BusTopology({"events": [CurrentEvent]}),
        )
        subscription = bus.subscription("events")

        @subscription.handler(CurrentEvent)
        async def consume(event: CurrentEvent) -> None:
            observed.append(event)

        asyncio.run(bus.run(idle_timeout=0.2))
        bus.close()
        assertion(
            result,
            "eventbus_consumed",
            len(observed) == 1
            and observed[0].label == fixture["event"]["payload"]["label"],
        )
        assertion(
            result,
            "eventbus_root_causality",
            observed[0].causation_id is None
            and observed[0].correlation_id == observed[0].event_id,
        )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
