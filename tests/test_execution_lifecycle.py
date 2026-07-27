from __future__ import annotations

import asyncio

import pytest
from localqueue import SimpleQueue
from localqueue import localqueue as _native
from localqueue.bus import BaseEvent, BusTopology, EventBus, SequenceSource, event


@event(identity="key")
class Imported(BaseEvent):
    key: str


def test_pending_execution_is_shared_and_empty_source_finalizes(tmp_path) -> None:
    bus = EventBus(str(tmp_path), topology=BusTopology({"imports": [Imported]}))

    @bus.source(SequenceSource([], fingerprint="empty-v1"), checkpoint="imports")
    def imports(value: str) -> Imported:
        return Imported(key=value)

    async def run() -> None:
        first = await bus._open_execution(imports)
        second = await bus._open_execution(imports)
        assert first.execution_id == second.execution_id
        assert second.resumed is True
        result = await first.run(timeout=1)
        assert result.source_completed is True
        assert result.completed is True
        assert result.total == result.ready == result.processing == 0

    try:
        asyncio.run(run())
    finally:
        bus.close()


def test_execution_claim_is_exclusive_and_stale_receipt_is_fenced(tmp_path) -> None:
    queue = SimpleQueue(str(tmp_path), name="q")
    native = queue._get_native()
    try:
        execution_id, _ = native._execution_open(
            "run", "bus", "source", "checkpoint", "v1", None
        )
        assert native._execution_claim_source(execution_id, "one", 60_000) is True
        assert native._execution_claim_source(execution_id, "two", 60_000) is False
        with pytest.raises(_native.ExecutionLeaseLost):
            native._execution_mark_source_completed_claimed(execution_id, "two")
        assert (
            native._execution_mark_source_completed_claimed(execution_id, "one") is True
        )
    finally:
        queue.close()
