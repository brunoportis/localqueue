from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest
from localqueue import SimpleQueue
from localqueue import localqueue as _native
from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    SequenceSource,
    SourceChanged,
    event,
)
from localqueue.bus.execution import _ExecutionHandle


@event(identity="key")
class Imported(BaseEvent):
    event_name = "execution-lifecycle.imported"

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


def test_execution_ingests_and_waits_for_its_tracked_delivery(tmp_path) -> None:
    bus = EventBus(str(tmp_path), topology=BusTopology({"imports": [Imported]}))
    handled: list[str] = []

    @bus.subscription("imports").handler(Imported)
    def handle(event: Imported) -> None:
        handled.append(event.key)

    @bus.source(
        SequenceSource([Imported(key="one"), Imported(key="two")], fingerprint="v1"),
        checkpoint="imports",
        batch_size=1,
    )
    def imports(event: Imported) -> Imported:
        return event

    async def run() -> None:
        execution = await bus._open_execution(imports)
        workers = asyncio.create_task(bus.run())
        try:
            result = await execution.run(timeout=2)
        finally:
            workers.cancel()
            await asyncio.gather(workers, return_exceptions=True)
        assert result.source_completed is True
        assert result.items_committed == 2
        assert result.events_dispatched == 2
        assert result.batches_committed == 2
        assert result.ready == result.processing == 0
        assert result.acknowledged == result.total == 2

    try:
        asyncio.run(run())
        assert handled == ["one", "two"]
    finally:
        bus.close()


def test_execution_rejects_invalid_source_contract_before_opening(tmp_path) -> None:
    bus = EventBus(str(tmp_path), topology=BusTopology({"imports": [Imported]}))

    @bus.source(["not-resumable"], checkpoint="imports")
    def generic(value: str) -> Imported:
        return Imported(key=value)

    @bus.source(SequenceSource([], fingerprint=""), checkpoint="empty-fingerprint")
    def empty_fingerprint(value: str) -> Imported:
        return Imported(key=value)

    async def run() -> None:
        with pytest.raises(TypeError, match="ResumableSource"):
            await bus._open_execution(generic)
        with pytest.raises(ValueError, match="non-empty source fingerprint"):
            await bus._open_execution(empty_fingerprint)

    try:
        asyncio.run(run())
    finally:
        bus.close()


def test_execution_rejects_changed_checkpoint_source_before_opening(tmp_path) -> None:
    bus = EventBus(str(tmp_path), topology=BusTopology({"imports": [Imported]}))

    @bus.source(SequenceSource([], fingerprint="v2"), checkpoint="imports")
    def changed(value: str) -> Imported:
        return Imported(key=value)

    async def run() -> None:
        await bus.ingest(
            SequenceSource([Imported(key="original")], fingerprint="v1"),
            checkpoint="imports",
        )
        with pytest.raises(SourceChanged):
            await bus._open_execution(changed)

    try:
        asyncio.run(run())
    finally:
        bus.close()


def test_execution_timeout_validation_and_contended_source_claim(tmp_path) -> None:
    bus = EventBus(str(tmp_path), topology=BusTopology({"imports": [Imported]}))

    @bus.source(SequenceSource([], fingerprint="v1"), checkpoint="imports")
    def imports(value: str) -> Imported:
        return Imported(key=value)

    async def run() -> None:
        execution = await bus._open_execution(imports)
        native = bus._get_native()
        assert native._execution_claim_source(
            str(execution.execution_id), "other", 60_000
        )
        with pytest.raises(ValueError, match="positive finite"):
            await execution.run(timeout=0)
        with pytest.raises(TimeoutError):
            await execution.run(timeout=0.01)
        assert native._execution_release_source_lease(
            str(execution.execution_id), "other"
        )

    try:
        asyncio.run(run())
    finally:
        bus.close()


def test_execution_heartbeat_renews_owned_source_claim(tmp_path, monkeypatch) -> None:
    bus = EventBus(str(tmp_path), topology=BusTopology({"imports": [Imported]}))
    native = bus._get_native()
    execution_id, _ = native._execution_open(
        str(uuid4()), bus.name, "source", "checkpoint", "v1", None
    )
    assert native._execution_claim_source(execution_id, "owner", 60_000)
    handle = _ExecutionHandle(bus, None, UUID(execution_id), False)
    monkeypatch.setattr("localqueue.bus.execution._LEASE_MS", 3)

    async def run() -> None:
        heartbeat = asyncio.create_task(handle._heartbeat("owner"))
        try:
            await asyncio.sleep(0.01)
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
        assert handle.inspect().source_lease_until is not None

    try:
        asyncio.run(run())
    finally:
        bus.close()


def test_execution_invalid_source_record_releases_lease(tmp_path) -> None:
    class InvalidSource:
        fingerprint = "v1"

        def open(self, cursor):
            del cursor
            yield object()

    bus = EventBus(str(tmp_path), topology=BusTopology({"imports": [Imported]}))

    @bus.source(InvalidSource(), checkpoint="imports")
    def imports(value: object) -> Imported:
        return Imported(key=str(value))

    async def run() -> None:
        execution = await bus._open_execution(imports)
        with pytest.raises(TypeError, match="SourceRecord"):
            await execution.run(timeout=1)
        assert execution.inspect().source_completed is False

    try:
        asyncio.run(run())
    finally:
        bus.close()
