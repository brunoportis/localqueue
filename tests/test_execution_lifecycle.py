from __future__ import annotations

import asyncio
import multiprocessing as mp
import os
import sqlite3
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
from localqueue.bus.execution import _ExecutionHandle, _ExecutionSnapshot
from localqueue.bus.ingestion import (
    _ClaimedExecutionIngestion,
    _run_claimed_execution_ingestion,
)


@event(identity="key")
class Imported(BaseEvent):
    event_name = "execution-lifecycle.imported"

    key: str


def _commit_execution_batch_then_crash(path: str) -> None:
    queue = SimpleQueue(path, name="q")
    native = queue._get_native()
    execution_id, _ = native._execution_open("crashed", "bus", "source", "checkpoint", "v1")
    claimed, *_ = native._execution_claim_source(execution_id, "owner", 60_000)
    assert claimed
    native._enqueue_batch_with_claimed_execution(
        [], None, ("bus", "checkpoint", None, None, "cursor-1", "v1", 1),
        execution_id, "owner", 0, 1,
    )
    os._exit(0)


def _concurrently_open_execution(path: str, result: object) -> None:
    queue = SimpleQueue(path, name="q")
    try:
        execution_id, created = queue._get_native()._execution_open(
            str(uuid4()), "bus", "source", "checkpoint", "v1"
        )
        result.put((execution_id, created))
    finally:
        queue.close()


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
        finished = await second.wait(timeout=1)
        assert result.source_completed is True
        assert result.completed is True
        assert finished.execution_id == result.execution_id
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
            "run", "bus", "source", "checkpoint", "v1"
        )
        assert native._execution_claim_source(execution_id, "one", 60_000)[0] is True
        assert native._execution_claim_source(execution_id, "two", 60_000)[0] is False
        with pytest.raises(_native.ExecutionLeaseLost):
            native._execution_mark_source_completed_claimed(execution_id, "two")
        assert (
            native._execution_mark_source_completed_claimed(execution_id, "one") is True
        )
    finally:
        queue.close()


def test_execution_claim_fences_reset_recreate_with_same_checkpoint_version(
    tmp_path,
) -> None:
    queue = SimpleQueue(str(tmp_path), name="q")
    native = queue._get_native()
    try:
        execution_id, _ = native._execution_open(
            "run", "bus", "source", "checkpoint", "v1"
        )
        claimed, cursor, fingerprint, generation, version = (
            native._execution_claim_source(execution_id, "owner", 60_000)
        )
        assert (claimed, cursor, fingerprint, generation, version) == (
            True,
            None,
            None,
            None,
            None,
        )
        native._enqueue_batch_with_claimed_execution(
            [],
            None,
            ("bus", "checkpoint", None, None, "old-cursor", "v1", 1),
            execution_id,
            "owner",
            0,
            1,
        )
        old = native._checkpoint_inspect("bus", "checkpoint")
        assert old is not None
        assert native._execution_release_source_lease(execution_id, "owner")
        assert native._checkpoint_reset("bus", "checkpoint")
        native._enqueue_batch_with_identity_and_checkpoint(
            [],
            None,
            ("bus", "checkpoint", None, None, "new-cursor", "v1", 1),
        )
        new = native._checkpoint_inspect("bus", "checkpoint")
        assert new is not None
        assert old[0] == "old-cursor"
        assert new[0] == "new-cursor"
        assert old[3] == new[3]
        assert old[2] != new[2]

        with pytest.raises(_native.CheckpointConflict):
            native._execution_claim_source(execution_id, "next-owner", 60_000)
    finally:
        queue.close()


def test_execution_resumes_after_process_crashes_with_committed_batch(tmp_path) -> None:
    context = mp.get_context("spawn")
    child = context.Process(target=_commit_execution_batch_then_crash, args=(str(tmp_path),))
    child.start()
    child.join(timeout=10)
    assert child.exitcode == 0

    with sqlite3.connect(tmp_path / "localqueue.db") as connection:
        connection.execute(
            "UPDATE event_bus_execution_runtime SET source_lease_until=0 WHERE execution_id='crashed'"
        )

    queue = SimpleQueue(str(tmp_path), name="q")
    native = queue._get_native()
    try:
        execution_id, created = native._execution_open(
            "replacement", "bus", "source", "checkpoint", "v1"
        )
        assert (execution_id, created) == ("crashed", False)
        claimed, cursor, fingerprint, generation, version = native._execution_claim_source(
            execution_id, "replacement-owner", 60_000
        )
        assert claimed is True
        assert (cursor, fingerprint, generation, version) == (
            "cursor-1", "v1", native._checkpoint_inspect("bus", "checkpoint")[2], 1
        )
        assert native._execution_mark_source_completed_claimed(
            execution_id, "replacement-owner"
        )
    finally:
        queue.close()


def test_execution_open_converges_across_bounded_spawn_processes(tmp_path) -> None:
    context = mp.get_context("spawn")
    result = context.Queue()
    processes = [
        context.Process(target=_concurrently_open_execution, args=(str(tmp_path), result))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    opened = [result.get(timeout=10) for _ in processes]
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    assert len({execution_id for execution_id, _ in opened}) == 1
    assert sum(created for _, created in opened) == 1


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


def test_execution_rejects_checkpoint_generation_without_runtime(tmp_path) -> None:
    bus = EventBus(str(tmp_path), topology=BusTopology({"imports": [Imported]}))

    @bus.source(SequenceSource(["one"], fingerprint="v1"), checkpoint="imports")
    def imports(value: str) -> Imported:
        return Imported(key=value)

    async def run() -> None:
        await imports.ingest()
        with pytest.raises(_native.LocalQueueError, match="not owned"):
            await bus._open_execution(imports)

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
        )[0]
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
        str(uuid4()), bus.name, "source", "checkpoint", "v1"
    )
    assert native._execution_claim_source(execution_id, "owner", 60_000)[0]
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


def test_execution_propagates_heartbeat_failure_and_cancels_ingestion(
    tmp_path, monkeypatch
) -> None:
    """A lost lease must stop an active source before another batch can commit."""
    bus = EventBus(str(tmp_path), topology=BusTopology({"imports": [Imported]}))
    ingestion_started = asyncio.Event()
    ingestion_cancelled = asyncio.Event()

    @bus.source(SequenceSource([], fingerprint="v1"), checkpoint="imports")
    def imports(value: str) -> Imported:
        return Imported(key=value)

    async def blocked_ingestion(*_args: object) -> None:
        ingestion_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            ingestion_cancelled.set()

    async def lost_lease(_receipt: str) -> None:
        await ingestion_started.wait()
        raise _native.ExecutionLeaseLost("lease reclaimed")

    async def run() -> None:
        execution = await bus._open_execution(imports)
        monkeypatch.setattr(
            "localqueue.bus.execution._run_claimed_execution_ingestion",
            blocked_ingestion,
        )
        monkeypatch.setattr(execution, "_heartbeat", lost_lease)
        with pytest.raises(_native.ExecutionLeaseLost, match="reclaimed"):
            await execution.run(timeout=1)
        assert ingestion_cancelled.is_set()
        assert execution.inspect().source_lease_until is None

    try:
        asyncio.run(run())
    finally:
        bus.close()


def test_execution_retries_claim_and_rechecks_finalization_before_returning(
    monkeypatch,
) -> None:
    """A retry that reopens work between snapshots keeps wait() polling."""
    execution_id = uuid4()

    def snapshot(*, completed: bool = False, ready: int = 0) -> _ExecutionSnapshot:
        return _ExecutionSnapshot(
            execution_id=execution_id,
            source_name="source",
            checkpoint_name="checkpoint",
            source_fingerprint="v1",
            checkpoint_generation="generation",
            source_completed=True,
            source_completed_at=1,
            completed_at=2 if completed else None,
            items_committed=0,
            events_dispatched=0,
            events_unrouted=0,
            deliveries_inserted=0,
            deliveries_deduplicated=0,
            batches_committed=0,
            total=ready,
            ready=ready,
            processing=0,
            acknowledged=0,
            failed=0,
            source_lease_until=None,
            created_at=1,
            updated_at=1,
        )

    class Native:
        def __init__(self) -> None:
            self.finalize_calls = 0

        def _execution_claim_source(
            self, _execution_id: str, _receipt: str, _lease_ms: int
        ) -> tuple[bool, None, None, None, None]:
            return False, None, None, None, None

        def _execution_finalize_if_complete(self, _execution_id: str) -> bool:
            self.finalize_calls += 1
            return self.finalize_calls == 2

    class Bus:
        def __init__(self) -> None:
            self.native = Native()

        def _get_native(self) -> Native:
            return self.native

    bus = Bus()
    handle = _ExecutionHandle(bus, None, execution_id, False)
    snapshots = iter(
        (
            _ExecutionSnapshot(
                execution_id=execution_id,
                source_name="source",
                checkpoint_name="checkpoint",
                source_fingerprint="v1",
                checkpoint_generation="generation",
                source_completed=False,
                source_completed_at=None,
                completed_at=None,
                items_committed=0,
                events_dispatched=0,
                events_unrouted=0,
                deliveries_inserted=0,
                deliveries_deduplicated=0,
                batches_committed=0,
                total=0,
                ready=0,
                processing=0,
                acknowledged=0,
                failed=0,
                source_lease_until=None,
                created_at=1,
                updated_at=1,
            ),
            snapshot(),
            snapshot(),
            snapshot(ready=1),
            snapshot(),
            snapshot(completed=True),
        )
    )
    monkeypatch.setattr(handle, "inspect", lambda: next(snapshots))
    monkeypatch.setattr("localqueue.bus.execution._POLL_SECONDS", 0)

    result = asyncio.run(handle.run(timeout=1))

    assert result.completed is True
    assert bus.native.finalize_calls == 2


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


def test_claimed_execution_splits_an_impossible_batch(tmp_path, monkeypatch) -> None:
    bus = EventBus(str(tmp_path), topology=BusTopology({"imports": [Imported]}))

    class Native:
        def __init__(self) -> None:
            self.calls = 0

        def _enqueue_batch_with_claimed_execution(self, *args):
            del args
            self.calls += 1
            if self.calls == 1:
                raise _native._FullImpossible()
            return [], "generation", self.calls

    native = Native()
    monkeypatch.setattr(bus, "_get_native", lambda: native)

    async def run() -> None:
        await _run_claimed_execution_ingestion(
            bus,
            SequenceSource(
                [Imported(key="one"), Imported(key="two")], fingerprint="v1"
            ),
            _ClaimedExecutionIngestion(
                checkpoint="imports",
                transform=None,
                batch_size=2,
                max_pending=None,
                execution_id="run",
                receipt="receipt",
                start_cursor=None,
                generation=None,
                version=None,
                fingerprint="v1",
            ),
        )

    try:
        asyncio.run(run())
        assert native.calls == 3
    finally:
        bus.close()
