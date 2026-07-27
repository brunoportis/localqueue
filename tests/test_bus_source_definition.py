from __future__ import annotations

import asyncio

import pytest
from localqueue import DeliveryPolicy
from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    NoSubscribers,
    SequenceSource,
    SourceDefinition,
    event,
)


@event(identity="key")
class Imported(BaseEvent):
    key: str


def make_bus(path) -> EventBus:
    return EventBus(
        str(path),
        topology=BusTopology({"imports": [Imported]}),
        delivery=DeliveryPolicy(lease_seconds=30.0, max_retries=1),
    )


def test_source_definition_exposes_declaration_and_delegates(tmp_path):
    bus = make_bus(tmp_path)
    rows = [{"key": "one"}, {"key": "two"}]

    @bus.source(rows, checkpoint=None, batch_size=2)
    def imports(row: dict[str, str]) -> Imported:
        return Imported(key=row["key"])

    try:
        assert isinstance(imports, SourceDefinition)
        assert imports.name == "imports"
        assert imports.bus is bus
        assert imports.source is rows
        assert imports.transform.__name__ == "imports"
        assert imports.checkpoint is None
        assert imports.config.batch_size == 2
        assert imports.config.max_pending is None

        imports.config.batch_size = 1
        result = asyncio.run(imports.ingest())

        assert result.items_read == 2
        assert result.batches_committed == 2
        assert imports.config.frozen is True
        with pytest.raises(RuntimeError, match="source configuration is frozen"):
            imports.config.batch_size = 3
    finally:
        bus.close()


@pytest.mark.parametrize(
    "field, value",
    [
        ("batch_size", 0),
        ("batch_size", True),
        ("max_pending", 0),
        ("max_pending", True),
    ],
)
def test_source_config_validates_assignments(tmp_path, field, value):
    bus = make_bus(tmp_path)

    @bus.source([])
    def imports(row: str) -> Imported:
        return Imported(key=row)

    try:
        with pytest.raises((TypeError, ValueError)):
            setattr(imports.config, field, value)
    finally:
        bus.close()


def test_source_definition_freezes_before_ingestion_failure(tmp_path):
    bus = EventBus(str(tmp_path), topology=BusTopology({}))

    @bus.source(["one"])
    def imports(row: str) -> Imported:
        return Imported(key=row)

    try:
        with pytest.raises(NoSubscribers):
            asyncio.run(imports.ingest())
        assert imports.config.frozen is True
    finally:
        bus.close()


def test_async_source_and_transform_work(tmp_path):
    bus = make_bus(tmp_path)

    async def rows():
        yield "one"
        yield "two"

    @bus.source(rows())
    async def imports(row: str) -> Imported:
        return Imported(key=row)

    try:
        result = asyncio.run(imports.ingest())
        assert result.items_read == 2
        assert result.events_dispatched == 2
    finally:
        bus.close()


def test_configuration_freezes_as_soon_as_ingestion_starts(tmp_path):
    bus = make_bus(tmp_path)
    source_started = asyncio.Event()
    release_source = asyncio.Event()

    async def rows():
        source_started.set()
        await release_source.wait()
        yield "one"

    @bus.source(rows())
    def imports(row: str) -> Imported:
        return Imported(key=row)

    async def exercise() -> None:
        task = asyncio.create_task(imports.ingest())
        await source_started.wait()
        with pytest.raises(RuntimeError, match="frozen"):
            imports.config.max_pending = 10
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    try:
        asyncio.run(exercise())
        assert imports.config.frozen is True
    finally:
        bus.close()


def test_invalid_definition_config_does_not_consume_the_source(tmp_path):
    bus = make_bus(tmp_path)
    consumed = False

    def rows():
        nonlocal consumed
        consumed = True
        yield "one"

    try:
        with pytest.raises(ValueError, match="batch_size"):
            bus.source(rows(), batch_size=0)
        assert consumed is False
    finally:
        bus.close()


def test_resumable_source_resumes_on_a_second_call(tmp_path):
    bus = make_bus(tmp_path)

    @bus.source(SequenceSource(["one", "two"], fingerprint="v1"), checkpoint="rows")
    def imports(row: str) -> Imported:
        return Imported(key=row)

    try:
        first = asyncio.run(imports.ingest())
        second = asyncio.run(imports.ingest())
        assert first.items_read == 2
        assert second.items_read == 0
        assert second.checkpoint is not None
        assert second.checkpoint.resumed is True
    finally:
        bus.close()
