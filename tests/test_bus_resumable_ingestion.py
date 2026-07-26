from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Sequence
from pathlib import Path

import pytest
from localqueue import CheckpointConflict, DeliveryPolicy
from localqueue import localqueue as native_module
from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    SequenceSource,
    SourceChanged,
    SourceRecord,
    event,
)


class Tick(BaseEvent):
    event_name = "resumable-ingestion.tick"

    seq: int


@event(identity="key")
class Keyed(BaseEvent):
    event_name = "resumable-ingestion.keyed"

    key: str
    value: str


S1 = "__bus__:test:s1"


def make_bus(path, topology=None, **kwargs) -> EventBus:
    return EventBus(
        str(path),
        name="test",
        topology=BusTopology(topology if topology is not None else {"s1": ["*"]}),
        delivery=DeliveryPolicy(lease_seconds=30.0, max_retries=1),
        **kwargs,
    )


def run(coro):
    return asyncio.run(coro)


def queue_seqs(path: Path, queue: str) -> list[int]:
    connection = sqlite3.connect(path / "localqueue.db")
    try:
        rows = connection.execute(
            "SELECT payload FROM messages WHERE queue = ? ORDER BY id", (queue,)
        ).fetchall()
    finally:
        connection.close()
    return [json.loads(row[0])["payload"]["seq"] for row in rows]


def keyed_source(count: int, *, fingerprint: str = "v1") -> SequenceSource:
    return SequenceSource(
        [Keyed(key=f"k{i}", value=f"v{i}") for i in range(count)],
        fingerprint=fingerprint,
    )


class SpySource:
    """ResumableSource wrapper recording open() calls."""

    def __init__(self, inner) -> None:
        self._inner = inner
        self.open_calls: list[str | None] = []

    @property
    def fingerprint(self):
        return self._inner.fingerprint

    def open(self, cursor):
        self.open_calls.append(cursor)
        return self._inner.open(cursor)


class FailingSource:
    """ResumableSource that raises mid-iteration, after committed batches."""

    fingerprint = "v1"

    def __init__(self, events, fail_at: int) -> None:
        self._events = events
        self._fail_at = fail_at

    def open(self, cursor):
        start = int(cursor) if cursor else 0

        def gen():
            for index in range(start, len(self._events)):
                if index >= self._fail_at:
                    raise RuntimeError("source died")
                yield SourceRecord(self._events[index], str(index + 1))

        return gen()


class RecordingSequence(Sequence):
    """Sequence recording every __getitem__ access."""

    def __init__(self, items) -> None:
        self._items = items
        self.accesses: list[int] = []

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, index):
        self.accesses.append(index)
        return self._items[index]


class TestResumableCommit:
    def test_transform_failure_leaves_batch_uncommitted(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        events = [Tick(seq=i) for i in range(5)]

        def failing_transform(event):
            if event.seq == 2:
                raise RuntimeError("transform died")
            return event

        try:
            with pytest.raises(RuntimeError, match="transform died"):
                run(
                    bus.ingest(
                        SequenceSource(events, fingerprint="v1"),
                        checkpoint="import",
                        transform=failing_transform,
                        batch_size=2,
                    )
                )
            # The first batch committed; the interrupted batch did not, and
            # the cursor still points at its first item.
            state = bus.checkpoint("import").inspect()
            assert state is not None
            assert state.cursor == "2"
            assert state.items_committed == 2

            result = run(
                bus.ingest(
                    SequenceSource(events, fingerprint="v1"),
                    checkpoint="import",
                    batch_size=2,
                )
            )
            assert result.items_read == 3
            assert result.deliveries_inserted == 3
            assert result.checkpoint is not None
            assert result.checkpoint.start_cursor == "2"
            assert result.checkpoint.end_cursor == "5"
            assert result.checkpoint.resumed is True
            assert queue_seqs(tmp_path / "bus", S1) == [0, 1, 2, 3, 4]
        finally:
            bus.close()

    def test_source_failure_keeps_prior_batches(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        events = [Tick(seq=i) for i in range(6)]
        try:
            with pytest.raises(RuntimeError, match="source died"):
                run(
                    bus.ingest(
                        FailingSource(events, fail_at=4),
                        checkpoint="import",
                        batch_size=2,
                    )
                )
            state = bus.checkpoint("import").inspect()
            assert state is not None
            assert state.cursor == "4"
            assert state.batches_committed == 2

            result = run(
                bus.ingest(
                    SequenceSource(events, fingerprint="v1"),
                    checkpoint="import",
                    batch_size=2,
                )
            )
            assert result.items_read == 2
            assert result.deliveries_inserted == 2
            assert queue_seqs(tmp_path / "bus", S1) == [0, 1, 2, 3, 4, 5]
        finally:
            bus.close()

    def test_rerun_deduplicated_advances_cursor_with_zero_inserts(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            first = run(bus.ingest(keyed_source(3), checkpoint="import", batch_size=2))
            assert first.deliveries_inserted == 3
            assert bus.checkpoint("import").reset() is True

            rerun = run(bus.ingest(keyed_source(3), checkpoint="import", batch_size=2))
            assert rerun.items_read == 3
            assert rerun.deliveries_inserted == 0
            assert rerun.deliveries_deduplicated == 3
            assert rerun.checkpoint is not None
            assert rerun.checkpoint.end_cursor == "3"
            assert rerun.checkpoint.resumed is False
            state = bus.checkpoint("import").inspect()
            assert state is not None
            assert state.cursor == "3"
        finally:
            bus.close()

    def test_unrouted_batch_is_checkpoint_only_commit(self, tmp_path):
        bus = make_bus(tmp_path / "bus", topology={}, require_subscribers=False)
        try:
            result = run(
                bus.ingest(
                    SequenceSource([Tick(seq=i) for i in range(3)]),
                    checkpoint="import",
                    batch_size=2,
                )
            )
            assert result.items_read == 3
            assert result.events_unrouted == 3
            assert result.events_dispatched == 0
            assert result.deliveries_total == 0
            assert result.batches_committed == 2
            state = bus.checkpoint("import").inspect()
            assert state is not None
            assert state.cursor == "3"
            assert state.items_committed == 3
            connection = sqlite3.connect(tmp_path / "bus" / "localqueue.db")
            try:
                (count,) = connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE queue LIKE '__bus__:test:%'"
                ).fetchone()
            finally:
                connection.close()
            assert count == 0
        finally:
            bus.close()

    def test_split_first_half_advances_cursor_independently(
        self, tmp_path, monkeypatch
    ):
        bus = make_bus(tmp_path / "bus")
        original_native = bus._native_queue

        class SplitThenFail:
            """Native boundary fake that splits once and fails its second half.

            ``_commit_resumable_group`` invokes the native boundary in a
            worker thread. Keeping this fake fully in Python avoids making a
            thread-affinity test of the PyO3 object while still asserting the
            source-item split and checkpoint arguments passed to that boundary.
            """

            def __init__(self) -> None:
                self.singles = 0
                self.entries = []
                self.state = None

            def _checkpoint_inspect(self, bus_name, checkpoint_name):
                return self.state

            def _enqueue_batch_with_identity_and_checkpoint(
                self, entries, capacity, checkpoint
            ):
                if len(entries) > 1:
                    raise native_module._FullImpossible("too many")
                self.singles += 1
                if self.singles == 2:
                    raise RuntimeError("second half died")
                bus_name, name, expected, cursor, fingerprint, item_count = checkpoint
                assert (bus_name, name, expected) == ("test", "import", None)
                self.entries.extend(entries)
                self.state = (cursor, fingerprint, 1, item_count, 1, 0, 0)
                return ([(index + 1, True) for index in range(len(entries))], 1)

            def close(self):
                original_native.close()

        native = SplitThenFail()
        monkeypatch.setattr(bus, "_native_queue", native)
        try:
            with pytest.raises(RuntimeError, match="second half died"):
                run(
                    bus.ingest(
                        SequenceSource([Tick(seq=1), Tick(seq=2)]),
                        checkpoint="import",
                        batch_size=2,
                        max_pending=10,
                    )
                )
            # The first half committed with its own cursor even though the
            # second half never committed.
            state = bus.checkpoint("import").inspect()
            assert state is not None
            assert state.cursor == "1"
            assert state.items_committed == 1
            assert [
                json.loads(entry[1])["payload"]["seq"] for entry in native.entries
            ] == [1]
        finally:
            bus.close()


class TestCheckpointGuards:
    def test_fingerprint_mismatch_raises_before_consuming(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            run(bus.ingest(keyed_source(2, fingerprint="v1"), checkpoint="import"))
            changed = SpySource(keyed_source(2, fingerprint="v2"))
            with pytest.raises(SourceChanged, match="'import'.*'v1'.*'v2'"):
                run(bus.ingest(changed, checkpoint="import"))
            assert changed.open_calls == []
            assert bus.checkpoint("import").inspect() is not None
        finally:
            bus.close()

    def test_fingerprint_mismatch_message_mentions_reset(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            run(bus.ingest(keyed_source(1, fingerprint="v1"), checkpoint="import"))
            with pytest.raises(SourceChanged, match="reset the checkpoint"):
                run(bus.ingest(keyed_source(1, fingerprint="v2"), checkpoint="import"))
        finally:
            bus.close()

    def test_checkpoint_conflict_propagates(self, tmp_path, monkeypatch):
        bus = make_bus(tmp_path / "bus")
        native = bus._native_queue

        class ConflictInjector:
            """Externally advance the checkpoint after the first commit."""

            def __init__(self) -> None:
                self.bumped = False

            def _checkpoint_inspect(self, bus_name, checkpoint_name):
                return native._checkpoint_inspect(bus_name, checkpoint_name)

            def _enqueue_batch_with_identity_and_checkpoint(
                self, entries, capacity, checkpoint
            ):
                result = native._enqueue_batch_with_identity_and_checkpoint(
                    entries, capacity, checkpoint
                )
                if not self.bumped:
                    self.bumped = True
                    bus_name, name, _expected, cursor, fingerprint, _n = checkpoint
                    native._enqueue_batch_with_identity_and_checkpoint(
                        [], None, (bus_name, name, result[1], cursor, fingerprint, 0)
                    )
                return result

            def close(self):
                return native.close()

        monkeypatch.setattr(bus, "_native_queue", ConflictInjector())
        try:
            with pytest.raises(CheckpointConflict):
                run(
                    bus.ingest(
                        SequenceSource([Tick(seq=i) for i in range(4)]),
                        checkpoint="import",
                        batch_size=2,
                    )
                )
            # The first batch stayed committed; the conflicting batch did not.
            connection = sqlite3.connect(tmp_path / "bus" / "localqueue.db")
            try:
                (count,) = connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE queue = ?", (S1,)
                ).fetchone()
            finally:
                connection.close()
            assert count == 2
            state = bus.checkpoint("import").inspect()
            assert state is not None
            assert state.cursor == "2"
            assert state.version == 2
        finally:
            bus.close()

    def test_non_resumable_source_with_checkpoint_raises_type_error(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            with pytest.raises(TypeError, match="ResumableSource"):
                run(bus.ingest([Tick(seq=1)], checkpoint="import"))
            assert bus.checkpoint("import").inspect() is None
        finally:
            bus.close()

    def test_invalid_checkpoint_names_rejected(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            with pytest.raises(TypeError, match="'checkpoint'"):
                run(bus.ingest(keyed_source(1), checkpoint=1))
            with pytest.raises(ValueError, match="'checkpoint'"):
                run(bus.ingest(keyed_source(1), checkpoint=""))
            with pytest.raises(TypeError, match="'name'"):
                bus.checkpoint(1)
            with pytest.raises(ValueError, match="'name'"):
                bus.checkpoint("")
        finally:
            bus.close()


class TestCheckpointHandle:
    def test_inspect_and_reset_happy_path(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        handle = bus.checkpoint("import")
        try:
            assert handle.name == "import"
            assert handle.inspect() is None
            assert handle.reset() is False

            run(bus.ingest(keyed_source(5), checkpoint="import", batch_size=2))
            state = handle.inspect()
            assert state is not None
            assert state.cursor == "5"
            assert state.source_fingerprint == "v1"
            assert state.version == 3
            assert state.items_committed == 5
            assert state.batches_committed == 3
            assert isinstance(state.created_at, int)
            assert isinstance(state.updated_at, int)

            assert handle.reset() is True
            assert handle.inspect() is None
        finally:
            bus.close()

    def test_reset_preserves_deliveries(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            run(bus.ingest(keyed_source(2), checkpoint="import"))
            connection = sqlite3.connect(tmp_path / "bus" / "localqueue.db")
            try:
                (count,) = connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE queue = ?", (S1,)
                ).fetchone()
            finally:
                connection.close()
            assert count == 2
            assert bus.checkpoint("import").reset() is True
            assert bus.checkpoint("import").inspect() is None
            connection = sqlite3.connect(tmp_path / "bus" / "localqueue.db")
            try:
                (count,) = connection.execute(
                    "SELECT COUNT(*) FROM messages WHERE queue = ?", (S1,)
                ).fetchone()
            finally:
                connection.close()
            assert count == 2
        finally:
            bus.close()


class TestNoCheckpointParity:
    def test_result_without_checkpoint_is_unchanged(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            result = run(bus.ingest([Tick(seq=1), Tick(seq=2)], batch_size=1))
            assert result.checkpoint is None
            assert result.deliveries_inserted == 2
            assert result.batches_committed == 2
        finally:
            bus.close()

    def test_resumable_source_without_checkpoint_behaves_like_plain(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        try:
            # Without checkpoint= a ResumableSource is just an Iterable? No:
            # it is not iterable, so it must be rejected like any non-source.
            with pytest.raises(TypeError):
                run(bus.ingest(SequenceSource([Tick(seq=1)])))
        finally:
            bus.close()


class TestSequenceSource:
    def test_cursor_resumes_without_touching_earlier_items(self):
        recorded = RecordingSequence(["a", "b", "c", "d"])
        source = SequenceSource(recorded)
        records = list(source.open("2"))
        assert recorded.accesses == [2, 3]
        assert [record.value for record in records] == ["c", "d"]
        assert [record.cursor for record in records] == ["3", "4"]

    def test_empty_and_none_cursor_start_at_zero(self):
        recorded = RecordingSequence(["a", "b"])
        source = SequenceSource(recorded)
        assert [record.value for record in source.open(None)] == ["a", "b"]
        assert [record.value for record in source.open("")] == ["a", "b"]
        assert recorded.accesses == [0, 1, 0, 1]

    def test_cursor_at_end_yields_nothing(self):
        source = SequenceSource(["a", "b"])
        assert list(source.open("2")) == []

    def test_invalid_cursors_rejected(self):
        source = SequenceSource(["a", "b"])
        for cursor in ("x", "-1", "1.5", "3"):
            with pytest.raises(ValueError, match="cursor"):
                list(source.open(cursor))

    def test_invalid_constructor_arguments_rejected(self):
        with pytest.raises(TypeError, match="'sequence'"):
            SequenceSource((x for x in range(3)))
        with pytest.raises(TypeError, match="'fingerprint'"):
            SequenceSource(["a"], fingerprint=1)

    def test_source_record_fields(self):
        record = SourceRecord(value=42, cursor="7")
        assert record.value == 42
        assert record.cursor == "7"

    def test_end_to_end_resume_indexes_only_remaining_items(self, tmp_path):
        bus = make_bus(tmp_path / "bus")
        recorded = RecordingSequence([Tick(seq=i) for i in range(4)])
        try:
            first = run(
                bus.ingest(
                    SequenceSource(recorded, fingerprint="v1"),
                    checkpoint="import",
                    batch_size=2,
                )
            )
            assert first.checkpoint is not None
            assert first.checkpoint.end_cursor == "4"
            assert recorded.accesses == [0, 1, 2, 3]

            recorded.accesses.clear()
            rerun = run(
                bus.ingest(
                    SequenceSource(recorded, fingerprint="v1"),
                    checkpoint="import",
                    batch_size=2,
                )
            )
            assert rerun.items_read == 0
            assert recorded.accesses == []
        finally:
            bus.close()
