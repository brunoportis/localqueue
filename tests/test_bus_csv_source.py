from __future__ import annotations

import asyncio
import csv
import json
import sqlite3
from pathlib import Path

import pytest
from localqueue import DeliveryPolicy, Empty
from localqueue.bus import (
    BaseEvent,
    BusTopology,
    CsvRow,
    CsvSource,
    CsvSourceError,
    EventBus,
    SourceChanged,
    event,
)


@event(identity="cnpj")
class ContactCreationRequested(BaseEvent):
    event_name = "csv-source.contact"

    cnpj: str
    name: str


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


def write_csv(path: Path, text: str) -> Path:
    path.write_bytes(text.encode("utf-8"))
    return path


def to_contact(row: CsvRow) -> ContactCreationRequested:
    return ContactCreationRequested(**row)


def queue_cnpjs(path: Path, queue: str) -> list[str]:
    connection = sqlite3.connect(path / "localqueue.db")
    try:
        rows = connection.execute(
            "SELECT payload FROM messages WHERE queue = ? ORDER BY id", (queue,)
        ).fetchall()
    finally:
        connection.close()
    return [json.loads(row[0])["payload"]["cnpj"] for row in rows]


class CountingCsvSource(CsvSource):
    """CsvSource counting every row consumed from the csv reader."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.rows_read = 0

    def _make_reader(self, stream):
        reader = super()._make_reader(stream)
        source = self

        class CountingReader:
            @property
            def line_num(self):
                return reader.line_num

            def __iter__(self):
                return self

            def __next__(self):
                row = next(reader)
                source.rows_read += 1
                return row

        return CountingReader()


class CloseTrackingCsvSource(CsvSource):
    """CsvSource that exposes whether its current stream was closed."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.closed = False

    def _iterate(self, stream, position):
        try:
            yield from super()._iterate(stream, position)
        finally:
            self.closed = stream.closed


class TestCsvSourceBasics:
    def test_simple_csv_with_header(self, tmp_path):
        path = write_csv(tmp_path / "contacts.csv", "cnpj,name\n1,Ana\n2,Bob\n")
        records = list(CsvSource(path).open(None))
        assert [record.value["cnpj"] for record in records] == ["1", "2"]
        assert [record.value["name"] for record in records] == ["Ana", "Bob"]
        assert [record.value.record_number for record in records] == [1, 2]
        assert [record.value.line_number for record in records] == [2, 3]

    def test_explicit_fieldnames_first_record_is_data(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "1,Ana\n2,Bob\n")
        source = CsvSource(path, fieldnames=["cnpj", "name"])
        records = list(source.open(None))
        assert [record.value["cnpj"] for record in records] == ["1", "2"]
        assert [record.value.record_number for record in records] == [1, 2]
        assert [record.value.line_number for record in records] == [1, 2]

    def test_comma_and_semicolon_delimiters(self, tmp_path):
        comma = write_csv(tmp_path / "comma.csv", "a,b\n1,2\n")
        semicolon = write_csv(tmp_path / "semicolon.csv", "a;b\n1;2\n")
        (row_a,) = [record.value for record in CsvSource(comma).open(None)]
        (row_b,) = [
            record.value for record in CsvSource(semicolon, delimiter=";").open(None)
        ]
        assert row_a["b"] == "2"
        assert row_b["b"] == "2"

    def test_utf8_with_and_without_bom(self, tmp_path):
        without = write_csv(tmp_path / "plain.csv", "a,b\n1,2\n")
        with_bom = tmp_path / "bom.csv"
        with_bom.write_bytes("a,b\n1,2\n".encode("utf-8-sig"))
        (plain_row,) = [record.value for record in CsvSource(without).open(None)]
        (bom_row,) = [record.value for record in CsvSource(with_bom).open(None)]
        assert plain_row["a"] == "1"
        assert bom_row["a"] == "1"

    def test_multibyte_characters(self, tmp_path):
        path = write_csv(
            tmp_path / "multi.csv", "name,city\nJosé,São Paulo\n太郎,東京\n"
        )
        rows = [record.value for record in CsvSource(path).open(None)]
        assert rows[0]["name"] == "José"
        assert rows[1]["city"] == "東京"

    def test_quoted_commas(self, tmp_path):
        path = write_csv(tmp_path / "quoted.csv", 'a,b\n"1,000",x\n')
        (row,) = [record.value for record in CsvSource(path).open(None)]
        assert row["a"] == "1,000"
        assert row["b"] == "x"

    def test_quoted_newlines_multiline_record(self, tmp_path):
        path = write_csv(tmp_path / "multi_line.csv", 'a,b\n"line1\nline2",x\n3,y\n')
        records = list(CsvSource(path).open(None))
        assert len(records) == 2
        first, second = records
        assert first.value["a"] == "line1\nline2"
        assert first.value.record_number == 1
        assert first.value.line_number == 3
        assert second.value.record_number == 2
        assert second.value.line_number == 4

    def test_escaped_quotes(self, tmp_path):
        path = write_csv(tmp_path / "quotes.csv", 'a,b\n"he said ""hi""",x\n')
        (row,) = [record.value for record in CsvSource(path).open(None)]
        assert row["a"] == 'he said "hi"'

    def test_windows_crlf_newlines(self, tmp_path):
        path = tmp_path / "crlf.csv"
        path.write_bytes(b'a,b\r\n"line1\r\nline2",x\r\n3,y\r\n')
        records = list(CsvSource(path).open(None))
        assert len(records) == 2
        assert records[0].value["a"] == "line1\r\nline2"
        assert records[0].value.line_number == 3
        assert records[1].value.record_number == 2
        assert records[1].value.line_number == 4
        # Resume must land exactly on the second record.
        resumed = list(CsvSource(path).open(records[0].cursor))
        assert [record.value["a"] for record in resumed] == ["3"]


class TestCsvSourceResume:
    def test_resume_starts_at_next_record(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a,b\n1,x\n2,y\n3,z\n")
        source = CsvSource(path)
        first_two = []
        for record in source.open(None):
            first_two.append(record)
            if len(first_two) == 2:
                break
        resumed = list(CsvSource(path).open(first_two[-1].cursor))
        assert [record.value["a"] for record in resumed] == ["3"]
        assert resumed[0].value.record_number == 3

    def test_resume_does_not_replay_earlier_records(self, tmp_path):
        path = tmp_path / "big.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            handle.write("a,b\n")
            for index in range(50_000):
                handle.write(f"{index},v{index}\n")
        source = CountingCsvSource(path)
        cursor = None
        for record in source.open(None):
            if record.value.record_number == 40_000:
                cursor = record.cursor
                break
        assert cursor is not None

        resumed_source = CountingCsvSource(path)
        iterator = resumed_source.open(cursor)
        first = next(iterator)
        assert first.value.record_number == 40_001
        assert first.value["a"] == "40000"
        iterator.close()
        # Only the header plus the resumed record were consumed: an O(1) seek.
        assert resumed_source.rows_read == 2

    def test_resume_after_multiline_record_across_instances(self, tmp_path):
        path = write_csv(tmp_path / "multi.csv", 'a,b\n1,"x\ny"\n2,z\n3,w\n')
        first = CsvSource(path)
        iterator = first.open(None)
        multiline = next(iterator)  # record 1 spans lines 2-3
        assert multiline.value["b"] == "x\ny"
        cursor = multiline.cursor
        iterator.close()

        # "Restart": a brand-new CsvSource seeks straight to the cursor.
        second = CsvSource(path)
        resumed = list(second.open(cursor))
        assert [record.value["a"] for record in resumed] == ["2", "3"]
        assert [record.value.record_number for record in resumed] == [2, 3]
        assert [record.value.line_number for record in resumed] == [4, 5]

    def test_open_with_empty_cursor_starts_at_beginning(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a\n1\n")
        assert len(list(CsvSource(path).open(""))) == 1

    def test_cursor_payload_shape(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a\n1\n")
        (record,) = CsvSource(path).open(None)
        payload = json.loads(record.cursor)
        assert payload["version"] == 1
        assert isinstance(payload["cookie"], int)
        assert payload["record"] == 1
        assert payload["line"] == 2

    def test_resume_with_utf16(self, tmp_path):
        path = tmp_path / "data.csv"
        path.write_bytes("a,b\n1,á\n2,é\n".encode("utf-16"))
        source = CsvSource(path, encoding="utf-16")
        iterator = source.open(None)
        first = next(iterator)
        iterator.close()

        resumed = list(CsvSource(path, encoding="utf-16").open(first.cursor))
        assert [record.value["a"] for record in resumed] == ["2"]

    def test_resume_with_stateful_encoding_uses_opaque_cookie(self, tmp_path):
        path = tmp_path / "data.csv"
        path.write_bytes("a,b\n1,日本\n2,語\n".encode("iso2022_jp"))
        source = CsvSource(path, encoding="iso2022_jp")
        iterator = source.open(None)
        first = next(iterator)
        iterator.close()

        assert json.loads(first.cursor)["cookie"] > path.stat().st_size
        resumed = list(CsvSource(path, encoding="iso2022_jp").open(first.cursor))
        assert [record.value["a"] for record in resumed] == ["2"]


class TestCsvSourceErrors:
    def test_invalid_cursor_json(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a\n1\n")
        source = CsvSource(path)
        with pytest.raises(CsvSourceError):
            list(source.open("{not json"))
        with pytest.raises(CsvSourceError):
            list(source.open("[1, 2]"))
        with pytest.raises(CsvSourceError):
            list(source.open('{"version": 1, "cookie": "x", "record": 1, "line": 1}'))
        with pytest.raises(CsvSourceError):
            list(source.open('{"version": 1, "cookie": -1, "record": 1, "line": 1}'))

    def test_unknown_cursor_version(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a\n1\n")
        source = CsvSource(path)
        cursor = json.dumps({"version": 99, "cookie": 0, "record": 0, "line": 0})
        with pytest.raises(CsvSourceError, match="version"):
            list(source.open(cursor))

    def test_invalid_cursor_cookie_is_normalized(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a\n1\n")
        source = CsvSource(path)
        cursor = json.dumps({"version": 1, "cookie": 2**63, "record": 1, "line": 2})
        with pytest.raises(CsvSourceError, match="cannot seek") as info:
            list(source.open(cursor))
        assert isinstance(info.value.cause, OSError)

    @pytest.mark.parametrize("field", ["record", "line"])
    def test_cursor_rejects_negative_record_and_line(self, tmp_path, field):
        path = write_csv(tmp_path / "data.csv", "a\n1\n")
        cursor = {"version": 1, "cookie": 0, "record": 0, "line": 0}
        cursor[field] = -1
        with pytest.raises(CsvSourceError, match="invalid cursor"):
            list(CsvSource(path).open(json.dumps(cursor)))

    @pytest.mark.parametrize("delimiter", ["", "||"])
    def test_invalid_delimiter_is_rejected_by_constructor(self, tmp_path, delimiter):
        path = write_csv(tmp_path / "data.csv", "a,b\n1,2\n")
        with pytest.raises(CsvSourceError, match="single character"):
            CsvSource(path, delimiter=delimiter)

    def test_duplicate_header(self, tmp_path):
        path = write_csv(tmp_path / "dup.csv", "a,a\n1,2\n")
        with pytest.raises(CsvSourceError, match="duplicate"):
            list(CsvSource(path).open(None))

    def test_duplicate_explicit_fieldnames(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "1,2\n")
        with pytest.raises(CsvSourceError, match="duplicate"):
            CsvSource(path, fieldnames=["a", "a"])

    def test_extra_column(self, tmp_path):
        path = write_csv(tmp_path / "extra.csv", "a,b\n1,2,3\n")
        with pytest.raises(CsvSourceError, match="expected 2 columns, found 3"):
            list(CsvSource(path).open(None))

    def test_missing_column(self, tmp_path):
        path = write_csv(tmp_path / "missing.csv", "a,b,c\n1,2\n")
        with pytest.raises(CsvSourceError, match="expected 3 columns, found 2"):
            list(CsvSource(path).open(None))

    def test_error_message_includes_path_record_and_line(self, tmp_path):
        path = write_csv(tmp_path / "contacts.csv", "a,b\n1,2\n3,4,5\n")
        with pytest.raises(
            CsvSourceError,
            match=r"contacts\.csv: record 2, line 3: expected 2 columns, found 3",
        ) as info:
            list(CsvSource(path).open(None))
        assert info.value.record_number == 2
        assert info.value.line_number == 3

    def test_malformed_csv_strict(self, tmp_path):
        path = write_csv(tmp_path / "bad.csv", 'a,b\n"unclosed,2\n')
        with pytest.raises(CsvSourceError, match="malformed"):
            list(CsvSource(path).open(None))

    def test_blank_line_is_a_shape_error(self, tmp_path):
        path = write_csv(tmp_path / "blank.csv", "a,b\n1,2\n\n3,4\n")
        with pytest.raises(CsvSourceError, match="expected 2 columns, found 0"):
            list(CsvSource(path).open(None))

    def test_empty_file_with_expected_header(self, tmp_path):
        path = write_csv(tmp_path / "empty.csv", "")
        with pytest.raises(CsvSourceError, match="header"):
            list(CsvSource(path).open(None))

    def test_missing_file(self, tmp_path):
        with pytest.raises(CsvSourceError, match="stat"):
            CsvSource(tmp_path / "nope.csv")

    def test_file_replaced_between_construction_and_open(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a\n1\n")
        source = CsvSource(path)
        path.write_bytes(b"a\n1\n2\n")  # replaced: size changed
        with pytest.raises(CsvSourceError, match="changed"):
            list(source.open(None))

    def test_file_truncated_between_construction_and_open(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a\n1\n2\n3\n")
        source = CsvSource(path)
        path.write_bytes(b"a\n")
        with pytest.raises(CsvSourceError, match="changed"):
            list(source.open(None))

    def test_external_fingerprint_still_detects_replacement_before_open(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a\n1\n")
        source = CsvSource(path, fingerprint="job")
        path.write_bytes(b"a\n1\n2\n")
        with pytest.raises(CsvSourceError, match="changed"):
            list(source.open(None))

    def test_invalid_constructor_arguments(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a\n1\n")
        with pytest.raises(TypeError, match="'path'"):
            CsvSource(123)
        with pytest.raises(TypeError, match="'encoding'"):
            CsvSource(path, encoding=1)
        with pytest.raises(TypeError, match="'dialect'"):
            CsvSource(path, dialect=1)
        with pytest.raises(TypeError, match="'delimiter'"):
            CsvSource(path, delimiter=1)
        with pytest.raises(TypeError, match="'strict'"):
            CsvSource(path, strict="yes")
        with pytest.raises(TypeError, match="'fingerprint'"):
            CsvSource(path, fingerprint=1)
        with pytest.raises(TypeError, match="'fieldnames'"):
            CsvSource(path, fieldnames="a,b")
        with pytest.raises(CsvSourceError, match="strings"):
            CsvSource(path, fieldnames=["a", 1])


class TestCsvSourceFingerprint:
    def test_fingerprint_includes_parsing_configuration(self, tmp_path):
        comma = write_csv(tmp_path / "comma.csv", "a,b\n1,2\n")
        semicolon = write_csv(tmp_path / "semicolon.csv", "a;b\n1;2\n")
        base = CsvSource(comma)
        other_delimiter = CsvSource(comma, delimiter=";")
        other_file = CsvSource(semicolon, delimiter=";")
        assert base.fingerprint != other_delimiter.fingerprint
        assert other_delimiter.fingerprint != other_file.fingerprint

    def test_external_fingerprint_combined_with_configuration(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a,b\n1,2\n")
        first = CsvSource(path, fingerprint="export-job:98f0c42")
        same = CsvSource(path, fingerprint="export-job:98f0c42")
        other_delimiter = CsvSource(
            path, delimiter=";", fingerprint="export-job:98f0c42"
        )
        other_identity = CsvSource(path, fingerprint="export-job:other")
        assert first.fingerprint == same.fingerprint
        assert first.fingerprint != other_delimiter.fingerprint
        assert first.fingerprint != other_identity.fingerprint

    def test_automatic_fingerprint_changes_with_file(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a\n1\n")
        before = CsvSource(path).fingerprint
        path.write_bytes(b"a\n1\n2\n")
        after = CsvSource(path).fingerprint
        assert before != after

    def test_external_fingerprint_ignores_file_snapshot(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a\n1\n")
        before = CsvSource(path, fingerprint="job").fingerprint
        path.write_bytes(b"a\n1\n2\n")
        after = CsvSource(path, fingerprint="job").fingerprint
        assert before == after

    def test_fingerprint_includes_strict_and_effective_dialect_options(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a,b\n1,2\n")
        base = CsvSource(path)
        assert base.fingerprint != CsvSource(path, strict=False).fingerprint
        assert base.fingerprint != CsvSource(path, dialect="unix").fingerprint

        class DifferentQuote(csv.Dialect):
            delimiter = ","
            quotechar = "'"
            doublequote = True
            escapechar = None
            quoting = csv.QUOTE_MINIMAL
            skipinitialspace = False
            lineterminator = "\n"

        class DifferentEscape(DifferentQuote):
            quotechar = '"'
            doublequote = False
            escapechar = "\\"

        assert base.fingerprint != CsvSource(path, dialect=DifferentQuote).fingerprint
        assert base.fingerprint != CsvSource(path, dialect=DifferentEscape).fingerprint

    def test_named_dialect_is_frozen_at_construction(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a;b\n1;2\n")
        name = "csv-source-frozen-dialect"
        csv.register_dialect(name, delimiter=";", quotechar='"')
        try:
            source = CsvSource(path, dialect=name)
            fingerprint = source.fingerprint
            csv.register_dialect(name, delimiter=",", quotechar="'")
            assert source.fingerprint == fingerprint
            assert [record.value["b"] for record in source.open(None)] == ["2"]
        finally:
            csv.unregister_dialect(name)

    def test_accepts_standard_dialect_object(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "a,b\n1,2\n")
        assert [
            record.value["b"]
            for record in CsvSource(path, dialect=csv.get_dialect("excel")).open(None)
        ] == ["2"]


class TestCsvRow:
    def test_mapping_behavior(self, tmp_path):
        path = write_csv(tmp_path / "data.csv", "cnpj,name\n1,Ana\n")
        (record,) = CsvSource(path).open(None)
        row = record.value
        assert isinstance(row, CsvRow)
        assert row["cnpj"] == "1"
        assert "name" in row
        assert "missing" not in row
        assert len(row) == 2
        assert sorted(iter(row)) == ["cnpj", "name"]
        assert sorted(row.keys()) == ["cnpj", "name"]
        assert row.get("missing", "fallback") == "fallback"
        contact = ContactCreationRequested(**row)
        assert contact.cnpj == "1"
        assert contact.name == "Ana"
        assert row.record_number == 1
        assert row.line_number == 2
        with pytest.raises(AttributeError):
            row.record_number = 9

    def test_copies_constructor_mapping(self):
        data = {"cnpj": "1"}
        row = CsvRow(data, record_number=1, line_number=2)
        data["cnpj"] = "changed"
        assert row["cnpj"] == "1"


class TestCsvSourceStreaming:
    def test_open_returns_a_lazy_generator(self, tmp_path):
        path = tmp_path / "large.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            handle.write("a,b\n")
            for index in range(200_000):
                handle.write(f"{index},v{index}\n")
        source = CountingCsvSource(path)
        iterator = source.open(None)
        assert source.rows_read == 0
        first = next(iterator)
        assert first.value.record_number == 1
        assert first.value["a"] == "0"
        # Only the header plus one record were consumed so far.
        assert source.rows_read == 2
        cookie = json.loads(first.cursor)["cookie"]
        assert cookie < path.stat().st_size
        iterator.close()

    def test_ingestion_of_large_file_streams(self, tmp_path):
        path = tmp_path / "large.csv"
        count = 200_000
        with path.open("w", encoding="utf-8", newline="") as handle:
            handle.write("cnpj,name\n")
            for index in range(count):
                handle.write(f"{index},name{index}\n")
        bus = make_bus(tmp_path / "bus")
        try:
            result = run(
                bus.ingest(
                    CsvSource(path),
                    checkpoint="import",
                    transform=to_contact,
                    batch_size=5_000,
                )
            )
            assert result.items_read == count
            assert result.deliveries_inserted == count
        finally:
            bus.close()


class TestCsvSourceCheckpoint:
    def test_checkpoint_end_to_end(self, tmp_path):
        path = write_csv(tmp_path / "contacts.csv", "cnpj,name\n1,Ana\n2,Bob\n3,Cid\n")
        bus = make_bus(tmp_path / "bus")
        try:
            result = run(
                bus.ingest(
                    CsvSource(path),
                    checkpoint="import",
                    transform=to_contact,
                    batch_size=2,
                )
            )
            assert result.items_read == 3
            assert result.deliveries_inserted == 3
            assert result.batches_committed == 2
            assert result.checkpoint is not None
            assert result.checkpoint.resumed is False
            state = bus.checkpoint("import").inspect()
            assert state is not None
            payload = json.loads(state.cursor)
            assert payload["version"] == 1
            assert payload["record"] == 3
            assert queue_cnpjs(tmp_path / "bus", S1) == ["1", "2", "3"]
        finally:
            bus.close()


class TestCsvSourceResourceClosure:
    def test_normal_ingestion_closes_file(self, tmp_path):
        path = write_csv(tmp_path / "contacts.csv", "cnpj,name\n1,Ana\n")
        source = CloseTrackingCsvSource(path)
        bus = make_bus(tmp_path / "bus")
        try:
            run(bus.ingest(source, checkpoint="import", transform=to_contact))
            assert source.closed
        finally:
            bus.close()

    def test_transform_failure_closes_file(self, tmp_path):
        path = write_csv(tmp_path / "contacts.csv", "cnpj,name\n1,Ana\n")
        source = CloseTrackingCsvSource(path)
        bus = make_bus(tmp_path / "bus")
        try:
            with pytest.raises(RuntimeError, match="transform failed"):
                run(
                    bus.ingest(
                        source,
                        checkpoint="import",
                        transform=lambda row: (_ for _ in ()).throw(
                            RuntimeError("transform failed")
                        ),
                    )
                )
            assert source.closed
        finally:
            bus.close()

    def test_source_failure_closes_file(self, tmp_path):
        path = write_csv(tmp_path / "contacts.csv", "cnpj,name\n1,Ana\n2\n")
        source = CloseTrackingCsvSource(path)
        bus = make_bus(tmp_path / "bus")
        try:
            with pytest.raises(CsvSourceError, match="expected 2 columns"):
                run(bus.ingest(source, checkpoint="import", transform=to_contact))
            assert source.closed
        finally:
            bus.close()

    def test_cancellation_and_bus_close_during_backpressure_close_file(self, tmp_path):
        path = write_csv(tmp_path / "contacts.csv", "cnpj,name\n1,Ana\n2,Bob\n")

        async def stop_ingestion(stop_bus: bool):
            source = CloseTrackingCsvSource(path)
            bus = make_bus(tmp_path / ("close-bus" if stop_bus else "cancel"))
            try:
                task = asyncio.create_task(
                    bus.ingest(
                        source,
                        checkpoint="import",
                        transform=to_contact,
                        batch_size=1,
                        max_pending=1,
                    )
                )
                await asyncio.sleep(0.05)
                if stop_bus:
                    bus.close()
                    with pytest.raises(RuntimeError, match="closed"):
                        await task
                else:
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                assert source.closed
            finally:
                if bus._native_queue is not None:
                    bus.close()

        run(stop_ingestion(False))
        run(stop_ingestion(True))


class TestCsvSourceIngestion:
    def test_crash_and_resume_without_reprocessing(self, tmp_path):
        path = write_csv(
            tmp_path / "contacts.csv",
            "cnpj,name\n" + "".join(f"{i},n{i}\n" for i in range(6)),
        )
        bus = make_bus(tmp_path / "bus")
        transformed: list[str] = []

        def flaky_transform(row: CsvRow):
            if row["cnpj"] == "4" and not transformed_done[0]:
                raise RuntimeError("transform died")
            transformed.append(row["cnpj"])
            return to_contact(row)

        transformed_done = [False]
        try:
            with pytest.raises(RuntimeError, match="transform died"):
                run(
                    bus.ingest(
                        CsvSource(path),
                        checkpoint="import",
                        transform=flaky_transform,
                        batch_size=2,
                    )
                )
            state = bus.checkpoint("import").inspect()
            assert state is not None
            assert json.loads(state.cursor)["record"] == 4

            transformed_done[0] = True
            result = run(
                bus.ingest(
                    CsvSource(path),
                    checkpoint="import",
                    transform=flaky_transform,
                    batch_size=2,
                )
            )
            assert result.items_read == 2
            assert result.checkpoint is not None
            assert result.checkpoint.resumed is True
            assert queue_cnpjs(tmp_path / "bus", S1) == [str(i) for i in range(6)]
            # Every row was transformed exactly once across both runs.
            assert sorted(transformed) == [str(i) for i in range(6)]
        finally:
            bus.close()

    def test_file_change_between_runs_raises_source_changed(self, tmp_path):
        path = write_csv(tmp_path / "contacts.csv", "cnpj,name\n1,Ana\n")
        bus = make_bus(tmp_path / "bus")
        try:
            run(bus.ingest(CsvSource(path), checkpoint="import", transform=to_contact))
            path.write_bytes(b"cnpj,name\n1,Ana\n2,Bob\n")
            with pytest.raises(SourceChanged):
                run(
                    bus.ingest(
                        CsvSource(path), checkpoint="import", transform=to_contact
                    )
                )
        finally:
            bus.close()

    def test_backpressure_does_not_consume_file_eagerly(self, tmp_path):
        path = tmp_path / "contacts.csv"
        count = 1_000
        with path.open("w", encoding="utf-8", newline="") as handle:
            handle.write("cnpj,name\n")
            for index in range(count):
                handle.write(f"{index},n{index}\n")
        source = CountingCsvSource(path)
        bus = make_bus(tmp_path / "bus")
        try:

            async def main():
                task = asyncio.create_task(
                    bus.ingest(
                        source,
                        checkpoint="import",
                        transform=to_contact,
                        batch_size=5,
                        max_pending=5,
                    )
                )
                queue = bus._open_subscription_queue("s1")
                try:
                    # With max_pending=5 and nobody draining, ingestion must
                    # stop after a few batches instead of reading the file.
                    await asyncio.sleep(0.5)
                    read_while_blocked = source.rows_read
                    while not task.done():
                        try:
                            queue.ack(queue.get_nowait())
                        except Empty:
                            await asyncio.sleep(0.001)
                finally:
                    queue.close()
                result = await task
                return result, read_while_blocked

            result, read_while_blocked = run(asyncio.wait_for(main(), 30))
            # Blocked on backpressure, the source had only read a small
            # prefix of the 1,000-record file (header + a few batches).
            assert read_while_blocked < count // 2
            assert result.items_read == count
            # The file was streamed record by record, never buffered whole:
            # exactly header + data rows passed through the reader.
            assert source.rows_read == count + 1
        finally:
            bus.close()
