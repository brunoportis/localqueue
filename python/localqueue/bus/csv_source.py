"""Resumable CSV ingestion source for :class:`EventBus`.

:class:`CsvSource` streams a CSV file lazily, pairing every data record
(:class:`CsvRow`) with an opaque cursor. The cursor wraps the seekable
cookie of ``TextIOWrapper.tell()``, so resuming is an O(1) ``seek`` — no
linear replay of earlier records. To keep ``tell()`` valid the underlying
stream is only ever advanced through ``readline()`` calls fed to
``csv.reader``; it is never iterated directly.

Contract: the file must remain immutable while an ingestion run reads it.
The snapshot (size, mtime, device/inode) is captured with ``os.fstat`` on
the open descriptor when ``open`` runs and verified again at EOF; any
change raises :class:`CsvSourceError`. No per-line stat is performed.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
from collections.abc import Iterator, Mapping
from typing import Any, override

from localqueue.bus.sources import SourceRecord

__all__ = ["CsvRow", "CsvSource", "CsvSourceError"]

_CURSOR_VERSION = 1

# The csv module exposes no __version__; its behavior is tied to the
# interpreter, so the parser identity is the Python major.minor version.
_PARSER_VERSION = f"python-csv/{sys.version_info.major}.{sys.version_info.minor}"


class CsvSourceError(Exception):
    """Error raised by :class:`CsvSource` for any CSV/source failure.

    Carries the file ``path``, the logical ``record_number`` and physical
    ``line_number`` when known (``None`` otherwise), and the underlying
    ``cause`` when the failure wraps another exception.
    """

    def __init__(
        self,
        path: os.PathLike[str] | str,
        message: str,
        *,
        record_number: int | None = None,
        line_number: int | None = None,
        cause: BaseException | None = None,
    ) -> None:
        self.path = os.fspath(path)
        self.record_number = record_number
        self.line_number = line_number
        self.cause = cause
        location = f"{self.path}"
        if record_number is not None and line_number is not None:
            location += f": record {record_number}, line {line_number}"
        super().__init__(f"{location}: {message}")


class CsvRow(Mapping[str, str]):
    """One immutable CSV data record as a string mapping.

    Behaves like a read-only ``dict[str, str]``: ``row["cnpj"]``,
    ``"cnpj" in row``, ``len(row)``, iteration, and ``Event(**row)``
    unpacking all work. ``record_number`` is the 1-based logical record
    index counted from the first data record; ``line_number`` is the
    physical line where the record ended (they differ for quoted
    multi-line records).
    """

    __slots__ = ("_data", "_record_number", "_line_number")

    def __init__(
        self, data: dict[str, str], *, record_number: int, line_number: int
    ) -> None:
        self._data = data
        self._record_number = record_number
        self._line_number = line_number

    @property
    def record_number(self) -> int:
        return self._record_number

    @property
    def line_number(self) -> int:
        return self._line_number

    @override
    def __getitem__(self, key: str) -> str:
        return self._data[key]

    @override
    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    @override
    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return (
            f"CsvRow({self._data!r}, record_number={self._record_number}, "
            f"line_number={self._line_number})"
        )


def _snapshot_from_stat(stat_result: os.stat_result) -> dict[str, Any]:
    return {
        "size": stat_result.st_size,
        "mtime_ns": stat_result.st_mtime_ns,
        "st_dev": getattr(stat_result, "st_dev", None),
        "st_ino": getattr(stat_result, "st_ino", None),
    }


def _dialect_config(dialect: str | csv.Dialect) -> Any:
    if isinstance(dialect, str):
        return {"name": dialect}
    return {
        "parameters": {
            "delimiter": dialect.delimiter,
            "quotechar": dialect.quotechar,
            "doublequote": dialect.doublequote,
            "escapechar": dialect.escapechar,
            "lineterminator": dialect.lineterminator,
            "quoting": dialect.quoting,
            "skipinitialspace": dialect.skipinitialspace,
        }
    }


def _canonical_hash(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class CsvSource:
    """Resumable source streaming rows of a CSV file.

    The file must stay immutable for the duration of an ingestion run;
    ``open`` snapshots the open descriptor with ``os.fstat`` and re-checks
    it at EOF, raising :class:`CsvSourceError` if the file changed.

    ``fingerprint=None`` computes an automatic fingerprint from the file
    snapshot (size, mtime, device/inode — never the full content) plus the
    parser version and parsing configuration; the file must exist at
    construction time in this mode. A user-provided ``fingerprint`` string
    is an external identity: it is combined with the parsing configuration
    only (changing ``delimiter`` still changes the fingerprint), the file
    snapshot is excluded, and modification between runs is not detected by
    the fingerprint — but the during-run immutability check still applies.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        encoding: str = "utf-8-sig",
        dialect: str | csv.Dialect = "excel",
        delimiter: str | None = None,
        fieldnames: list[str] | tuple[str, ...] | None = None,
        strict: bool = True,
        fingerprint: str | None = None,
    ) -> None:
        if not isinstance(path, (str, os.PathLike)):
            raise TypeError("'path' must be a string or os.PathLike")
        if not isinstance(encoding, str):
            raise TypeError("'encoding' must be a string")
        if not isinstance(dialect, (str, csv.Dialect)):
            raise TypeError("'dialect' must be a string or csv.Dialect")
        if delimiter is not None and not isinstance(delimiter, str):
            raise TypeError("'delimiter' must be a string or None")
        if not isinstance(strict, bool):
            raise TypeError("'strict' must be a boolean")
        if fingerprint is not None and not isinstance(fingerprint, str):
            raise TypeError("'fingerprint' must be a string or None")
        if fieldnames is not None:
            if not isinstance(fieldnames, (list, tuple)):
                raise TypeError("'fieldnames' must be a list or tuple of strings")
            self._validate_fieldnames(path, fieldnames)

        self._path = os.fspath(path)
        self._encoding = encoding
        self._dialect = dialect
        self._delimiter = delimiter
        self._fieldnames = list(fieldnames) if fieldnames is not None else None
        self._strict = strict

        config = {
            "parser": _PARSER_VERSION,
            "encoding": encoding,
            "dialect": _dialect_config(dialect),
            "delimiter": self._effective_delimiter(),
            "header": self._fieldnames if self._fieldnames is not None else "header",
        }
        if fingerprint is not None:
            # External identity: replaces the file snapshot, still combined
            # with the parsing configuration.
            self.fingerprint = _canonical_hash(
                {"external": fingerprint, "config": config}
            )
            self._snapshot: dict[str, Any] | None = None
        else:
            try:
                stat_result = os.stat(self._path)
            except OSError as error:
                raise CsvSourceError(
                    self._path, "cannot stat file", cause=error
                ) from error
            self._snapshot = _snapshot_from_stat(stat_result)
            self.fingerprint = _canonical_hash(
                {"snapshot": self._snapshot, "config": config}
            )

    def open(self, cursor: str | None) -> Iterator[SourceRecord[CsvRow]]:
        position = self._parse_cursor(cursor)
        try:
            stream = open(self._path, encoding=self._encoding, newline="")
        except (OSError, LookupError) as error:
            raise CsvSourceError(self._path, "cannot open file", cause=error) from error
        return self._iterate(stream, position)

    # -- internals ---------------------------------------------------------

    def _effective_delimiter(self) -> str:
        if self._delimiter is not None:
            return self._delimiter
        if isinstance(self._dialect, csv.Dialect):
            return self._dialect.delimiter
        try:
            return csv.get_dialect(self._dialect).delimiter
        except csv.Error as error:
            raise CsvSourceError(
                self._path, f"unknown csv dialect {self._dialect!r}", cause=error
            ) from error

    def _validate_fieldnames(
        self, path: str | os.PathLike[str], fieldnames: list[str] | tuple[str, ...]
    ) -> None:
        for name in fieldnames:
            if not isinstance(name, str):
                raise CsvSourceError(
                    path, f"fieldnames must be strings, found {name!r}"
                )
        if len(set(fieldnames)) != len(fieldnames):
            raise CsvSourceError(path, f"duplicate fieldnames: {list(fieldnames)!r}")

    def _parse_cursor(self, cursor: str | None) -> tuple[int, int, int] | None:
        if cursor is None or cursor == "":
            return None
        try:
            payload = json.loads(cursor)
        except (json.JSONDecodeError, TypeError) as error:
            raise CsvSourceError(
                self._path, f"invalid cursor {cursor!r}", cause=error
            ) from error
        if not isinstance(payload, dict):
            raise CsvSourceError(self._path, f"invalid cursor {cursor!r}")
        version = payload.get("version")
        if version != _CURSOR_VERSION:
            raise CsvSourceError(
                self._path, f"unknown cursor version {version!r} in {cursor!r}"
            )
        cookie = payload.get("cookie")
        record = payload.get("record")
        line = payload.get("line")
        if (
            not isinstance(cookie, int)
            or isinstance(cookie, bool)
            or cookie < 0
            or not isinstance(record, int)
            or isinstance(record, bool)
            or not isinstance(line, int)
            or isinstance(line, bool)
        ):
            raise CsvSourceError(self._path, f"invalid cursor {cursor!r}")
        return cookie, record, line

    def _make_reader(self, stream: Any) -> Any:
        kwargs: dict[str, Any] = {"dialect": self._dialect, "strict": self._strict}
        if self._delimiter is not None:
            kwargs["delimiter"] = self._delimiter
        try:
            # Feed the reader through readline() only: iterating the
            # TextIOWrapper directly would invalidate tell()/seek().
            return csv.reader(iter(stream.readline, ""), **kwargs)
        except (csv.Error, LookupError) as error:
            raise CsvSourceError(
                self._path, "cannot create csv reader", cause=error
            ) from error

    def _read_header(self, stream: Any) -> tuple[list[str], int]:
        reader = self._make_reader(stream)
        try:
            header = next(reader, None)
        except (csv.Error, UnicodeDecodeError) as error:
            raise CsvSourceError(
                self._path, "malformed CSV while reading header", cause=error
            ) from error
        if header is None:
            raise CsvSourceError(self._path, "empty file: expected a header record")
        self._validate_fieldnames(self._path, header)
        return header, reader.line_num

    def _iterate(
        self, stream: Any, position: tuple[int, int, int] | None
    ) -> Iterator[SourceRecord[CsvRow]]:
        try:
            try:
                open_snapshot = _snapshot_from_stat(os.fstat(stream.fileno()))
            except OSError as error:
                raise CsvSourceError(
                    self._path, "cannot stat open file", cause=error
                ) from error
            if self._snapshot is not None and open_snapshot != self._snapshot:
                raise CsvSourceError(
                    self._path,
                    "file changed between CsvSource construction and open()",
                )

            if self._fieldnames is not None:
                fieldnames = self._fieldnames
                header_lines = 0
            else:
                fieldnames, header_lines = self._read_header(stream)
            expected_columns = len(fieldnames)

            if position is None:
                record_number = 0
                base_line = header_lines
                reader = self._make_reader(stream)
            else:
                cookie, record_number, base_line = position
                if cookie > open_snapshot["size"]:
                    raise CsvSourceError(
                        self._path,
                        f"cursor cookie {cookie} is past the end of the file",
                    )
                try:
                    stream.seek(cookie)
                except OSError as error:
                    raise CsvSourceError(
                        self._path,
                        f"cannot seek to cursor cookie {cookie}",
                        cause=error,
                    ) from error
                reader = self._make_reader(stream)

            while True:
                try:
                    row = next(reader, None)
                except (csv.Error, UnicodeDecodeError) as error:
                    raise CsvSourceError(
                        self._path,
                        "malformed CSV",
                        record_number=record_number + 1,
                        line_number=base_line + reader.line_num,
                        cause=error,
                    ) from error
                if row is None:
                    break
                record_number += 1
                line_number = base_line + reader.line_num
                if len(row) != expected_columns:
                    raise CsvSourceError(
                        self._path,
                        f"expected {expected_columns} columns, found {len(row)}",
                        record_number=record_number,
                        line_number=line_number,
                    )
                cookie = stream.tell()
                cursor = json.dumps(
                    {
                        "version": _CURSOR_VERSION,
                        "cookie": cookie,
                        "record": record_number,
                        "line": line_number,
                    },
                    separators=(",", ":"),
                )
                value = CsvRow(
                    dict(zip(fieldnames, row)),
                    record_number=record_number,
                    line_number=line_number,
                )
                yield SourceRecord(value=value, cursor=cursor)

            try:
                eof_snapshot = _snapshot_from_stat(os.fstat(stream.fileno()))
            except OSError as error:
                raise CsvSourceError(
                    self._path, "cannot stat file at end of ingestion", cause=error
                ) from error
            if eof_snapshot != open_snapshot:
                raise CsvSourceError(self._path, "file changed during ingestion")
        finally:
            stream.close()
