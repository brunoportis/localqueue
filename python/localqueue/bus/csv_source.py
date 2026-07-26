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
from dataclasses import asdict, dataclass
from typing import Any, cast, override

from localqueue.bus.sources import SourceRecord

__all__ = ["CsvRow", "CsvSource", "CsvSourceError"]

_CURSOR_VERSION = 1

# The csv module exposes no __version__; its behavior is tied to the
# interpreter, so the parser identity is the Python major.minor version.
_PARSER_VERSION = f"python-csv/{sys.version_info.major}.{sys.version_info.minor}"

_DIALECT_ATTRIBUTES = (
    "delimiter",
    "quotechar",
    "doublequote",
    "escapechar",
    "quoting",
    "skipinitialspace",
)


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
        self._data = dict(data)
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


@dataclass(frozen=True, slots=True)
class _CsvFormat:
    """Effective parser configuration, frozen at construction time.

    Resolved once from the dialect (a name looked up via
    ``csv.get_dialect`` or a ``csv.Dialect`` instance) plus the
    ``delimiter``/``strict`` overrides. Used both in the fingerprint and
    to build the ``csv.reader`` — the dialect name is never re-resolved
    at ``open`` time, so re-registering the name later cannot change an
    existing source.
    """

    delimiter: str
    quotechar: str | None
    doublequote: bool
    escapechar: str | None
    quoting: int
    skipinitialspace: bool
    strict: bool


def _canonical_hash(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _is_dialect_like(value: object) -> bool:
    """Return whether ``value`` is a dialect returned by the csv module.

    ``csv.get_dialect()`` returns the C implementation's private Dialect
    type, which is not an ``isinstance(..., csv.Dialect)`` on every Python
    version.  The public reader API accepts it, so accept the same effective
    dialect shape here.
    """
    return all(hasattr(value, attribute) for attribute in _DIALECT_ATTRIBUTES)


class CsvSource:
    """Resumable source streaming rows of a CSV file.

    The file must stay immutable for the duration of an ingestion run;
    ``open`` snapshots the open descriptor with ``os.fstat`` and re-checks
    it at EOF, raising :class:`CsvSourceError` if the file changed.

    ``fingerprint=None`` computes an automatic fingerprint from the file
    snapshot (size, mtime, device/inode — never the full content) plus the
    parser version and the frozen parsing configuration. A user-provided
    ``fingerprint`` string is an external identity: it is combined with
    the parsing configuration only (changing ``delimiter`` still changes
    the fingerprint) and the snapshot is excluded from the hash, so
    modification between runs is not detected by the fingerprint. In both
    modes the file must exist at construction time and the snapshot still
    protects the construction → open → EOF window: ``open`` re-stats the
    open descriptor and compares it with the construction snapshot, and
    re-checks it again at EOF, raising :class:`CsvSourceError` on change.
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
        if not isinstance(dialect, str) and not _is_dialect_like(dialect):
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
        self._format = self._resolve_format(path, dialect, delimiter, strict)
        self._fieldnames = list(fieldnames) if fieldnames is not None else None

        config = {
            "parser": _PARSER_VERSION,
            "encoding": encoding,
            "format": asdict(self._format),
            "header": self._fieldnames if self._fieldnames is not None else "header",
        }
        try:
            stat_result = os.stat(self._path)
        except OSError as error:
            raise CsvSourceError(self._path, "cannot stat file", cause=error) from error
        self._snapshot = _snapshot_from_stat(stat_result)
        if fingerprint is not None:
            # External identity: replaces the file snapshot in the hash,
            # still combined with the parsing configuration. The snapshot
            # is still captured above and guards construction → open → EOF.
            self.fingerprint = _canonical_hash(
                {"external": fingerprint, "config": config}
            )
        else:
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

    def _resolve_format(
        self,
        path: str | os.PathLike[str],
        dialect: str | csv.Dialect,
        delimiter: str | None,
        strict: bool,
    ) -> _CsvFormat:
        resolved: Any
        if isinstance(dialect, str):
            try:
                resolved = csv.get_dialect(dialect)
            except csv.Error as error:
                raise CsvSourceError(
                    path, f"unknown csv dialect {dialect!r}", cause=error
                ) from error
        else:
            resolved = dialect
        effective_delimiter = delimiter if delimiter is not None else resolved.delimiter
        if not isinstance(effective_delimiter, str) or len(effective_delimiter) != 1:
            raise CsvSourceError(
                path,
                f"'delimiter' must be a single character, "
                f"found {effective_delimiter!r}",
            )
        quotechar = resolved.quotechar
        if (
            quotechar is not None
            and (not isinstance(quotechar, str) or len(quotechar) != 1)
        ):
            raise CsvSourceError(
                path, "dialect quotechar must be a single character or None"
            )
        escapechar = resolved.escapechar
        if (
            escapechar is not None
            and (not isinstance(escapechar, str) or len(escapechar) != 1)
        ):
            raise CsvSourceError(
                path, "dialect escapechar must be a single character or None"
            )
        return _CsvFormat(
            delimiter=effective_delimiter,
            quotechar=quotechar,
            doublequote=bool(resolved.doublequote),
            escapechar=escapechar,
            quoting=int(resolved.quoting),
            skipinitialspace=bool(resolved.skipinitialspace),
            strict=strict,
        )

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
            or record < 0
            or not isinstance(line, int)
            or isinstance(line, bool)
            or line < 0
        ):
            raise CsvSourceError(self._path, f"invalid cursor {cursor!r}")
        return cookie, record, line

    def _make_reader(self, stream: Any) -> Any:
        try:
            # Feed the reader through readline() only: iterating the
            # TextIOWrapper directly would invalidate tell()/seek().
            return csv.reader(
                iter(stream.readline, ""),
                delimiter=self._format.delimiter,
                quotechar=self._format.quotechar,
                doublequote=self._format.doublequote,
                escapechar=self._format.escapechar,
                quoting=cast(Any, self._format.quoting),
                skipinitialspace=self._format.skipinitialspace,
                strict=self._format.strict,
            )
        except (csv.Error, TypeError) as error:
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
            if open_snapshot != self._snapshot:
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
                # The cookie is an opaque TextIOWrapper position, not a byte
                # offset: with stateful encodings a valid cookie may exceed
                # the file size, so it is never compared against st_size.
                try:
                    stream.seek(cookie)
                except (OSError, ValueError, OverflowError) as error:
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
