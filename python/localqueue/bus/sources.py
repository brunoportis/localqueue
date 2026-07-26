"""Resumable ingestion sources for :class:`EventBus`.

A resumable source pairs every item with a cursor that marks the position
*after* that item. ``EventBus.ingest(..., checkpoint=...)`` persists the
cursor of every committed batch, so an interrupted run can resume exactly
where the last committed batch ended instead of re-consuming the source.
"""

from __future__ import annotations

from collections.abc import AsyncIterable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar, runtime_checkable

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class SourceRecord(Generic[T]):
    """One source item paired with the cursor positioned after it.

    ``cursor`` is an opaque position token owned by the source; it is
    persisted verbatim in the ingestion checkpoint once the batch that
    contains this record commits.
    """

    value: T
    cursor: str


@runtime_checkable
class ResumableSource(Protocol[T]):
    """Source able to resume iteration from a previously persisted cursor.

    ``open`` is called with the cursor stored by the last committed
    checkpoint batch — ``None`` when no checkpoint exists yet — and must
    yield :class:`SourceRecord` items starting immediately after that
    position. ``fingerprint`` identifies the source snapshot; when it
    changes between runs, ingestion raises ``SourceChanged`` before any
    item is consumed.
    """

    fingerprint: str | None

    def open(
        self, cursor: str | None
    ) -> Iterable[SourceRecord[T]] | AsyncIterable[SourceRecord[T]]: ...


class SequenceSource(Generic[T]):
    """Resumable source over a :class:`collections.abc.Sequence`.

    The sequence is never materialized or copied: items are indexed lazily,
    starting at the position encoded by the cursor. The cursor is the
    decimal string of the next index, so ``"N"`` resumes at index ``N``
    without touching earlier items. ``None`` or ``""`` starts at index 0.
    """

    def __init__(
        self, sequence: Sequence[T], *, fingerprint: str | None = None
    ) -> None:
        if not isinstance(sequence, Sequence):
            raise TypeError("'sequence' must be a collections.abc.Sequence")
        if fingerprint is not None and not isinstance(fingerprint, str):
            raise TypeError("'fingerprint' must be a string or None")
        self._sequence = sequence
        self.fingerprint = fingerprint

    def open(self, cursor: str | None) -> Iterator[SourceRecord[T]]:
        start = self._resolve_cursor(cursor)
        for index in range(start, len(self._sequence)):
            yield SourceRecord(value=self._sequence[index], cursor=str(index + 1))

    def _resolve_cursor(self, cursor: str | None) -> int:
        if cursor is None or cursor == "":
            return 0
        if not isinstance(cursor, str) or not cursor.isdecimal():
            raise ValueError(
                f"invalid SequenceSource cursor {cursor!r}: "
                "expected the decimal string of the next index"
            )
        start = int(cursor)
        if start > len(self._sequence):
            raise ValueError(
                f"invalid SequenceSource cursor {cursor!r}: "
                f"index is past the end of a sequence of length {len(self._sequence)}"
            )
        return start
