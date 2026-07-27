"""Generic incremental event ingestion for :class:`EventBus`.

This module implements the batch-atomic ingestion pipeline behind
``EventBus.ingest``. It is kept separate from ``bus.py`` to keep the bus
module small and to avoid import cycles: it never imports ``bus.py`` at
module level, it only receives the bus instance at call time.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
)
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar
from uuid import UUID

from localqueue import localqueue as _native
from localqueue.bus.event import BaseEvent
from localqueue.bus.identity import prepare_event_persistence
from localqueue.bus.sources import ResumableSource, SourceRecord
from localqueue.exceptions import Full

if TYPE_CHECKING:
    from localqueue.bus.bus import EventBus
    from localqueue.bus.context import HandlerContext

_ContextT = TypeVar("_ContextT", bound="HandlerContext")

_MAX_BATCH_SIZE = 2**31 - 1
_MAX_PENDING_BOUND = 2**63 - 1
_BACKOFF_INITIAL_SECONDS = 0.01
_BACKOFF_MULTIPLIER = 1.5
_BACKOFF_CAP_SECONDS = 0.25


@dataclass(frozen=True, slots=True)
class IngestionResult:
    """Aggregate counters for one completed ``EventBus.ingest`` run.

    The result is constant-size: per-event receipts are never retained.
    """

    items_read: int
    events_dispatched: int
    events_unrouted: int
    deliveries_inserted: int
    deliveries_deduplicated: int
    batches_committed: int
    elapsed_seconds: float
    checkpoint: CheckpointProgress | None = None

    @property
    def deliveries_total(self) -> int:
        """Return inserted plus deduplicated deliveries across all queues."""
        return self.deliveries_inserted + self.deliveries_deduplicated


@dataclass(frozen=True, slots=True)
class CheckpointProgress:
    """Progress of one resumable ``EventBus.ingest(..., checkpoint=...)`` run.

    ``start_cursor`` is the persisted cursor the run resumed from (``None``
    when no checkpoint existed); ``end_cursor`` is the effective cursor after
    the run. When no batch commits, it equals ``start_cursor``. ``resumed``
    reports whether a checkpoint row already existed when the run started.
    """

    name: str
    start_cursor: str | None
    end_cursor: str | None
    resumed: bool


@dataclass(frozen=True, slots=True)
class CheckpointState:
    """Persisted state of one ingestion checkpoint.

    ``created_at`` and ``updated_at`` are epoch milliseconds, matching the
    native timestamps convention (see ``FailedMessage.created_at``).
    """

    cursor: str
    source_fingerprint: str | None
    version: int
    items_committed: int
    batches_committed: int
    created_at: int
    updated_at: int


class SourceChanged(Exception):
    """Raised when a resumable source no longer matches its checkpoint.

    The checkpoint stored a ``source_fingerprint`` that differs from the
    fingerprint of the source passed to ``EventBus.ingest``. It is raised
    before any source item is consumed; nothing is committed.
    """


class IngestionCheckpoint(Generic[_ContextT]):
    """Handle to inspect or reset one durable ingestion checkpoint.

    Obtained via ``EventBus.checkpoint(name)``. Resetting only removes the
    stored position; deliveries already committed stay in their queues.
    """

    def __init__(self, bus: EventBus[_ContextT], name: str) -> None:
        self._bus = bus
        self._name = name

    @property
    def name(self) -> str:
        """Return the checkpoint name."""
        return self._name

    def inspect(self) -> CheckpointState | None:
        """Return the persisted state, or ``None`` if never started."""
        row = self._bus._get_native()._checkpoint_inspect(self._bus.name, self._name)
        if row is None:
            return None
        return CheckpointState(
            cursor=row[0],
            source_fingerprint=row[1],
            version=row[3],
            items_committed=row[4],
            batches_committed=row[5],
            created_at=row[6],
            updated_at=row[7],
        )

    def reset(self) -> bool:
        """Delete the stored position; return ``True`` if one existed."""
        return bool(
            self._bus._get_native()._checkpoint_reset(self._bus.name, self._name)
        )


@dataclass(frozen=True, slots=True)
class _PreparedDispatch:
    """One routed event, fully prepared exactly once before any commit.

    The same instance is reused across backpressure retries and
    ``_FullImpossible`` splits; nothing is re-transformed, re-serialized,
    or re-identified after preparation.
    """

    event_id: UUID
    event_type: str
    subscriptions: tuple[str, ...]
    payload: bytes
    job_id: str
    dedup_key: str | None
    dedup_fingerprint: str | None


@dataclass(slots=True)
class _IngestionCounters:
    """Mutable delivery counters accumulated across committed batches."""

    inserted: int = 0
    deduplicated: int = 0
    batches: int = 0


@dataclass(frozen=True, slots=True)
class _PreparedSourceItem:
    """One consumed resumable-source record and its prepared dispatch.

    ``dispatch`` is ``None`` for unrouted events: the item still counts for
    items_read and items_committed, and its cursor still advances.
    """

    cursor: str
    dispatch: _PreparedDispatch | None


@dataclass(slots=True)
class _CheckpointTracker:
    """Mutable compare-and-swap state threaded through committed batches."""

    name: str
    fingerprint: str | None
    expected_generation: str | None
    expected_version: int | None
    end_cursor: str | None


async def _iterate_async(iterator: AsyncIterator[object]) -> AsyncIterator[object]:
    try:
        async for item in iterator:
            yield item
    finally:
        # Deterministic cleanup: propagate aclose() to the wrapped async
        # iterator so sources holding resources (e.g. open files) release
        # them even when the run stops early.
        aclose = getattr(iterator, "aclose", None)
        if aclose is not None:
            await aclose()


async def _iterate_sync(iterator: Iterator[object]) -> AsyncIterator[object]:
    # Sync sources are advanced incrementally in the event loop, one next()
    # per item. Blocking I/O sources should provide an AsyncIterable instead;
    # per-item next() is deliberately not wrapped in asyncio.to_thread so the
    # source is never read ahead of the current group.
    try:
        while True:
            try:
                yield next(iterator)
            except StopIteration:
                return
    finally:
        # Deterministic cleanup: propagate close() to the wrapped sync
        # iterator (e.g. a generator holding an open file).
        close = getattr(iterator, "close", None)
        if close is not None:
            close()


def _validate_transform_item(value: object, index: int) -> BaseEvent:
    if not isinstance(value, BaseEvent):
        raise TypeError(
            f"transform returned {type(value).__name__} for source item "
            f"{index}; expected BaseEvent"
        )
    return value


def _prepare_dispatch(
    bus: EventBus[_ContextT], event: BaseEvent
) -> _PreparedDispatch | None:
    """Prepare one routed event exactly once; ``None`` when unrouted."""
    # Ensure consumers registered only by wildcard or string can rebuild the
    # typed event.
    bus.registry.register(type(event))
    subscriptions = bus._subscriptions_for(event.event_type)
    if not subscriptions:
        if bus.require_subscribers:
            # Deferred import: ingestion.py must not import bus.py at module
            # level because bus.py imports this module.
            from localqueue.bus.bus import NoSubscribers

            raise NoSubscribers(f"no subscription for {event.event_type!r}")
        return None
    prepared = prepare_event_persistence(event)
    identity = prepared.identity
    payload = bus._serialize_envelope(event, prepared.payload)
    return _PreparedDispatch(
        event_id=event.event_id,
        event_type=event.event_type,
        subscriptions=subscriptions,
        payload=payload,
        job_id=identity.job_id,
        dedup_key=identity.dedup_key,
        dedup_fingerprint=identity.dedup_fingerprint,
    )


def _flatten_entries(
    bus: EventBus[_ContextT], group: list[_PreparedDispatch]
) -> list[tuple[str, bytes, str | None, str | None, str | None]]:
    # Source order is preserved within each queue; events are never grouped
    # or reordered by event type.
    entries: list[tuple[str, bytes, str | None, str | None, str | None]] = []
    for dispatch in group:
        for subscription in dispatch.subscriptions:
            entries.append(
                (
                    bus._queue_name(subscription),
                    dispatch.payload,
                    dispatch.job_id,
                    dispatch.dedup_key,
                    dispatch.dedup_fingerprint,
                )
            )
    return entries


def _capacity_entries(
    bus: EventBus[_ContextT], group: list[_PreparedDispatch], max_pending: int
) -> list[tuple[str, int]]:
    queues: list[tuple[str, int]] = []
    seen: set[str] = set()
    for dispatch in group:
        for subscription in dispatch.subscriptions:
            queue_name = bus._queue_name(subscription)
            if queue_name not in seen:
                seen.add(queue_name)
                queues.append((queue_name, max_pending))
    return queues


async def _commit_group(
    bus: EventBus[_ContextT],
    group: list[_PreparedDispatch],
    max_pending: int | None,
    counters: _IngestionCounters,
) -> None:
    """Commit one prepared group in a single native transaction.

    Retries temporary ``Full`` backpressure with bounded async backoff and
    splits the prepared group recursively on ``_FullImpossible``. The source
    is not advanced and the group is not rebuilt while waiting.
    """
    if not group:
        return
    entries = _flatten_entries(bus, group)
    capacity = (
        None if max_pending is None else _capacity_entries(bus, group, max_pending)
    )
    delay = _BACKOFF_INITIAL_SECONDS
    while True:
        # Every attempt re-checks that the bus is still open.
        native = bus._get_native()
        try:
            commit = asyncio.create_task(
                asyncio.to_thread(
                    native._enqueue_batch_with_identity, entries, capacity
                )
            )
            try:
                outcomes = await asyncio.shield(commit)
            except asyncio.CancelledError:
                # A Python task cannot stop SQLite work already running in a
                # worker thread. Do not expose cancellation while that commit
                # is still in flight: settle it first, then propagate the
                # original cancellation.
                while not commit.done():
                    try:
                        await asyncio.shield(commit)
                    except asyncio.CancelledError:
                        # Repeated cancellation requests must not reopen the
                        # same ambiguity while SQLite is still running.
                        continue
                if not commit.cancelled():
                    commit.exception()
                raise
            break
        except _native._FullImpossible:
            if len(group) == 1:
                # Invariant violation: with max_pending >= 1 one event costs
                # at most one new row per subscription queue.
                raise Full(
                    "a single event exceeds 'max_pending' on an empty "
                    "subscription queue"
                ) from None
            midpoint = len(group) // 2
            await _commit_group(bus, group[:midpoint], max_pending, counters)
            await _commit_group(bus, group[midpoint:], max_pending, counters)
            return
        except _native.Full:
            # Temporary backpressure. CancelledError propagates immediately.
            await asyncio.sleep(delay)
            delay = min(delay * _BACKOFF_MULTIPLIER, _BACKOFF_CAP_SECONDS)
        except _native.LocalQueueError as error:
            # `close()` can run after the native handle was captured above but
            # before its worker-thread call begins. Preserve EventBus's public
            # closed-bus error instead of leaking that native timing detail.
            if bus._native_queue is None:
                raise RuntimeError("event bus is closed") from error
            raise
    offset = 0
    for dispatch in group:
        for _message_id, inserted in outcomes[
            offset : offset + len(dispatch.subscriptions)
        ]:
            if inserted:
                counters.inserted += 1
            else:
                counters.deduplicated += 1
        offset += len(dispatch.subscriptions)
    counters.batches += 1


def _validate_batch_size(batch_size: object) -> int:
    """Validate and return an ingestion batch size."""
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise TypeError("'batch_size' must be a positive integer")
    if not 1 <= batch_size <= _MAX_BATCH_SIZE:
        raise ValueError(f"'batch_size' must be between 1 and {_MAX_BATCH_SIZE}")
    return batch_size


def _validate_max_pending(max_pending: object) -> int | None:
    """Validate and return an optional per-queue pending limit."""
    if max_pending is not None:
        if isinstance(max_pending, bool) or not isinstance(max_pending, int):
            raise TypeError("'max_pending' must be a positive integer or None")
        if not 1 <= max_pending <= _MAX_PENDING_BOUND:
            raise ValueError(
                f"'max_pending' must be between 1 and {_MAX_PENDING_BOUND}"
            )
    return max_pending


def _validate_ingestion_args(
    transform: Callable[[object], object] | None,
    batch_size: int,
    max_pending: int | None,
) -> None:
    """Validate shared ingestion arguments before any item is consumed."""
    _validate_batch_size(batch_size)
    _validate_max_pending(max_pending)
    if transform is not None and not callable(transform):
        raise TypeError("'transform' must be callable or None")


def _iterate_source(
    source: Iterable[object] | AsyncIterable[object],
) -> AsyncIterator[object]:
    try:
        return _iterate_async(aiter(source))  # type: ignore[arg-type]
    except TypeError:
        pass
    try:
        return _iterate_sync(iter(source))  # type: ignore[arg-type]
    except TypeError:
        pass
    raise TypeError("'source' must be an Iterable or AsyncIterable")


def _open_source(
    source: Iterable[object] | AsyncIterable[object],
    transform: Callable[[object], object] | None,
    batch_size: int,
    max_pending: int | None,
) -> AsyncIterator[object]:
    """Validate every argument before any source item is consumed."""
    _validate_ingestion_args(transform, batch_size, max_pending)
    return _iterate_source(source)


async def _resolve_event(
    transform: Callable[[object], object] | None, item: object, index: int
) -> BaseEvent:
    """Resolve one consumed item to a BaseEvent, exactly once."""
    if transform is None:
        if not isinstance(item, BaseEvent):
            raise TypeError(
                f"source item {index} is a {type(item).__name__}; expected BaseEvent"
            )
        return item
    candidate = transform(item)
    value = await candidate if inspect.isawaitable(candidate) else candidate
    return _validate_transform_item(value, index)


async def run_ingestion(
    bus: EventBus[_ContextT],
    source: Iterable[object] | AsyncIterable[object],
    *,
    transform: Callable[[object], object] | None = None,
    batch_size: int = 1_000,
    max_pending: int | None = None,
) -> IngestionResult:
    """Consume ``source`` incrementally and fan events out in atomic batches.

    Each group of up to ``batch_size`` consumed items commits in one native
    transaction across all subscription queues. Batches 1..k stay committed
    when a later batch fails; ingestion is incremental, not all-or-nothing.

    :param bus: the EventBus that owns routing, serialization, and identity.
    :param source: iterable or async iterable; never materialized, never
        measured with ``len()``, and never read ahead of the current group.
        Synchronous iteration runs on the event-loop thread. Blocking I/O
        sources should provide an AsyncIterable or be offloaded explicitly.
    :param transform: optional callable applied exactly once per consumed
        item; it may return a BaseEvent or an awaitable resolving to one.
        Synchronous transforms run on the event-loop thread.
    :param batch_size: maximum number of source items per native transaction.
        Delivery count and memory also depend on subscription fan-out and
        payload size.
    :param max_pending: optional per-subscription-queue pending bound for
        this run; ephemeral and never persisted as queue configuration.
    :returns: aggregate counters for the completed run.
    """
    items = _open_source(source, transform, batch_size, max_pending)

    started = time.monotonic()
    counters = _IngestionCounters()
    items_read = 0
    events_dispatched = 0
    events_unrouted = 0
    index = 0
    group_size = 0
    group: list[_PreparedDispatch] = []

    try:
        while True:
            try:
                item = await items.__anext__()
            except StopAsyncIteration:
                break
            index += 1
            items_read += 1
            event = await _resolve_event(transform, item, index)
            prepared = _prepare_dispatch(bus, event)
            if prepared is None:
                events_unrouted += 1
            else:
                group.append(prepared)
                events_dispatched += 1
            group_size += 1
            if group_size >= batch_size:
                await _commit_group(bus, group, max_pending, counters)
                group = []
                group_size = 0
        await _commit_group(bus, group, max_pending, counters)
    finally:
        # Close the source iterator deterministically (transform/source
        # failure, bus close, or task cancellation included), releasing any
        # resource held by the source instead of relying on the GC.
        aclose = getattr(items, "aclose", None)
        if aclose is not None:
            await aclose()

    elapsed = time.monotonic() - started
    return IngestionResult(
        items_read=items_read,
        events_dispatched=events_dispatched,
        events_unrouted=events_unrouted,
        deliveries_inserted=counters.inserted,
        deliveries_deduplicated=counters.deduplicated,
        batches_committed=counters.batches,
        elapsed_seconds=elapsed,
    )


async def _commit_resumable_group(
    bus: EventBus[_ContextT],
    group: list[_PreparedSourceItem],
    max_pending: int | None,
    counters: _IngestionCounters,
    tracker: _CheckpointTracker,
) -> None:
    """Commit one prepared group and its checkpoint in one transaction.

    Same retry/split protocol as :func:`_commit_group`, with two additions:
    the cursor of the last group item and the item count ride along in the
    native transaction, and the checkpoint version is a compare-and-swap
    token. ``Full`` retries reuse the same expected version (nothing was
    committed); ``CheckpointConflict`` propagates to the caller.
    """
    if not group:
        return
    dispatches = [item.dispatch for item in group if item.dispatch is not None]
    entries = _flatten_entries(bus, dispatches)
    capacity = (
        None if max_pending is None else _capacity_entries(bus, dispatches, max_pending)
    )
    cursor = group[-1].cursor
    delay = _BACKOFF_INITIAL_SECONDS
    while True:
        # Every attempt re-checks that the bus is still open.
        native = bus._get_native()
        try:
            commit = asyncio.create_task(
                asyncio.to_thread(
                    native._enqueue_batch_with_identity_and_checkpoint,
                    entries,
                    capacity,
                    (
                        bus.name,
                        tracker.name,
                        tracker.expected_generation,
                        tracker.expected_version,
                        cursor,
                        tracker.fingerprint,
                        len(group),
                    ),
                )
            )
            try:
                outcomes, new_generation, new_version = await asyncio.shield(commit)
            except asyncio.CancelledError:
                # A Python task cannot stop SQLite work already running in a
                # worker thread. Do not expose cancellation while that commit
                # is still in flight: settle it first, then propagate the
                # original cancellation.
                while not commit.done():
                    try:
                        await asyncio.shield(commit)
                    except asyncio.CancelledError:
                        # Repeated cancellation requests must not reopen the
                        # same ambiguity while SQLite is still running.
                        continue
                if not commit.cancelled():
                    commit.exception()
                raise
            break
        except _native._FullImpossible:
            if len(group) == 1:
                # Invariant violation: with max_pending >= 1 one event costs
                # at most one new row per subscription queue.
                raise Full(
                    "a single event exceeds 'max_pending' on an empty "
                    "subscription queue"
                ) from None
            midpoint = len(group) // 2
            await _commit_resumable_group(
                bus, group[:midpoint], max_pending, counters, tracker
            )
            await _commit_resumable_group(
                bus, group[midpoint:], max_pending, counters, tracker
            )
            return
        except _native.Full:
            # Temporary backpressure. CancelledError propagates immediately.
            await asyncio.sleep(delay)
            delay = min(delay * _BACKOFF_MULTIPLIER, _BACKOFF_CAP_SECONDS)
        except _native.LocalQueueError as error:
            # See `_commit_group`: close may race with a queued native call.
            if bus._native_queue is None:
                raise RuntimeError("event bus is closed") from error
            raise
    tracker.expected_generation = new_generation
    tracker.expected_version = new_version
    tracker.end_cursor = cursor
    offset = 0
    for dispatch in dispatches:
        for _message_id, inserted in outcomes[
            offset : offset + len(dispatch.subscriptions)
        ]:
            if inserted:
                counters.inserted += 1
            else:
                counters.deduplicated += 1
        offset += len(dispatch.subscriptions)
    counters.batches += 1


async def _consume_resumable_source(
    bus: EventBus[_ContextT],
    items: AsyncIterator[object],
    transform: Callable[[object], object] | None,
    batch_size: int,
    max_pending: int | None,
    counters: _IngestionCounters,
    tracker: _CheckpointTracker,
) -> tuple[int, int, int]:
    """Consume and commit source records, closing the adapter on every exit."""
    items_read = 0
    events_dispatched = 0
    events_unrouted = 0
    index = 0
    group: list[_PreparedSourceItem] = []

    try:
        while True:
            try:
                record = await items.__anext__()
            except StopAsyncIteration:
                break
            index += 1
            items_read += 1
            if not isinstance(record, SourceRecord):
                raise TypeError(
                    f"resumable source item {index} is a "
                    f"{type(record).__name__}; expected SourceRecord"
                )
            if not isinstance(record.cursor, str):
                raise TypeError(
                    f"cursor of resumable source item {index} is a "
                    f"{type(record.cursor).__name__}; expected str"
                )
            event = await _resolve_event(transform, record.value, index)
            prepared = _prepare_dispatch(bus, event)
            if prepared is None:
                events_unrouted += 1
            else:
                events_dispatched += 1
            group.append(_PreparedSourceItem(cursor=record.cursor, dispatch=prepared))
            if len(group) >= batch_size:
                await _commit_resumable_group(
                    bus, group, max_pending, counters, tracker
                )
                group = []
        await _commit_resumable_group(bus, group, max_pending, counters, tracker)
    finally:
        # Its finally propagates close/aclose to the original source iterator.
        aclose = getattr(items, "aclose", None)
        if aclose is not None:
            await aclose()

    return items_read, events_dispatched, events_unrouted


async def run_resumable_ingestion(
    bus: EventBus[_ContextT],
    source: ResumableSource[object],
    *,
    checkpoint: str,
    transform: Callable[[object], object] | None = None,
    batch_size: int = 1_000,
    max_pending: int | None = None,
) -> IngestionResult:
    """Consume a resumable source, persisting a cursor per committed batch.

    Behaves exactly like :func:`run_ingestion`, additionally committing the
    cursor of the last item of each batch in the same native transaction as
    the batch deliveries. A rerun inspects the checkpoint first, resumes
    from the stored cursor, and rejects a source whose fingerprint changed
    (:class:`SourceChanged`) before consuming any item.

    :param bus: the EventBus that owns routing, serialization, and identity.
    :param source: a ResumableSource; ``open`` is called exactly once with
        the stored cursor (``None`` when no checkpoint exists yet).
    :param checkpoint: durable checkpoint name, scoped to the bus name.
    :param transform: optional callable applied exactly once per consumed
        item; it may return a BaseEvent or an awaitable resolving to one.
    :param batch_size: maximum number of source items per native transaction.
    :param max_pending: optional per-subscription-queue pending bound for
        this run; ephemeral and never persisted as queue configuration.
    :returns: aggregate counters plus :class:`CheckpointProgress`.
    """
    if isinstance(checkpoint, bool) or not isinstance(checkpoint, str):
        raise TypeError("'checkpoint' must be a non-empty string")
    if not checkpoint:
        raise ValueError("'checkpoint' must be a non-empty string")
    if not isinstance(source, ResumableSource):
        raise TypeError(
            "'source' must satisfy the ResumableSource protocol "
            "when 'checkpoint' is given"
        )
    _validate_ingestion_args(transform, batch_size, max_pending)

    # Inspect the checkpoint before the source is opened, so a fingerprint
    # mismatch aborts before any item is consumed.
    fingerprint = source.fingerprint
    if fingerprint is not None and not isinstance(fingerprint, str):
        raise TypeError("'source.fingerprint' must be a string or None")
    stored = bus._get_native()._checkpoint_inspect(bus.name, checkpoint)
    if stored is not None:
        start_cursor, stored_fingerprint, generation, version = (
            stored[0],
            stored[1],
            stored[2],
            stored[3],
        )
        if stored_fingerprint != fingerprint:
            raise SourceChanged(
                f"checkpoint {checkpoint!r} was recorded for source fingerprint "
                f"{stored_fingerprint!r}, but the current source fingerprint is "
                f"{fingerprint!r}; reset the checkpoint to re-ingest"
            )
        expected_version: int | None = version
        expected_generation: str | None = generation
        resumed = True
    else:
        start_cursor = None
        expected_generation = None
        expected_version = None
        resumed = False
    tracker = _CheckpointTracker(
        name=checkpoint,
        fingerprint=fingerprint,
        expected_generation=expected_generation,
        expected_version=expected_version,
        end_cursor=start_cursor,
    )
    items = _iterate_source(source.open(start_cursor))

    started = time.monotonic()
    counters = _IngestionCounters()
    items_read, events_dispatched, events_unrouted = await _consume_resumable_source(
        bus, items, transform, batch_size, max_pending, counters, tracker
    )

    elapsed = time.monotonic() - started
    return IngestionResult(
        items_read=items_read,
        events_dispatched=events_dispatched,
        events_unrouted=events_unrouted,
        deliveries_inserted=counters.inserted,
        deliveries_deduplicated=counters.deduplicated,
        batches_committed=counters.batches,
        elapsed_seconds=elapsed,
        checkpoint=CheckpointProgress(
            name=checkpoint,
            start_cursor=start_cursor,
            end_cursor=tracker.end_cursor,
            resumed=resumed,
        ),
    )


@dataclass(frozen=True, slots=True)
class _ClaimedExecutionIngestion:
    """Private execution-specific state carried through claimed source batches."""

    checkpoint: str
    transform: Callable[[object], object] | None
    batch_size: int
    max_pending: int | None
    execution_id: str
    receipt: str
    start_cursor: str | None
    generation: str | None
    version: int | None
    fingerprint: str


async def _run_claimed_execution_ingestion(
    bus: EventBus[_ContextT],
    source: ResumableSource[object],
    claimed: _ClaimedExecutionIngestion,
) -> None:
    """Private resumable route whose source batches are fenced by an execution lease."""
    tracker = _CheckpointTracker(
        claimed.checkpoint,
        claimed.fingerprint,
        claimed.generation,
        claimed.version,
        claimed.start_cursor,
    )
    items = _iterate_source(source.open(claimed.start_cursor))

    async def commit(group: list[_PreparedSourceItem]) -> None:
        if not group:
            return
        dispatches = [item.dispatch for item in group if item.dispatch is not None]
        entries = _flatten_entries(bus, dispatches)
        capacity = (
            None
            if claimed.max_pending is None
            else _capacity_entries(bus, dispatches, claimed.max_pending)
        )
        dispatched = len(dispatches)
        unrouted = len(group) - dispatched
        while True:
            try:
                task = asyncio.create_task(
                    asyncio.to_thread(
                        bus._get_native()._enqueue_batch_with_claimed_execution,
                        entries,
                        capacity,
                        (
                            bus.name,
                            claimed.checkpoint,
                            tracker.expected_generation,
                            tracker.expected_version,
                            group[-1].cursor,
                            claimed.fingerprint,
                            len(group),
                        ),
                        claimed.execution_id,
                        claimed.receipt,
                        dispatched,
                        unrouted,
                    )
                )
                try:
                    _, next_generation, next_version = await asyncio.shield(task)
                except asyncio.CancelledError:
                    while not task.done():
                        try:
                            await asyncio.shield(task)
                        except asyncio.CancelledError:
                            continue
                    if not task.cancelled():
                        task.exception()
                    raise
                tracker.expected_generation, tracker.expected_version = (
                    next_generation,
                    next_version,
                )
                tracker.end_cursor = group[-1].cursor
                return
            except _native._FullImpossible:
                if len(group) == 1:
                    raise Full(
                        "a single event exceeds 'max_pending' on an empty subscription queue"
                    ) from None
                middle = len(group) // 2
                await commit(group[:middle])
                await commit(group[middle:])
                return
            except _native.Full:
                await asyncio.sleep(_BACKOFF_INITIAL_SECONDS)

    group: list[_PreparedSourceItem] = []
    index = 0
    try:
        async for record in items:
            index += 1
            if not isinstance(record, SourceRecord) or not isinstance(
                record.cursor, str
            ):
                raise TypeError(
                    f"resumable source item {index} must be a SourceRecord with a string cursor"
                )
            event = await _resolve_event(claimed.transform, record.value, index)
            group.append(
                _PreparedSourceItem(record.cursor, _prepare_dispatch(bus, event))
            )
            if len(group) >= claimed.batch_size:
                await commit(group)
                group = []
        await commit(group)
    finally:
        aclose = getattr(items, "aclose", None)
        if aclose is not None:
            await aclose()
