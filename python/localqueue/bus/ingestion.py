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
from typing import TYPE_CHECKING, TypeVar
from uuid import UUID

from localqueue import localqueue as _native
from localqueue.bus.event import BaseEvent
from localqueue.bus.identity import prepare_event_persistence
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

    @property
    def deliveries_total(self) -> int:
        """Return inserted plus deduplicated deliveries across all queues."""
        return self.deliveries_inserted + self.deliveries_deduplicated


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


async def _iterate_async(iterator: AsyncIterator[object]) -> AsyncIterator[object]:
    async for item in iterator:
        yield item


async def _iterate_sync(iterator: Iterator[object]) -> AsyncIterator[object]:
    # Sync sources are advanced incrementally in the event loop, one next()
    # per item. Blocking I/O sources should provide an AsyncIterable instead;
    # per-item next() is deliberately not wrapped in asyncio.to_thread so the
    # source is never read ahead of the current group.
    while True:
        try:
            yield next(iterator)
        except StopIteration:
            return


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


def _open_source(
    source: Iterable[object] | AsyncIterable[object],
    transform: Callable[[object], object] | None,
    batch_size: int,
    max_pending: int | None,
) -> AsyncIterator[object]:
    """Validate every argument before any source item is consumed."""
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise TypeError("'batch_size' must be a positive integer")
    if not 1 <= batch_size <= _MAX_BATCH_SIZE:
        raise ValueError(f"'batch_size' must be between 1 and {_MAX_BATCH_SIZE}")
    if max_pending is not None:
        if isinstance(max_pending, bool) or not isinstance(max_pending, int):
            raise TypeError("'max_pending' must be a positive integer or None")
        if not 1 <= max_pending <= _MAX_PENDING_BOUND:
            raise ValueError(
                f"'max_pending' must be between 1 and {_MAX_PENDING_BOUND}"
            )
    if transform is not None and not callable(transform):
        raise TypeError("'transform' must be callable or None")
    try:
        return _iterate_async(aiter(source))  # type: ignore[arg-type]
    except TypeError:
        pass
    try:
        return _iterate_sync(iter(source))  # type: ignore[arg-type]
    except TypeError:
        pass
    raise TypeError("'source' must be an Iterable or AsyncIterable")


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
