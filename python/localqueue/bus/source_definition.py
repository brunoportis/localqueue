"""Declarative source definitions backed by :meth:`EventBus.ingest`."""

from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from typing import TYPE_CHECKING, Generic, TypeVar, cast

from localqueue.bus.event import BaseEvent
from localqueue.bus.ingestion import (
    IngestionResult,
    _validate_batch_size,
    _validate_max_pending,
)
from localqueue.bus.sources import ResumableSource

if TYPE_CHECKING:
    from localqueue.bus.bus import EventBus
    from localqueue.bus.context import HandlerContext

ItemT = TypeVar("ItemT")
EventT = TypeVar("EventT", bound=BaseEvent)


class SourceConfig:
    """Mutable ingestion settings that freeze when a source first runs."""

    __slots__ = ("_batch_size", "_frozen", "_max_pending")

    def __init__(
        self, *, batch_size: int = 1_000, max_pending: int | None = None
    ) -> None:
        self._frozen = False
        self._batch_size = _validate_batch_size(batch_size)
        self._max_pending = _validate_max_pending(max_pending)

    @property
    def batch_size(self) -> int:
        """Maximum source items per committed ingestion batch."""
        return self._batch_size

    @batch_size.setter
    def batch_size(self, value: int) -> None:
        self._ensure_mutable()
        self._batch_size = _validate_batch_size(value)

    @property
    def max_pending(self) -> int | None:
        """Optional per-subscription pending-delivery limit."""
        return self._max_pending

    @max_pending.setter
    def max_pending(self, value: int | None) -> None:
        self._ensure_mutable()
        self._max_pending = _validate_max_pending(value)

    @property
    def frozen(self) -> bool:
        """Whether this configuration can no longer be changed."""
        return self._frozen

    def _freeze(self) -> None:
        self._frozen = True

    def _ensure_mutable(self) -> None:
        if self._frozen:
            raise RuntimeError("source configuration is frozen after ingestion starts")


class SourceDefinition(Generic[ItemT, EventT]):
    """A named source and transform declaration delegated to ``EventBus.ingest``."""

    def __init__(
        self,
        *,
        bus: EventBus[HandlerContext],
        source: Iterable[ItemT] | AsyncIterable[ItemT] | ResumableSource[ItemT],
        transform: Callable[[ItemT], EventT | Awaitable[EventT]],
        checkpoint: str | None,
        config: SourceConfig,
    ) -> None:
        self._bus = bus
        self._source = source
        self._transform = transform
        self._checkpoint = checkpoint
        self._config = config

    @property
    def name(self) -> str:
        """The transform's display name."""
        return self._transform.__name__

    @property
    def bus(self) -> EventBus[HandlerContext]:
        """The bus that owns this source definition."""
        return self._bus

    @property
    def source(self) -> Iterable[ItemT] | AsyncIterable[ItemT] | ResumableSource[ItemT]:
        """The underlying source passed to ingestion."""
        return self._source

    @property
    def transform(self) -> Callable[[ItemT], EventT | Awaitable[EventT]]:
        """The item-to-event transformation."""
        return self._transform

    @property
    def checkpoint(self) -> str | None:
        """The explicit checkpoint name, when this source is resumable."""
        return self._checkpoint

    @property
    def config(self) -> SourceConfig:
        """The source's mutable-before-run ingestion configuration."""
        return self._config

    async def ingest(self) -> IngestionResult:
        """Freeze configuration and delegate directly to ``EventBus.ingest``."""
        self._config._freeze()
        ingest = cast(Callable[..., Awaitable[IngestionResult]], self._bus.ingest)
        return await ingest(
            self._source,
            checkpoint=self._checkpoint,
            transform=self._transform,
            batch_size=self._config.batch_size,
            max_pending=self._config.max_pending,
        )
