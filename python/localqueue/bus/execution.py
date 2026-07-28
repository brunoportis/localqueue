"""Private durable finite-execution lifecycle for :mod:`localqueue.bus`."""

from __future__ import annotations

import asyncio
import math
import secrets
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast
from uuid import UUID

from localqueue.bus.ingestion import (
    _ClaimedExecutionIngestion,
    _run_claimed_execution_ingestion,
)

_LEASE_MS = 60_000
_POLL_SECONDS = 0.05


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """Terminal, durable outcome of one finite source execution."""

    execution_id: UUID
    resumed: bool
    source_name: str
    checkpoint_name: str
    source_fingerprint: str
    checkpoint_generation: str | None
    source_completed: bool
    source_completed_at: datetime | None
    completed_at: datetime
    items_committed: int
    events_dispatched: int
    events_unrouted: int
    deliveries_inserted: int
    deliveries_deduplicated: int
    batches_committed: int
    deliveries_total: int
    deliveries_ready: int
    deliveries_processing: int
    deliveries_acknowledged: int
    deliveries_failed: int
    created_at: datetime
    updated_at: datetime

    @property
    def completed(self) -> bool:
        return self.completed_at is not None

    @property
    def succeeded(self) -> bool:
        return self.completed and self.deliveries_failed == 0

    def raise_for_failures(self) -> None:
        """Raise :class:`ExecutionFailed` when a delivery failed terminally."""
        if self.deliveries_failed:
            raise ExecutionFailed(self)


class ExecutionFailed(RuntimeError):
    """A finite execution completed with terminal failed deliveries."""

    result: ExecutionResult

    def __init__(self, result: ExecutionResult) -> None:
        self.result = result
        noun = "delivery" if result.deliveries_failed == 1 else "deliveries"
        super().__init__(
            f"execution {result.execution_id} completed with "
            f"{result.deliveries_failed} failed {noun}"
        )


@dataclass(frozen=True, slots=True)
class _ExecutionSnapshot:
    execution_id: UUID
    source_name: str
    checkpoint_name: str
    source_fingerprint: str
    checkpoint_generation: str | None
    source_completed: bool
    source_completed_at: int | None
    completed_at: int | None
    items_committed: int
    events_dispatched: int
    events_unrouted: int
    deliveries_inserted: int
    deliveries_deduplicated: int
    batches_committed: int
    total: int
    ready: int
    processing: int
    acknowledged: int
    failed: int
    source_lease_until: int | None
    created_at: int
    updated_at: int

    @property
    def completed(self) -> bool:
        return self.completed_at is not None


def _utc_from_milliseconds(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _to_execution_result(
    snapshot: _ExecutionSnapshot, *, resumed: bool
) -> ExecutionResult:
    """Convert one terminal private snapshot at the public API boundary."""
    terminal = (
        snapshot.source_completed
        and snapshot.completed_at is not None
        and snapshot.ready == 0
        and snapshot.processing == 0
        and snapshot.acknowledged + snapshot.failed == snapshot.total
    )
    if not terminal:
        raise RuntimeError(
            f"execution {snapshot.execution_id} returned a non-terminal snapshot"
        )
    assert snapshot.completed_at is not None
    return ExecutionResult(
        execution_id=snapshot.execution_id,
        resumed=resumed,
        source_name=snapshot.source_name,
        checkpoint_name=snapshot.checkpoint_name,
        source_fingerprint=snapshot.source_fingerprint,
        checkpoint_generation=snapshot.checkpoint_generation,
        source_completed=snapshot.source_completed,
        source_completed_at=(
            _utc_from_milliseconds(snapshot.source_completed_at)
            if snapshot.source_completed_at is not None
            else None
        ),
        completed_at=_utc_from_milliseconds(snapshot.completed_at),
        items_committed=snapshot.items_committed,
        events_dispatched=snapshot.events_dispatched,
        events_unrouted=snapshot.events_unrouted,
        deliveries_inserted=snapshot.deliveries_inserted,
        deliveries_deduplicated=snapshot.deliveries_deduplicated,
        batches_committed=snapshot.batches_committed,
        deliveries_total=snapshot.total,
        deliveries_ready=snapshot.ready,
        deliveries_processing=snapshot.processing,
        deliveries_acknowledged=snapshot.acknowledged,
        deliveries_failed=snapshot.failed,
        created_at=_utc_from_milliseconds(snapshot.created_at),
        updated_at=_utc_from_milliseconds(snapshot.updated_at),
    )


def _snapshot(row: tuple[tuple[object, ...], tuple[object, ...]]) -> _ExecutionSnapshot:
    fields = (*row[0], *row[1])
    return _ExecutionSnapshot(UUID(str(fields[0])), *(fields[1:]))  # type: ignore[arg-type]


class _ExecutionHandle:
    def __init__(
        self,
        bus: Any,
        source: Any,
        execution_id: UUID,
        resumed: bool,
    ) -> None:
        self._bus, self._source, self._id, self._resumed = (
            bus,
            source,
            execution_id,
            resumed,
        )

    @property
    def execution_id(self) -> UUID:
        return self._id

    @property
    def resumed(self) -> bool:
        return self._resumed

    def inspect(self) -> _ExecutionSnapshot:
        return _snapshot(
            cast(
                tuple[tuple[object, ...], tuple[object, ...]],
                self._bus._get_native()._execution_snapshot(str(self._id)),
            )
        )

    async def wait(self, *, timeout: float | None = None) -> _ExecutionSnapshot:
        self._validate_timeout(timeout)
        return await self._with_timeout(self._wait(), timeout)

    async def run(
        self, *, timeout: float | None = None, operation_id: str | None = None
    ) -> _ExecutionSnapshot:
        self._validate_timeout(timeout)
        return await self._with_timeout(self._run(operation_id), timeout)

    @staticmethod
    def _validate_timeout(timeout: float | None) -> None:
        if timeout is not None and (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("'timeout' must be a positive finite number or None")

    async def _with_timeout(
        self, coro: Coroutine[object, object, _ExecutionSnapshot], timeout: float | None
    ) -> _ExecutionSnapshot:
        return (
            await asyncio.wait_for(coro, timeout) if timeout is not None else await coro
        )

    async def _run(self, operation_id: str | None) -> _ExecutionSnapshot:
        while not self.inspect().source_completed:
            receipt = secrets.token_urlsafe(24)
            (
                claimed,
                cursor,
                checkpoint_fingerprint,
                generation,
                version,
            ) = await asyncio.to_thread(
                self._bus._get_native()._execution_claim_source,
                str(self._id),
                receipt,
                _LEASE_MS,
                *((operation_id,) if operation_id is not None else ()),
            )
            if not claimed:
                await asyncio.sleep(_POLL_SECONDS)
                continue
            heartbeat = asyncio.create_task(
                self._heartbeat(receipt, operation_id)
                if operation_id is not None
                else self._heartbeat(receipt)
            )
            try:
                definition = cast(Any, self._source)
                checkpoint_name = cast(str, definition.checkpoint)
                source = cast(Any, definition.source)
                ingestion = asyncio.create_task(
                    _run_claimed_execution_ingestion(
                        self._bus,
                        source,
                        _ClaimedExecutionIngestion(
                            checkpoint=checkpoint_name,
                            transform=definition.transform,
                            batch_size=definition.config.batch_size,
                            max_pending=definition.config.max_pending,
                            execution_id=str(self._id),
                            receipt=receipt,
                            start_cursor=cursor,
                            generation=generation,
                            version=version,
                            fingerprint=(
                                source.fingerprint
                                if checkpoint_fingerprint is None
                                else checkpoint_fingerprint
                            ),
                            operation_id=operation_id,
                        ),
                    )
                )
                done, _ = await asyncio.wait(
                    {ingestion, heartbeat}, return_when=asyncio.FIRST_COMPLETED
                )
                if heartbeat in done:
                    heartbeat.result()
                await ingestion
                await asyncio.to_thread(
                    self._bus._get_native()._execution_mark_source_completed_claimed,
                    str(self._id),
                    receipt,
                    *((operation_id,) if operation_id is not None else ()),
                )
            finally:
                if "ingestion" in locals() and not ingestion.done():
                    ingestion.cancel()
                    await asyncio.gather(ingestion, return_exceptions=True)
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
                await asyncio.shield(
                    asyncio.to_thread(
                        self._bus._get_native()._execution_release_source_lease,
                        str(self._id),
                        receipt,
                    )
                )
        return await self._wait(operation_id)

    async def _heartbeat(self, receipt: str, operation_id: str | None = None) -> None:
        while True:
            await asyncio.sleep(_LEASE_MS / 3000)
            await asyncio.to_thread(
                self._bus._get_native()._execution_extend_source_lease,
                str(self._id),
                receipt,
                _LEASE_MS,
                *((operation_id,) if operation_id is not None else ()),
            )

    async def _wait(self, operation_id: str | None = None) -> _ExecutionSnapshot:
        while True:
            snapshot = self.inspect()
            if snapshot.source_completed and snapshot.ready == snapshot.processing == 0:
                await asyncio.to_thread(
                    self._bus._get_native()._execution_finalize_if_complete,
                    str(self._id),
                    *((operation_id,) if operation_id is not None else ()),
                )
                snapshot = self.inspect()
                if snapshot.completed and snapshot.ready == snapshot.processing == 0:
                    return snapshot
            await asyncio.sleep(_POLL_SECONDS)
