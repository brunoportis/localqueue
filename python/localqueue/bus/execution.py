"""Private durable finite-execution lifecycle for :mod:`localqueue.bus`."""

from __future__ import annotations

import asyncio
import math
import secrets
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any, cast
from uuid import UUID

from localqueue.bus.ingestion import (
    _ClaimedExecutionIngestion,
    _run_claimed_execution_ingestion,
)

_LEASE_MS = 60_000
_POLL_SECONDS = 0.05


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

    async def run(self, *, timeout: float | None = None) -> _ExecutionSnapshot:
        self._validate_timeout(timeout)
        return await self._with_timeout(self._run(), timeout)

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

    async def _run(self) -> _ExecutionSnapshot:
        while not self.inspect().source_completed:
            receipt = secrets.token_urlsafe(24)
            claimed = await asyncio.to_thread(
                self._bus._get_native()._execution_claim_source,
                str(self._id),
                receipt,
                _LEASE_MS,
            )
            if not claimed:
                await asyncio.sleep(_POLL_SECONDS)
                continue
            heartbeat = asyncio.create_task(self._heartbeat(receipt))
            try:
                definition = cast(Any, self._source)
                checkpoint_name = cast(str, definition.checkpoint)
                source = cast(Any, definition.source)
                checkpoint = self._bus.checkpoint(checkpoint_name).inspect()
                checkpoint_row = self._bus._get_native()._checkpoint_inspect(
                    self._bus.name, checkpoint_name
                )
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
                            start_cursor=None
                            if checkpoint is None
                            else checkpoint.cursor,
                            generation=None
                            if checkpoint_row is None
                            else checkpoint_row[2],
                            version=None if checkpoint is None else checkpoint.version,
                            fingerprint=source.fingerprint,
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
        return await self._wait()

    async def _heartbeat(self, receipt: str) -> None:
        while True:
            await asyncio.sleep(_LEASE_MS / 3000)
            await asyncio.to_thread(
                self._bus._get_native()._execution_extend_source_lease,
                str(self._id),
                receipt,
                _LEASE_MS,
            )

    async def _wait(self) -> _ExecutionSnapshot:
        while True:
            snapshot = self.inspect()
            if snapshot.source_completed and snapshot.ready == snapshot.processing == 0:
                await asyncio.to_thread(
                    self._bus._get_native()._execution_finalize_if_complete,
                    str(self._id),
                )
                snapshot = self.inspect()
                if snapshot.completed and snapshot.ready == snapshot.processing == 0:
                    return snapshot
            await asyncio.sleep(_POLL_SECONDS)
