"""Event bus subscription consumption loop."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Optional, TypedDict, cast

from pydantic import ValidationError

from localqueue.bus.bus import (
    WILDCARD,
    _AsyncStoredEventHandler,
    _is_async_callable,
)
from localqueue.bus.event import BaseEvent
from localqueue.core import SimpleQueue
from localqueue.exceptions import Empty, LeaseExpired
from localqueue.job import Job

if TYPE_CHECKING:
    from localqueue.bus.bus import EventBus

log = logging.getLogger(__name__)
_POLL_INTERVAL = 0.1


class _LeaseState(TypedDict):
    lease_lost: bool


@dataclass(frozen=True)
class _ParsedEnvelope:
    raw: dict[object, object]
    event_type: str
    payload: dict[object, object]


@dataclass(frozen=True)
class _EnvelopeError:
    message: str


@dataclass(frozen=True)
class _ReconstructedEvent:
    value: BaseEvent


@dataclass(frozen=True)
class _ReconstructionError:
    message: str


async def _deadline_timer(timeout: float) -> None:
    """Complete after an individual handler's configured deadline."""
    await asyncio.sleep(timeout)


async def _observe_cancelled_handler(
    handler_task: asyncio.Future[object],
) -> tuple[str, Exception | None]:
    """Observe a timed-out handler without conflating consumer cancellation."""
    try:
        await handler_task
    except asyncio.CancelledError:
        return "cancelled", None
    except Exception as error:  # noqa: BLE001 - cleanup failure is reported
        return "error", error
    return "returned", None


async def _run_async_handler(
    handler: _AsyncStoredEventHandler,
    event: BaseEvent,
    timeout: float | None,
) -> bool:
    """Run an async handler and return whether its internal deadline elapsed.

    A completed handler wins a simultaneous timer completion. Once the timer
    wins, the deadline remains authoritative even when cancellation is
    suppressed or cleanup raises.
    """
    handler_task = asyncio.ensure_future(handler(event))
    if timeout is None:
        await handler_task
        return False

    timer_task = asyncio.create_task(_deadline_timer(timeout))
    try:
        done, _ = await asyncio.wait(
            (handler_task, timer_task), return_when=asyncio.FIRST_COMPLETED
        )
        if handler_task in done:
            timer_task.cancel()
            await asyncio.gather(timer_task, return_exceptions=True)
            await handler_task
            return False

        # The timer won. Preserve that state even if cooperative cancellation
        # lets the handler return normally or raise during cleanup.
        handler_task.cancel()
        observer_task = asyncio.create_task(_observe_cancelled_handler(handler_task))
        try:
            outcome, cleanup_error = await asyncio.shield(observer_task)
        except asyncio.CancelledError:
            # This cancellation reached the consumer while it was waiting for
            # cleanup, so it has precedence over the internal timeout.
            handler_task.cancel()
            timer_task.cancel()
            await asyncio.gather(
                handler_task, timer_task, observer_task, return_exceptions=True
            )
            raise
        if outcome == "error" and cleanup_error is not None:
            log.warning(
                "Timed-out handler cleanup failed with %s",
                type(cleanup_error).__name__,
            )
        await asyncio.gather(timer_task, return_exceptions=True)
        return True
    except asyncio.CancelledError:
        handler_task.cancel()
        timer_task.cancel()
        await asyncio.gather(handler_task, timer_task, return_exceptions=True)
        raise


async def run_consumer(
    bus: "EventBus", subscription: str, *, idle_timeout: Optional[float] = None
) -> None:
    """Consume ``subscription`` until cancellation or an idle timeout.

    Each non-blocking poll runs in a worker thread. A bounded set of delivery
    tasks keeps heartbeats and transitions independent while preventing new
    claims when every configured subscription slot is occupied.

    When cancelled, active delivery tasks are cancelled before the queue is
    closed, and ``CancelledError`` propagates to the caller.
    """
    bus._begin_consuming(subscription)
    queue: SimpleQueue[object] | None = None
    active: set[asyncio.Task[None]] = set()
    delivery_order: dict[asyncio.Task[None], int] = {}
    next_delivery_order = 0

    def reap(done: set[asyncio.Task[None]]) -> None:
        """Observe every completed delivery before propagating one failure."""
        primary: BaseException | None = None
        for task in sorted(done, key=delivery_order.__getitem__):
            active.discard(task)
            delivery_order.pop(task)
            try:
                task.result()
            except BaseException as error:
                if primary is None:
                    primary = error
        if primary is not None:
            raise primary

    async def wait_for_delivery(timeout: Optional[float] = None) -> None:
        """Wait for one delivery completion and consume its result."""
        if not active:
            return
        done, _ = await asyncio.wait(
            active,
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        reap(done)

    try:
        queue = bus._open_subscription_queue(subscription)
        concurrency = bus._concurrency_for(subscription)
        idle_since: Optional[float] = None
        while True:
            if len(active) >= concurrency:
                await wait_for_delivery()
                continue
            try:
                job = await asyncio.to_thread(queue.get, False)
            except Empty:
                if active:
                    await wait_for_delivery(timeout=_POLL_INTERVAL)
                    continue
                if idle_timeout is not None:
                    now = asyncio.get_running_loop().time()
                    idle_since = idle_since if idle_since is not None else now
                    if now - idle_since >= idle_timeout:
                        return
                await asyncio.sleep(_POLL_INTERVAL)
                continue
            idle_since = None
            task = asyncio.create_task(_process_delivery(bus, subscription, queue, job))
            active.add(task)
            delivery_order[task] = next_delivery_order
            next_delivery_order += 1
    finally:
        for task in active:
            task.cancel()
        if active:
            await asyncio.gather(*active, return_exceptions=True)
        if queue is not None:
            queue.close()
        bus._end_consuming(subscription)


async def _heartbeat(
    queue: SimpleQueue[object],
    job: Job[object],
    interval: float,
    state: _LeaseState,
) -> None:
    """Renew the lease while the handler runs, stopping if it is lost."""
    lease_seconds = queue.delivery.lease_seconds
    while True:
        await asyncio.sleep(interval)
        try:
            await asyncio.to_thread(queue.extend_lease, job, lease_seconds)
        except Exception:  # noqa: BLE001 - includes LeaseExpired
            log.warning("Job %s lost its lease while the handler ran", job.id)
            state["lease_lost"] = True
            return


async def _transition(
    queue: SimpleQueue[object],
    operation: Literal["ack", "nack", "fail"],
    job: Job[object],
    *,
    last_error: str | None = None,
) -> None:
    """Apply ACK/NACK/fail without letting LeaseExpired stop the consumer."""
    try:
        if operation == "ack":
            await asyncio.to_thread(queue.ack, job)
        elif operation == "nack":
            await asyncio.to_thread(queue.nack, job, last_error=last_error)
        else:
            await asyncio.to_thread(queue.fail, job, last_error=last_error)
    except LeaseExpired:
        log.warning(
            "Job %s lost its lease before the transition; discarding the result",
            job.id,
        )


def _parse_envelope(value: object) -> _ParsedEnvelope | _EnvelopeError:
    """Validate and narrow an untrusted subscription-queue payload."""
    if not isinstance(value, dict):
        return _EnvelopeError(
            f"malformed envelope: expected a JSON object, got {type(value).__name__}"
        )
    event_type = value.get("event_type")
    if not isinstance(event_type, str):
        return _EnvelopeError("malformed envelope: missing or invalid 'event_type'")
    payload = value.get("payload")
    if not isinstance(payload, dict):
        return _EnvelopeError("malformed envelope: missing or invalid 'payload'")
    return _ParsedEnvelope(raw=value, event_type=event_type, payload=payload)


def _reconstruct_event(
    bus: "EventBus",
    envelope: _ParsedEnvelope,
) -> _ReconstructedEvent | _ReconstructionError:
    """Resolve and validate the concrete Pydantic event."""
    event_type = envelope.event_type
    cls = bus.registry.resolve(event_type)
    if cls is None:
        return _ReconstructionError(f"unknown event: {event_type!r}")

    try:
        event_data: dict[object, object] = {
            **envelope.payload,
            "event_id": envelope.raw["event_id"],
            "event_created_at": envelope.raw["event_created_at"],
        }
        for field in ("correlation_id", "causation_id"):
            if field in envelope.raw:
                event_data[field] = envelope.raw[field]
        # The class is resolved at runtime and Pydantic validates this dynamic
        # mapping, including rejecting non-string keyword keys. Erase key and
        # value types only for that constructor call.
        return _ReconstructedEvent(cls(**cast(dict[str, Any], event_data)))
    except (ValidationError, KeyError, TypeError, ValueError) as exc:
        return _ReconstructionError(f"invalid payload for {event_type!r}: {exc}")


async def _process_delivery(
    bus: "EventBus",
    subscription: str,
    queue: SimpleQueue[object],
    job: Job[object],
) -> None:
    parsed = _parse_envelope(job.data)
    if isinstance(parsed, _EnvelopeError):
        await _transition(queue, "fail", job, last_error=parsed.message)
        return

    reconstructed = _reconstruct_event(bus, parsed)
    if isinstance(reconstructed, _ReconstructionError):
        await _transition(
            queue,
            "fail",
            job,
            last_error=reconstructed.message,
        )
        return

    event = reconstructed.value
    event_type = parsed.event_type
    registration = bus._handlers.get((subscription, event_type)) or bus._handlers.get(
        (subscription, WILDCARD)
    )
    if registration is None:
        await _transition(
            queue,
            "fail",
            job,
            last_error=(
                f"no handler registered for {event_type!r} "
                f"in {subscription!r} in this process"
            ),
        )
        return

    # The heartbeat renews the lease while the handler runs. If the lease is
    # lost, discard the result because another worker may have claimed it.
    state = {"lease_lost": False}
    interval = max(queue.delivery.lease_seconds / 3, 0.05)
    heartbeat: asyncio.Task[None] | None = asyncio.create_task(
        _heartbeat(queue, job, interval, state)
    )
    try:
        handler = registration.handler
        if _is_async_callable(handler):
            timed_out = await _run_async_handler(handler, event, registration.timeout)
            if timed_out:
                # Keep renewing the lease through cooperative handler cleanup,
                # then stop the heartbeat before making the final decision.
                active_heartbeat = heartbeat
                if active_heartbeat is not None:
                    active_heartbeat.cancel()
                    await asyncio.gather(active_heartbeat, return_exceptions=True)
                heartbeat = None
                if state["lease_lost"]:
                    log.warning(
                        "Job %s lost its lease while its handler timed out; "
                        "discarding the result",
                        job.id,
                    )
                    return
                timeout_error = f"handler timeout after {registration.timeout} seconds"
                log.warning("Job %s %s", job.id, timeout_error)
                await _transition(queue, "nack", job, last_error=timeout_error)
                return
        else:
            # Run synchronous handlers outside the event-loop thread.
            result = await asyncio.to_thread(handler, event)
            if result is not None and inspect.isawaitable(result):
                # Safety net for a synchronous handler that returned an awaitable.
                await result
    except registration.permanent_errors as exc:
        await _transition(queue, "fail", job, last_error=f"permanent failure: {exc}")
    except Exception as exc:  # noqa: BLE001 - transient failure, retry it
        await _transition(queue, "nack", job, last_error=str(exc))
    else:
        if state["lease_lost"]:
            log.warning(
                "Job %s lost its lease while the handler ran; discarding the result",
                job.id,
            )
            return
        await _transition(queue, "ack", job)
    finally:
        if heartbeat is not None:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
