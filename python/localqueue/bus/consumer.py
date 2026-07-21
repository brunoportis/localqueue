"""Event bus subscription consumption loop."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
from typing import TYPE_CHECKING, Any, Optional
from uuid import UUID

from pydantic import ValidationError

from localqueue.bus.bus import WILDCARD
from localqueue.exceptions import Empty, LeaseExpired
from localqueue.job import Job

if TYPE_CHECKING:
    from localqueue.bus.bus import EventBus

log = logging.getLogger(__name__)
_POLL_INTERVAL = 0.1


async def run_consumer(
    bus: "EventBus", subscription: str, *, idle_timeout: Optional[float] = None
) -> None:
    """Consume ``subscription`` until cancellation or an idle timeout.

    Each non-blocking poll runs in a worker thread. When the queue is empty,
    the event loop waits for the polling interval. ``CancelledError`` closes
    the queue in ``finally`` and propagates.
    """
    queue = bus._open_subscription_queue(subscription)
    try:
        idle_since: Optional[float] = None
        while True:
            try:
                job = await asyncio.to_thread(queue.get, False)
            except Empty:
                if idle_timeout is not None:
                    now = asyncio.get_running_loop().time()
                    idle_since = idle_since if idle_since is not None else now
                    if now - idle_since >= idle_timeout:
                        return
                await asyncio.sleep(_POLL_INTERVAL)
                continue
            idle_since = None
            await _process_delivery(bus, subscription, queue, job)
    finally:
        queue.close()


async def _heartbeat(
    queue: Any, job: Job, interval: float, state: dict[str, bool]
) -> None:
    """Renew the lease while the handler runs, stopping if it is lost."""
    lease_seconds = queue.lease_seconds
    while True:
        await asyncio.sleep(interval)
        try:
            await asyncio.to_thread(queue.extend_lease, job, lease_seconds)
        except Exception:  # noqa: BLE001 - includes LeaseExpired
            log.warning("Job %s lost its lease while the handler ran", job.id)
            state["lease_lost"] = True
            return


async def _transition(queue: Any, operation: Any, job: Job, **kwargs: Any) -> None:
    """Apply ACK/NACK/fail without letting LeaseExpired stop the consumer."""
    try:
        await asyncio.to_thread(operation, job, **kwargs)
    except LeaseExpired:
        log.warning(
            "Job %s lost its lease before the transition; discarding the result",
            job.id,
        )


def _envelope_error(envelope: Any) -> Optional[str]:
    """Validate the minimum structure of a deserialized envelope."""
    if not isinstance(envelope, dict):
        return (
            f"malformed envelope: expected a JSON object, got {type(envelope).__name__}"
        )
    if not isinstance(envelope.get("event_type"), str):
        return "malformed envelope: missing or invalid 'event_type'"
    if not isinstance(envelope.get("payload"), dict):
        return "malformed envelope: missing or invalid 'payload'"
    return None


async def _process_delivery(
    bus: "EventBus", subscription: str, queue: Any, job: Job
) -> None:
    envelope = job.data
    error = _envelope_error(envelope)
    if error is not None:
        await _transition(queue, queue.fail, job, last_error=error)
        return

    event_type = envelope["event_type"]
    cls = bus.registry.resolve(event_type)
    if cls is None:
        # An unknown type is a permanent failure; retrying cannot fix it.
        await _transition(
            queue,
            queue.fail,
            job,
            last_error=f"unknown event: {event_type!r}",
        )
        return

    try:
        event = cls(
            event_id=UUID(envelope["event_id"]),
            event_created_at=envelope["event_created_at"],
            **envelope["payload"],
        )
    except (ValidationError, KeyError, TypeError, ValueError) as exc:
        # An invalid payload is a permanent failure; retrying cannot fix it.
        await _transition(
            queue,
            queue.fail,
            job,
            last_error=f"invalid payload for {event_type!r}: {exc}",
        )
        return

    registration = bus._handlers.get((subscription, event_type)) or bus._handlers.get(
        (subscription, WILDCARD)
    )
    if registration is None:
        await _transition(
            queue,
            queue.fail,
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
    interval = max(queue.lease_seconds / 3, 0.05)
    heartbeat = asyncio.create_task(_heartbeat(queue, job, interval, state))
    try:
        handler = registration.handler
        if inspect.iscoroutinefunction(handler):
            result = await handler(event)
        else:
            # Run synchronous handlers outside the event-loop thread.
            result = await asyncio.to_thread(handler, event)
        if inspect.isawaitable(result):
            # Safety net for a synchronous handler that returned an awaitable.
            await result
    except registration.permanent_errors as exc:
        await _transition(
            queue, queue.fail, job, last_error=f"permanent failure: {exc}"
        )
    except Exception as exc:  # noqa: BLE001 - transient failure, retry it
        await _transition(queue, queue.nack, job, last_error=str(exc))
    else:
        if state["lease_lost"]:
            log.warning(
                "Job %s lost its lease while the handler ran; discarding the result",
                job.id,
            )
            return
        await _transition(queue, queue.ack, job)
    finally:
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat
