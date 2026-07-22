"""Event bus subscription consumption loop."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
from typing import TYPE_CHECKING, Any, Optional

from pydantic import ValidationError

from localqueue.bus.bus import WILDCARD, _is_async_callable
from localqueue.exceptions import Empty, LeaseExpired
from localqueue.job import Job

if TYPE_CHECKING:
    from localqueue.bus.bus import EventBus

log = logging.getLogger(__name__)
_POLL_INTERVAL = 0.1


async def _deadline_timer(timeout: float) -> None:
    """Complete after an individual handler's configured deadline."""
    await asyncio.sleep(timeout)


async def _run_async_handler(handler: Any, event: Any, timeout: float | None) -> bool:
    """Run an async handler and return whether its internal deadline elapsed.

    A completed handler wins a simultaneous timer completion. Once the timer
    wins, the deadline remains authoritative even when cancellation is
    suppressed or cleanup raises.
    """
    handler_task = asyncio.create_task(handler(event))
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
        try:
            await handler_task
        except asyncio.CancelledError:
            pass
        except BaseException as error:  # noqa: BLE001 - cleanup is observed
            log.warning(
                "Timed-out handler cleanup failed with %s", type(error).__name__
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
    queue: Any | None = None
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
        event_data = {
            **envelope["payload"],
            "event_id": envelope["event_id"],
            "event_created_at": envelope["event_created_at"],
        }
        for field in ("correlation_id", "causation_id"):
            if field in envelope:
                event_data[field] = envelope[field]
        event = cls(**event_data)
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
    heartbeat: asyncio.Task[None] | None = asyncio.create_task(
        _heartbeat(queue, job, interval, state)
    )
    try:
        handler = registration.handler
        result: Any = None
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
                await _transition(queue, queue.nack, job, last_error=timeout_error)
                return
        else:
            # Run synchronous handlers outside the event-loop thread.
            result = await asyncio.to_thread(handler, event)
        if result is not None and inspect.isawaitable(result):
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
        if heartbeat is not None:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
