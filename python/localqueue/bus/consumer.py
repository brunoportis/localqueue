"""Event bus subscription consumption loop."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
from typing import TYPE_CHECKING, Literal, Optional, TypedDict, cast

from localqueue.bus.bus import (
    _AsyncStoredEventHandler,
    _is_async_callable,
    _StoredEventHandler,
)
from localqueue.bus.context import ContextT, HandlerContext, RuntimeContext
from localqueue.bus.control import Reject, Retry
from localqueue.bus.envelope import (
    EnvelopeError,
    parse_envelope,
    reconstruct_event,
)
from localqueue.bus.event import BaseEvent, InvalidEventIdentity, derive_from_returned
from localqueue.bus.identity import prepare_event_persistence
from localqueue.bus.retry import RetryPolicy
from localqueue.bus.topology import WILDCARD
from localqueue.core import SimpleQueue
from localqueue.deadletter import FailureReason
from localqueue.exceptions import DeduplicationConflict, Empty, LeaseExpired
from localqueue.job import Job

if TYPE_CHECKING:
    from localqueue.bus.bus import EventBus

log = logging.getLogger(__name__)
_POLL_INTERVAL = 0.1


class _LeaseState(TypedDict):
    lease_lost: bool


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
    context: HandlerContext,
    accepts_context: bool,
    timeout: float | None,
) -> tuple[bool, object]:
    """Run an async handler and return whether its internal deadline elapsed.

    A completed handler wins a simultaneous timer completion. Once the timer
    wins, the deadline remains authoritative even when cancellation is
    suppressed or cleanup raises.
    """
    result = handler(event, context) if accepts_context else handler(event)
    handler_task = asyncio.ensure_future(result)
    if timeout is None:
        return False, await handler_task

    timer_task = asyncio.create_task(_deadline_timer(timeout))
    try:
        done, _ = await asyncio.wait(
            (handler_task, timer_task), return_when=asyncio.FIRST_COMPLETED
        )
        if handler_task in done:
            timer_task.cancel()
            await asyncio.gather(timer_task, return_exceptions=True)
            return False, await handler_task

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
        return True, None
    except asyncio.CancelledError:
        handler_task.cancel()
        timer_task.cancel()
        await asyncio.gather(handler_task, timer_task, return_exceptions=True)
        raise


async def _create_handler_context(
    bus: "EventBus[ContextT]",
    event: BaseEvent,
    job: Job[object],
    handler_name: str,
) -> HandlerContext:
    """Create the configured context for one delivery attempt."""
    runtime = RuntimeContext(
        event_id=str(event.event_id),
        attempt=job.attempts + 1,
        handler_name=handler_name,
    )
    factory = bus.context_factory
    if factory is None:
        return HandlerContext(runtime)
    if _is_async_callable(cast(_StoredEventHandler, factory)):
        context = factory(runtime)
    else:
        context = await asyncio.to_thread(factory, runtime)
    if inspect.isawaitable(context):
        context = await context
    return cast(HandlerContext, context)


async def _invoke_sync_handler(
    handler: _StoredEventHandler,
    event: BaseEvent,
    context: HandlerContext,
    accepts_context: bool,
) -> object:
    """Run a synchronous handler outside the event loop."""
    if accepts_context:
        result = await asyncio.to_thread(handler, event, context)
    else:
        result = await asyncio.to_thread(handler, event)
    if inspect.isawaitable(result):
        return await result
    return result


def _unsupported_result_error(handler_name: str, result: object) -> str:
    return (
        f"handler {handler_name!r} returned unsupported result type "
        f"{type(result).__name__!r}; expected None or BaseEvent"
    )


async def _commit_handler_result(
    bus: "EventBus[ContextT]",
    queue: SimpleQueue[object],
    job: Job[object],
    parent: BaseEvent,
    result: object,
    handler_name: str,
) -> None:
    if result is None:
        await _transition(queue, "ack", job)
        return
    if not isinstance(result, BaseEvent):
        await _transition(
            queue,
            "fail",
            job,
            last_error=_unsupported_result_error(handler_name, result),
            reason=FailureReason.PERMANENT_HANDLER_ERROR,
        )
        return

    event = derive_from_returned(result, parent)
    bus.registry.register(type(event))
    subscriptions = bus._subscriptions_for(event.event_type)
    if not subscriptions:
        if bus.require_subscribers:
            await _transition(
                queue,
                "fail",
                job,
                last_error=(
                    f"handler {handler_name!r} returned event type "
                    f"{event.event_type!r} with no subscribers"
                ),
                reason=FailureReason.PERMANENT_HANDLER_ERROR,
            )
        else:
            await _transition(queue, "ack", job)
        return

    prepared = prepare_event_persistence(event)
    identity = prepared.identity
    payload = await asyncio.to_thread(bus._serialize_envelope, event, prepared.payload)
    targets: list[tuple[str, str | None, str | None, str | None]] = [
        (
            bus._queue_name(subscription),
            identity.job_id,
            identity.dedup_key,
            identity.dedup_fingerprint,
        )
        for subscription in subscriptions
    ]
    try:
        await asyncio.to_thread(
            queue._ack_and_fanout, job, payload=payload, targets=targets
        )
    except LeaseExpired:
        log.warning(
            "Job %s lost its lease before the transition; discarding the result",
            job.id,
        )


async def run_consumer(
    bus: "EventBus[ContextT]",
    subscription: str,
    *,
    idle_timeout: Optional[float] = None,
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
        retry_policy = bus._retry_for(subscription)
        idle_since: Optional[float] = None
        while True:
            if len(active) >= concurrency:
                await wait_for_delivery()
                continue
            try:
                if retry_policy is None:
                    job = await asyncio.to_thread(queue.get, False)
                else:
                    job = await asyncio.to_thread(
                        queue._get_with_max_attempts,
                        max_attempts=retry_policy.max_attempts,
                        block=False,
                    )
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
    reason: FailureReason | None = None,
    delay: float = 0.0,
    failure_category: str | None = None,
) -> None:
    """Apply ACK/NACK/fail without letting LeaseExpired stop the consumer."""
    try:
        if operation == "ack":
            await asyncio.to_thread(queue.ack, job)
        elif operation == "nack":
            await asyncio.to_thread(
                queue._nack_with_reason,
                job,
                delay=delay,
                last_error=last_error,
                reason=reason,
            )
        else:
            await asyncio.to_thread(
                queue._fail_with_reason,
                job,
                last_error=last_error,
                reason=reason or FailureReason.EXPLICIT_PERMANENT_FAILURE,
                failure_category=failure_category,
            )
    except LeaseExpired:
        log.warning(
            "Job %s lost its lease before the transition; discarding the result",
            job.id,
        )


async def _retry_delivery(
    queue: SimpleQueue[object],
    job: Job[object],
    *,
    policy: RetryPolicy | None,
    last_error: str | None,
    explicit_after: float | None = None,
    reason: FailureReason | None = None,
) -> None:
    """Persist one retry decision without sleeping the consumer."""
    if explicit_after is not None:
        delay = explicit_after
    elif policy is not None:
        delay = policy._delay_for(job.attempts + 1)
    else:
        delay = 0.0
    await _transition(
        queue,
        "nack",
        job,
        last_error=last_error,
        delay=delay,
        reason=None if policy is not None else reason,
    )


async def _handle_delivery_exception(
    queue: SimpleQueue[object],
    job: Job[object],
    error: BaseException,
    *,
    permanent_errors: tuple[type[BaseException], ...],
    policy: RetryPolicy | None,
) -> None:
    if isinstance(error, asyncio.CancelledError):
        raise error
    if isinstance(error, Reject):
        await _transition(
            queue,
            "fail",
            job,
            last_error=error.reason,
            reason=FailureReason.REJECTED,
            failure_category=error.category,
        )
    elif isinstance(error, Retry):
        await _retry_delivery(
            queue,
            job,
            policy=policy,
            last_error=error.reason,
            explicit_after=error.after,
        )
    elif isinstance(error, permanent_errors):
        await _transition(
            queue,
            "fail",
            job,
            last_error=f"permanent failure: {error}",
            reason=FailureReason.PERMANENT_HANDLER_ERROR,
        )
    elif isinstance(error, Exception):
        await _retry_delivery(queue, job, policy=policy, last_error=str(error))
    else:
        raise error


async def _handle_commit_exception(
    queue: SimpleQueue[object],
    job: Job[object],
    error: Exception,
    *,
    handler_name: str,
    result: object,
    policy: RetryPolicy | None,
) -> None:
    if isinstance(error, InvalidEventIdentity):
        await _transition(
            queue,
            "fail",
            job,
            last_error=(
                f"handler {handler_name!r} returned invalid "
                f"{type(result).__name__} identity: {error}"
            ),
            reason=FailureReason.PERMANENT_HANDLER_ERROR,
            failure_category="invalid_event_identity",
        )
    elif isinstance(error, DeduplicationConflict):
        await _transition(
            queue,
            "fail",
            job,
            last_error=(
                f"handler {handler_name!r} returned {type(result).__name__} "
                "with an event identity conflict"
            ),
            reason=FailureReason.PERMANENT_HANDLER_ERROR,
            failure_category="deduplication_conflict",
        )
    else:
        await _retry_delivery(queue, job, policy=policy, last_error=str(error))


async def _process_delivery(
    bus: "EventBus[ContextT]",
    subscription: str,
    queue: SimpleQueue[object],
    job: Job[object],
) -> None:
    parsed = parse_envelope(job.data)
    if isinstance(parsed, EnvelopeError):
        await _transition(
            queue, "fail", job, last_error=parsed.message, reason=parsed.reason
        )
        return

    reconstructed = reconstruct_event(bus.registry, parsed)
    if isinstance(reconstructed, EnvelopeError):
        await _transition(
            queue,
            "fail",
            job,
            last_error=reconstructed.message,
            reason=reconstructed.reason,
        )
        return

    event = reconstructed.value
    event_type = parsed.event_type
    registration = bus._handlers.get((subscription, event_type)) or bus._handlers.get(
        (subscription, WILDCARD)
    )
    policy = bus._retry_for(subscription)
    if registration is None:
        await _transition(
            queue,
            "fail",
            job,
            last_error=(
                f"no handler registered for {event_type!r} "
                f"in {subscription!r} in this process"
            ),
            reason=FailureReason.NO_HANDLER,
        )
        return

    # The heartbeat renews the lease while the handler runs. If the lease is
    # lost, discard the result because another worker may have claimed it.
    state: _LeaseState = {"lease_lost": False}
    interval = max(queue.delivery.lease_seconds / 3, 0.05)
    heartbeat: asyncio.Task[None] | None = asyncio.create_task(
        _heartbeat(queue, job, interval, state)
    )
    try:
        handler = registration.handler
        context = await _create_handler_context(
            bus, event, job, registration.handler_name
        )
        if _is_async_callable(handler):
            timed_out, result = await _run_async_handler(
                handler,
                event,
                context,
                registration.accepts_context,
                registration.timeout,
            )
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
                await _retry_delivery(
                    queue,
                    job,
                    policy=policy,
                    last_error=timeout_error,
                    reason=FailureReason.HANDLER_TIMEOUT,
                )
                return
        else:
            result = await _invoke_sync_handler(
                handler, event, context, registration.accepts_context
            )
    except BaseException as exc:
        await _handle_delivery_exception(
            queue,
            job,
            exc,
            permanent_errors=registration.permanent_errors,
            policy=policy,
        )
    else:
        if state["lease_lost"]:
            log.warning(
                "Job %s lost its lease while the handler ran; discarding the result",
                job.id,
            )
            return
        try:
            await _commit_handler_result(
                bus,
                queue,
                job,
                event,
                result,
                registration.handler_name,
            )
        except Exception as exc:  # noqa: BLE001 - classified by commit phase
            await _handle_commit_exception(
                queue,
                job,
                exc,
                handler_name=registration.handler_name,
                result=result,
                policy=policy,
            )
    finally:
        if heartbeat is not None:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
