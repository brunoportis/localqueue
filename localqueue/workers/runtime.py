from __future__ import annotations

import asyncio
import time
import threading
from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from queue import Empty
from typing import Any, TypeVar, cast, TYPE_CHECKING

from ..failure import is_permanent_failure
from ..idempotency import IdempotencyRecord, IdempotencyStore
from ..queue import _error_payload
from ..retry import PersistentAsyncRetrying, PersistentRetrying

if TYPE_CHECKING:
    from ..queue import PersistentQueue
    from ..results import ResultStore
    from ..stores import QueueMessage

WrappedFn = TypeVar("WrappedFn", bound=Callable[..., Any])
_UNSET = object()


async def _run_in_daemon_thread(
    fn: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}
    done = threading.Event()

    def runner() -> None:
        try:
            result["value"] = fn(*args, **kwargs)
        except BaseException as exc:
            error["value"] = exc
        finally:
            done.set()

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    while not done.is_set():
        await asyncio.sleep(0.001)
    if "value" in error:
        raise error["value"]
    return result["value"]


def _resolve_dead_letter_on_failure(
    *,
    dead_letter_on_failure: bool | object,
    dead_letter_on_exhaustion: bool | object,
) -> bool:
    if (
        dead_letter_on_failure is not _UNSET
        and dead_letter_on_exhaustion is not _UNSET
        and dead_letter_on_failure != dead_letter_on_exhaustion
    ):
        raise ValueError(
            "pass either dead_letter_on_failure= or "
            "dead_letter_on_exhaustion=, not conflicting values"
        )
    if dead_letter_on_failure is not _UNSET:
        return cast("bool", dead_letter_on_failure)
    if dead_letter_on_exhaustion is not _UNSET:
        return cast("bool", dead_letter_on_exhaustion)
    return True


def _validate_release_delay(release_delay: float) -> None:
    if release_delay < 0:
        raise ValueError("release_delay must be greater than or equal to zero")


def _validate_min_interval(min_interval: float) -> None:
    if min_interval < 0:
        raise ValueError("min_interval must be greater than or equal to zero")


def _validate_circuit_breaker(
    circuit_breaker_failures: int, circuit_breaker_cooldown: float
) -> None:
    if circuit_breaker_failures < 0:
        raise ValueError(
            "circuit_breaker_failures must be greater than or equal to zero"
        )
    if circuit_breaker_cooldown < 0:
        raise ValueError(
            "circuit_breaker_cooldown must be greater than or equal to zero"
        )
    if circuit_breaker_failures == 0 and circuit_breaker_cooldown != 0:
        raise ValueError(
            "circuit breaker requires circuit_breaker_failures > 0 when "
            "circuit_breaker_cooldown is set"
        )
    if circuit_breaker_failures > 0 and circuit_breaker_cooldown == 0:
        raise ValueError(
            "circuit breaker requires circuit_breaker_cooldown > 0 when "
            "circuit_breaker_failures is set"
        )


@dataclass(slots=True)
class WorkerPolicyState:
    last_started_at: float | None = None
    consecutive_failures: int = 0
    breaker_open_until: float | None = None


class PersistentWorkerConfig:
    dead_letter_on_failure: bool
    dead_letter_on_exhaustion: bool
    release_delay: float
    min_interval: float
    circuit_breaker_failures: int
    circuit_breaker_cooldown: float
    retry_kwargs: dict[str, Any]

    def __init__(
        self,
        *,
        dead_letter_on_failure: bool | object = _UNSET,
        dead_letter_on_exhaustion: bool | object = _UNSET,
        release_delay: float = 0.0,
        min_interval: float = 0.0,
        circuit_breaker_failures: int = 0,
        circuit_breaker_cooldown: float = 0.0,
        **retry_kwargs: Any,
    ) -> None:
        resolved = _resolve_dead_letter_on_failure(
            dead_letter_on_failure=dead_letter_on_failure,
            dead_letter_on_exhaustion=dead_letter_on_exhaustion,
        )
        _validate_release_delay(release_delay)
        _validate_min_interval(min_interval)
        _validate_circuit_breaker(circuit_breaker_failures, circuit_breaker_cooldown)
        self.dead_letter_on_failure = resolved
        self.dead_letter_on_exhaustion = resolved
        self.release_delay = release_delay
        self.min_interval = min_interval
        self.circuit_breaker_failures = circuit_breaker_failures
        self.circuit_breaker_cooldown = circuit_breaker_cooldown
        self.retry_kwargs = dict(retry_kwargs)

    def with_overrides(
        self,
        *,
        dead_letter_on_failure: bool | object = _UNSET,
        dead_letter_on_exhaustion: bool | object = _UNSET,
        release_delay: float | object = _UNSET,
        min_interval: float | object = _UNSET,
        circuit_breaker_failures: int | object = _UNSET,
        circuit_breaker_cooldown: float | object = _UNSET,
        **retry_kwargs: Any,
    ) -> PersistentWorkerConfig:
        merged_retry_kwargs = {**self.retry_kwargs, **retry_kwargs}
        resolved = _resolve_dead_letter_on_failure(
            dead_letter_on_failure=dead_letter_on_failure,
            dead_letter_on_exhaustion=dead_letter_on_exhaustion,
        )
        if release_delay is not _UNSET:
            _validate_release_delay(cast("float", release_delay))
        if min_interval is not _UNSET:
            _validate_min_interval(cast("float", min_interval))
        if (
            circuit_breaker_failures is not _UNSET
            or circuit_breaker_cooldown is not _UNSET
        ):
            resolved_breaker_failures = (
                self.circuit_breaker_failures
                if circuit_breaker_failures is _UNSET
                else cast("int", circuit_breaker_failures)
            )
            resolved_breaker_cooldown = (
                self.circuit_breaker_cooldown
                if circuit_breaker_cooldown is _UNSET
                else cast("float", circuit_breaker_cooldown)
            )
            _validate_circuit_breaker(
                resolved_breaker_failures, resolved_breaker_cooldown
            )
        return PersistentWorkerConfig(
            dead_letter_on_failure=(
                self.dead_letter_on_failure
                if dead_letter_on_failure is _UNSET
                and dead_letter_on_exhaustion is _UNSET
                else resolved
            ),
            release_delay=(
                self.release_delay
                if release_delay is _UNSET
                else cast("float", release_delay)
            ),
            min_interval=(
                self.min_interval
                if min_interval is _UNSET
                else cast("float", min_interval)
            ),
            circuit_breaker_failures=(
                self.circuit_breaker_failures
                if circuit_breaker_failures is _UNSET
                else cast("int", circuit_breaker_failures)
            ),
            circuit_breaker_cooldown=(
                self.circuit_breaker_cooldown
                if circuit_breaker_cooldown is _UNSET
                else cast("float", circuit_breaker_cooldown)
            ),
            **merged_retry_kwargs,
        )


def _resolve_config(
    config: PersistentWorkerConfig | None,
    *,
    dead_letter_on_failure: bool | object,
    dead_letter_on_exhaustion: bool | object,
    release_delay: float | object,
    min_interval: float | object,
    circuit_breaker_failures: int | object,
    circuit_breaker_cooldown: float | object,
    retry_kwargs: dict[str, Any],
) -> PersistentWorkerConfig:
    base = config if config is not None else PersistentWorkerConfig()
    return base.with_overrides(
        dead_letter_on_failure=dead_letter_on_failure,
        dead_letter_on_exhaustion=dead_letter_on_exhaustion,
        release_delay=release_delay,
        min_interval=min_interval,
        circuit_breaker_failures=circuit_breaker_failures,
        circuit_breaker_cooldown=circuit_breaker_cooldown,
        **retry_kwargs,
    )


def _resolve_retry_kwargs(
    queue: PersistentQueue, retry_kwargs: dict[str, Any]
) -> dict[str, Any]:
    if not queue.retry_defaults:
        return dict(retry_kwargs)
    return {**queue.retry_defaults, **retry_kwargs}


def _sleep_for_policy(state: WorkerPolicyState, config: PersistentWorkerConfig) -> None:
    now = time.time()
    if state.breaker_open_until is not None:
        remaining = state.breaker_open_until - now
        if remaining > 0:
            time.sleep(remaining)
        state.breaker_open_until = None
        now = time.time()
    if state.last_started_at is not None and config.min_interval > 0:
        delay = config.min_interval - (now - state.last_started_at)
        if delay > 0:
            time.sleep(delay)
    state.last_started_at = time.time()


async def _sleep_for_policy_async(
    state: WorkerPolicyState, config: PersistentWorkerConfig
) -> None:
    now = time.time()
    if state.breaker_open_until is not None:
        remaining = state.breaker_open_until - now
        if remaining > 0:
            await asyncio.sleep(remaining)
        state.breaker_open_until = None
        now = time.time()
    if state.last_started_at is not None and config.min_interval > 0:
        delay = config.min_interval - (now - state.last_started_at)
        if delay > 0:
            await asyncio.sleep(delay)
    state.last_started_at = time.time()


def _record_failure(
    state: WorkerPolicyState,
    config: PersistentWorkerConfig,
    *,
    permanent: bool,
) -> None:
    if permanent:
        state.consecutive_failures = 0
        return
    state.consecutive_failures += 1
    if (
        config.circuit_breaker_failures > 0
        and state.consecutive_failures >= config.circuit_breaker_failures
    ):
        state.breaker_open_until = time.time() + config.circuit_breaker_cooldown
        state.consecutive_failures = 0


def _record_success(state: WorkerPolicyState) -> None:
    state.consecutive_failures = 0


def _idempotency_store(queue: PersistentQueue) -> IdempotencyStore | None:
    return cast(
        "IdempotencyStore | None",
        getattr(queue.delivery_policy, "idempotency_store", None),
    )


def _result_policy(queue: PersistentQueue) -> Any:
    return getattr(queue.delivery_policy, "result_policy", None)


def _commit_policy(queue: PersistentQueue) -> Any:
    return getattr(queue.delivery_policy, "commit_policy", None)


def _result_store(queue: PersistentQueue) -> ResultStore | None:
    result_policy = _result_policy(queue)
    return cast("ResultStore | None", getattr(result_policy, "result_store", None))


def _outbox_store(queue: PersistentQueue) -> ResultStore | None:
    commit_policy = _commit_policy(queue)
    return cast("ResultStore | None", getattr(commit_policy, "outbox_store", None))


def _prepare_store(queue: PersistentQueue) -> ResultStore | None:
    commit_policy = _commit_policy(queue)
    return cast("ResultStore | None", getattr(commit_policy, "prepare_store", None))


def _commit_store(queue: PersistentQueue) -> ResultStore | None:
    commit_policy = _commit_policy(queue)
    return cast("ResultStore | None", getattr(commit_policy, "commit_store", None))


def _saga_store(queue: PersistentQueue) -> ResultStore | None:
    commit_policy = _commit_policy(queue)
    return cast("ResultStore | None", getattr(commit_policy, "saga_store", None))


def _result_key(message: QueueMessage) -> str | None:
    if message.dedupe_key is None:
        return None
    return f"dedupe:{message.queue}:{message.dedupe_key}"


def _cached_result(queue: PersistentQueue, record: IdempotencyRecord) -> Any:
    result_store = _result_store(queue)
    if result_store is not None and record.result_key is not None:
        return result_store.load(record.result_key)
    return record.metadata.get("result")


def _begin_idempotent_handling(
    queue: PersistentQueue, message: QueueMessage
) -> tuple[bool, Any]:
    store = _idempotency_store(queue)
    if store is None or message.dedupe_key is None:
        return False, None
    record = store.load(message.dedupe_key)
    if record is not None and record.status == "succeeded":
        _ = queue.ack(message)
        result_policy = _result_policy(queue)
        if bool(getattr(result_policy, "returns_cached_result", False)):
            return True, _cached_result(queue, record)
        return True, None
    first_seen_at = time.time() if record is None else record.first_seen_at
    store.save(
        message.dedupe_key,
        IdempotencyRecord(
            status="pending",
            first_seen_at=first_seen_at,
            metadata={"queue": message.queue, "message_id": message.id},
        ),
    )
    return False, None


def _complete_idempotent_handling(
    queue: PersistentQueue,
    message: QueueMessage,
    *,
    status: str,
    result: Any = None,
    error: BaseException | None = None,
) -> None:
    store = _idempotency_store(queue)
    if store is None or message.dedupe_key is None:
        return
    existing = store.load(message.dedupe_key)
    first_seen_at = time.time() if existing is None else existing.first_seen_at
    metadata: dict[str, Any] = {"queue": message.queue, "message_id": message.id}
    result_policy = _result_policy(queue)
    result_key = None if existing is None else existing.result_key
    if bool(getattr(result_policy, "stores_result", False)) and status == "succeeded":
        result_store = _result_store(queue)
        if result_store is None:
            metadata["result"] = result
            result_key = "inline"
        else:
            resolved_result_key = _result_key(message)
            if resolved_result_key is not None:
                result_store.save(resolved_result_key, result)
                result_key = resolved_result_key
    if error is not None:
        metadata["last_error"] = _error_payload(error)
    store.save(
        message.dedupe_key,
        IdempotencyRecord(
            status=cast("Any", status),
            first_seen_at=first_seen_at,
            completed_at=time.time(),
            result_key=result_key,
            metadata=metadata,
        ),
    )


def _commit_outbox(
    queue: PersistentQueue,
    message: QueueMessage,
    *,
    result: Any,
    result_key: str | None,
) -> None:
    commit_policy = _commit_policy(queue)
    if getattr(commit_policy, "mode", None) != "transactional-outbox":
        return
    outbox_store = _outbox_store(queue)
    if outbox_store is None:
        return
    outbox_key = _result_key(message)
    if outbox_key is None:
        return
    outbox_store.save(
        f"outbox:{outbox_key}",
        {
            "queue": message.queue,
            "message_id": message.id,
            "dedupe_key": message.dedupe_key,
            "result_key": result_key,
            "result": result,
        },
    )


def _commit_two_phase(
    queue: PersistentQueue,
    message: QueueMessage,
    *,
    result: Any,
    result_key: str | None,
) -> None:
    commit_policy = _commit_policy(queue)
    if getattr(commit_policy, "mode", None) != "two-phase":
        return
    prepare_store = _prepare_store(queue)
    commit_store = _commit_store(queue)
    if prepare_store is None and commit_store is None:
        return
    phase_key = _result_key(message)
    if phase_key is None:
        return
    prepare_payload = {
        "queue": message.queue,
        "message_id": message.id,
        "dedupe_key": message.dedupe_key,
        "result_key": result_key,
        "result": result,
        "phase": "prepare",
    }
    commit_payload = {
        "queue": message.queue,
        "message_id": message.id,
        "dedupe_key": message.dedupe_key,
        "result_key": result_key,
        "result": result,
        "phase": "commit",
    }
    if prepare_store is not None:
        prepare_store.save(f"prepare:{phase_key}", prepare_payload)
    if commit_store is None:
        return
    commit_store.save(f"commit:{phase_key}", commit_payload)


def _commit_saga(
    queue: PersistentQueue,
    message: QueueMessage,
    *,
    phase: str,
    result: Any = None,
    error: BaseException | None = None,
) -> None:
    commit_policy = _commit_policy(queue)
    if getattr(commit_policy, "mode", None) != "saga":
        return
    saga_store = _saga_store(queue)
    if saga_store is None:
        return
    phase_key = _result_key(message)
    if phase_key is None:
        return
    payload: dict[str, Any] = {
        "queue": message.queue,
        "message_id": message.id,
        "dedupe_key": message.dedupe_key,
        "phase": phase,
    }
    if result is not None:
        payload["result"] = result
    if error is not None:
        payload["error"] = _error_payload(error)
    saga_store.save(f"saga:{phase}:{phase_key}", payload)


def persistent_worker(
    queue: PersistentQueue,
    *,
    config: PersistentWorkerConfig | None = None,
    worker_id: str | None = None,
    dead_letter_on_failure: bool | object = _UNSET,
    dead_letter_on_exhaustion: bool | object = _UNSET,
    release_delay: float | object = _UNSET,
    **retry_kwargs: Any,
) -> Callable[[WrappedFn], Callable[..., Any]]:
    worker_config = _resolve_config(
        config,
        dead_letter_on_failure=dead_letter_on_failure,
        dead_letter_on_exhaustion=dead_letter_on_exhaustion,
        release_delay=release_delay,
        min_interval=_UNSET,
        circuit_breaker_failures=_UNSET,
        circuit_breaker_cooldown=_UNSET,
        retry_kwargs=retry_kwargs,
    )
    retry_kwargs = _resolve_retry_kwargs(queue, worker_config.retry_kwargs)
    policy_state = WorkerPolicyState()

    def decorator(fn: WrappedFn) -> Callable[..., Any]:
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            if worker_id is not None:
                queue.record_worker_heartbeat(worker_id)
            _sleep_for_policy(policy_state, worker_config)
            message = queue.get_message()
            short_circuited, cached_result = _begin_idempotent_handling(queue, message)
            if short_circuited:
                _record_success(policy_state)
                return cached_result
            retryer = PersistentRetrying(key=message.id, **retry_kwargs)
            try:
                result = retryer(fn, message.value, *args, **kwargs)
            except Exception as exc:
                _complete_idempotent_handling(
                    queue, message, status="failed", error=exc
                )
                _commit_saga(queue, message, phase="compensate", error=exc)
                if is_permanent_failure(exc) or worker_config.dead_letter_on_failure:
                    queue.dead_letter(message, error=exc)
                else:
                    queue.release(message, delay=worker_config.release_delay, error=exc)
                _record_failure(
                    policy_state,
                    worker_config,
                    permanent=is_permanent_failure(exc),
                )
                raise
            finally:
                if worker_id is not None:
                    queue.record_worker_heartbeat(worker_id)
            _record_success(policy_state)
            _complete_idempotent_handling(
                queue,
                message,
                status="succeeded",
                result=result,
            )
            result_key = None
            if _result_store(queue) is not None:
                result_key = _result_key(message)
            _commit_outbox(queue, message, result=result, result_key=result_key)
            _commit_two_phase(queue, message, result=result, result_key=result_key)
            _commit_saga(queue, message, phase="forward", result=result)
            queue.ack(message)
            return result

        return wrapped

    return decorator


def persistent_async_worker(
    queue: PersistentQueue,
    *,
    config: PersistentWorkerConfig | None = None,
    worker_id: str | None = None,
    dead_letter_on_failure: bool | object = _UNSET,
    dead_letter_on_exhaustion: bool | object = _UNSET,
    release_delay: float | object = _UNSET,
    **retry_kwargs: Any,
) -> Callable[[WrappedFn], Callable[..., Any]]:
    worker_config = _resolve_config(
        config,
        dead_letter_on_failure=dead_letter_on_failure,
        dead_letter_on_exhaustion=dead_letter_on_exhaustion,
        release_delay=release_delay,
        min_interval=_UNSET,
        circuit_breaker_failures=_UNSET,
        circuit_breaker_cooldown=_UNSET,
        retry_kwargs=retry_kwargs,
    )
    retry_kwargs = _resolve_retry_kwargs(queue, worker_config.retry_kwargs)
    policy_state = WorkerPolicyState()

    def decorator(fn: WrappedFn) -> Callable[..., Any]:
        async def wrapped(*args: Any, **kwargs: Any) -> Any:
            if worker_id is not None:
                queue.record_worker_heartbeat(worker_id)
            await _sleep_for_policy_async(policy_state, worker_config)
            message = await _get_message_async(queue)
            short_circuited, cached_result = _begin_idempotent_handling(queue, message)
            if short_circuited:
                _record_success(policy_state)
                return cached_result
            retryer = PersistentAsyncRetrying(key=message.id, **retry_kwargs)
            try:
                result = await retryer(fn, message.value, *args, **kwargs)
            except Exception as exc:
                _complete_idempotent_handling(
                    queue, message, status="failed", error=exc
                )
                _commit_saga(queue, message, phase="compensate", error=exc)
                if is_permanent_failure(exc) or worker_config.dead_letter_on_failure:
                    queue.dead_letter(message, error=exc)
                else:
                    queue.release(
                        message,
                        delay=worker_config.release_delay,
                        error=exc,
                    )
                _record_failure(
                    policy_state,
                    worker_config,
                    permanent=is_permanent_failure(exc),
                )
                raise
            finally:
                if worker_id is not None:
                    queue.record_worker_heartbeat(worker_id)
            _record_success(policy_state)
            _complete_idempotent_handling(
                queue,
                message,
                status="succeeded",
                result=result,
            )
            result_key = None
            if _result_store(queue) is not None:
                result_key = _result_key(message)
            _commit_outbox(queue, message, result=result, result_key=result_key)
            _commit_two_phase(queue, message, result=result, result_key=result_key)
            _commit_saga(queue, message, phase="forward", result=result)
            queue.ack(message)
            return result

        return wrapped

    return decorator


async def _get_message_async(queue: PersistentQueue) -> QueueMessage:
    while True:
        try:
            return await _run_in_daemon_thread(queue.get_message, False)
        except Empty:
            notification = getattr(queue, "notification_policy", None)
            clear = getattr(notification, "clear", None)
            wait_async = cast(
                "Callable[[], Awaitable[object]] | None",
                getattr(notification, "wait_async", None),
            )
            if callable(clear) and callable(wait_async):
                clear()
                try:
                    return await _run_in_daemon_thread(queue.get_message, False)
                except Empty:
                    _ = await wait_async()
                    continue
            await asyncio.sleep(0.05)
