from __future__ import annotations

import asyncio
import time
import threading
from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from queue import Empty
from typing import Any, TypeVar, cast, TYPE_CHECKING

from ..queue import _error_payload
from ..retry import PersistentAsyncRetrying, PersistentRetrying
from .domain import (
    _CommitCoordinator,
    _IdempotencyCoordinator,
    _WorkerOutcomeCoordinator,
    _WorkerPolicyAccess,
    _result_key as _domain_result_key,
)

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


def _idempotency_store(queue: PersistentQueue) -> Any:
    return _WorkerPolicyAccess(queue).idempotency_store()


def _result_policy(queue: PersistentQueue) -> Any:
    return _WorkerPolicyAccess(queue).result_policy()


def _commit_policy(queue: PersistentQueue) -> Any:
    return _WorkerPolicyAccess(queue).commit_policy()


def _result_store(queue: PersistentQueue) -> ResultStore | None:
    return _WorkerPolicyAccess(queue).result_store()


def _outbox_store(queue: PersistentQueue) -> ResultStore | None:
    return _WorkerPolicyAccess(queue).outbox_store()


def _prepare_store(queue: PersistentQueue) -> ResultStore | None:
    return _WorkerPolicyAccess(queue).prepare_store()


def _commit_store(queue: PersistentQueue) -> ResultStore | None:
    return _WorkerPolicyAccess(queue).commit_store()


def _saga_store(queue: PersistentQueue) -> ResultStore | None:
    return _WorkerPolicyAccess(queue).saga_store()


def _result_key(message: QueueMessage) -> str | None:
    return _domain_result_key(message)


def _begin_idempotent_handling(
    queue: PersistentQueue, message: QueueMessage
) -> tuple[bool, Any]:
    return _IdempotencyCoordinator(_WorkerPolicyAccess(queue)).begin(
        message,
        now_fn=time.time,
        ack_duplicate=queue.ack,
    )


def _complete_idempotent_handling(
    queue: PersistentQueue,
    message: QueueMessage,
    *,
    status: str,
    result: Any = None,
    error: BaseException | None = None,
) -> None:
    _IdempotencyCoordinator(_WorkerPolicyAccess(queue)).complete(
        message,
        status=status,
        now_fn=time.time,
        result=result,
        error_payload=None if error is None else _error_payload(error),
    )


def _commit_outbox(
    queue: PersistentQueue,
    message: QueueMessage,
    *,
    result: Any,
    result_key: str | None,
) -> None:
    _CommitCoordinator(_WorkerPolicyAccess(queue)).commit_outbox(
        message,
        result=result,
        result_key=result_key,
    )


def _commit_two_phase(
    queue: PersistentQueue,
    message: QueueMessage,
    *,
    result: Any,
    result_key: str | None,
) -> None:
    _CommitCoordinator(_WorkerPolicyAccess(queue)).commit_two_phase(
        message,
        result=result,
        result_key=result_key,
    )


def _commit_saga(
    queue: PersistentQueue,
    message: QueueMessage,
    *,
    phase: str,
    result: Any = None,
    error: BaseException | None = None,
) -> None:
    _CommitCoordinator(_WorkerPolicyAccess(queue)).commit_saga(
        message,
        phase=phase,
        result=result,
        error_payload=None if error is None else _error_payload(error),
    )


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
    policy_access = _WorkerPolicyAccess(queue)
    idempotency = _IdempotencyCoordinator(policy_access)
    commits = _CommitCoordinator(policy_access)
    outcomes = _WorkerOutcomeCoordinator(queue, worker_config)

    def decorator(fn: WrappedFn) -> Callable[..., Any]:
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            if worker_id is not None:
                queue.record_worker_heartbeat(worker_id)
            _sleep_for_policy(policy_state, worker_config)
            message = queue.get_message()
            short_circuited, cached_result = idempotency.begin(
                message,
                now_fn=time.time,
                ack_duplicate=queue.ack,
            )
            if short_circuited:
                _record_success(policy_state)
                return cached_result
            retryer = PersistentRetrying(key=message.id, **retry_kwargs)
            try:
                result = retryer(fn, message.value, *args, **kwargs)
            except Exception as exc:
                error_payload = _error_payload(exc)
                idempotency.complete(
                    message,
                    status="failed",
                    now_fn=time.time,
                    error_payload=error_payload,
                )
                commits.compensate_failure(message, error_payload=error_payload)
                permanent = outcomes.finish_failure(message, exc)
                _record_failure(
                    policy_state,
                    worker_config,
                    permanent=permanent,
                )
                raise
            finally:
                if worker_id is not None:
                    queue.record_worker_heartbeat(worker_id)
            _record_success(policy_state)
            idempotency.complete(
                message,
                status="succeeded",
                now_fn=time.time,
                result=result,
            )
            result_key = None
            if commits.result_store() is not None:
                result_key = _result_key(message)
            commits.commit_success(message, result=result, result_key=result_key)
            _ = outcomes.ack_success(message)
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
    policy_access = _WorkerPolicyAccess(queue)
    idempotency = _IdempotencyCoordinator(policy_access)
    commits = _CommitCoordinator(policy_access)
    outcomes = _WorkerOutcomeCoordinator(queue, worker_config)

    def decorator(fn: WrappedFn) -> Callable[..., Any]:
        async def wrapped(*args: Any, **kwargs: Any) -> Any:
            if worker_id is not None:
                queue.record_worker_heartbeat(worker_id)
            await _sleep_for_policy_async(policy_state, worker_config)
            message = await _get_message_async(queue)
            short_circuited, cached_result = idempotency.begin(
                message,
                now_fn=time.time,
                ack_duplicate=queue.ack,
            )
            if short_circuited:
                _record_success(policy_state)
                return cached_result
            retryer = PersistentAsyncRetrying(key=message.id, **retry_kwargs)
            try:
                result = await retryer(fn, message.value, *args, **kwargs)
            except Exception as exc:
                error_payload = _error_payload(exc)
                idempotency.complete(
                    message,
                    status="failed",
                    now_fn=time.time,
                    error_payload=error_payload,
                )
                commits.compensate_failure(message, error_payload=error_payload)
                permanent = outcomes.finish_failure(message, exc)
                _record_failure(
                    policy_state,
                    worker_config,
                    permanent=permanent,
                )
                raise
            finally:
                if worker_id is not None:
                    queue.record_worker_heartbeat(worker_id)
            _record_success(policy_state)
            idempotency.complete(
                message,
                status="succeeded",
                now_fn=time.time,
                result=result,
            )
            result_key = None
            if commits.result_store() is not None:
                result_key = _result_key(message)
            commits.commit_success(message, result=result, result_key=result_key)
            _ = outcomes.ack_success(message)
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
