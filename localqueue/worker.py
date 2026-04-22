from __future__ import annotations

import asyncio
from collections.abc import Callable
from queue import Empty
from typing import Any, TypeVar, cast

from .retry import PersistentAsyncRetrying, PersistentRetrying

from .failure import is_permanent_failure
from .queue import PersistentQueue
from .store import QueueMessage

WrappedFn = TypeVar("WrappedFn", bound=Callable[..., Any])
_UNSET = object()


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
        return cast(bool, dead_letter_on_failure)
    if dead_letter_on_exhaustion is not _UNSET:
        return cast(bool, dead_letter_on_exhaustion)
    return True


def _validate_release_delay(release_delay: float) -> None:
    if release_delay < 0:
        raise ValueError("release_delay must be greater than or equal to zero")


class PersistentWorkerConfig:
    dead_letter_on_failure: bool
    dead_letter_on_exhaustion: bool
    release_delay: float
    retry_kwargs: dict[str, Any]

    def __init__(
        self,
        *,
        dead_letter_on_failure: bool | object = _UNSET,
        dead_letter_on_exhaustion: bool | object = _UNSET,
        release_delay: float = 0.0,
        **retry_kwargs: Any,
    ) -> None:
        resolved = _resolve_dead_letter_on_failure(
            dead_letter_on_failure=dead_letter_on_failure,
            dead_letter_on_exhaustion=dead_letter_on_exhaustion,
        )
        _validate_release_delay(release_delay)
        self.dead_letter_on_failure = resolved
        self.dead_letter_on_exhaustion = resolved
        self.release_delay = release_delay
        self.retry_kwargs = dict(retry_kwargs)

    def with_overrides(
        self,
        *,
        dead_letter_on_failure: bool | object = _UNSET,
        dead_letter_on_exhaustion: bool | object = _UNSET,
        release_delay: float | object = _UNSET,
        **retry_kwargs: Any,
    ) -> PersistentWorkerConfig:
        merged_retry_kwargs = {**self.retry_kwargs, **retry_kwargs}
        resolved = _resolve_dead_letter_on_failure(
            dead_letter_on_failure=dead_letter_on_failure,
            dead_letter_on_exhaustion=dead_letter_on_exhaustion,
        )
        if release_delay is not _UNSET:
            _validate_release_delay(cast(float, release_delay))
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
                else cast(float, release_delay)
            ),
            **merged_retry_kwargs,
        )


def _resolve_config(
    config: PersistentWorkerConfig | None,
    *,
    dead_letter_on_failure: bool | object,
    dead_letter_on_exhaustion: bool | object,
    release_delay: float | object,
    retry_kwargs: dict[str, Any],
) -> PersistentWorkerConfig:
    base = config if config is not None else PersistentWorkerConfig()
    return base.with_overrides(
        dead_letter_on_failure=dead_letter_on_failure,
        dead_letter_on_exhaustion=dead_letter_on_exhaustion,
        release_delay=release_delay,
        **retry_kwargs,
    )


def persistent_worker(
    queue: PersistentQueue,
    *,
    config: PersistentWorkerConfig | None = None,
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
        retry_kwargs=retry_kwargs,
    )

    def decorator(fn: WrappedFn) -> Callable[..., Any]:
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            message = queue.get_message()
            retryer = PersistentRetrying(key=message.id, **worker_config.retry_kwargs)
            try:
                result = retryer(fn, message.value, *args, **kwargs)
            except Exception as exc:
                if is_permanent_failure(exc) or worker_config.dead_letter_on_failure:
                    queue.dead_letter(message, error=exc)
                else:
                    queue.release(message, delay=worker_config.release_delay, error=exc)
                raise
            queue.ack(message)
            return result

        return wrapped

    return decorator


def persistent_async_worker(
    queue: PersistentQueue,
    *,
    config: PersistentWorkerConfig | None = None,
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
        retry_kwargs=retry_kwargs,
    )

    def decorator(fn: WrappedFn) -> Callable[..., Any]:
        async def wrapped(*args: Any, **kwargs: Any) -> Any:
            message = await _get_message_async(queue)
            retryer = PersistentAsyncRetrying(
                key=message.id, **worker_config.retry_kwargs
            )
            try:
                result = await retryer(fn, message.value, *args, **kwargs)
            except Exception as exc:
                if is_permanent_failure(exc) or worker_config.dead_letter_on_failure:
                    queue.dead_letter(message, error=exc)
                else:
                    queue.release(
                        message,
                        delay=worker_config.release_delay,
                        error=exc,
                    )
                raise
            queue.ack(message)
            return result

        return wrapped

    return decorator


async def _get_message_async(queue: PersistentQueue) -> QueueMessage:
    while True:
        try:
            return queue.get_message(block=False)
        except Empty:
            await asyncio.sleep(0.05)
