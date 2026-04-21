from __future__ import annotations

import functools
import inspect
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, TypeVar, cast

from tenacity import DoAttempt, DoSleep
from tenacity import AsyncRetrying, RetryCallState, Retrying
from tenacity import _utils
from tenacity.after import after_nothing
from tenacity.asyncio import _portable_async_sleep
from tenacity.before import before_nothing
from tenacity.nap import sleep as tenacity_sleep
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from .store import AttemptStore, LMDBAttemptStore, RetryRecord, SQLiteAttemptStore

WrappedFn = TypeVar("WrappedFn", bound=Callable[..., Any])
_UNSET = object()
_default_store_local = threading.local()


def _sqlite_default_store_factory() -> AttemptStore:
    return SQLiteAttemptStore("persistence_db.sqlite3")


_default_store_factory: Callable[[], AttemptStore] = _sqlite_default_store_factory
_default_store_factory_lock = threading.Lock()


class PersistentRetryExhausted(RuntimeError):
    key: str
    attempts: int

    def __init__(self, key: str, attempts: int) -> None:
        super().__init__(
            f"retry budget already exhausted for key={key!r} after {attempts} attempts"
        )
        self.key = key
        self.attempts = attempts


@dataclass(slots=True)
class PersistentCallContext:
    key: str
    record: RetryRecord
    starting_attempts: int


class PersistentRetryState(RetryCallState):
    _retry_state: RetryCallState
    _record: RetryRecord
    _starting_attempts: int

    def __init__(  # pyright: ignore[reportMissingSuperCall]
        self, retry_state: RetryCallState, record: RetryRecord, starting_attempts: int
    ) -> None:
        self._retry_state = retry_state
        self._record = record
        self._starting_attempts = starting_attempts

    def __getattr__(self, name: str) -> Any:
        return getattr(self._retry_state, name)

    @property
    def attempt_number(self) -> int:  # pyright: ignore[reportIncompatibleVariableOverride]
        return self._starting_attempts + self._retry_state.attempt_number

    @property
    def start_time(self) -> float:
        return self._record.first_attempt_at

    @property
    def seconds_since_start(self) -> float | None:
        if self._retry_state.outcome_timestamp is None:
            return None
        return max(time.time() - self._record.first_attempt_at, 0.0)


def _get_default_store() -> AttemptStore:
    store = getattr(_default_store_local, "store", None)
    if store is None:
        with _default_store_factory_lock:
            factory = _default_store_factory
        store = factory()
        _default_store_local.store = store
    return cast(AttemptStore, store)


def configure_default_store(store: AttemptStore | None = None) -> None:
    if store is None:
        existing_store = getattr(_default_store_local, "store", None)
        close = getattr(existing_store, "close", None)
        if callable(close):
            close()
        if existing_store is not None:
            del _default_store_local.store
        return
    _default_store_local.store = store


def configure_default_store_factory(factory: Callable[[], AttemptStore]) -> None:
    global _default_store_factory
    with _default_store_factory_lock:
        _default_store_factory = factory
    configure_default_store(None)


def _validate_store_args(
    store: AttemptStore | None, store_path: str | Path | None
) -> None:
    if store is not None and store_path is not None:
        raise ValueError("pass either store= or store_path=, not both")


def _default_key_fn(
    fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> str:
    raise ValueError("persistent retries require an explicit key= or key_fn=")


def key_from_argument(
    name: str,
) -> Callable[[Callable[..., Any], tuple[Any, ...], dict[str, Any]], str]:
    if not name:
        raise ValueError("argument name cannot be empty")

    def resolve_key(
        fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> str:
        signature = inspect.signature(fn)
        bound = signature.bind_partial(*args, **kwargs)
        if name not in bound.arguments:
            raise ValueError(
                f"could not derive a persistent retry key from argument {name!r}"
            )
        return str(bound.arguments[name])

    return resolve_key


def key_from_attr(
    argument_name: str, attribute_name: str, *, prefix: str | None = None
) -> Callable[[Callable[..., Any], tuple[Any, ...], dict[str, Any]], str]:
    if not argument_name:
        raise ValueError("argument name cannot be empty")
    if not attribute_name:
        raise ValueError("attribute name cannot be empty")
    if prefix == "":
        raise ValueError("prefix cannot be empty")

    def resolve_key(
        fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> str:
        signature = inspect.signature(fn)
        bound = signature.bind_partial(*args, **kwargs)
        if argument_name not in bound.arguments:
            raise ValueError(
                f"could not derive a persistent retry key from argument "
                f"{argument_name!r}"
            )

        value = bound.arguments[argument_name]
        if not hasattr(value, attribute_name):
            raise ValueError(
                f"could not derive a persistent retry key from "
                f"{argument_name}.{attribute_name}"
            )

        key = str(getattr(value, attribute_name))
        if prefix is None:
            return key
        return f"{prefix}:{key}"

    return resolve_key


def idempotency_key_from_id(
    argument_name: str, *, prefix: str | None = None
) -> Callable[[Callable[..., Any], tuple[Any, ...], dict[str, Any]], str]:
    return key_from_attr(argument_name, "id", prefix=prefix)


def _build_effective_tenacity_kwargs(
    base_cls: type[Any], tenacity_kwargs: dict[str, Any]
) -> dict[str, Any]:
    signature = inspect.signature(base_cls.__init__)
    effective: dict[str, Any] = {}
    remaining = dict(tenacity_kwargs)
    has_var_keyword = False

    for name, parameter in signature.parameters.items():
        if name == "self":
            continue
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            has_var_keyword = True
            continue
        if name in remaining:
            effective[name] = remaining.pop(name)
            continue
        if parameter.default is not inspect.Parameter.empty:
            effective[name] = parameter.default

    if remaining:
        if not has_var_keyword:
            unknown = ", ".join(sorted(remaining))
            raise TypeError(f"unexpected Tenacity arguments: {unknown}")
        effective.update(remaining)

    return effective


class _PersistentMixin:
    _store: AttemptStore | None
    _store_path: Path | None
    _local: threading.local
    _retrying: Retrying | AsyncRetrying
    key: str | None
    key_fn: Callable[[Callable[..., Any], tuple[Any, ...], dict[str, Any]], str]
    clear_on_success: bool
    _base_cls: type[Any]
    _tenacity_kwargs: dict[str, Any]
    _user_stop: Any
    _user_wait: Any
    _user_retry: Any
    _user_before: Any
    _user_after: Any
    _user_before_sleep: Any
    _user_retry_error_callback: Any

    def __init__(
        self,
        *,
        base_cls: type[Any],
        store: AttemptStore | None = None,
        store_path: str | Path | None = None,
        key: str | None = None,
        key_fn: Callable[[Callable[..., Any], tuple[Any, ...], dict[str, Any]], str]
        | None = None,
        clear_on_success: bool = True,
        max_tries: int | None = None,
        **tenacity_kwargs: Any,
    ) -> None:
        _validate_store_args(store, store_path)
        if max_tries is not None and "stop" in tenacity_kwargs:
            raise ValueError("pass either max_tries= or stop=, not both")
        if max_tries is not None:
            tenacity_kwargs["stop"] = stop_after_attempt(max_tries)

        effective_kwargs = _build_effective_tenacity_kwargs(base_cls, tenacity_kwargs)

        self._store = store
        self._store_path = Path(store_path) if store_path is not None else None
        self.key = key
        self.key_fn = key_fn or _default_key_fn
        self.clear_on_success = clear_on_success
        self._base_cls = base_cls
        self._tenacity_kwargs = dict(tenacity_kwargs)
        self._user_stop = effective_kwargs["stop"]
        self._user_wait = effective_kwargs["wait"]
        self._user_retry = effective_kwargs["retry"]
        self._user_before = effective_kwargs["before"]
        self._user_after = effective_kwargs["after"]
        self._user_before_sleep = effective_kwargs["before_sleep"]
        self._user_retry_error_callback = effective_kwargs["retry_error_callback"]

        forwarded_kwargs = dict(effective_kwargs)
        forwarded_kwargs["stop"] = self._wrap_stop()
        forwarded_kwargs["wait"] = self._wrap_wait()
        forwarded_kwargs["retry"] = self._wrap_retry()
        forwarded_kwargs["before"] = self._wrap_callback(self._user_before)
        forwarded_kwargs["after"] = self._wrap_callback(self._user_after)
        forwarded_kwargs["before_sleep"] = self._wrap_callback(self._user_before_sleep)
        forwarded_kwargs["retry_error_callback"] = self._wrap_retry_error_callback()

        self._local = threading.local()
        self._retrying = base_cls(**forwarded_kwargs)

    def _get_store(self) -> AttemptStore:
        if self._store is None:
            if self._store_path is not None:
                self._store = LMDBAttemptStore(self._store_path)
            else:
                self._store = _get_default_store()
        return self._store

    def _current_context(self) -> PersistentCallContext:
        context = getattr(self._local, "persistent_context", None)
        if context is None:
            raise RuntimeError("persistent retry context not initialized")
        return cast(PersistentCallContext, context)

    def _translate_state(self, retry_state: RetryCallState) -> PersistentRetryState:
        context = self._current_context()
        return PersistentRetryState(
            retry_state, context.record, context.starting_attempts
        )

    def _resolve_key(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> str:
        if self.key is not None:
            return self.key
        return self.key_fn(fn, args, kwargs)

    def _load_context(
        self, fn: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> PersistentCallContext:
        key = self._resolve_key(fn, args, kwargs)
        record = self._get_store().load(key) or RetryRecord.new()
        return PersistentCallContext(
            key=key,
            record=record,
            starting_attempts=record.attempts,
        )

    def _persist_attempt(self, retry_state: RetryCallState, *, exhausted: bool) -> None:
        context = self._current_context()
        context.record.attempts = self._translate_state(retry_state).attempt_number
        context.record.exhausted = exhausted
        self._get_store().save(context.key, context.record)

    def _clear_if_success(self, retry_state: RetryCallState) -> None:
        if not self.clear_on_success:
            return
        outcome = retry_state.outcome
        if outcome is not None and not outcome.failed:
            self._get_store().delete(self._current_context().key)

    def reset(self, key: str) -> None:
        self._get_store().delete(key)

    def get_record(self, key: str) -> RetryRecord | None:
        return self._get_store().load(key)

    def _wrap_retry(self) -> Callable[[RetryCallState], bool]:
        def wrapped_retry(retry_state: RetryCallState) -> bool:
            return bool(self._user_retry(self._translate_state(retry_state)))

        return wrapped_retry

    def _wrap_wait(self) -> Callable[[RetryCallState], float]:
        def wrapped_wait(retry_state: RetryCallState) -> float:
            if self._user_wait is None:
                return 0.0
            return float(self._user_wait(self._translate_state(retry_state)))

        return wrapped_wait

    def _wrap_stop(self) -> Callable[[RetryCallState], bool]:
        def wrapped_stop(retry_state: RetryCallState) -> bool:
            should_stop = bool(self._user_stop(self._translate_state(retry_state)))
            self._persist_attempt(retry_state, exhausted=should_stop)
            return should_stop

        return wrapped_stop

    def _wrap_callback(
        self, callback: Callable[[RetryCallState], Any] | None
    ) -> Callable[[RetryCallState], Any] | None:
        if callback is None:
            return None

        def wrapped_callback(retry_state: RetryCallState) -> Any:
            return callback(self._translate_state(retry_state))

        return wrapped_callback

    def _wrap_retry_error_callback(self) -> Callable[[RetryCallState], Any] | None:
        if self._user_retry_error_callback is None:
            return None

        def wrapped_retry_error_callback(retry_state: RetryCallState) -> Any:
            self._persist_attempt(retry_state, exhausted=True)
            return self._user_retry_error_callback(self._translate_state(retry_state))

        return wrapped_retry_error_callback

    @property
    def statistics(self) -> dict[str, Any]:
        return cast(dict[str, Any], self._retrying.statistics)

    def begin(self) -> None:
        self._retrying.begin()

    def iter(self, retry_state: RetryCallState) -> Any:
        return self._retrying.iter(retry_state=retry_state)

    def wraps(self, f: WrappedFn) -> WrappedFn:
        @functools.wraps(
            f, functools.WRAPPER_ASSIGNMENTS + ("__defaults__", "__kwdefaults__")
        )
        def wrapped_f(*args: Any, **kwargs: Any) -> Any:
            copy = self.copy()
            wrapped_f.statistics = copy.statistics  # type: ignore[attr-defined]
            return cast(Any, copy)(f, *args, **kwargs)

        def retry_with(*args: Any, **kwargs: Any) -> WrappedFn:
            return self.copy(*args, **kwargs).wraps(f)

        wrapped_f.retry = self  # type: ignore[attr-defined]
        wrapped_f.retry_with = retry_with  # type: ignore[attr-defined]
        wrapped_f.statistics = {}  # type: ignore[attr-defined]
        return cast(WrappedFn, wrapped_f)

    def copy(self, **kwargs: Any) -> "_PersistentMixin":
        store = kwargs.pop("store", self._store)
        store_path = kwargs.pop("store_path", _UNSET)
        key = kwargs.pop("key", self.key)
        key_fn = kwargs.pop("key_fn", self.key_fn)
        clear_on_success = kwargs.pop("clear_on_success", self.clear_on_success)
        max_tries = kwargs.pop("max_tries", _UNSET)
        tenacity_kwargs = dict(self._tenacity_kwargs)
        tenacity_kwargs.update(kwargs)
        if max_tries is not _UNSET:
            _ = tenacity_kwargs.pop("stop", None)

        return self.__class__(
            store=cast(AttemptStore | None, store),
            store_path=self._store_path
            if store_path is _UNSET
            else cast(str | Path | None, store_path),
            key=cast(str | None, key),
            key_fn=cast(
                Callable[[Callable[..., Any], tuple[Any, ...], dict[str, Any]], str]
                | None,
                key_fn,
            ),
            clear_on_success=cast(bool, clear_on_success),
            **({} if max_tries is _UNSET else {"max_tries": max_tries}),
            **tenacity_kwargs,
        )


class PersistentRetrying(_PersistentMixin):
    def __init__(
        self,
        *,
        store: AttemptStore | None = None,
        store_path: str | Path | None = None,
        key: str | None = None,
        key_fn: Callable[[Callable[..., Any], tuple[Any, ...], dict[str, Any]], str]
        | None = None,
        clear_on_success: bool = True,
        max_tries: int | None = None,
        **tenacity_kwargs: Any,
    ) -> None:
        tenacity_kwargs.setdefault("sleep", tenacity_sleep)
        tenacity_kwargs.setdefault(
            "wait", wait_exponential(multiplier=1, min=2, max=10)
        )
        tenacity_kwargs.setdefault("retry", retry_if_exception_type())
        tenacity_kwargs.setdefault("before", before_nothing)
        tenacity_kwargs.setdefault("after", after_nothing)
        tenacity_kwargs.setdefault("reraise", True)
        super().__init__(
            base_cls=Retrying,
            store=store,
            store_path=store_path,
            key=key,
            key_fn=key_fn,
            clear_on_success=clear_on_success,
            max_tries=max_tries,
            **tenacity_kwargs,
        )

    def __call__(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        self.begin()
        context = self._load_context(fn, args, kwargs)
        if context.record.exhausted:
            raise PersistentRetryExhausted(context.key, context.record.attempts)

        self._local.persistent_context = context
        try:
            retry_state = RetryCallState(
                retry_object=cast(Any, self._retrying), fn=fn, args=args, kwargs=kwargs
            )
            while True:
                do = self.iter(retry_state=retry_state)
                if isinstance(do, DoAttempt):
                    try:
                        result = fn(*args, **kwargs)
                    except BaseException:  # noqa: B902
                        exc_info = sys.exc_info()
                        if exc_info[0] is None:
                            raise
                        retry_state.set_exception(
                            cast(
                                "tuple[type[BaseException], BaseException, Any]",
                                exc_info,
                            )
                        )
                    else:
                        retry_state.set_result(result)
                elif isinstance(do, DoSleep):
                    retry_state.prepare_for_next_attempt()
                    cast(Any, self._retrying).sleep(do)
                else:
                    self._clear_if_success(retry_state)
                    return do
        finally:
            if hasattr(self._local, "persistent_context"):
                del self._local.persistent_context


class PersistentAsyncRetrying(_PersistentMixin):
    def __init__(
        self,
        *,
        store: AttemptStore | None = None,
        store_path: str | Path | None = None,
        key: str | None = None,
        key_fn: Callable[[Callable[..., Any], tuple[Any, ...], dict[str, Any]], str]
        | None = None,
        clear_on_success: bool = True,
        max_tries: int | None = None,
        **tenacity_kwargs: Any,
    ) -> None:
        tenacity_kwargs.setdefault("sleep", _portable_async_sleep)
        tenacity_kwargs.setdefault(
            "wait", wait_exponential(multiplier=1, min=2, max=10)
        )
        tenacity_kwargs.setdefault("retry", retry_if_exception_type())
        tenacity_kwargs.setdefault("before", before_nothing)
        tenacity_kwargs.setdefault("after", after_nothing)
        tenacity_kwargs.setdefault("reraise", True)
        super().__init__(
            base_cls=AsyncRetrying,
            store=store,
            store_path=store_path,
            key=key,
            key_fn=key_fn,
            clear_on_success=clear_on_success,
            max_tries=max_tries,
            **tenacity_kwargs,
        )

    def _wrap_retry(self) -> Callable[[RetryCallState], Any]:
        if _utils.is_coroutine_callable(self._user_retry):

            async def async_wrapped_retry(retry_state: RetryCallState) -> bool:
                return bool(await self._user_retry(self._translate_state(retry_state)))

            return async_wrapped_retry

        def wrapped_retry(retry_state: RetryCallState) -> bool:
            return bool(self._user_retry(self._translate_state(retry_state)))

        return wrapped_retry

    def _wrap_wait(self) -> Callable[[RetryCallState], Any]:
        if self._user_wait is None:

            async def empty_wait(retry_state: RetryCallState) -> float:
                return 0.0

            return empty_wait

        if _utils.is_coroutine_callable(self._user_wait):

            async def async_wrapped_wait(retry_state: RetryCallState) -> float:
                return float(await self._user_wait(self._translate_state(retry_state)))

            return async_wrapped_wait

        def wrapped_wait(retry_state: RetryCallState) -> float:
            return float(self._user_wait(self._translate_state(retry_state)))

        return wrapped_wait

    def _wrap_stop(self) -> Callable[[RetryCallState], Any]:
        if _utils.is_coroutine_callable(self._user_stop):

            async def async_wrapped_stop(retry_state: RetryCallState) -> bool:
                should_stop = bool(
                    await self._user_stop(self._translate_state(retry_state))
                )
                self._persist_attempt(retry_state, exhausted=should_stop)
                return should_stop

            return async_wrapped_stop

        def wrapped_stop(retry_state: RetryCallState) -> bool:
            should_stop = bool(self._user_stop(self._translate_state(retry_state)))
            self._persist_attempt(retry_state, exhausted=should_stop)
            return should_stop

        return wrapped_stop

    def _wrap_callback(
        self, callback: Callable[[RetryCallState], Any] | None
    ) -> Callable[[RetryCallState], Any] | None:
        if callback is None:
            return None
        if _utils.is_coroutine_callable(callback):

            async def async_wrapped_callback(retry_state: RetryCallState) -> Any:
                return await callback(self._translate_state(retry_state))

            return async_wrapped_callback

        def wrapped_callback(retry_state: RetryCallState) -> Any:
            return callback(self._translate_state(retry_state))

        return wrapped_callback

    def _wrap_retry_error_callback(self) -> Callable[[RetryCallState], Any] | None:
        if self._user_retry_error_callback is None:
            return None
        if _utils.is_coroutine_callable(self._user_retry_error_callback):

            async def async_wrapped_retry_error_callback(
                retry_state: RetryCallState,
            ) -> Any:
                self._persist_attempt(retry_state, exhausted=True)
                return await self._user_retry_error_callback(
                    self._translate_state(retry_state)
                )

            return async_wrapped_retry_error_callback

        def wrapped_retry_error_callback(retry_state: RetryCallState) -> Any:
            self._persist_attempt(retry_state, exhausted=True)
            return self._user_retry_error_callback(self._translate_state(retry_state))

        return wrapped_retry_error_callback

    async def __call__(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        self.begin()
        context = self._load_context(fn, args, kwargs)
        if context.record.exhausted:
            raise PersistentRetryExhausted(context.key, context.record.attempts)

        self._local.persistent_context = context
        try:
            retry_state = RetryCallState(
                retry_object=cast(Any, self._retrying), fn=fn, args=args, kwargs=kwargs
            )
            while True:
                do = await cast(AsyncRetrying, self._retrying).iter(
                    retry_state=retry_state
                )
                if isinstance(do, DoAttempt):
                    try:
                        result = fn(*args, **kwargs)
                        if inspect.isawaitable(result):
                            result = await result
                    except BaseException:  # noqa: B902
                        exc_info = sys.exc_info()
                        if exc_info[0] is None:
                            raise
                        retry_state.set_exception(
                            cast(
                                "tuple[type[BaseException], BaseException, Any]",
                                exc_info,
                            )
                        )
                    else:
                        retry_state.set_result(result)
                elif isinstance(do, DoSleep):
                    retry_state.prepare_for_next_attempt()
                    await cast(Any, self._retrying).sleep(do)
                else:
                    self._clear_if_success(retry_state)
                    return do
        finally:
            if hasattr(self._local, "persistent_context"):
                del self._local.persistent_context


def persistent_retry(**kwargs: Any) -> Callable[[WrappedFn], WrappedFn]:
    return cast(Callable[[WrappedFn], WrappedFn], PersistentRetrying(**kwargs).wraps)


def persistent_async_retry(**kwargs: Any) -> Callable[[WrappedFn], WrappedFn]:
    return cast(
        Callable[[WrappedFn], WrappedFn], PersistentAsyncRetrying(**kwargs).wraps
    )
