"""Persistent event bus built on localqueue."""

from __future__ import annotations

import asyncio
import inspect
import math
from collections.abc import AsyncIterable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Awaitable,
    Callable,
    Generic,
    Optional,
    Protocol,
    TypeGuard,
    TypeVar,
    cast,
    overload,
)
from uuid import UUID

from localqueue import localqueue as _native
from localqueue.bus.context import ContextFactory, ContextT, HandlerContext
from localqueue.bus.event import BaseEvent, event_type_of
from localqueue.bus.identity import business_payload, prepare_event_persistence
from localqueue.bus.ingestion import (
    IngestionCheckpoint,
    IngestionResult,
    run_ingestion,
    run_resumable_ingestion,
)
from localqueue.bus.registry import EVENT_REGISTRY, EventRegistry
from localqueue.bus.retry import RetryPolicy
from localqueue.bus.source_definition import SourceConfig, SourceDefinition
from localqueue.bus.sources import ResumableSource
from localqueue.bus.subscription import Subscription
from localqueue.bus.topology import (
    WILDCARD,
    BusTopology,
    EventPattern,
    normalize_event_pattern,
    validate_name,
)
from localqueue.core import JsonSerializer, Serializer, SimpleQueue
from localqueue.policies import DeliveryPolicy, DurabilityMode, _durability_fsync

_EventT = TypeVar("_EventT", bound=BaseEvent)
EventT = TypeVar("EventT", bound=BaseEvent)
ItemT = TypeVar("ItemT")
HandlerResult = BaseEvent | None
HandlerReturn = HandlerResult | Awaitable[HandlerResult]
_HandlerResultT = TypeVar("_HandlerResultT", bound=HandlerReturn)
_StoredEventHandler = Callable[..., object]
_AsyncStoredEventHandler = Callable[..., Awaitable[object]]


class _EventBusSerializer(Protocol):
    """Directional serializer contract for the EventBus envelope boundary."""

    def dumps(self, obj: dict[str, object], /) -> bytes: ...

    def loads(self, data: bytes, /) -> object: ...


class _EventHandlerDecorator(Protocol[_EventT, ContextT]):
    @overload
    def __call__(
        self,
        handler: Callable[[_EventT], _HandlerResultT],
        /,
    ) -> Callable[[_EventT], _HandlerResultT]: ...

    @overload
    def __call__(
        self,
        handler: Callable[[_EventT, ContextT], _HandlerResultT],
        /,
    ) -> Callable[[_EventT, ContextT], _HandlerResultT]: ...


class NoSubscribers(Exception):
    """Raised by ``dispatch`` when the topology has no matching route."""


@dataclass(frozen=True)
class DispatchReceipt:
    """Receipt for a dispatch that has already committed to the database."""

    event_id: UUID
    event_type: str
    subscriptions: tuple[str, ...]
    message_ids: tuple[int, ...]
    inserted: tuple[bool, ...]

    @property
    def inserted_subscriptions(self) -> tuple[str, ...]:
        return tuple(
            subscription
            for subscription, inserted in zip(self.subscriptions, self.inserted)
            if inserted
        )

    @property
    def deduplicated_subscriptions(self) -> tuple[str, ...]:
        return tuple(
            subscription
            for subscription, inserted in zip(self.subscriptions, self.inserted)
            if not inserted
        )

    @property
    def inserted_count(self) -> int:
        return sum(self.inserted)

    @property
    def deduplicated_count(self) -> int:
        return len(self.inserted) - self.inserted_count


@dataclass(frozen=True)
class _HandlerRegistration:
    handler: _StoredEventHandler
    permanent_errors: tuple[type[BaseException], ...]
    timeout: float | None
    handler_name: str
    accepts_context: bool


def _is_async_callable(
    handler: _StoredEventHandler,
) -> TypeGuard[_AsyncStoredEventHandler]:
    """Return whether ``handler`` can be invoked as an async callable."""
    return inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(
        getattr(handler, "__call__", None)
    )


def _accepts_context(handler: _StoredEventHandler) -> bool:
    """Return whether a handler explicitly requires a context argument."""
    try:
        signature = inspect.signature(handler)
    except (TypeError, ValueError):
        return False
    marker = object()
    if _can_bind(signature, marker):
        return False
    if _can_bind(signature, marker, marker):
        return True
    raise TypeError("handler must accept either (event) or (event, context)")


def _can_bind(signature: inspect.Signature, *args: object) -> bool:
    """Return whether a handler signature can be called with ``args``."""
    try:
        signature.bind(*args)
    except TypeError:
        return False
    return True


def _validate_concurrency(concurrency: object) -> int:
    """Validate and narrow a process-local subscription concurrency bound."""
    if not isinstance(concurrency, int) or isinstance(concurrency, bool):
        raise TypeError("'concurrency' must be a positive integer")
    if concurrency <= 0:
        raise ValueError("'concurrency' must be a positive integer")
    return concurrency


def _validate_timeout(timeout: object) -> float | None:
    """Validate and normalize an optional handler timeout."""
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise TypeError("'timeout' must be a positive number or None")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("'timeout' must be a positive finite number")
    return float(timeout)


class EventBus(Generic[ContextT]):
    """Atomically fan events out to durable subscriptions.

    Each subscription is an internal ``__bus__:{bus}:{subscription}`` queue in
    the same ``localqueue.db``. Workers in multiple processes compete for the
    same queue as a consumer group.
    """

    def __init__(  # noqa: PLR0913 - additive public EventBus configuration
        self,
        path: str,
        name: str = "default",
        *,
        topology: BusTopology | None = None,
        concurrency: int = 1,
        delivery: DeliveryPolicy = DeliveryPolicy(),
        durability: DurabilityMode = DurabilityMode.RELAXED,
        require_subscribers: bool = True,
        serializer: Optional[_EventBusSerializer] = None,
        registry: EventRegistry = EVENT_REGISTRY,
        context_factory: ContextFactory[ContextT] | None = None,
    ) -> None:
        """Initialize an EventBus with routing and shared policies.

        :param path: directory where the SQLite database is stored.
        :param name: logical bus name.
        :param topology: optional initial routing snapshot for dispatched events.
        :param concurrency: default process-local concurrency per subscription.
        :param delivery: lease duration and retry policy for every delivery.
        :param durability: durability intent for fanout and subscription queues.
        :param require_subscribers: reject dispatches with no matching route.
        :param serializer: optional event-envelope serialization strategy.
        :param registry: event reconstruction strategy.
        :param context_factory: optional factory called for every delivery attempt.
        """
        self._validate_name(name, "name")
        if topology is not None and not isinstance(topology, BusTopology):
            raise TypeError("'topology' must be a BusTopology or None")
        if not isinstance(delivery, DeliveryPolicy):
            raise TypeError("'delivery' must be a DeliveryPolicy")
        validated_concurrency = _validate_concurrency(concurrency)
        fsync = _durability_fsync(durability)

        self.path = Path(path)
        self.name = name
        self.topology = topology if topology is not None else BusTopology({})
        self._default_concurrency = validated_concurrency
        self.delivery = delivery
        self.durability = durability
        self.require_subscribers = require_subscribers
        self.serializer = serializer
        self.registry = registry
        self.context_factory: ContextFactory[ContextT] | None = context_factory

        self.path.mkdir(parents=True, exist_ok=True)
        db_path = self.path / "localqueue.db"
        # This NativeQueue only owns atomic dispatch fan-out. Subscription
        # SimpleQueue instances share the same persistent database.
        self._native_queue: Optional[_native.NativeQueue] = _native.NativeQueue(
            str(db_path),
            f"__bus__:{name}",
            max_attempts=delivery.max_retries + 1,
            fsync=fsync,
        )

        self._handlers: dict[tuple[str, str], _HandlerRegistration] = {}
        self._subscription_concurrency: dict[str, int] = {}
        self._subscription_retry: dict[str, RetryPolicy] = {}
        self._frozen_subscriptions: set[str] = set()
        self._running_subscriptions: set[str] = set()
        self._run_active = False

    @staticmethod
    def _validate_name(value: str, field: str) -> None:
        validate_name(value, field)

    @property
    def concurrency(self) -> int:
        """Return the immutable process-local default per subscription."""
        return self._default_concurrency

    def _queue_name(self, subscription: str) -> str:
        return f"__bus__:{self.name}:{subscription}"

    def _pattern_key(self, pattern: EventPattern) -> str:
        try:
            return normalize_event_pattern(pattern)
        except (TypeError, ValueError) as error:
            raise type(error)(
                "'pattern' must be a BaseEvent subclass, a non-empty event type, or '*'"
            ) from error

    @overload
    def handler(
        self,
        event: type[_EventT],
        handler: None = None,
        *,
        subscription: str | None = None,
        concurrency: int | None = None,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> _EventHandlerDecorator[_EventT, ContextT]: ...

    @overload
    def handler(
        self,
        event: type[_EventT],
        handler: Callable[[_EventT], _HandlerResultT],
        *,
        subscription: str | None = None,
        concurrency: int | None = None,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[_EventT], _HandlerResultT]: ...

    @overload
    def handler(
        self,
        event: type[_EventT],
        handler: Callable[[_EventT, ContextT], _HandlerResultT],
        *,
        subscription: str | None = None,
        concurrency: int | None = None,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[_EventT, ContextT], _HandlerResultT]: ...

    def handler(
        self,
        event: object,
        handler: object = None,
        *,
        subscription: str | None = None,
        concurrency: int | None = None,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> object:
        """Declare a durable event route and register its local handler."""
        if not (isinstance(event, type) and issubclass(event, BaseEvent)):
            raise TypeError(
                "EventBus.handler requires a subclass of BaseEvent; "
                "use bus.on for string patterns"
            )
        event_class = cast(type[BaseEvent], event)
        event_type = event_type_of(event_class)
        if "*" in event_type:
            raise ValueError(
                "EventBus.handler does not support wildcard event types; "
                "use bus.on for wildcard patterns"
            )
        resolved_subscription = event_type if subscription is None else subscription
        try:
            validate_name(resolved_subscription, "subscription")
        except ValueError as error:
            if subscription is None:
                raise ValueError(
                    f"default subscription {resolved_subscription!r} is invalid; "
                    "provide subscription='contact-requested'"
                ) from error
            raise
        if concurrency is not None:
            _validate_concurrency(concurrency)
        if retry is not None and not isinstance(retry, RetryPolicy):
            raise TypeError("'retry' must be a RetryPolicy or None")
        return self._register_handler_untyped(
            resolved_subscription,
            event_class,
            handler,
            permanent_errors=permanent_errors,
            timeout=timeout,
            declare_route=True,
            concurrency=concurrency,
            retry=retry,
        )

    @overload
    def on(
        self,
        pattern: type[_EventT],
        handler: None = None,
        *,
        subscription: str,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> _EventHandlerDecorator[_EventT, ContextT]: ...

    @overload
    def on(
        self,
        pattern: type[_EventT],
        handler: Callable[[_EventT], _HandlerResultT],
        *,
        subscription: str,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[_EventT], _HandlerResultT]: ...

    @overload
    def on(
        self,
        pattern: type[_EventT],
        handler: Callable[[_EventT, ContextT], _HandlerResultT],
        *,
        subscription: str,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[_EventT, ContextT], _HandlerResultT]: ...

    @overload
    def on(
        self,
        pattern: str,
        handler: None = None,
        *,
        subscription: str,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> _EventHandlerDecorator[BaseEvent, ContextT]: ...

    @overload
    def on(
        self,
        pattern: str,
        handler: Callable[[BaseEvent], _HandlerResultT],
        *,
        subscription: str,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[BaseEvent], _HandlerResultT]: ...

    @overload
    def on(
        self,
        pattern: str,
        handler: Callable[[BaseEvent, ContextT], _HandlerResultT],
        *,
        subscription: str,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[BaseEvent, ContextT], _HandlerResultT]: ...

    def on(
        self,
        pattern: EventPattern,
        handler: object = None,
        *,
        subscription: str,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> object:
        """Register a handler through a declared subscription."""
        if retry is not None and not isinstance(retry, RetryPolicy):
            raise TypeError("'retry' must be a RetryPolicy or None")
        self.subscription(subscription)
        return self._register_handler(
            subscription,
            pattern,
            handler,
            permanent_errors=permanent_errors,
            timeout=timeout,
            retry=retry,
        )

    def subscription(
        self, name: str, *, concurrency: int | None = None
    ) -> Subscription[ContextT]:
        """Return a local handler binder for a declared subscription."""
        if not self.topology.has_subscription(name):
            raise ValueError(
                f"subscription {name!r} is not declared in the bus topology"
            )
        if concurrency is not None:
            validated_concurrency = _validate_concurrency(concurrency)
            if name in self._frozen_subscriptions:
                raise RuntimeError(
                    f"subscription {name!r} concurrency must be configured before run"
                )
            configured = self._subscription_concurrency.get(name)
            if configured is not None and configured != validated_concurrency:
                raise ValueError(
                    f"subscription {name!r} is already configured with "
                    f"concurrency={configured}"
                )
            self._subscription_concurrency[name] = validated_concurrency
        return Subscription(self, name)

    def _concurrency_for(self, subscription: str) -> int:
        """Return this process's configured bound for ``subscription``."""
        return self._subscription_concurrency.get(
            subscription, self._default_concurrency
        )

    def _retry_for(self, subscription: str) -> RetryPolicy | None:
        """Return the explicit retry policy for ``subscription``, if any."""
        return self._subscription_retry.get(subscription)

    def _ensure_retry_compatible(
        self, subscription: str, retry: RetryPolicy | None
    ) -> None:
        """Reject a process-local subscription policy conflict."""
        configured = self._subscription_retry.get(subscription)
        if retry is not None and configured is not None and configured != retry:
            raise ValueError(
                f"subscription {subscription!r} is already configured with "
                "a conflicting retry policy"
            )

    def _begin_consuming(self, subscription: str) -> None:
        """Freeze configuration and claim the local runner for a subscription."""
        if subscription in self._running_subscriptions:
            raise RuntimeError(f"subscription {subscription!r} is already running")
        self._frozen_subscriptions.add(subscription)
        self._running_subscriptions.add(subscription)

    def _end_consuming(self, subscription: str) -> None:
        """Release the local runner while retaining frozen configuration."""
        self._running_subscriptions.discard(subscription)

    def _ensure_handler_registration_open(self, subscription: str) -> None:
        """Reject handler changes after the relevant consumption lifecycle starts."""
        if self._run_active:
            raise RuntimeError("handlers must be registered before EventBus.run starts")
        if subscription in self._frozen_subscriptions:
            raise RuntimeError(
                f"subscription {subscription!r} handlers must be registered before run"
            )

    @overload
    def _register_handler(
        self,
        subscription: str,
        pattern: type[_EventT],
        handler: None = None,
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
        retry: RetryPolicy | None = None,
    ) -> _EventHandlerDecorator[_EventT, ContextT]: ...

    @overload
    def _register_handler(
        self,
        subscription: str,
        pattern: type[_EventT],
        handler: Callable[[_EventT], _HandlerResultT],
        *,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[_EventT], _HandlerResultT]: ...

    @overload
    def _register_handler(
        self,
        subscription: str,
        pattern: type[_EventT],
        handler: Callable[[_EventT, ContextT], _HandlerResultT],
        *,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[_EventT, ContextT], _HandlerResultT]: ...

    @overload
    def _register_handler(
        self,
        subscription: str,
        pattern: str,
        handler: None = None,
        *,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> _EventHandlerDecorator[BaseEvent, ContextT]: ...

    @overload
    def _register_handler(
        self,
        subscription: str,
        pattern: str,
        handler: Callable[[BaseEvent], _HandlerResultT],
        *,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[BaseEvent], _HandlerResultT]: ...

    @overload
    def _register_handler(
        self,
        subscription: str,
        pattern: str,
        handler: Callable[[BaseEvent, ContextT], _HandlerResultT],
        *,
        retry: RetryPolicy | None = None,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[BaseEvent, ContextT], _HandlerResultT]: ...

    @overload
    def _register_handler(
        self,
        subscription: str,
        pattern: EventPattern,
        handler: object = None,
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
        retry: RetryPolicy | None = None,
    ) -> object: ...

    def _register_handler(
        self,
        subscription: str,
        pattern: EventPattern,
        handler: object = None,
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
        retry: RetryPolicy | None = None,
    ) -> object:
        """Register a process-local handler without changing bus topology."""
        return self._register_handler_untyped(
            subscription,
            pattern,
            handler,
            permanent_errors=permanent_errors,
            timeout=timeout,
            retry=retry,
        )

    def _register_handler_untyped(
        self,
        subscription: str,
        pattern: EventPattern,
        handler: object,
        *,
        permanent_errors: tuple[type[BaseException], ...],
        timeout: float | None,
        declare_route: bool = False,
        concurrency: int | None = None,
        retry: RetryPolicy | None = None,
    ) -> object:
        """Validate and atomically commit one local handler registration."""
        validate_name(subscription, "subscription")
        key = self._pattern_key(pattern)
        if not isinstance(permanent_errors, (tuple, list)) or not all(
            isinstance(exc, type) and issubclass(exc, BaseException)
            for exc in permanent_errors
        ):
            raise TypeError(
                "'permanent_errors' must be a tuple or list of exception classes"
            )
        validated_timeout = _validate_timeout(timeout)
        validated_concurrency = (
            _validate_concurrency(concurrency) if concurrency is not None else None
        )
        if retry is not None and not isinstance(retry, RetryPolicy):
            raise TypeError("'retry' must be a RetryPolicy or None")

        def decorator(fn: object) -> object:
            self._ensure_handler_registration_open(subscription)
            if not callable(fn):
                raise TypeError("'handler' must be callable")
            # The registry is heterogeneous. The pattern key retains the
            # EventT relationship validated by the public overloads, so erase
            # that parameter type exactly once when storing the callable.
            stored_handler = cast(_StoredEventHandler, fn)
            if validated_timeout is not None and not _is_async_callable(stored_handler):
                raise TypeError("'timeout' is only supported for async handlers")
            accepts_context = _accepts_context(stored_handler)
            combo = (subscription, key)
            if combo in self._handlers:
                raise ValueError(
                    f"handler already registered for ({subscription!r}, {key!r})"
                )
            self._ensure_retry_compatible(subscription, retry)
            configured = self._subscription_concurrency.get(subscription)
            if (
                validated_concurrency is not None
                and configured is not None
                and configured != validated_concurrency
            ):
                raise ValueError(
                    f"subscription {subscription!r} is already configured with "
                    f"concurrency={configured}"
                )
            if declare_route:
                new_topology = self.topology._with_route(subscription, pattern)
            else:
                if not self.topology.has_subscription(subscription):
                    raise ValueError(
                        f"subscription {subscription!r} is not declared in the "
                        "bus topology"
                    )
                if key != WILDCARD and not self.topology.routes(subscription, key):
                    raise ValueError(
                        f"subscription {subscription!r} does not route event type "
                        f"{key!r}"
                    )
                new_topology = self.topology
            registration = _HandlerRegistration(
                handler=stored_handler,
                permanent_errors=tuple(permanent_errors),
                timeout=validated_timeout,
                handler_name=getattr(fn, "__name__", type(fn).__name__),
                accepts_context=accepts_context,
            )
            if isinstance(pattern, type) and issubclass(pattern, BaseEvent):
                self.registry.register(pattern)
            self.topology = new_topology
            if validated_concurrency is not None:
                self._subscription_concurrency[subscription] = validated_concurrency
            if retry is not None:
                self._subscription_retry[subscription] = retry
            self._handlers[combo] = registration
            return fn

        if handler is None:
            return decorator
        return decorator(handler)

    def register(self, cls: type[_EventT]) -> type[_EventT]:
        """Register an event class without attaching a handler."""
        return self.registry.register(cls)

    def _subscriptions_for(self, event_type: str) -> tuple[str, ...]:
        return self.topology.subscriptions_for(event_type)

    def _get_native(self) -> "_native.NativeQueue":
        native = self._native_queue
        if native is None:
            raise RuntimeError("event bus is closed")
        return native

    def serialize_envelope(self, event: BaseEvent) -> bytes:
        """Serialize the persistent envelope once per dispatch."""
        return self._serialize_envelope(event, business_payload(event))

    def _serialize_envelope(
        self, event: BaseEvent, payload: dict[str, object]
    ) -> bytes:
        """Serialize an envelope from an already prepared business payload."""
        envelope: dict[str, object] = {
            "event_id": str(event.event_id),
            "correlation_id": str(event.correlation_id),
            "causation_id": (
                None if event.causation_id is None else str(event.causation_id)
            ),
            "event_type": event.event_type,
            "event_schema": event.event_schema,
            "event_created_at": event.event_created_at.isoformat(),
            "payload": payload,
        }
        serializer = self.serializer or JsonSerializer[object]()
        return serializer.dumps(envelope)

    def dispatch(self, event: BaseEvent) -> DispatchReceipt:
        """Publish an event to every matching subscription.

        The event is serialized once and passed through one native call and
        one transaction. This method returns only after commit.
        """
        if not isinstance(event, BaseEvent):
            raise TypeError("'event' must be a BaseEvent instance")

        # Ensure consumers registered only by wildcard or string can rebuild
        # the typed event.
        self.registry.register(type(event))

        subscriptions = self._subscriptions_for(event.event_type)
        if not subscriptions:
            if self.require_subscribers:
                raise NoSubscribers(f"no subscription for {event.event_type!r}")
            return DispatchReceipt(
                event_id=event.event_id,
                event_type=event.event_type,
                subscriptions=(),
                message_ids=(),
                inserted=(),
            )

        prepared = prepare_event_persistence(event)
        identity = prepared.identity
        payload = self._serialize_envelope(event, prepared.payload)
        targets: list[tuple[str, str | None, str | None, str | None]] = [
            (
                self._queue_name(subscription),
                identity.job_id,
                identity.dedup_key,
                identity.dedup_fingerprint,
            )
            for subscription in subscriptions
        ]
        outcomes = self._get_native()._fanout_with_identity(payload, targets)
        return DispatchReceipt(
            event_id=event.event_id,
            event_type=event.event_type,
            subscriptions=subscriptions,
            message_ids=tuple(outcome[0] for outcome in outcomes),
            inserted=tuple(outcome[1] for outcome in outcomes),
        )

    async def dispatch_async(self, event: BaseEvent) -> DispatchReceipt:
        """Asynchronous variant of :meth:`dispatch`."""
        return await asyncio.to_thread(self.dispatch, event)

    @overload
    def source(
        self,
        source: ResumableSource[ItemT],
        *,
        checkpoint: str,
        batch_size: int = 1_000,
        max_pending: int | None = None,
    ) -> Callable[
        [Callable[[ItemT], EventT | Awaitable[EventT]]], SourceDefinition[ItemT, EventT]
    ]: ...

    @overload
    def source(
        self,
        source: Iterable[ItemT] | AsyncIterable[ItemT],
        *,
        checkpoint: str | None = None,
        batch_size: int = 1_000,
        max_pending: int | None = None,
    ) -> Callable[
        [Callable[[ItemT], EventT | Awaitable[EventT]]], SourceDefinition[ItemT, EventT]
    ]: ...

    def source(
        self,
        source: object,
        *,
        checkpoint: str | None = None,
        batch_size: int = 1_000,
        max_pending: int | None = None,
    ) -> object:
        """Declare a typed source that delegates execution to :meth:`ingest`."""
        config = SourceConfig(batch_size=batch_size, max_pending=max_pending)

        def define(transform: object) -> object:
            return SourceDefinition(
                bus=cast("EventBus[HandlerContext]", self),
                source=cast(
                    "Iterable[object] | AsyncIterable[object] | ResumableSource[object]",
                    source,
                ),
                transform=cast(
                    "Callable[[object], BaseEvent | Awaitable[BaseEvent]]", transform
                ),
                checkpoint=checkpoint,
                config=config,
            )

        return define

    @overload
    async def ingest(
        self,
        source: ResumableSource[EventT],
        *,
        checkpoint: str,
        transform: None = None,
        batch_size: int = 1_000,
        max_pending: int | None = None,
    ) -> IngestionResult: ...

    @overload
    async def ingest(
        self,
        source: ResumableSource[ItemT],
        *,
        checkpoint: str,
        transform: Callable[[ItemT], EventT | Awaitable[EventT]],
        batch_size: int = 1_000,
        max_pending: int | None = None,
    ) -> IngestionResult: ...

    @overload
    async def ingest(
        self,
        source: Iterable[EventT] | AsyncIterable[EventT],
        *,
        transform: None = None,
        batch_size: int = 1_000,
        max_pending: int | None = None,
    ) -> IngestionResult: ...

    @overload
    async def ingest(
        self,
        source: Iterable[ItemT] | AsyncIterable[ItemT],
        *,
        transform: Callable[[ItemT], EventT | Awaitable[EventT]],
        batch_size: int = 1_000,
        max_pending: int | None = None,
    ) -> IngestionResult: ...

    async def ingest(
        self,
        source: object,
        *,
        checkpoint: str | None = None,
        transform: Callable[[object], object] | None = None,
        batch_size: int = 1_000,
        max_pending: int | None = None,
    ) -> IngestionResult:
        """Ingest events from a generic source in atomic batches.

        The source is consumed incrementally — never materialized, never
        measured, and never read ahead of the current group. Up to
        ``batch_size`` consumed items are fanned out in one native
        transaction across all subscription queues. Ingestion is incremental
        and batch-atomic: batches 1..k stay committed when a later batch
        fails; the whole run is not all-or-nothing.

        ``transform`` (when given) runs exactly once per consumed item and
        may return a BaseEvent or an awaitable resolving to one. Synchronous
        source iteration and synchronous transforms run on the event-loop
        thread; use an async source/transform or explicitly offload blocking
        work.

        ``batch_size`` limits source items, not deliveries. Memory use and
        native transaction size also grow with payload size and subscription
        fan-out.

        ``max_pending`` is an ephemeral per-subscription-queue pending bound
        for this run only: it is not durable queue configuration, plain
        ``dispatch`` may exceed it later, backlog produced by other producers
        counts against it, and only READY/LEASED rows count (ACKED/FAILED do
        not). Temporary backpressure retries with bounded async backoff; a
        batch that can never fit is split into order-preserving halves.

        With ``checkpoint`` (a durable checkpoint name scoped to this bus),
        ``source`` must satisfy the ``ResumableSource`` protocol: each item
        is a ``SourceRecord`` carrying the cursor positioned after it, and
        every committed batch atomically persists the cursor of its last
        item in the same native transaction. A rerun inspects the checkpoint
        before opening the source, resumes from the stored cursor, and
        raises ``SourceChanged`` — before consuming any item — when the
        stored source fingerprint differs from ``source.fingerprint``. Use
        ``bus.checkpoint(name)`` to inspect or reset the stored position.

        Without ``checkpoint`` there is no resume: restarting a generic
        source re-consumes it from the beginning. Events that opt into
        durable identity are deduplicated on re-ingestion; events without
        identity are persisted as new occurrences.

        Cancellation while a native batch is in flight waits for that batch
        to settle before ``CancelledError`` is propagated. The batch may
        commit; once cancellation is observed by the caller, no commit
        remains running in the background.
        """
        if checkpoint is not None:
            return await run_resumable_ingestion(
                self,
                cast(ResumableSource[object], source),
                checkpoint=checkpoint,
                transform=transform,
                batch_size=batch_size,
                max_pending=max_pending,
            )
        return await run_ingestion(
            self,
            cast(Iterable[object] | AsyncIterable[object], source),
            transform=transform,
            batch_size=batch_size,
            max_pending=max_pending,
        )

    def checkpoint(self, name: str) -> IngestionCheckpoint[ContextT]:
        """Return a handle to inspect or reset one ingestion checkpoint.

        The checkpoint is identified by ``name`` within this bus. Resetting
        only removes the stored position; committed deliveries are kept.
        """
        if isinstance(name, bool) or not isinstance(name, str):
            raise TypeError("'name' must be a non-empty string")
        if not name:
            raise ValueError("'name' must be a non-empty string")
        return IngestionCheckpoint(self, name)

    def _open_subscription_queue(self, subscription: str) -> SimpleQueue[object]:
        # EventBus is the only producer for subscription queues and always
        # dumps a known dict envelope. SimpleQueue's invariant serializer
        # additionally describes writes that this private queue path never
        # performs, so erase that narrower input only at this handoff.
        queue_serializer = (
            cast(Serializer[object], self.serializer)
            if self.serializer is not None
            else None
        )
        return SimpleQueue[object](
            str(self.path),
            name=self._queue_name(subscription),
            delivery=self.delivery,
            durability=self.durability,
            serializer=queue_serializer,
        )

    async def run(self, *, idle_timeout: Optional[float] = None) -> None:
        """Consume every subscription registered in this process.

        Runs until cancelled. ``CancelledError`` closes the queues and
        propagates. ``idle_timeout`` stops gracefully after the queues have
        remained empty for that many seconds, which is useful in tests.
        """
        from localqueue.bus.consumer import run_consumer

        if self._run_active:
            raise RuntimeError("EventBus.run is already running")
        self._run_active = True
        try:
            subscriptions = sorted({sub for (sub, _) in self._handlers})
            consumers = [
                asyncio.create_task(
                    run_consumer(self, subscription, idle_timeout=idle_timeout)
                )
                for subscription in subscriptions
            ]
            try:
                await asyncio.gather(*consumers)
            except BaseException:
                for consumer in consumers:
                    consumer.cancel()
                await asyncio.gather(*consumers, return_exceptions=True)
                raise
        finally:
            self._run_active = False

    async def run_subscription(
        self, subscription: str, *, idle_timeout: Optional[float] = None
    ) -> None:
        """Consume only ``subscription`` with the same contract as :meth:`run`."""
        from localqueue.bus.consumer import run_consumer

        self._validate_name(subscription, "subscription")
        if not self.topology.has_subscription(subscription):
            raise ValueError(
                f"subscription {subscription!r} is not declared in the bus topology"
            )
        if not any(
            registered_subscription == subscription
            for registered_subscription, _pattern in self._handlers
        ):
            raise RuntimeError(
                f"no handler is registered for subscription {subscription!r}"
            )
        await run_consumer(self, subscription, idle_timeout=idle_timeout)

    def close(self) -> None:
        """Close the NativeQueue used for dispatch."""
        native = self._native_queue
        if native is not None:
            # Publish the closed state before tearing down the native handle.
            # In-flight ingestion retries must observe closure instead of
            # starting another SQLite attempt while close waits for the handle.
            self._native_queue = None
            native.close()
