"""Persistent event bus built on localqueue."""

from __future__ import annotations

import asyncio
import inspect
import math
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Awaitable,
    Callable,
    Optional,
    Protocol,
    TypeGuard,
    TypeVar,
    cast,
    overload,
)
from uuid import UUID

from localqueue import localqueue as _native
from localqueue.bus.event import BaseEvent
from localqueue.bus.registry import EVENT_REGISTRY, EventRegistry
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
_HandlerResultT = TypeVar("_HandlerResultT")
_StoredEventHandler = Callable[[BaseEvent], object]
_AsyncStoredEventHandler = Callable[[BaseEvent], Awaitable[object]]


class _EventHandlerDecorator(Protocol[_EventT]):
    def __call__(
        self,
        handler: Callable[[_EventT], _HandlerResultT],
        /,
    ) -> Callable[[_EventT], _HandlerResultT]: ...


class NoSubscribers(Exception):
    """Raised by ``dispatch`` when the topology has no matching route."""


@dataclass(frozen=True)
class DispatchReceipt:
    """Receipt for a dispatch that has already committed to the database."""

    event_id: UUID
    event_type: str
    subscriptions: tuple[str, ...]
    message_ids: tuple[int, ...]


@dataclass(frozen=True)
class _HandlerRegistration:
    handler: _StoredEventHandler
    permanent_errors: tuple[type[BaseException], ...]
    timeout: float | None


def _is_async_callable(
    handler: _StoredEventHandler,
) -> TypeGuard[_AsyncStoredEventHandler]:
    """Return whether ``handler`` can be invoked as an async callable."""
    return inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(
        getattr(handler, "__call__", None)
    )


class EventBus:
    """Atomically fan events out to durable subscriptions.

    Each subscription is an internal ``__bus__:{bus}:{subscription}`` queue in
    the same ``localqueue.db``. Workers in multiple processes compete for the
    same queue as a consumer group.
    """

    def __init__(
        self,
        path: str,
        name: str = "default",
        *,
        topology: BusTopology,
        delivery: DeliveryPolicy = DeliveryPolicy(),
        durability: DurabilityMode = DurabilityMode.RELAXED,
        require_subscribers: bool = True,
        serializer: Optional[Serializer[object]] = None,
        registry: EventRegistry = EVENT_REGISTRY,
    ) -> None:
        """Initialize an EventBus with explicit routing and shared policies.

        :param path: directory where the SQLite database is stored.
        :param name: logical bus name.
        :param topology: static routing strategy for dispatched events.
        :param delivery: lease duration and retry policy for every delivery.
        :param durability: durability intent for fanout and subscription queues.
        :param require_subscribers: reject dispatches with no matching route.
        :param serializer: optional event-envelope serialization strategy.
        :param registry: event reconstruction strategy.
        """
        self._validate_name(name, "name")
        if not isinstance(topology, BusTopology):
            raise TypeError("'topology' must be a BusTopology")
        if not isinstance(delivery, DeliveryPolicy):
            raise TypeError("'delivery' must be a DeliveryPolicy")
        fsync = _durability_fsync(durability)

        self.path = Path(path)
        self.name = name
        self.topology = topology
        self.delivery = delivery
        self.durability = durability
        self.require_subscribers = require_subscribers
        self.serializer = serializer
        self.registry = registry

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
        self._frozen_subscriptions: set[str] = set()
        self._running_subscriptions: set[str] = set()

    @staticmethod
    def _validate_name(value: str, field: str) -> None:
        validate_name(value, field)

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
    def on(
        self,
        pattern: type[_EventT],
        handler: None = None,
        *,
        subscription: str,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> _EventHandlerDecorator[_EventT]: ...

    @overload
    def on(
        self,
        pattern: type[_EventT],
        handler: Callable[[_EventT], _HandlerResultT],
        *,
        subscription: str,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[_EventT], _HandlerResultT]: ...

    @overload
    def on(
        self,
        pattern: str,
        handler: None = None,
        *,
        subscription: str,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> _EventHandlerDecorator[BaseEvent]: ...

    @overload
    def on(
        self,
        pattern: str,
        handler: Callable[[BaseEvent], _HandlerResultT],
        *,
        subscription: str,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[BaseEvent], _HandlerResultT]: ...

    def on(
        self,
        pattern: EventPattern,
        handler: object = None,
        *,
        subscription: str,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> object:
        """Register a handler through a declared subscription."""
        self.subscription(subscription)
        return self._register_handler(
            subscription,
            pattern,
            handler,
            permanent_errors=permanent_errors,
            timeout=timeout,
        )

    def subscription(
        self, name: str, *, concurrency: int | None = None
    ) -> Subscription:
        """Return a local handler binder for a declared subscription."""
        if not self.topology.has_subscription(name):
            raise ValueError(
                f"subscription {name!r} is not declared in the bus topology"
            )
        if concurrency is not None:
            if name in self._frozen_subscriptions:
                raise RuntimeError(
                    f"subscription {name!r} concurrency must be configured before run"
                )
            if not isinstance(concurrency, int) or isinstance(concurrency, bool):
                raise TypeError("'concurrency' must be a positive integer")
            if concurrency <= 0:
                raise ValueError("'concurrency' must be a positive integer")
            configured = self._subscription_concurrency.get(name)
            if configured is not None and configured != concurrency:
                raise ValueError(
                    f"subscription {name!r} is already configured with "
                    f"concurrency={configured}"
                )
            self._subscription_concurrency[name] = concurrency
        return Subscription(self, name)

    def _concurrency_for(self, subscription: str) -> int:
        """Return this process's configured bound for ``subscription``."""
        return self._subscription_concurrency.get(subscription, 1)

    def _begin_consuming(self, subscription: str) -> None:
        """Freeze configuration and claim the local runner for a subscription."""
        if subscription in self._running_subscriptions:
            raise RuntimeError(f"subscription {subscription!r} is already running")
        self._frozen_subscriptions.add(subscription)
        self._running_subscriptions.add(subscription)

    def _end_consuming(self, subscription: str) -> None:
        """Release the local runner while retaining frozen configuration."""
        self._running_subscriptions.discard(subscription)

    @overload
    def _register_handler(
        self,
        subscription: str,
        pattern: type[_EventT],
        handler: None = None,
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> _EventHandlerDecorator[_EventT]: ...

    @overload
    def _register_handler(
        self,
        subscription: str,
        pattern: type[_EventT],
        handler: Callable[[_EventT], _HandlerResultT],
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[_EventT], _HandlerResultT]: ...

    @overload
    def _register_handler(
        self,
        subscription: str,
        pattern: str,
        handler: None = None,
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> _EventHandlerDecorator[BaseEvent]: ...

    @overload
    def _register_handler(
        self,
        subscription: str,
        pattern: str,
        handler: Callable[[BaseEvent], _HandlerResultT],
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> Callable[[BaseEvent], _HandlerResultT]: ...

    @overload
    def _register_handler(
        self,
        subscription: str,
        pattern: EventPattern,
        handler: object = None,
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> object: ...

    def _register_handler(
        self,
        subscription: str,
        pattern: EventPattern,
        handler: object = None,
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
        timeout: float | None = None,
    ) -> object:
        """Register a process-local handler without changing bus topology."""
        if not self.topology.has_subscription(subscription):
            raise ValueError(
                f"subscription {subscription!r} is not declared in the bus topology"
            )
        key = self._pattern_key(pattern)
        if key != WILDCARD and not self.topology.routes(subscription, key):
            raise ValueError(
                f"subscription {subscription!r} does not route event type {key!r}"
            )
        if not isinstance(permanent_errors, (tuple, list)) or not all(
            isinstance(exc, type) and issubclass(exc, BaseException)
            for exc in permanent_errors
        ):
            raise TypeError(
                "'permanent_errors' must be a tuple or list of exception classes"
            )
        if timeout is not None:
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                raise TypeError("'timeout' must be a positive number or None")
            if not math.isfinite(timeout) or timeout <= 0:
                raise ValueError("'timeout' must be a positive finite number")

        def decorator(fn: object) -> object:
            if not callable(fn):
                raise TypeError("'handler' must be callable")
            # The registry is heterogeneous. The pattern key retains the
            # EventT relationship validated by the public overloads, so erase
            # that parameter type exactly once when storing the callable.
            stored_handler = cast(_StoredEventHandler, fn)
            if timeout is not None and not _is_async_callable(stored_handler):
                raise TypeError("'timeout' is only supported for async handlers")
            combo = (subscription, key)
            if combo in self._handlers:
                raise ValueError(
                    f"handler already registered for ({subscription!r}, {key!r})"
                )
            if isinstance(pattern, type) and issubclass(pattern, BaseEvent):
                self.registry.register(pattern)
            self._handlers[combo] = _HandlerRegistration(
                handler=stored_handler,
                permanent_errors=tuple(permanent_errors),
                timeout=float(timeout) if timeout is not None else None,
            )
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
        envelope = {
            "event_id": str(event.event_id),
            "correlation_id": str(event.correlation_id),
            "causation_id": (
                None if event.causation_id is None else str(event.causation_id)
            ),
            "event_type": event.event_type,
            "event_schema": event.event_schema,
            "event_created_at": event.event_created_at.isoformat(),
            "payload": event.model_dump(
                mode="json",
                exclude={
                    "event_id",
                    "correlation_id",
                    "causation_id",
                    "event_created_at",
                },
            ),
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
            )

        payload = self.serialize_envelope(event)
        targets: list[tuple[str, str | None]] = [
            (self._queue_name(subscription), str(event.event_id))
            for subscription in subscriptions
        ]
        message_ids = self._get_native().fanout(payload, targets)
        return DispatchReceipt(
            event_id=event.event_id,
            event_type=event.event_type,
            subscriptions=subscriptions,
            message_ids=tuple(message_ids),
        )

    async def dispatch_async(self, event: BaseEvent) -> DispatchReceipt:
        """Asynchronous variant of :meth:`dispatch`."""
        return await asyncio.to_thread(self.dispatch, event)

    def _open_subscription_queue(self, subscription: str) -> SimpleQueue[object]:
        return SimpleQueue[object](
            str(self.path),
            name=self._queue_name(subscription),
            delivery=self.delivery,
            durability=self.durability,
            serializer=self.serializer,
        )

    async def run(self, *, idle_timeout: Optional[float] = None) -> None:
        """Consume every subscription registered in this process.

        Runs until cancelled. ``CancelledError`` closes the queues and
        propagates. ``idle_timeout`` stops gracefully after the queues have
        remained empty for that many seconds, which is useful in tests.
        """
        from localqueue.bus.consumer import run_consumer

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
        if self._native_queue is not None:
            self._native_queue.close()
            self._native_queue = None
