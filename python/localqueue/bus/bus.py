"""Persistent event bus built on localqueue."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional
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
    handler: Callable[[Any], Any]
    permanent_errors: tuple[type[BaseException], ...]


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
        lease_seconds: float = 60.0,
        max_retries: int = 3,
        fsync: bool = False,
        require_subscribers: bool = True,
        serializer: Optional[Serializer] = None,
        registry: EventRegistry = EVENT_REGISTRY,
    ) -> None:
        self._validate_name(name, "name")
        if not isinstance(topology, BusTopology):
            raise TypeError("'topology' must be a BusTopology")
        if not lease_seconds > 0:
            raise ValueError("'lease_seconds' must be positive")
        if max_retries < 0:
            raise ValueError("'max_retries' must be non-negative")

        self.path = Path(path)
        self.name = name
        self.topology = topology
        self.lease_seconds = lease_seconds
        self.max_retries = max_retries
        self.fsync = fsync
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
            max_attempts=max_retries + 1,
            fsync=fsync,
        )

        self._handlers: dict[tuple[str, str], _HandlerRegistration] = {}
        self._subscription_concurrency: dict[str, int] = {}
        self._started_subscriptions: set[str] = set()

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

    def on(
        self,
        pattern: EventPattern,
        handler: Optional[Callable[[Any], Any]] = None,
        *,
        subscription: str,
        permanent_errors: tuple[type[BaseException], ...] = (),
    ) -> Callable[[Any], Any]:
        """Register a handler through a declared subscription."""
        return self.subscription(subscription).handler(
            pattern,
            handler,
            permanent_errors=permanent_errors,
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
            if name in self._started_subscriptions:
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
        return Subscription(
            self, name, concurrency=self._subscription_concurrency.get(name, 1)
        )

    def _concurrency_for(self, subscription: str) -> int:
        """Return this process's configured bound for ``subscription``."""
        return self._subscription_concurrency.get(subscription, 1)

    def _begin_consuming(self, subscription: str) -> None:
        """Freeze subscription concurrency before its consumer starts."""
        self._started_subscriptions.add(subscription)

    def _register_handler(
        self,
        subscription: str,
        pattern: EventPattern,
        handler: Optional[Callable[[Any], Any]] = None,
        *,
        permanent_errors: tuple[type[BaseException], ...] = (),
    ) -> Callable[[Any], Any]:
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

        def decorator(fn: Callable[[Any], Any]) -> Callable[[Any], Any]:
            if not callable(fn):
                raise TypeError("'handler' must be callable")
            combo = (subscription, key)
            if combo in self._handlers:
                raise ValueError(
                    f"handler already registered for ({subscription!r}, {key!r})"
                )
            if isinstance(pattern, type) and issubclass(pattern, BaseEvent):
                self.registry.register(pattern)
            self._handlers[combo] = _HandlerRegistration(
                handler=fn, permanent_errors=tuple(permanent_errors)
            )
            return fn

        if handler is None:
            return decorator
        return decorator(handler)

    def register(self, cls: type[BaseEvent]) -> type[BaseEvent]:
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
        serializer = self.serializer or JsonSerializer()
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

    def _open_subscription_queue(self, subscription: str) -> SimpleQueue:
        return SimpleQueue(
            str(self.path),
            name=self._queue_name(subscription),
            lease_seconds=self.lease_seconds,
            max_retries=self.max_retries,
            fsync=self.fsync,
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
        await asyncio.gather(
            *(
                run_consumer(self, subscription, idle_timeout=idle_timeout)
                for subscription in subscriptions
            )
        )

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
