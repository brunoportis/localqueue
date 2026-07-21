"""Barramento de eventos persistente sobre o localqueue."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional, Union
from uuid import UUID

from localqueue import localqueue as _native
from localqueue.bus.event import BaseEvent, event_type_of
from localqueue.bus.registry import EVENT_REGISTRY, EventRegistry
from localqueue.core import JsonSerializer, Serializer, SimpleQueue

_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")

WILDCARD = "*"


class NoSubscribers(Exception):
    """Levantada por ``dispatch`` quando nenhum handler corresponde ao evento."""


@dataclass(frozen=True)
class DispatchReceipt:
    """Resultado de um dispatch já commitado no banco."""

    event_id: UUID
    event_type: str
    subscriptions: tuple[str, ...]
    message_ids: tuple[int, ...]


@dataclass(frozen=True)
class _HandlerRegistration:
    handler: Callable[[Any], Any]
    permanent_errors: tuple[type[BaseException], ...]


class EventBus:
    """Fan-out atômico de eventos para subscriptions persistidas.

    Cada subscription é uma fila interna ``__bus__:{bus}:{subscription}`` no
    mesmo ``localqueue.db``; workers de vários processos competem pela mesma
    fila (consumer group).
    """

    def __init__(
        self,
        path: str,
        name: str = "default",
        *,
        lease_seconds: float = 60.0,
        max_retries: int = 3,
        fsync: bool = False,
        require_subscribers: bool = True,
        serializer: Optional[Serializer] = None,
        registry: EventRegistry = EVENT_REGISTRY,
    ) -> None:
        self._validate_name(name, "name")
        if max_retries < 0:
            raise ValueError("'max_retries' deve ser não negativo")

        self.path = Path(path)
        self.name = name
        self.lease_seconds = lease_seconds
        self.max_retries = max_retries
        self.require_subscribers = require_subscribers
        self.serializer = serializer
        self.registry = registry

        self.path.mkdir(parents=True, exist_ok=True)
        db_path = self.path / "localqueue.db"
        # NativeQueue própria apenas para o fanout atômico do dispatch; a
        # persistência é a mesma das SimpleQueue das subscriptions.
        self._native_queue: Optional[_native.NativeQueue] = _native.NativeQueue(
            str(db_path),
            f"__bus__:{name}",
            max_attempts=max_retries + 1,
            fsync=fsync,
        )

        self._handlers: dict[tuple[str, str], _HandlerRegistration] = {}

    @staticmethod
    def _validate_name(value: str, field: str) -> None:
        if not isinstance(value, str) or not _NAME_RE.match(value):
            raise ValueError(
                f"'{field}' inválido: use { _NAME_RE.pattern } "
                "(sem ':' e não vazio)"
            )

    def _queue_name(self, subscription: str) -> str:
        return f"__bus__:{self.name}:{subscription}"

    def _pattern_key(self, pattern: Union[type[BaseEvent], str]) -> str:
        if isinstance(pattern, type) and issubclass(pattern, BaseEvent):
            self.registry.register(pattern)
            return event_type_of(pattern)
        if isinstance(pattern, str) and pattern.strip():
            return pattern
        raise TypeError(
            "'pattern' deve ser uma subclasse de BaseEvent ou string não vazia"
        )

    def on(
        self,
        pattern: Union[type[BaseEvent], str],
        handler: Optional[Callable[[Any], Any]] = None,
        *,
        subscription: str,
        permanent_errors: tuple[type[BaseException], ...] = (),
    ) -> Callable[[Any], Any]:
        """Registra ``handler`` para ``pattern`` em ``subscription``.

        Serve como chamada direta ou decorator. ``pattern`` pode ser uma
        classe :class:`BaseEvent`, uma string de ``event_type`` ou ``"*"``.
        Handler exato vence wildcard dentro da mesma subscription.
        """
        self._validate_name(subscription, "subscription")
        key = self._pattern_key(pattern)

        def decorator(fn: Callable[[Any], Any]) -> Callable[[Any], Any]:
            combo = (subscription, key)
            if combo in self._handlers:
                raise ValueError(
                    f"handler já registrado para ({subscription!r}, {key!r})"
                )
            self._handlers[combo] = _HandlerRegistration(
                handler=fn, permanent_errors=tuple(permanent_errors)
            )
            return fn

        if handler is None:
            return decorator
        return decorator(handler)

    def register(self, cls: type[BaseEvent]) -> type[BaseEvent]:
        """Registra uma classe de evento no registry (sem handler)."""
        return self.registry.register(cls)

    def _subscriptions_for(self, event_type: str) -> tuple[str, ...]:
        subscriptions = {
            subscription
            for (subscription, key) in self._handlers
            if key == event_type or key == WILDCARD
        }
        return tuple(sorted(subscriptions))

    def _get_native(self) -> "_native.NativeQueue":
        native = self._native_queue
        if native is None:
            raise RuntimeError("barramento fechado")
        return native

    def serialize_envelope(self, event: BaseEvent) -> bytes:
        """Serializa o envelope persistido (uma única vez por dispatch)."""
        envelope = {
            "event_id": str(event.event_id),
            "event_type": event.event_type,
            "event_schema": event.event_schema,
            "event_created_at": event.event_created_at.isoformat(),
            "payload": event.model_dump(
                mode="json", exclude={"event_id", "event_created_at"}
            ),
        }
        serializer = self.serializer or JsonSerializer()
        return serializer.dumps(envelope)

    def dispatch(self, event: BaseEvent) -> DispatchReceipt:
        """Publica o evento em todas as subscriptions interessadas.

        Uma única serialização e uma única chamada nativa (uma transação);
        só retorna após o commit.
        """
        if not isinstance(event, BaseEvent):
            raise TypeError("'event' deve ser uma instância de BaseEvent")

        subscriptions = self._subscriptions_for(event.event_type)
        if not subscriptions:
            if self.require_subscribers:
                raise NoSubscribers(
                    f"nenhuma subscription para {event.event_type!r}"
                )
            return DispatchReceipt(
                event_id=event.event_id,
                event_type=event.event_type,
                subscriptions=(),
                message_ids=(),
            )

        payload = self.serialize_envelope(event)
        targets = [
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
        """Versão assíncrona de :meth:`dispatch`."""
        return await asyncio.to_thread(self.dispatch, event)

    def _open_subscription_queue(self, subscription: str) -> SimpleQueue:
        return SimpleQueue(
            str(self.path),
            name=self._queue_name(subscription),
            lease_seconds=self.lease_seconds,
            max_retries=self.max_retries,
            serializer=self.serializer,
        )

    async def run(self, *, idle_timeout: Optional[float] = None) -> None:
        """Consome todas as subscriptions registradas neste processo.

        Roda até ser cancelada (``CancelledError`` fecha as filas e propaga).
        ``idle_timeout`` (segundos de fila vazia) encerra graciosamente —
        útil em testes.
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
        """Consome apenas ``subscription`` (mesmo contrato de :meth:`run`)."""
        from localqueue.bus.consumer import run_consumer

        self._validate_name(subscription, "subscription")
        await run_consumer(self, subscription, idle_timeout=idle_timeout)

    def close(self) -> None:
        """Fecha a NativeQueue usada no dispatch."""
        if self._native_queue is not None:
            self._native_queue.close()
            self._native_queue = None
