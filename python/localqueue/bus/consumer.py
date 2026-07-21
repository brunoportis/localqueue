"""Loop de consumo de subscriptions do barramento."""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import TYPE_CHECKING, Any, Optional
from uuid import UUID

from pydantic import ValidationError

from localqueue.bus.bus import WILDCARD
from localqueue.exceptions import Empty
from localqueue.job import Job

if TYPE_CHECKING:
    from localqueue.bus.bus import EventBus

log = logging.getLogger(__name__)


async def run_consumer(
    bus: "EventBus", subscription: str, *, idle_timeout: Optional[float] = None
) -> None:
    """Consome a fila de ``subscription`` até cancelamento (ou idle).

    O loop usa ``get`` com timeout curto para responder rápido a
    cancelamento; ``CancelledError`` fecha a fila no ``finally`` e propaga.
    """
    queue = bus._open_subscription_queue(subscription)
    try:
        idle_since: Optional[float] = None
        while True:
            try:
                job = queue.get(block=True, timeout=0.1)
            except Empty:
                # Cede o loop de eventos para permitir cancelamento.
                await asyncio.sleep(0)
                if idle_timeout is not None:
                    now = asyncio.get_running_loop().time()
                    idle_since = idle_since if idle_since is not None else now
                    if now - idle_since >= idle_timeout:
                        return
                continue
            idle_since = None
            await _process_delivery(bus, subscription, queue, job)
    finally:
        queue.close()


async def _process_delivery(
    bus: "EventBus", subscription: str, queue: Any, job: Job
) -> None:
    envelope = job.data
    event_type = envelope.get("event_type")

    cls = bus.registry.resolve(event_type) if event_type else None
    if cls is None:
        # Erro permanente: tipo desconhecido não adianta retentar.
        queue.fail(job, f"evento desconhecido: {event_type!r}")
        return

    try:
        event = cls(
            event_id=UUID(envelope["event_id"]),
            event_created_at=envelope["event_created_at"],
            **envelope["payload"],
        )
    except (ValidationError, KeyError, TypeError, ValueError) as error:
        # Erro permanente: payload inválido não vai validar numa retentativa.
        queue.fail(job, f"payload inválido para {event_type!r}: {error}")
        return

    registration = bus._handlers.get(
        (subscription, event_type)
    ) or bus._handlers.get((subscription, WILDCARD))
    if registration is None:
        queue.fail(
            job,
            f"nenhum handler registrado para {event_type!r} "
            f"em {subscription!r} neste processo",
        )
        return

    try:
        result = registration.handler(event)
        if inspect.isawaitable(result):
            await result
    except registration.permanent_errors as error:
        queue.fail(job, f"erro permanente: {error}")
    except Exception as error:  # noqa: BLE001 - erro transitório: retenta
        queue.nack(job, last_error=str(error))
    else:
        queue.ack(job)
