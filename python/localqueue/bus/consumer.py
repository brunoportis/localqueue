"""Loop de consumo de subscriptions do barramento."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
from typing import TYPE_CHECKING, Any, Optional
from uuid import UUID

from pydantic import ValidationError

from localqueue.bus.bus import WILDCARD
from localqueue.exceptions import Empty, LeaseExpired
from localqueue.job import Job

if TYPE_CHECKING:
    from localqueue.bus.bus import EventBus

log = logging.getLogger(__name__)
_POLL_INTERVAL = 0.1


async def run_consumer(
    bus: "EventBus", subscription: str, *, idle_timeout: Optional[float] = None
) -> None:
    """Consome a fila de ``subscription`` até cancelamento (ou idle).

    Cada sondagem não bloqueante roda em thread separada; quando a fila está
    vazia, o intervalo é aguardado no event loop. ``CancelledError`` fecha a
    fila no ``finally`` e propaga.
    """
    queue = bus._open_subscription_queue(subscription)
    try:
        idle_since: Optional[float] = None
        while True:
            try:
                job = await asyncio.to_thread(queue.get, False)
            except Empty:
                if idle_timeout is not None:
                    now = asyncio.get_running_loop().time()
                    idle_since = idle_since if idle_since is not None else now
                    if now - idle_since >= idle_timeout:
                        return
                await asyncio.sleep(_POLL_INTERVAL)
                continue
            idle_since = None
            await _process_delivery(bus, subscription, queue, job)
    finally:
        queue.close()


async def _heartbeat(queue: Any, job: Job, interval: float, state: dict) -> None:
    """Renova o lease enquanto o handler roda; para ao perder o lease."""
    lease_seconds = queue.lease_seconds
    while True:
        await asyncio.sleep(interval)
        try:
            await asyncio.to_thread(queue.extend_lease, job, lease_seconds)
        except Exception:  # noqa: BLE001 - inclui LeaseExpired
            log.warning("Lease do job %s perdido durante o handler", job.id)
            state["lease_lost"] = True
            return


async def _transition(
    queue: Any, operation: Any, job: Job, **kwargs: Any
) -> None:
    """Aplica ack/nack/fail sem deixar LeaseExpired encerrar o consumer."""
    try:
        await asyncio.to_thread(operation, job, **kwargs)
    except LeaseExpired:
        log.warning(
            "Lease do job %s expirou antes da transição; resultado descartado",
            job.id,
        )


def _envelope_error(envelope: Any) -> Optional[str]:
    """Valida a estrutura mínima do envelope desserializado."""
    if not isinstance(envelope, dict):
        return (
            f"envelope malformado: esperado objeto JSON, "
            f"recebido {type(envelope).__name__}"
        )
    if not isinstance(envelope.get("event_type"), str):
        return "envelope malformado: 'event_type' ausente ou inválido"
    if not isinstance(envelope.get("payload"), dict):
        return "envelope malformado: 'payload' ausente ou inválido"
    return None


async def _process_delivery(
    bus: "EventBus", subscription: str, queue: Any, job: Job
) -> None:
    envelope = job.data
    error = _envelope_error(envelope)
    if error is not None:
        await _transition(queue, queue.fail, job, last_error=error)
        return

    event_type = envelope["event_type"]
    cls = bus.registry.resolve(event_type)
    if cls is None:
        # Erro permanente: tipo desconhecido não adianta retentar.
        await _transition(
            queue, queue.fail, job,
            last_error=f"evento desconhecido: {event_type!r}",
        )
        return

    try:
        event = cls(
            event_id=UUID(envelope["event_id"]),
            event_created_at=envelope["event_created_at"],
            **envelope["payload"],
        )
    except (ValidationError, KeyError, TypeError, ValueError) as exc:
        # Erro permanente: payload inválido não vai validar numa retentativa.
        await _transition(
            queue, queue.fail, job,
            last_error=f"payload inválido para {event_type!r}: {exc}",
        )
        return

    registration = bus._handlers.get(
        (subscription, event_type)
    ) or bus._handlers.get((subscription, WILDCARD))
    if registration is None:
        await _transition(
            queue, queue.fail, job,
            last_error=(
                f"nenhum handler registrado para {event_type!r} "
                f"em {subscription!r} neste processo"
            ),
        )
        return

    # Heartbeat renova o lease enquanto o handler roda; se o lease for
    # perdido, o resultado é descartado (outra worker pode ter assumido).
    state = {"lease_lost": False}
    interval = max(queue.lease_seconds / 3, 0.05)
    heartbeat = asyncio.create_task(_heartbeat(queue, job, interval, state))
    try:
        handler = registration.handler
        if inspect.iscoroutinefunction(handler):
            result = await handler(event)
        else:
            # Handler síncrono fora da thread do event loop.
            result = await asyncio.to_thread(handler, event)
        if inspect.isawaitable(result):
            # Rede de segurança: handler sync que retornou um awaitable.
            await result
    except registration.permanent_errors as exc:
        await _transition(
            queue, queue.fail, job, last_error=f"erro permanente: {exc}"
        )
    except Exception as exc:  # noqa: BLE001 - erro transitório: retenta
        await _transition(queue, queue.nack, job, last_error=str(exc))
    else:
        if state["lease_lost"]:
            log.warning(
                "Job %s perdeu o lease durante o handler; resultado descartado",
                job.id,
            )
            return
        await _transition(queue, queue.ack, job)
    finally:
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat
