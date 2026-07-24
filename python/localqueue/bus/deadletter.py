"""Typed EventBus dead-letter inspection."""

from __future__ import annotations

from dataclasses import dataclass

from localqueue.bus.envelope import (
    EnvelopeError,
    parse_envelope,
    reconstruct_event,
)
from localqueue.bus.event import BaseEvent
from localqueue.bus.registry import EventRegistry
from localqueue.deadletter import FailedMessage, FailureReason


@dataclass(frozen=True, slots=True)
class FailedDelivery:
    """Immutable failed subscription delivery with raw operational evidence."""

    id: int
    subscription: str
    event: BaseEvent | None
    event_type: str | None
    raw_payload: bytes
    attempts: int
    reason: FailureReason
    last_error: str | None
    created_at: float
    updated_at: float
    inspection_error: str | None


def inspect_delivery(
    subscription: str,
    message: FailedMessage[object],
    registry: EventRegistry,
) -> FailedDelivery:
    event = None
    event_type = None
    inspection_error = message.decode_error
    if message.decoded:
        parsed = parse_envelope(message.data)
        if isinstance(parsed, EnvelopeError):
            event_type = parsed.event_type
            inspection_error = parsed.message
        else:
            event_type = parsed.event_type
            reconstructed = reconstruct_event(registry, parsed)
            if isinstance(reconstructed, EnvelopeError):
                inspection_error = reconstructed.message
            else:
                event = reconstructed.value
    return FailedDelivery(
        id=message.id,
        subscription=subscription,
        event=event,
        event_type=event_type,
        raw_payload=message.raw_payload,
        attempts=message.attempts,
        reason=message.reason,
        last_error=message.last_error,
        created_at=message.created_at,
        updated_at=message.updated_at,
        inspection_error=inspection_error,
    )
