"""Pure inspection helpers for untrusted persisted EventBus envelopes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from pydantic import ValidationError

from localqueue.bus.event import BaseEvent
from localqueue.bus.registry import EventRegistry
from localqueue.deadletter import FailureReason


@dataclass(frozen=True)
class ParsedEnvelope:
    raw: dict[object, object]
    event_type: str
    payload: dict[object, object]


@dataclass(frozen=True)
class EnvelopeError:
    message: str
    reason: FailureReason
    event_type: str | None = None


@dataclass(frozen=True)
class ReconstructedEvent:
    value: BaseEvent


def parse_envelope(value: object) -> ParsedEnvelope | EnvelopeError:
    """Validate and narrow an untrusted subscription-queue payload."""
    if not isinstance(value, dict):
        return EnvelopeError(
            f"malformed envelope: expected a JSON object, got {type(value).__name__}",
            FailureReason.INVALID_ENVELOPE,
        )
    event_type = value.get("event_type")
    if not isinstance(event_type, str):
        return EnvelopeError(
            "malformed envelope: missing or invalid 'event_type'",
            FailureReason.INVALID_ENVELOPE,
        )
    payload = value.get("payload")
    if not isinstance(payload, dict):
        return EnvelopeError(
            "malformed envelope: missing or invalid 'payload'",
            FailureReason.INVALID_ENVELOPE,
            event_type,
        )
    return ParsedEnvelope(raw=value, event_type=event_type, payload=payload)


def reconstruct_event(
    registry: EventRegistry,
    envelope: ParsedEnvelope,
) -> ReconstructedEvent | EnvelopeError:
    """Resolve and validate the concrete Pydantic event."""
    event_type = envelope.event_type
    cls = registry.resolve(event_type)
    if cls is None:
        return EnvelopeError(
            f"unknown event: {event_type!r}",
            FailureReason.UNKNOWN_EVENT_TYPE,
            event_type,
        )
    try:
        event_data: dict[object, object] = {
            **envelope.payload,
            "event_id": envelope.raw["event_id"],
            "event_created_at": envelope.raw["event_created_at"],
        }
        for field in ("correlation_id", "causation_id"):
            if field in envelope.raw:
                event_data[field] = envelope.raw[field]
        return ReconstructedEvent(cls(**cast(dict[str, Any], event_data)))
    except (ValidationError, KeyError, TypeError, ValueError) as error:
        return EnvelopeError(
            f"invalid payload for {event_type!r}: {error}",
            FailureReason.INVALID_PAYLOAD,
            event_type,
        )
