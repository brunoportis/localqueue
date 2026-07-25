"""Canonical persistence identity for EventBus events."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from localqueue.bus.event import BaseEvent

_EVENT_METADATA_FIELDS = {
    "event_id",
    "correlation_id",
    "causation_id",
    "event_created_at",
}


class InvalidEventIdentity(ValueError):
    """Raised before persistence when a declared event identity is invalid."""


@dataclass(frozen=True)
class _EventPersistenceIdentity:
    job_id: str
    dedup_key: str | None
    dedup_fingerprint: str | None


def business_payload(event: BaseEvent) -> dict[str, object]:
    """Return exactly the business payload persisted in an event envelope."""
    return event.model_dump(mode="json", exclude=_EVENT_METADATA_FIELDS)


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def prepare_persistence_identity(
    event: BaseEvent, payload: dict[str, object]
) -> _EventPersistenceIdentity:
    fields = type(event).__dict__.get("__event_identity_fields__")
    if fields is None:
        return _EventPersistenceIdentity(str(event.event_id), None, None)
    identity = event.model_dump(mode="json", include=set(fields))
    for field in fields:
        value = identity[field]
        if value is None or (isinstance(value, str) and not value.strip()):
            raise InvalidEventIdentity(
                f"{event.event_type} identity field {field!r} must be non-null "
                "and non-blank"
            )
        try:
            canonical_json(value)
        except (TypeError, ValueError) as error:
            raise InvalidEventIdentity(
                f"{event.event_type} identity field {field!r} must have a "
                "deterministic finite JSON value"
            ) from error
    key_digest = hashlib.sha256(
        canonical_json({"event_schema": event.event_schema, "identity": identity})
    ).hexdigest()
    fingerprint = hashlib.sha256(
        canonical_json({"event_schema": event.event_schema, "payload": payload})
    ).hexdigest()
    return _EventPersistenceIdentity(
        str(event.event_id),
        f"event-identity:v1:{key_digest}",
        f"event-payload:v1:{fingerprint}",
    )
