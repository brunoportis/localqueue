"""Typed public dead-letter records and stable failure classifications."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar

_PayloadT = TypeVar("_PayloadT")


class FailureReason(str, Enum):
    """Stable classifications persisted for terminal queue transitions."""

    RETRIES_EXHAUSTED = "retries_exhausted"
    PERMANENT_HANDLER_ERROR = "permanent_handler_error"
    UNKNOWN_EVENT_TYPE = "unknown_event_type"
    INVALID_ENVELOPE = "invalid_envelope"
    INVALID_PAYLOAD = "invalid_payload"
    HANDLER_TIMEOUT = "handler_timeout"
    EXPLICIT_PERMANENT_FAILURE = "explicit_permanent_failure"
    NO_HANDLER = "no_handler"
    LEGACY_UNKNOWN = "legacy_unknown"

    @classmethod
    def _from_stored(cls, value: str | None) -> FailureReason:
        try:
            return cls(value)
        except (TypeError, ValueError):
            return cls.LEGACY_UNKNOWN


@dataclass(frozen=True, slots=True)
class FailedMessage(Generic[_PayloadT]):
    """Immutable dead-letter snapshot retaining the exact stored payload."""

    id: int
    data: _PayloadT | None
    raw_payload: bytes
    attempts: int
    reason: FailureReason
    last_error: str | None
    created_at: float
    updated_at: float
    decode_error: str | None

    @property
    def decoded(self) -> bool:
        """Return whether the serializer decoded this record successfully."""
        return self.decode_error is None


def _exception_message(error: BaseException) -> str:
    name = type(error).__name__
    message = str(error)
    return f"{name}: {message}" if message else name
