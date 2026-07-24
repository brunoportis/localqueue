"""Base event model for the event bus."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, ClassVar, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

_EventT = TypeVar("_EventT", bound="BaseEvent")
_DERIVED_RESERVED_FIELDS = frozenset(
    {"event_id", "correlation_id", "causation_id", "event_created_at"}
)


def _correlation_from_event_id(validated_data: dict[str, Any]) -> UUID:
    """Read Pydantic's dynamically typed validated-data mapping."""
    return validated_data["event_id"]


class BaseEvent(BaseModel):
    """An event persisted by the bus.

    Subclasses define business fields. ``schema_version`` supports format
    evolution, while ``event_name`` overrides the persisted event name so it
    does not depend on the Python class name.
    """

    schema_version: ClassVar[int] = 1
    event_name: ClassVar[str | None] = None

    event_id: UUID = Field(default_factory=uuid4, frozen=True)
    correlation_id: UUID = Field(
        default_factory=_correlation_from_event_id, frozen=True
    )
    causation_id: UUID | None = Field(default=None, frozen=True)
    event_created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __init_subclass__(cls, **kwargs: Any) -> None:
        # Pydantic and Python class creation own this open-ended kwargs API.
        super().__init_subclass__(**kwargs)
        if cls.event_name is not None and not (
            isinstance(cls.event_name, str) and cls.event_name.strip()
        ):
            raise ValueError("'event_name' must be a non-empty string")

    @classmethod
    def from_parent(cls: type[_EventT], parent: BaseEvent, /, **data: Any) -> _EventT:
        """Create a derived event from dynamic Pydantic model input.

        ``data`` intentionally follows Pydantic's open-ended field API; the
        concrete subclass performs runtime validation.
        """
        if not isinstance(parent, BaseEvent):
            raise TypeError("'parent' must be a BaseEvent instance")
        conflicts = _DERIVED_RESERVED_FIELDS.intersection(data)
        if conflicts:
            names = ", ".join(sorted(conflicts))
            raise TypeError(f"from_parent does not accept reserved field(s): {names}")
        return cls(
            correlation_id=parent.correlation_id,
            causation_id=parent.event_id,
            **data,
        )

    @property
    def event_type(self) -> str:
        return type(self).event_name or type(self).__name__

    @property
    def event_schema(self) -> str:
        return f"{self.event_type}@{self.schema_version}"


def event_type_of(cls: type[BaseEvent]) -> str:
    """Resolve a class's ``event_type`` without instantiating it."""
    return cls.event_name or cls.__name__
