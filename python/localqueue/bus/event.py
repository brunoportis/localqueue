"""Base event model for the event bus."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, ClassVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class BaseEvent(BaseModel):
    """An event persisted by the bus.

    Subclasses define business fields. ``schema_version`` supports format
    evolution, while ``event_name`` overrides the persisted event name so it
    does not depend on the Python class name.
    """

    schema_version: ClassVar[int] = 1
    event_name: ClassVar[str | None] = None

    event_id: UUID = Field(default_factory=uuid4)
    event_created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if cls.event_name is not None and not (
            isinstance(cls.event_name, str) and cls.event_name.strip()
        ):
            raise ValueError("'event_name' must be a non-empty string")

    @property
    def event_type(self) -> str:
        return type(self).event_name or type(self).__name__

    @property
    def event_schema(self) -> str:
        return f"{self.event_type}@{self.schema_version}"


def event_type_of(cls: type[BaseEvent]) -> str:
    """Resolve a class's ``event_type`` without instantiating it."""
    return cls.event_name or cls.__name__
