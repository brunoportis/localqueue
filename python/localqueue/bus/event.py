"""Modelo base de eventos do barramento."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class BaseEvent(BaseModel):
    """Evento persistido no barramento.

    Subclasses definem os campos de negócio. ``schema_version`` permite
    evoluir o formato; ``event_name`` permite sobrescrever o nome do evento
    sem depender do nome da classe.
    """

    schema_version: ClassVar[int] = 1
    event_name: ClassVar[str | None] = None

    event_id: UUID = Field(default_factory=uuid4)
    event_created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC)
    )

    def __init_subclass__(cls, **kwargs):  # noqa: ANN001, ANN204
        super().__init_subclass__(**kwargs)
        if cls.event_name is not None and not (
            isinstance(cls.event_name, str) and cls.event_name.strip()
        ):
            raise ValueError("'event_name' deve ser uma string não vazia")

    @property
    def event_type(self) -> str:
        return type(self).event_name or type(self).__name__

    @property
    def event_schema(self) -> str:
        return f"{self.event_type}@{self.schema_version}"


def event_type_of(cls: type[BaseEvent]) -> str:
    """Resolve o ``event_type`` de uma classe sem instanciá-la."""
    return cls.event_name or cls.__name__
