"""Base event model for the event bus."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, ClassVar, TypeVar, cast
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

_EventT = TypeVar("_EventT", bound="BaseEvent")
_DERIVED_RESERVED_FIELDS = frozenset(
    {"event_id", "correlation_id", "causation_id", "event_created_at"}
)
_IdentityT = TypeVar("_IdentityT", bound=type["BaseEvent"])


class InvalidEventIdentity(ValueError):
    """Raised before persistence when a declared event identity is invalid."""


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


def event(*, identity: str | tuple[str, ...]) -> Callable[[_IdentityT], _IdentityT]:
    """Declare the business identity fields of one concrete event class."""
    if isinstance(identity, str):
        fields: tuple[str, ...] = (identity,)
    elif isinstance(identity, tuple) and all(
        isinstance(name, str) for name in identity
    ):
        fields = identity
    else:
        raise TypeError(
            "event identity must be a non-empty string or non-empty tuple of strings"
        )
    if not fields or any(not name.strip() for name in fields):
        raise ValueError(
            "event identity must be a non-empty string or non-empty tuple of non-empty strings"
        )
    if len(set(fields)) != len(fields):
        raise ValueError("event identity field names must be unique")

    def decorate(cls: _IdentityT) -> _IdentityT:
        if not isinstance(cls, type) or not issubclass(cls, BaseEvent):
            raise TypeError(
                f"event decorator target {getattr(cls, '__name__', cls)!r} "
                "must be a subclass of BaseEvent"
            )
        event_cls = cast(type[BaseEvent], cls)
        if "__event_identity_fields__" in cls.__dict__:
            configured = cls.__dict__["__event_identity_fields__"]
            if configured != fields:
                raise ValueError(
                    f"{cls.__name__} already has a conflicting event identity; "
                    "configure identity only once"
                )
            return cls
        for name in fields:
            if name in _DERIVED_RESERVED_FIELDS:
                raise ValueError(
                    f"{cls.__name__} identity field {name!r} is event metadata; "
                    "identity must name a business model field"
                )
            if name in event_cls.model_computed_fields:
                raise ValueError(
                    f"{cls.__name__} identity field {name!r} is computed; "
                    "identity must name a persisted model field"
                )
            if name not in event_cls.model_fields:
                raise ValueError(
                    f"{cls.__name__} identity field {name!r} does not exist; "
                    "identity must name a Python model field"
                )
            if event_cls.model_fields[name].exclude is True:
                raise InvalidEventIdentity(
                    f"{cls.__name__} identity field {name!r} is excluded; "
                    "identity must be present in the persisted payload"
                )
        setattr(cls, "__event_identity_fields__", cast(object, fields))
        return cls

    return decorate


def event_type_of(cls: type[BaseEvent]) -> str:
    """Resolve a class's ``event_type`` without instantiating it."""
    return cls.event_name or cls.__name__


def derive_from_returned(event: _EventT, parent: BaseEvent) -> _EventT:
    """Copy a returned event and fill only lineage fields the caller omitted."""
    updates: dict[str, UUID | None] = {}
    if "correlation_id" not in event.model_fields_set:
        updates["correlation_id"] = parent.correlation_id
    if "causation_id" not in event.model_fields_set:
        updates["causation_id"] = parent.event_id
    return event.model_copy(update=updates)
