from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Protocol

from ..adapters.dispatch import CallbackDispatcher  # noqa: F401

if TYPE_CHECKING:
    from ..stores import QueueMessage


class DispatchPolicy(Protocol):
    @property
    def dispatches(self) -> bool: ...

    @property
    def dispatches_on_put(self) -> bool: ...

    @property
    def handler_count(self) -> int: ...

    def dispatch(self, message: QueueMessage) -> None: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class NoDispatcher:
    """Dispatch policy where producers only enqueue messages."""

    dispatches: bool = False
    dispatches_on_put: bool = False
    handler_count: int = 0

    def dispatch(self, message: QueueMessage) -> None:
        _ = message

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


NO_DISPATCHER = NoDispatcher()

__all__ = [
    "CallbackDispatcher",
    "DispatchPolicy",
    "NO_DISPATCHER",
    "NoDispatcher",
]
