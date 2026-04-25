from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, TYPE_CHECKING


if TYPE_CHECKING:
    from ._types import BackpressureOverflow


class BackpressureStrategy(Protocol):
    @property
    def maxsize(self) -> int: ...

    @property
    def overflow(self) -> BackpressureOverflow: ...

    def is_full(self, *, qsize: int) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class BoundedBackpressure:
    """Bounds ready-message capacity with queue.Queue-compatible semantics."""

    maxsize: int = 0
    overflow: BackpressureOverflow = "block"

    def __post_init__(self) -> None:
        if self.maxsize < 0:
            raise ValueError("maxsize cannot be negative")
        if self.overflow not in ("block", "reject"):
            raise ValueError("backpressure overflow must be 'block' or 'reject'")

    def is_full(self, *, qsize: int) -> bool:
        return self.maxsize > 0 and qsize >= self.maxsize

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "bounded",
            "maxsize": self.maxsize,
            "overflow": self.overflow,
        }


@dataclass(frozen=True, slots=True)
class RejectingBackpressure:
    """Bounds ready-message capacity and rejects producers immediately."""

    maxsize: int = 0
    overflow: BackpressureOverflow = "reject"

    def __post_init__(self) -> None:
        if self.maxsize < 0:
            raise ValueError("maxsize cannot be negative")
        if self.overflow != "reject":
            raise ValueError("rejecting backpressure overflow must be 'reject'")

    def is_full(self, *, qsize: int) -> bool:
        return self.maxsize > 0 and qsize >= self.maxsize

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "rejecting",
            "maxsize": self.maxsize,
            "overflow": self.overflow,
        }
