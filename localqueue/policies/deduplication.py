from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol


class DeduplicationPolicy(Protocol):
    @property
    def deduplication(self) -> bool: ...

    @property
    def accepts_dedupe_key(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class DedupeKeySupport:
    """Deduplication policy where messages may carry an optional stable key."""

    deduplication: bool = True
    accepts_dedupe_key: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


DEDUPE_KEY_SUPPORT = DedupeKeySupport()


@dataclass(frozen=True, slots=True)
class NoDeduplication:
    """Deduplication policy where stable dedupe keys are not accepted."""

    deduplication: bool = False
    accepts_dedupe_key: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


NO_DEDUPLICATION = NoDeduplication()
