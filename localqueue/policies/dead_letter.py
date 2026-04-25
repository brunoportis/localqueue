from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol


class DeadLetterPolicy(Protocol):
    @property
    def dead_letters(self) -> bool: ...

    @property
    def stores_failures(self) -> bool: ...

    @property
    def supports_requeue(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class DeadLetterQueue:
    """Dead-letter policy where failed messages stay inspectable and requeueable."""

    dead_letters: bool = True
    stores_failures: bool = True
    supports_requeue: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


DEAD_LETTER_QUEUE = DeadLetterQueue()
