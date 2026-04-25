from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol


class AcknowledgementPolicy(Protocol):
    @property
    def acknowledgements(self) -> bool: ...

    @property
    def explicit_ack(self) -> bool: ...

    @property
    def removes_on_ack(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class ExplicitAcknowledgement:
    """Acknowledgement policy where consumers explicitly remove completed work."""

    acknowledgements: bool = True
    explicit_ack: bool = True
    removes_on_ack: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


EXPLICIT_ACKNOWLEDGEMENT = ExplicitAcknowledgement()
