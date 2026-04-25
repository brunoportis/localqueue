from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol, TYPE_CHECKING


if TYPE_CHECKING:
    from ._types import Locality


class LocalityPolicy(Protocol):
    @property
    def locality(self) -> Locality: ...

    @property
    def co_located_state(self) -> bool: ...

    @property
    def crosses_network_boundary(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class LocalQueuePlacement:
    """Locality policy where queue state is local to the process host."""

    locality: Locality = "local"
    co_located_state: bool = True
    crosses_network_boundary: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


LOCAL_QUEUE_PLACEMENT = LocalQueuePlacement()


@dataclass(frozen=True, slots=True)
class RemoteQueuePlacement:
    """Locality policy where queue state lives behind a remote boundary."""

    locality: Locality = "remote"
    co_located_state: bool = False
    crosses_network_boundary: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)
