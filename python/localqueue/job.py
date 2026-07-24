"""Job model returned by the queue."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from localqueue.core import SimpleQueue

_PayloadT = TypeVar("_PayloadT")


@dataclass
class Job(Generic[_PayloadT]):
    """An item leased from the queue for processing."""

    id: int
    data: _PayloadT
    attempts: int
    receipt: str
    lease_expires_at: float
    queue: SimpleQueue[_PayloadT]

    def extend_lease(self, seconds: float) -> None:
        """Extend this job's lease.

        Raises :class:`LeaseExpired` if the lease has already expired.
        """
        self.queue.extend_lease(self, seconds)
