"""Job model returned by the queue."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Job:
    """An item leased from the queue for processing."""

    id: int
    data: Any
    attempts: int
    receipt: str
    lease_expires_at: float
    queue: "SimpleQueue"  # type: ignore[name-defined]  # noqa: F821

    def extend_lease(self, seconds: float) -> None:
        """Extend this job's lease.

        Raises :class:`LeaseExpired` if the lease has already expired.
        """
        self.queue.extend_lease(self, seconds)
