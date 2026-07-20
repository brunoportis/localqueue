"""Modelo de job retornado pela fila."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Job:
    """Representa um item retirado da fila para processamento."""

    id: int
    data: Any
    attempts: int
    receipt: str
    lease_expires_at: float
    queue: "SimpleQueue"  # type: ignore[name-defined]  # noqa: F821

    def extend_lease(self, seconds: float) -> None:
        """Estende o lease do job.

        Levanta :class:`LeaseExpired` se o lease já tiver expirado.
        """
        self.queue.extend_lease(self, seconds)
