"""Shared semantic policies for queues and event deliveries."""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum


class DurabilityMode(str, Enum):
    """Durability intent translated to the corresponding native SQLite mode.

    ``RELAXED`` prioritizes throughput. ``DURABLE`` requests stronger
    protection for recent commits without promising survival across every
    filesystem, kernel, drive cache, controller, or hardware failure.
    """

    RELAXED = "relaxed"
    DURABLE = "durable"


@dataclass(frozen=True, slots=True)
class DeliveryPolicy:
    """Immutable lease and retry policy shared by queue and EventBus delivery."""

    lease_seconds: float = 60.0
    max_retries: int = 3

    def __post_init__(self) -> None:
        if isinstance(self.lease_seconds, bool) or not isinstance(
            self.lease_seconds, (int, float)
        ):
            raise TypeError("'lease_seconds' must be a number")
        if not math.isfinite(self.lease_seconds) or self.lease_seconds <= 0:
            raise ValueError("'lease_seconds' must be a positive finite number")
        if not isinstance(self.max_retries, int) or isinstance(self.max_retries, bool):
            raise TypeError("'max_retries' must be an integer")
        if self.max_retries < 0:
            raise ValueError("'max_retries' must be non-negative")
        object.__setattr__(self, "lease_seconds", float(self.lease_seconds))


def _durability_fsync(durability: DurabilityMode) -> bool:
    if not isinstance(durability, DurabilityMode):
        raise TypeError("'durability' must be a DurabilityMode")
    return durability is DurabilityMode.DURABLE


__all__ = ["DeliveryPolicy", "DurabilityMode"]
