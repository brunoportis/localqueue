from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class LeasePolicy(Protocol):
    @property
    def timeout(self) -> float: ...

    @property
    def uses_leases(self) -> bool: ...

    @property
    def expires_inflight(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class FixedLeaseTimeout:
    """Lease policy where inflight messages expire after a fixed timeout."""

    timeout: float = 30.0
    uses_leases: bool = True
    expires_inflight: bool = True

    def __post_init__(self) -> None:
        if self.timeout <= 0:
            raise ValueError("lease timeout must be greater than zero")

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "fixed-timeout",
            "timeout": self.timeout,
            "uses_leases": self.uses_leases,
            "expires_inflight": self.expires_inflight,
        }


FIXED_LEASE_TIMEOUT = FixedLeaseTimeout()
