"""Typed, immutable benchmark profile configuration."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

Durability = Literal["normal", "full"]


def _positive_int(value: int, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value <= 0:
        raise ValueError(f"{field} must be positive")
    return value


def _nonnegative_int(value: int, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")
    return value


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    name: str = "standard"
    version: int = 1
    durability: Durability = "normal"
    warmups: int = 3
    samples: int = 25
    payload_bytes: int = 128
    batch_sizes: tuple[int, ...] = (1, 10, 100, 1000)
    fanout_sizes: tuple[int, ...] = (1, 10, 100)
    lease_seconds: float = 60.0
    max_retries: int = 3
    scenario_order: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _positive_int(self.version, "version")
        _positive_int(self.warmups, "warmups")
        _positive_int(self.samples, "samples")
        _positive_int(self.payload_bytes, "payload_bytes")
        _nonnegative_int(self.max_retries, "max_retries")
        if (
            not isinstance(self.lease_seconds, (int, float))
            or isinstance(self.lease_seconds, bool)
            or self.lease_seconds <= 0
        ):
            raise ValueError("lease_seconds must be positive")
        if self.durability not in ("normal", "full"):
            raise ValueError("durability must be normal or full")
        if not self.batch_sizes or any(
            _positive_int(v, "batch size") != v for v in self.batch_sizes
        ):
            raise ValueError("batch_sizes must contain positive integers")
        if not self.fanout_sizes or any(
            _positive_int(v, "fanout size") != v for v in self.fanout_sizes
        ):
            raise ValueError("fanout_sizes must contain positive integers")

    @property
    def fsync(self) -> bool:
        return self.durability == "full"

    @classmethod
    def from_profile(
        cls, name: str, durability: Durability | None = None
    ) -> "BenchmarkConfig":
        from localqueue.benchmark.profiles import get_profile

        profile = get_profile(name)
        effective_durability = profile.durability if durability is None else durability
        return cls(**{**asdict(profile), "durability": effective_durability})
