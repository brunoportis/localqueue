"""Immutable public models for multiprocess benchmark extensions."""

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, Mapping

Role = Literal["producer", "consumer"]
Status = Literal["passed", "failed", "timeout"]
Durability = Literal["normal", "full"]


def _positive(value: int, name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class MultiprocessConfig:
    profile: str
    messages: int
    durability: Durability | None = None
    sample_limit: int = 1000
    timeout_seconds: float = 120.0
    large_db_rows: int = 1_000_000
    keep_workdir: bool = False

    def __post_init__(self) -> None:
        for value, name in (
            (self.messages, "messages"),
            (self.sample_limit, "sample_limit"),
            (self.large_db_rows, "large_db_rows"),
        ):
            _positive(value, name)
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


@dataclass(frozen=True, slots=True)
class MultiprocessScenarioConfig:
    producers: int
    consumers: int
    payload_requested_bytes: int
    durability: Durability

    def __post_init__(self) -> None:
        _positive(self.producers, "producers")
        _positive(self.consumers, "consumers")
        _positive(self.payload_requested_bytes, "payload_requested_bytes")

    def to_dict(self) -> dict[str, Any]:
        return {
            "producers": self.producers,
            "consumers": self.consumers,
            "payload_requested_bytes": self.payload_requested_bytes,
            "durability": self.durability,
        }


@dataclass(frozen=True, slots=True)
class ProcessResult:
    logical_id: str
    role: Role
    status: Status
    exit_code: int
    counts: Mapping[str, int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "counts", MappingProxyType(dict(self.counts)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "logical_id": self.logical_id,
            "role": self.role,
            "status": self.status,
            "exit_code": self.exit_code,
            "counts": dict(self.counts),
        }


@dataclass(frozen=True, slots=True)
class MetricSeries:
    population_count: int
    sample_count: int
    limit: int
    stride: int
    ordering_key: str = "global_message_id"
    method: str = "systematic"
    samples: tuple[Any, ...] = ()

    def __post_init__(self) -> None:
        _positive(self.population_count, "population_count")
        _positive(self.limit, "limit")
        _positive(self.stride, "stride")
        if self.sample_count != len(self.samples) or self.sample_count > self.limit:
            raise ValueError("sample_count must match samples and not exceed limit")

    def to_dict(self) -> dict[str, Any]:
        return {
            "population_count": self.population_count,
            "sample_count": self.sample_count,
            "limit": self.limit,
            "stride": self.stride,
            "ordering_key": self.ordering_key,
            "method": self.method,
            "samples": list(self.samples),
        }


@dataclass(frozen=True, slots=True)
class ThroughputResult:
    messages_produced: int
    messages_claimed: int
    messages_acked: int
    elapsed_ns: int

    def __post_init__(self) -> None:
        _positive(self.elapsed_ns, "elapsed_ns")

    def to_dict(self) -> dict[str, int | float]:
        return {
            "messages_produced": self.messages_produced,
            "messages_claimed": self.messages_claimed,
            "messages_acked": self.messages_acked,
            "elapsed_ns": self.elapsed_ns,
            "acked_per_second": self.messages_acked / (self.elapsed_ns / 1_000_000_000),
        }


@dataclass(frozen=True, slots=True)
class FileSnapshot:
    exists: bool
    size_bytes: int | None

    def __post_init__(self) -> None:
        if not self.exists and self.size_bytes is not None:
            raise ValueError("missing files must have null size")

    def to_dict(self) -> dict[str, int | bool | None]:
        return {"exists": self.exists, "size_bytes": self.size_bytes}


@dataclass(frozen=True, slots=True)
class IDValidation:
    method: str
    expected: Mapping[str, Any]
    observed: Mapping[str, Any]
    ok: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "expected": dict(self.expected),
            "observed": dict(self.observed),
            "ok": self.ok,
        }

    def __post_init__(self) -> None:
        object.__setattr__(self, "expected", MappingProxyType(dict(self.expected)))
        object.__setattr__(self, "observed", MappingProxyType(dict(self.observed)))


@dataclass(frozen=True, slots=True)
class LargeDatabaseResult:
    target_rows: int
    actual_rows: int
    batch_size: int
    preload_elapsed_ns: int

    def __post_init__(self) -> None:
        _positive(self.target_rows, "target_rows")
        _positive(self.batch_size, "batch_size")

    def to_dict(self) -> dict[str, int]:
        return {
            "target_rows": self.target_rows,
            "actual_rows": self.actual_rows,
            "batch_size": self.batch_size,
            "preload_elapsed_ns": self.preload_elapsed_ns,
        }
