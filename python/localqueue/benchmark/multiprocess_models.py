"""Immutable public models for multiprocess benchmark extensions."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class MultiprocessConfig:
    profile: str
    messages: int
    sample_limit: int = 1000
    timeout_seconds: float = 120.0
    large_db_rows: int = 1_000_000


@dataclass(frozen=True, slots=True)
class MultiprocessScenarioConfig:
    producers: int
    consumers: int
    payload_requested_bytes: int
    durability: str


@dataclass(frozen=True, slots=True)
class ProcessResult:
    logical_id: str
    role: str
    status: str
    exit_code: int
    counts: dict[str, int]


@dataclass(frozen=True, slots=True)
class MetricSeries:
    population_count: int
    sample_count: int
    limit: int
    stride: int
    ordering_key: str = "global_message_id"
    method: str = "systematic"
    samples: tuple[Any, ...] = ()


@dataclass(frozen=True, slots=True)
class ThroughputResult:
    messages_produced: int
    messages_claimed: int
    messages_acked: int
    elapsed_ns: int


@dataclass(frozen=True, slots=True)
class FileSnapshot:
    exists: bool
    size_bytes: int | None


@dataclass(frozen=True, slots=True)
class IDValidation:
    method: str
    expected: dict[str, Any]
    observed: dict[str, Any]
    ok: bool


@dataclass(frozen=True, slots=True)
class LargeDatabaseResult:
    target_rows: int
    actual_rows: int
    batch_size: int
    preload_elapsed_ns: int
