"""Deterministic benchmark metric calculations."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import Any


def percentile(samples: list[int], fraction: float) -> int:
    """Return nearest-rank percentile: ``sorted[ceil(p*n)-1]``."""
    if not samples:
        raise ValueError("percentile requires at least one sample")
    if not 0 <= fraction <= 1:
        raise ValueError("percentile must be between 0 and 1")
    rank = max(1, math.ceil(fraction * len(samples)))
    return sorted(samples)[min(rank, len(samples)) - 1]


@dataclass(frozen=True, slots=True)
class MetricSummary:
    count: int
    total_elapsed_ns: int
    minimum_ns: int
    maximum_ns: int
    mean_ns: float
    p50_ns: int
    p95_ns: int
    p99_ns: int
    operations_per_second: float
    messages_per_second: float | None = None
    batches_per_second: float | None = None
    dispatches_per_second: float | None = None
    deliveries_per_second: float | None = None

    @classmethod
    def from_samples(
        cls,
        samples: list[int],
        total_elapsed_ns: int,
        *,
        messages: int | None = None,
        batches: int | None = None,
        dispatches: int | None = None,
        deliveries: int | None = None,
    ) -> "MetricSummary":
        if not samples:
            raise ValueError("metric summary requires at least one sample")
        if total_elapsed_ns <= 0:
            raise ValueError("total elapsed time must be positive")
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in samples
        ):
            raise TypeError("samples must be non-negative integers")
        seconds = total_elapsed_ns / 1_000_000_000
        return cls(
            count=len(samples),
            total_elapsed_ns=total_elapsed_ns,
            minimum_ns=min(samples),
            maximum_ns=max(samples),
            mean_ns=statistics.fmean(samples),
            p50_ns=percentile(samples, 0.50),
            p95_ns=percentile(samples, 0.95),
            p99_ns=percentile(samples, 0.99),
            operations_per_second=len(samples) / seconds,
            messages_per_second=None if messages is None else messages / seconds,
            batches_per_second=None if batches is None else batches / seconds,
            dispatches_per_second=None if dispatches is None else dispatches / seconds,
            deliveries_per_second=None if deliveries is None else deliveries / seconds,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "total_elapsed_ns": self.total_elapsed_ns,
            "minimum_ns": self.minimum_ns,
            "maximum_ns": self.maximum_ns,
            "mean_ns": self.mean_ns,
            "p50_ns": self.p50_ns,
            "p95_ns": self.p95_ns,
            "p99_ns": self.p99_ns,
            "operations_per_second": self.operations_per_second,
            "messages_per_second": self.messages_per_second,
            "batches_per_second": self.batches_per_second,
            "dispatches_per_second": self.dispatches_per_second,
            "deliveries_per_second": self.deliveries_per_second,
        }
