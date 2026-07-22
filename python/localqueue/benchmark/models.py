"""Versioned JSON report model."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from localqueue.benchmark.metrics import MetricSummary


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    scenario_id: str
    operation: str
    parameters: dict[str, Any]
    work_units: dict[str, int]
    sqlite: dict[str, Any]
    warmup: dict[str, Any]
    measured_samples_ns: list[int]
    summary: MetricSummary | None
    correctness: dict[str, Any]
    status: str
    error: dict[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "operation": self.operation,
            "parameters": self.parameters,
            "work_units": self.work_units,
            "sqlite": self.sqlite,
            "warmup": self.warmup,
            "measured_samples_ns": self.measured_samples_ns,
            "summary": None if self.summary is None else self.summary.to_dict(),
            "correctness": self.correctness,
            "status": self.status,
            "error": self.error,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkReport:
    subject: dict[str, Any]
    environment: dict[str, Any]
    profile: dict[str, Any]
    run: dict[str, Any]
    scenarios: list[ScenarioResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "subject": self.subject,
            "environment": self.environment,
            "profile": {"profile_schema_version": 1, **self.profile},
            "run": self.run,
            "scenarios": [scenario.to_dict() for scenario in self.scenarios],
        }
