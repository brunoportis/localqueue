"""Canonical single-process benchmark harness for localqueue."""

from localqueue.benchmark.config import BenchmarkConfig
from localqueue.benchmark.errors import BenchmarkExecutionError
from localqueue.benchmark.models import BenchmarkReport, ScenarioResult
from localqueue.benchmark.multiprocess import (
    run_multiprocess_profile,
    run_multiprocess_scenario,
)
from localqueue.benchmark.multiprocess_models import (
    FileSnapshot,
    IDValidation,
    LargeDatabaseResult,
    MetricSeries,
    MultiprocessConfig,
    MultiprocessScenarioConfig,
    ProcessResult,
    ThroughputResult,
)
from localqueue.benchmark.render import render_markdown
from localqueue.benchmark.runner import run_profile

__all__ = [
    "BenchmarkConfig",
    "BenchmarkExecutionError",
    "BenchmarkReport",
    "ScenarioResult",
    "run_profile",
    "render_markdown",
    "run_multiprocess_scenario",
    "run_multiprocess_profile",
    "MultiprocessConfig",
    "MultiprocessScenarioConfig",
    "ProcessResult",
    "MetricSeries",
    "ThroughputResult",
    "FileSnapshot",
    "IDValidation",
    "LargeDatabaseResult",
]
