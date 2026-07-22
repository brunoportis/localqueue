"""Canonical single-process benchmark harness for localqueue."""

from localqueue.benchmark.config import BenchmarkConfig
from localqueue.benchmark.errors import BenchmarkExecutionError
from localqueue.benchmark.models import BenchmarkReport, ScenarioResult
from localqueue.benchmark.multiprocess import run_multiprocess_scenario
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
]
