"""Exceptions raised by the benchmark API."""

from __future__ import annotations

from typing import NoReturn


class BenchmarkExecutionError(RuntimeError):
    """A scenario failed after benchmark execution had started.

    ``cause`` retains the original exception for API callers while the CLI can
    present a short, stable error without printing a traceback.
    """

    def __init__(self, scenario_id: str, cause: BaseException) -> None:
        self.scenario_id = scenario_id
        self.cause = cause
        super().__init__(
            f"scenario {scenario_id!r} failed: {type(cause).__name__}: {cause}"
        )


def raise_execution_error(scenario_id: str, cause: BaseException) -> NoReturn:
    raise BenchmarkExecutionError(scenario_id, cause) from cause
