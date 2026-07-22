"""Exceptions raised by the benchmark API."""

from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import NoReturn


def sanitize_error_message(
    message: str, sensitive_paths: tuple[str | Path, ...] = ()
) -> str:
    """Return a short error message without benchmark-specific paths."""
    sanitized = str(message).replace("\n", " ")
    paths = [str(Path(path)) for path in sensitive_paths]
    paths.append(str(Path(tempfile.gettempdir())))
    for path in sorted(set(paths), key=len, reverse=True):
        sanitized = sanitized.replace(path, "<path>")
    sanitized = re.sub(r"localqueue-benchmark-[^/\\\s:'\"]+", "<temporary>", sanitized)
    return sanitized[:500]


class BenchmarkExecutionError(RuntimeError):
    """A scenario failed after benchmark execution had started.

    ``cause`` retains the original exception for API callers while the CLI can
    present a short, stable error without printing a traceback.
    """

    def __init__(self, scenario_id: str, cause: BaseException) -> None:
        self.scenario_id = scenario_id
        self.cause = cause
        super().__init__(
            f"scenario {scenario_id!r} failed: {type(cause).__name__}: "
            f"{sanitize_error_message(str(cause))}"
        )


def raise_execution_error(scenario_id: str, cause: BaseException) -> NoReturn:
    raise BenchmarkExecutionError(scenario_id, cause) from cause
