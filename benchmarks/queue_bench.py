"""Deprecated compatibility wrapper for the canonical benchmark module.

This script is not release evidence and no longer contains benchmark math.
Use ``python -m localqueue.benchmark`` for canonical localqueue results.
Comparisons with persist-queue belong in a separate, non-canonical tool.
"""

from __future__ import annotations

import warnings

from localqueue.benchmark.cli import main


def run() -> int:
    warnings.warn(
        "benchmarks/queue_bench.py is deprecated; use python -m localqueue.benchmark",
        DeprecationWarning,
        stacklevel=2,
    )
    return main()


if __name__ == "__main__":
    raise SystemExit(run())
