from __future__ import annotations

import time


def _deadline(timeout: float | None) -> float | None:
    if timeout is None:
        return None
    if timeout < 0:
        raise ValueError("'timeout' must be a non-negative number")
    return time.monotonic() + timeout


def _remaining(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return deadline - time.monotonic()


def _wait_time(remaining: float | None) -> float:
    if remaining is None:
        return 0.05
    return min(max(remaining, 0.0), 0.05)
