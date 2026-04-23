from __future__ import annotations

from typing import Protocol

_PERMANENT_FAILURE_TYPES = (
    ImportError,
    ModuleNotFoundError,
    NameError,
)


class _ExitCodeCarrier(Protocol):
    exit_code: int


def is_permanent_failure(error: BaseException | _ExitCodeCarrier) -> bool:
    if getattr(error, "exit_code", None) == 127:
        return True
    return isinstance(error, _PERMANENT_FAILURE_TYPES)
