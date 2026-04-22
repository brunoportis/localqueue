from __future__ import annotations

from typing import Any

_PERMANENT_FAILURE_TYPES = (
    ImportError,
    ModuleNotFoundError,
    NameError,
)


def is_permanent_failure(error: BaseException | Any) -> bool:
    if getattr(error, "exit_code", None) == 127:
        return True
    return isinstance(error, _PERMANENT_FAILURE_TYPES)
