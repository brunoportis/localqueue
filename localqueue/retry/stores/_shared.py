from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from types import ModuleType

_ENVS: dict[tuple[str, int], Any] = {}
_ENVS_LOCK = threading.Lock()
_SQLITE_RETRY_SCHEMA_VERSION = 1


def import_lmdb() -> ModuleType:
    try:
        import lmdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "LMDB support requires the optional dependency; "
            'install with `pip install "localqueue[lmdb]"`'
        ) from exc
    return lmdb
