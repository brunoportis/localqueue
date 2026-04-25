from __future__ import annotations

import threading
from copy import deepcopy
from typing import Any


class MemoryResultStore:
    _results: dict[str, Any]
    _lock: threading.Lock

    def __init__(self) -> None:
        self._results = {}
        self._lock = threading.Lock()

    def load(self, key: str) -> Any | None:
        with self._lock:
            value = self._results.get(key)
            if value is None:
                return None
            return deepcopy(value)

    def save(self, key: str, value: Any) -> None:
        with self._lock:
            self._results[key] = deepcopy(value)

    def delete(self, key: str) -> None:
        with self._lock:
            _ = self._results.pop(key, None)
