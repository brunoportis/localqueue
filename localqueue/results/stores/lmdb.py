from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ._shared import _ENVS, _ENVS_LOCK, import_lmdb
from .base import ResultStoreLockedError

if TYPE_CHECKING:
    import lmdb


class LMDBResultStore:
    path: Path
    _env: lmdb.Environment

    def __init__(self, path: str | Path, *, map_size: int = 10**7) -> None:
        lmdb = import_lmdb()
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        key = (str(self.path.resolve()), map_size)
        with _ENVS_LOCK:
            env = _ENVS.get(key)
            if env is None:
                try:
                    env = lmdb.open(
                        str(self.path),
                        map_size=map_size,
                        subdir=True,
                        lock=True,
                    )
                except lmdb.LockError as exc:
                    raise ResultStoreLockedError(self.path) from exc
                _ENVS[key] = env
            self._env = env

    def load(self, key: str) -> Any | None:
        with self._env.begin() as txn:
            raw = txn.get(key.encode("utf-8"))
        if raw is None:
            return None
        return json.loads(bytes(raw).decode("utf-8"))

    def save(self, key: str, value: Any) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
        with self._env.begin(write=True) as txn:
            _ = txn.put(key.encode("utf-8"), payload)

    def delete(self, key: str) -> None:
        with self._env.begin(write=True) as txn:
            _ = txn.delete(key.encode("utf-8"))
