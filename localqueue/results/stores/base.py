from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol


class ResultStore(Protocol):
    def load(self, key: str) -> Any | None: ...

    def save(self, key: str, value: Any) -> None: ...

    def delete(self, key: str) -> None: ...


class ResultStoreLockedError(RuntimeError):
    path: str

    def __init__(self, path: str | Path) -> None:
        resolved = str(Path(path).resolve())
        super().__init__(
            f"LMDB result store at {resolved!r} is locked by another process; "
            + "use a different path or stop the competing process"
        )
        self.path = resolved
