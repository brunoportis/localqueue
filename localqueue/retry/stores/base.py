from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass(slots=True)
class RetryRecord:
    attempts: int = 0
    first_attempt_at: float = 0.0
    exhausted: bool = False

    @classmethod
    def new(cls) -> "RetryRecord":
        return cls(attempts=0, first_attempt_at=time.time(), exhausted=False)


class AttemptStoreLockedError(RuntimeError):
    path: str

    def __init__(self, path: str | Path) -> None:
        resolved = str(Path(path).resolve())
        super().__init__(
            f"LMDB store at {resolved!r} is locked by another process; "
            + "use a different store_path/db path or stop the competing process"
        )
        self.path = resolved


class AttemptStore(Protocol):
    def load(self, key: str) -> RetryRecord | None: ...

    def save(self, key: str, record: RetryRecord) -> None: ...

    def delete(self, key: str) -> None: ...

    def prune_exhausted(self, *, older_than: float, now: float) -> int: ...

    def count_exhausted_older_than(self, *, older_than: float, now: float) -> int: ...
