from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol

IdempotencyStatus = Literal["pending", "succeeded", "failed"]


@dataclass(slots=True)
class IdempotencyRecord:
    status: IdempotencyStatus
    first_seen_at: float
    completed_at: float | None = None
    result_key: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def pending(cls) -> "IdempotencyRecord":
        return cls(status="pending", first_seen_at=time.time())


class IdempotencyStoreLockedError(RuntimeError):
    path: str

    def __init__(self, path: str | Path) -> None:
        resolved = str(Path(path).resolve())
        super().__init__(
            f"LMDB idempotency store at {resolved!r} is locked by another process; "
            + "use a different path or stop the competing process"
        )
        self.path = resolved


class IdempotencyStore(Protocol):
    def load(self, key: str) -> IdempotencyRecord | None: ...

    def save(self, key: str, record: IdempotencyRecord) -> None: ...

    def delete(self, key: str) -> None: ...

    def prune_completed(self, *, older_than: float, now: float) -> int: ...

    def count_completed_older_than(self, *, older_than: float, now: float) -> int: ...
