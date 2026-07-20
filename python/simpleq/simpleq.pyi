"""Type stubs para o módulo nativo simpleq.simpleq."""

from __future__ import annotations

from typing import Optional

class Lease:
    id: int
    payload: bytes
    attempts: int
    receipt: str
    lease_until: int

class Stats:
    ready: int
    processing: int
    acked: int
    failed: int

class NativeQueue:
    def __init__(
        self,
        path: str,
        queue: str,
        max_attempts: int = 3,
        fsync: bool = False,
    ) -> None: ...
    def put(self, payload: bytes, job_id: Optional[str] = None) -> int: ...
    def get(self, lease_ms: int) -> Optional[Lease]: ...
    def ack(self, id: int, receipt: str) -> None: ...
    def nack(
        self,
        id: int,
        receipt: str,
        delay_ms: int = 0,
        last_error: Optional[str] = None,
    ) -> None: ...
    def fail(self, id: int, receipt: str, last_error: Optional[str] = None) -> None: ...
    def extend_lease(self, id: int, receipt: str, extend_ms: int) -> int: ...
    def reclaim_expired(self, now: Optional[int] = None) -> int: ...
    def stats(self) -> Stats: ...
    def close(self) -> None: ...

class SimpleQError(Exception): ...
class Empty(SimpleQError): ...
class LeaseExpired(SimpleQError): ...
