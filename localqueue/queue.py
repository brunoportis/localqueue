from __future__ import annotations

import time
from pathlib import Path
from queue import Empty, Full
from threading import Condition
from typing import Any

from .store import QueueMessage, QueueStats, QueueStore, SQLiteQueueStore


class PersistentQueue:
    name: str
    lease_timeout: float
    maxsize: int
    _store: QueueStore | None
    _store_path: Path | None
    _condition: Condition
    _unfinished: list[QueueMessage]

    def __init__(
        self,
        name: str,
        *,
        store: QueueStore | None = None,
        store_path: str | Path | None = None,
        lease_timeout: float = 30.0,
        maxsize: int = 0,
    ) -> None:
        if store is not None and store_path is not None:
            raise ValueError("pass either store= or store_path=, not both")
        if lease_timeout <= 0:
            raise ValueError("lease_timeout must be greater than zero")
        if maxsize < 0:
            raise ValueError("maxsize cannot be negative")

        self.name = name
        self.lease_timeout = lease_timeout
        self.maxsize = maxsize
        self._store = store
        self._store_path = Path(store_path) if store_path is not None else None
        self._condition = Condition()
        self._unfinished = []

    def put(
        self,
        item: Any,
        block: bool = True,
        timeout: float | None = None,
        *,
        delay: float = 0.0,
    ) -> QueueMessage:
        if delay < 0:
            raise ValueError("delay cannot be negative")
        deadline = _deadline(timeout)
        with self._condition:
            while self.full():
                if not block:
                    raise Full
                remaining = _remaining(deadline)
                if remaining is not None and remaining <= 0:
                    raise Full
                _ = self._condition.wait(remaining)
            message = self._get_store().enqueue(
                self.name,
                item,
                available_at=time.time() + delay,
            )
            _ = self._condition.notify_all()
            return message

    def put_nowait(self, item: Any) -> QueueMessage:
        return self.put(item, block=False)

    def get(
        self,
        block: bool = True,
        timeout: float | None = None,
        *,
        leased_by: str | None = None,
    ) -> Any:
        message = self.get_message(block=block, timeout=timeout, leased_by=leased_by)
        with self._condition:
            self._unfinished.append(message)
        return message.value

    def get_nowait(self) -> Any:
        return self.get(block=False)

    def get_message(
        self,
        block: bool = True,
        timeout: float | None = None,
        *,
        leased_by: str | None = None,
    ) -> QueueMessage:
        deadline = _deadline(timeout)
        while True:
            with self._condition:
                message = self._get_store().dequeue(
                    self.name,
                    lease_timeout=self.lease_timeout,
                    now=time.time(),
                    leased_by=leased_by,
                )
                if message is not None:
                    _ = self._condition.notify_all()
                    return message
                if not block:
                    raise Empty
                remaining = _remaining(deadline)
                if remaining is not None and remaining <= 0:
                    raise Empty
                _ = self._condition.wait(_wait_time(remaining))

    def inspect(self, message_id: str) -> QueueMessage | None:
        with self._condition:
            return self._get_store().get(self.name, message_id)

    def ack(self, message: QueueMessage) -> bool:
        with self._condition:
            removed = self._get_store().ack(self.name, message.id)
            self._remove_unfinished(message.id)
            _ = self._condition.notify_all()
            return removed

    def release(
        self,
        message: QueueMessage,
        *,
        delay: float = 0.0,
        error: BaseException | str | None = None,
    ) -> bool:
        if delay < 0:
            raise ValueError("delay cannot be negative")
        failed_at = time.time() if error is not None else None
        with self._condition:
            released = self._get_store().release(
                self.name,
                message.id,
                available_at=time.time() + delay,
                last_error=_error_payload(error),
                failed_at=failed_at,
            )
            self._remove_unfinished(message.id)
            _ = self._condition.notify_all()
            return released

    def dead_letter(
        self, message: QueueMessage, *, error: BaseException | str | None = None
    ) -> bool:
        failed_at = time.time() if error is not None else None
        with self._condition:
            moved = self._get_store().dead_letter(
                self.name,
                message.id,
                last_error=_error_payload(error),
                failed_at=failed_at,
            )
            self._remove_unfinished(message.id)
            _ = self._condition.notify_all()
            return moved

    def task_done(self) -> None:
        with self._condition:
            if not self._unfinished:
                raise ValueError("task_done() called too many times")
            message = self._unfinished.pop(0)
            _ = self._get_store().ack(self.name, message.id)
            _ = self._condition.notify_all()

    def join(self) -> None:
        with self._condition:
            while self._unfinished:
                _ = self._condition.wait()

    def qsize(self) -> int:
        return self._get_store().qsize(self.name, now=time.time())

    def stats(self) -> QueueStats:
        return self._get_store().stats(self.name, now=time.time())

    def dead_letters(self, *, limit: int | None = None) -> list[QueueMessage]:
        return self._get_store().dead_letters(self.name, limit=limit)

    def requeue_dead(self, message: QueueMessage, *, delay: float = 0.0) -> bool:
        if delay < 0:
            raise ValueError("delay cannot be negative")
        with self._condition:
            requeued = self._get_store().requeue_dead(
                self.name,
                message.id,
                available_at=time.time() + delay,
            )
            _ = self._condition.notify_all()
            return requeued

    def prune_dead_letters(self, *, older_than: float) -> int:
        if older_than < 0:
            raise ValueError("older_than cannot be negative")
        with self._condition:
            removed = self._get_store().prune_dead_letters(
                self.name,
                older_than=older_than,
                now=time.time(),
            )
            _ = self._condition.notify_all()
            return removed

    def count_dead_letters_older_than(self, *, older_than: float) -> int:
        if older_than < 0:
            raise ValueError("older_than cannot be negative")
        return self._get_store().count_dead_letters_older_than(
            self.name,
            older_than=older_than,
            now=time.time(),
        )

    def empty(self) -> bool:
        return self._get_store().empty(self.name, now=time.time())

    def full(self) -> bool:
        return self.maxsize > 0 and self.qsize() >= self.maxsize

    def purge(self) -> int:
        with self._condition:
            self._unfinished.clear()
            count = self._get_store().purge(self.name)
            _ = self._condition.notify_all()
            return count

    def _get_store(self) -> QueueStore:
        if self._store is None:
            self._store = SQLiteQueueStore(
                self._store_path
                if self._store_path is not None
                else "localqueue_queue.sqlite3"
            )
        return self._store

    def _remove_unfinished(self, message_id: str) -> None:
        self._unfinished = [
            message for message in self._unfinished if message.id != message_id
        ]


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


def _error_payload(error: BaseException | str | None) -> dict[str, Any] | None:
    if error is None:
        return None
    if isinstance(error, BaseException):
        error_type = type(error)
        payload: dict[str, Any] = {
            "type": error_type.__name__,
            "module": error_type.__module__,
            "message": str(error),
        }
        for attr in ("command", "exit_code", "stdout", "stderr"):
            value = getattr(error, attr, None)
            if value is not None:
                payload[attr] = value
        return payload
    return {"type": None, "module": None, "message": str(error)}
