"""ThreadedDispatcher: dispatch policy using a thread pool executor."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..policies import MessageHandler
    from ..stores import QueueMessage


@dataclass(frozen=True, slots=True)
class ThreadedDispatcher:
    """Dispatch policy that calls handlers in a thread pool after enqueue.

    Handlers may be any callable accepting a QueueMessage. An explicit
    executor can be provided (useful for lifecycle control); when omitted a
    shared executor per max_workers value is used.
    """

    handlers: tuple[MessageHandler, ...]
    max_workers: int | None = None
    executor: ThreadPoolExecutor | None = None
    dispatches: bool = True
    dispatches_on_put: bool = True

    def __post_init__(self) -> None:
        handlers = tuple(self.handlers)
        if not handlers:
            raise ValueError("handlers cannot be empty")
        object.__setattr__(self, "handlers", handlers)

    @property
    def handler_count(self) -> int:
        return len(self.handlers)

    def dispatch(self, message: QueueMessage) -> None:
        executor = self.executor or _get_shared_executor(self.max_workers)
        for handler in self.handlers:
            executor.submit(handler, message)

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "threaded",
            "dispatches": self.dispatches,
            "dispatches_on_put": self.dispatches_on_put,
            "handler_count": self.handler_count,
            "max_workers": self.max_workers,
        }


# Shared executor management -------------------------------------------------
_shared_executors: dict[int | None, ThreadPoolExecutor] = {}
_executor_lock = threading.Lock()


def _get_shared_executor(max_workers: int | None) -> ThreadPoolExecutor:
    """Get or create a shared executor for the given max_workers config."""
    with _executor_lock:
        if max_workers not in _shared_executors:
            _shared_executors[max_workers] = ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="QueueThreadedDispatcher",
            )
        return _shared_executors[max_workers]


def shutdown_shared_executors(wait: bool = True) -> None:
    """Shutdown all shared executors managed by this module.

    Call this from application shutdown to avoid background threads lingering
    in long-running processes.

    Args:
        wait: If True, wait for running tasks to complete.
    """
    with _executor_lock:
        for ex in list(_shared_executors.values()):
            try:
                ex.shutdown(wait=wait)
            except Exception:
                pass
        _shared_executors.clear()
