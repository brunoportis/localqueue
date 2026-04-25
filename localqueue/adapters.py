from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    import asyncio
    from .stores import QueueMessage

MessageHandler = Callable[["QueueMessage"], object]


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

    def dispatch(self, message: "QueueMessage") -> None:
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
    with _executor_lock:
        if max_workers not in _shared_executors:
            _shared_executors[max_workers] = ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix="QueueThreadedDispatcher"
            )
        return _shared_executors[max_workers]


def shutdown_shared_executors(wait: bool = True) -> None:
    """Shutdown all shared executors managed by this module.

    Call this from application shutdown to avoid background threads lingering
    in long-running processes.
    """
    with _executor_lock:
        for ex in list(_shared_executors.values()):
            try:
                ex.shutdown(wait=wait)
            except Exception:
                pass
        _shared_executors.clear()


# Notification adapters ------------------------------------------------------

@dataclass(frozen=True, slots=True)
class InProcessNotification:
    """Notification policy that signals a threading.Event after enqueue."""

    event: threading.Event
    notifies: bool = True
    notifies_on_put: bool = True

    @property
    def listener_count(self) -> int:
        return 1

    def notify(self, message: "QueueMessage") -> None:
        _ = message
        self.event.set()

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "in-process-event",
            "notifies": self.notifies,
            "notifies_on_put": self.notifies_on_put,
            "listener_count": self.listener_count,
        }


@dataclass(frozen=True, slots=True)
class AsyncNotification:
    """Notification policy that signals an asyncio.Event after enqueue.

    To be safe when notify() is called from threads other than the event loop's
    thread, prefer passing the event loop explicitly via the ``loop`` argument.
    If ``loop`` is omitted, this adapter will try to locate the loop bound to
    the event and fall back to calling set() directly as a best-effort.
    """

    event: "asyncio.Event"
    loop: "asyncio.AbstractEventLoop | None" = None
    notifies: bool = True
    notifies_on_put: bool = True

    @property
    def listener_count(self) -> int:
        return 1

    def notify(self, message: "QueueMessage") -> None:
        _ = message
        # Prefer an explicit loop if provided
        loop = self.loop or getattr(self.event, "_loop", None)
        if loop is not None:
            # Schedule a thread-safe set on the event's loop.
            try:
                loop.call_soon_threadsafe(self.event.set)
                return
            except Exception:
                pass

        # Best-effort fallback (works if we're already on the event loop thread)
        try:
            self.event.set()
        except Exception:
            # Last-resort: nothing sensible to do here; fail silently to keep
            # notify() non-blocking.
            pass

    def as_dict(self) -> dict[str, object]:
        return {
            "type": "async-event",
            "notifies": self.notifies,
            "notifies_on_put": self.notifies_on_put,
            "listener_count": self.listener_count,
        }
