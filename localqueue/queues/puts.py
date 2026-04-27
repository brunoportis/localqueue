from __future__ import annotations

import time
from dataclasses import dataclass
from queue import Full
from typing import TYPE_CHECKING, Generic, TypeVar

from .timing import _deadline, _remaining

if TYPE_CHECKING:
    from ..stores import QueueMessage
    from .core import PersistentQueue

T = TypeVar("T")


def _fanout_dedupe_key(dedupe_key: str | None, subscriber: str) -> str | None:
    if dedupe_key is None:
        return None
    return f"{dedupe_key}:{subscriber}"


@dataclass(slots=True)
class _QueuePutCoordinator(Generic[T]):
    # Timing and validation stay as standalone utilities; enqueue coordination
    # lives here because fanout and post-put hooks are domain behavior.
    queue: PersistentQueue[T]

    def put(
        self,
        item: T,
        *,
        block: bool,
        timeout: float | None,
        delay: float,
        dedupe_key: str | None,
        priority: int,
    ) -> QueueMessage:
        if self.queue.routing_policy.fanout:
            return self.put_fanout(
                item,
                block=block,
                timeout=timeout,
                delay=delay,
                dedupe_key=dedupe_key,
                priority=priority,
            )
        deadline = _deadline(timeout)
        with self.queue._condition:
            while self.queue.full():
                if not block or self.queue.backpressure.overflow == "reject":
                    raise Full
                remaining = _remaining(deadline)
                if remaining is not None and remaining <= 0:
                    raise Full
                _ = self.queue._condition.wait(remaining)
            message = self.queue._get_store().enqueue(
                self.queue.name,
                item,
                available_at=time.time() + delay,
                dedupe_key=dedupe_key,
                priority=priority,
            )
            self.queue._condition.notify_all()
        self.run_post_put_hooks(message)
        return message

    def put_fanout(
        self,
        item: T,
        *,
        block: bool,
        timeout: float | None,
        delay: float,
        dedupe_key: str | None,
        priority: int,
    ) -> QueueMessage:
        subscribers = self.queue._subscriber_names()
        if not subscribers:
            raise ValueError(
                "publish-subscribe routing requires configured subscribers"
            )
        deadline = _deadline(timeout)
        messages: list[QueueMessage] = []
        for subscriber in subscribers:
            remaining = _remaining(deadline)
            message = self.queue.subscriber_queue(subscriber).put(
                item,
                block=block,
                timeout=remaining,
                delay=delay,
                dedupe_key=_fanout_dedupe_key(dedupe_key, subscriber),
                priority=priority,
            )
            messages.append(message)
        return messages[0]

    def run_post_put_hooks(self, message: QueueMessage) -> None:
        if self.queue.notification_policy.notifies_on_put:
            self.queue.notification_policy.notify(message)
        if self.queue.dispatch_policy.dispatches_on_put:
            self.queue.dispatch_policy.dispatch(message)
