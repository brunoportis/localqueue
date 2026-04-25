from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from ..stores import QueueMessage

AckTiming = Literal["before-delivery", "after-success", "manual"]
DeliveryGuarantee = Literal["at-least-once", "at-most-once", "effectively-once"]
Locality = Literal["local", "remote"]
RoutingPattern = Literal["point-to-point", "publish-subscribe"]
ConsumptionPattern = Literal["pull", "push"]
OrderingGuarantee = Literal["fifo-ready", "priority", "best-effort"]
CommitMode = Literal["local-atomic", "transactional-outbox", "two-phase", "saga"]
BackpressureOverflow = Literal["block", "reject"]
MessageHandler = Callable[["QueueMessage"], object]
MessageHandlers = MessageHandler | Iterable[MessageHandler]
