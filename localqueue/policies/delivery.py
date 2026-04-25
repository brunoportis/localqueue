from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Protocol

from .commit import CommitPolicy, LOCAL_ATOMIC_COMMIT
from .results import NO_RESULT_POLICY, ResultPolicy

if TYPE_CHECKING:
    from ._types import AckTiming, DeliveryGuarantee
    from ..idempotency import IdempotencyStore


class DeliveryPolicy(Protocol):
    @property
    def guarantee(self) -> DeliveryGuarantee: ...

    @property
    def ack_timing(self) -> AckTiming: ...

    @property
    def uses_leases(self) -> bool: ...

    @property
    def redelivers_expired_leases(self) -> bool: ...

    def as_dict(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class AtLeastOnceDelivery:
    """Delivery policy where messages are acknowledged after successful handling."""

    guarantee: DeliveryGuarantee = "at-least-once"
    ack_timing: AckTiming = "after-success"
    uses_leases: bool = True
    redelivers_expired_leases: bool = True

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


AT_LEAST_ONCE_DELIVERY = AtLeastOnceDelivery()


@dataclass(frozen=True, slots=True)
class AtMostOnceDelivery:
    """Delivery policy where messages are removed before user handling."""

    guarantee: DeliveryGuarantee = "at-most-once"
    ack_timing: AckTiming = "before-delivery"
    uses_leases: bool = False
    redelivers_expired_leases: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EffectivelyOnceDelivery:
    """Delivery policy that requires idempotent enqueue keys."""

    guarantee: DeliveryGuarantee = "effectively-once"
    ack_timing: AckTiming = "after-success"
    uses_leases: bool = True
    redelivers_expired_leases: bool = True
    requires_dedupe_key: bool = True
    idempotency_store: IdempotencyStore | None = None
    result_policy: ResultPolicy = NO_RESULT_POLICY
    commit_policy: CommitPolicy = LOCAL_ATOMIC_COMMIT

    def as_dict(self) -> dict[str, object]:
        return {
            "guarantee": self.guarantee,
            "ack_timing": self.ack_timing,
            "uses_leases": self.uses_leases,
            "redelivers_expired_leases": self.redelivers_expired_leases,
            "requires_dedupe_key": self.requires_dedupe_key,
            "idempotency_store": (
                None
                if self.idempotency_store is None
                else type(self.idempotency_store).__name__
            ),
            "result_policy": self.result_policy.as_dict(),
            "commit_policy": self.commit_policy.as_dict(),
        }
