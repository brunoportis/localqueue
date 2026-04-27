from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any

from .policies import (
    AT_LEAST_ONCE_DELIVERY,
    DEAD_LETTER_QUEUE,
    LOCAL_ATOMIC_COMMIT,
    NO_RESULT_POLICY,
    AtMostOnceDelivery,
    CommitPolicy,
    DeadLetterPolicy,
    DeliveryPolicy,
    EffectivelyOnceDelivery,
    QueuePolicySet,
    ResultPolicy,
)
from .queue import PersistentQueue
from .queues import _validate_retry_defaults
from .worker import PersistentWorkerConfig


class QoS(StrEnum):
    AT_LEAST_ONCE = "at-least-once"
    AT_MOST_ONCE = "at-most-once"
    EFFECTIVELY_ONCE = "effectively-once"


@dataclass(frozen=True, slots=True)
class QueueSpec:
    """Fluent builder for common queue and worker settings."""

    name: str
    delivery_policy: DeliveryPolicy | None = None
    dead_letter_policy: DeadLetterPolicy | None = None
    dead_letter_on_failure: bool | None = None
    release_delay: float = 0.0
    min_interval: float = 0.0
    circuit_breaker_failures: int = 0
    circuit_breaker_cooldown: float = 0.0
    _retry_items: tuple[tuple[str, Any], ...] = field(default_factory=tuple)

    @property
    def retry_kwargs(self) -> dict[str, Any]:
        return dict(self._retry_items)

    def with_name(self, name: str) -> QueueSpec:
        return replace(self, name=name)

    def with_qos(
        self,
        qos: QoS | str,
        *,
        idempotency_store: Any = None,
        result_policy: ResultPolicy = NO_RESULT_POLICY,
        commit_policy: CommitPolicy = LOCAL_ATOMIC_COMMIT,
    ) -> QueueSpec:
        resolved_qos = QoS(qos)
        if resolved_qos == QoS.AT_LEAST_ONCE:
            delivery_policy: DeliveryPolicy = AT_LEAST_ONCE_DELIVERY
        elif resolved_qos == QoS.AT_MOST_ONCE:
            delivery_policy = AtMostOnceDelivery()
        else:
            delivery_policy = EffectivelyOnceDelivery(
                idempotency_store=idempotency_store,
                result_policy=result_policy,
                commit_policy=commit_policy,
            )
        return replace(self, delivery_policy=delivery_policy)

    def with_dead_letter_queue(
        self, dead_letter_policy: DeadLetterPolicy = DEAD_LETTER_QUEUE
    ) -> QueueSpec:
        return replace(self, dead_letter_policy=dead_letter_policy)

    def with_dead_letter_on_failure(self, enabled: bool = True) -> QueueSpec:
        return replace(self, dead_letter_on_failure=enabled)

    def with_retry(
        self, *, max_retries: int | None = None, **retry_kwargs: Any
    ) -> QueueSpec:
        if max_retries is not None and "max_tries" in retry_kwargs:
            raise ValueError("pass either max_retries= or max_tries=, not both")
        merged = self.retry_kwargs
        if max_retries is not None:
            retry_kwargs["max_tries"] = max_retries
        merged.update(retry_kwargs)
        _validate_retry_defaults(merged)
        return replace(self, _retry_items=tuple(merged.items()))

    def with_circuit_breaker(
        self, *, threshold: int, cooldown: float | None = None
    ) -> QueueSpec:
        resolved_cooldown = (
            0.0 if threshold == 0 else (30.0 if cooldown is None else cooldown)
        )
        _ = PersistentWorkerConfig(
            release_delay=self.release_delay,
            min_interval=self.min_interval,
            circuit_breaker_failures=threshold,
            circuit_breaker_cooldown=resolved_cooldown,
        )
        return replace(
            self,
            circuit_breaker_failures=threshold,
            circuit_breaker_cooldown=resolved_cooldown,
        )

    def with_release_delay(self, delay: float) -> QueueSpec:
        _ = PersistentWorkerConfig(
            release_delay=delay,
            min_interval=self.min_interval,
            circuit_breaker_failures=self.circuit_breaker_failures,
            circuit_breaker_cooldown=self.circuit_breaker_cooldown,
        )
        return replace(self, release_delay=delay)

    def with_min_interval(self, interval: float) -> QueueSpec:
        _ = PersistentWorkerConfig(
            release_delay=self.release_delay,
            min_interval=interval,
            circuit_breaker_failures=self.circuit_breaker_failures,
            circuit_breaker_cooldown=self.circuit_breaker_cooldown,
        )
        return replace(self, min_interval=interval)

    def build_queue(self, **kwargs: Any) -> PersistentQueue[Any]:
        if "policy_set" in kwargs and (
            self.delivery_policy is not None or self.dead_letter_policy is not None
        ):
            raise ValueError(
                "pass queue policies via QueueSpec or build_queue(policy_set=), not both"
            )
        if "retry_defaults" in kwargs and self._retry_items:
            raise ValueError(
                "pass retry settings via QueueSpec.with_retry() or "
                "build_queue(retry_defaults=), not both"
            )

        queue_kwargs = dict(kwargs)
        policy_set = self._build_policy_set()
        if policy_set is not None:
            queue_kwargs["policy_set"] = policy_set
        if self._retry_items:
            queue_kwargs["retry_defaults"] = self.retry_kwargs
        return PersistentQueue(spec=self, **queue_kwargs)

    def build_worker_config(self, **overrides: Any) -> PersistentWorkerConfig:
        config_kwargs: dict[str, Any] = {
            **self.retry_kwargs,
            "release_delay": self.release_delay,
            "min_interval": self.min_interval,
            "circuit_breaker_failures": self.circuit_breaker_failures,
            "circuit_breaker_cooldown": self.circuit_breaker_cooldown,
        }
        if self.dead_letter_on_failure is not None:
            config_kwargs["dead_letter_on_failure"] = self.dead_letter_on_failure
        config_kwargs.update(overrides)
        return PersistentWorkerConfig(**config_kwargs)

    def _build_policy_set(self) -> QueuePolicySet | None:
        if self.delivery_policy is None and self.dead_letter_policy is None:
            return None
        return QueuePolicySet(
            delivery_policy=self.delivery_policy,
            dead_letter_policy=self.dead_letter_policy,
        )
