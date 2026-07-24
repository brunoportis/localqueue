from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError

import pytest
from localqueue import DeliveryPolicy, DurabilityMode, SimpleQueue
from localqueue.bus import BusTopology, EventBus


def test_durability_modes_have_stable_public_values() -> None:
    assert DurabilityMode.RELAXED.value == "relaxed"
    assert DurabilityMode.DURABLE.value == "durable"


class TestDeliveryPolicy:
    def test_defaults_preserve_existing_delivery_contract(self) -> None:
        policy = DeliveryPolicy()

        assert policy.lease_seconds == 60.0
        assert policy.max_retries == 3

    @pytest.mark.parametrize("lease_seconds", [0, -1, float("nan"), float("inf")])
    def test_rejects_non_positive_or_non_finite_lease(
        self, lease_seconds: float
    ) -> None:
        with pytest.raises(ValueError, match="lease_seconds.*positive finite"):
            DeliveryPolicy(lease_seconds=lease_seconds)

    @pytest.mark.parametrize("lease_seconds", [True, "30", object()])
    def test_rejects_non_numeric_lease(self, lease_seconds: object) -> None:
        with pytest.raises(TypeError, match="lease_seconds.*number"):
            DeliveryPolicy(lease_seconds=lease_seconds)  # type: ignore[arg-type]

    @pytest.mark.parametrize("lease_seconds", [1, 0.25])
    def test_accepts_integer_and_float_lease(self, lease_seconds: float) -> None:
        policy = DeliveryPolicy(lease_seconds=lease_seconds)

        assert policy.lease_seconds == float(lease_seconds)
        assert isinstance(policy.lease_seconds, float)

    def test_rejects_negative_retries(self) -> None:
        with pytest.raises(ValueError, match="max_retries.*non-negative"):
            DeliveryPolicy(max_retries=-1)

    @pytest.mark.parametrize("max_retries", [True, 1.0, "1", object()])
    def test_rejects_non_integer_retries(self, max_retries: object) -> None:
        with pytest.raises(TypeError, match="max_retries.*integer"):
            DeliveryPolicy(max_retries=max_retries)  # type: ignore[arg-type]

    def test_is_immutable(self) -> None:
        policy = DeliveryPolicy()

        with pytest.raises(FrozenInstanceError):
            policy.max_retries = 5  # type: ignore[misc]


class TestSimpleQueuePolicies:
    def test_common_constructor_uses_semantic_defaults(self, tmp_path) -> None:
        queue = SimpleQueue(str(tmp_path))
        try:
            assert queue.delivery == DeliveryPolicy()
            assert queue.durability is DurabilityMode.RELAXED
            assert queue.diagnostics().synchronous == 1
        finally:
            queue.close()

    @pytest.mark.parametrize(
        ("durability", "synchronous"),
        [
            (DurabilityMode.RELAXED, 1),
            (DurabilityMode.DURABLE, 2),
        ],
    )
    def test_durability_propagates_to_native_layer(
        self,
        tmp_path,
        durability: DurabilityMode,
        synchronous: int,
    ) -> None:
        queue = SimpleQueue(str(tmp_path / durability.value), durability=durability)
        try:
            assert queue.durability is durability
            assert queue.diagnostics().synchronous == synchronous
        finally:
            queue.close()

    @pytest.mark.parametrize("durability", ["relaxed", True, False, object()])
    def test_rejects_invalid_durability(self, tmp_path, durability: object) -> None:
        with pytest.raises(TypeError, match="durability.*DurabilityMode"):
            SimpleQueue(
                str(tmp_path / "invalid"),
                durability=durability,  # type: ignore[arg-type]
            )

    def test_uses_delivery_policy_for_leases_and_retries(self, tmp_path) -> None:
        policy = DeliveryPolicy(lease_seconds=0.05, max_retries=0)
        queue = SimpleQueue(str(tmp_path), delivery=policy)
        try:
            assert queue.delivery is policy
            queue.put({"task": "one"})
            job = queue.get_nowait()
            queue.nack(job)
            assert queue.stats()["failed"] == 1
        finally:
            queue.close()

    def test_old_constructor_parameters_are_not_supported(self) -> None:
        parameters = inspect.signature(SimpleQueue).parameters

        assert "fsync" not in parameters
        assert "lease_seconds" not in parameters
        assert "max_retries" not in parameters

    @pytest.mark.parametrize(
        "legacy", [{"fsync": True}, {"lease_seconds": 30}, {"max_retries": 5}]
    )
    def test_old_constructor_calls_fail_immediately(self, tmp_path, legacy) -> None:
        with pytest.raises(TypeError, match="unexpected keyword"):
            SimpleQueue(str(tmp_path), **legacy)

    def test_rejects_non_policy_delivery(self, tmp_path) -> None:
        with pytest.raises(TypeError, match="delivery.*DeliveryPolicy"):
            SimpleQueue(
                str(tmp_path),
                delivery=object(),  # type: ignore[arg-type]
            )


class TestEventBusPolicies:
    def test_common_constructor_uses_same_defaults_for_fanout_and_subscription(
        self, tmp_path
    ) -> None:
        bus = EventBus(str(tmp_path), topology=BusTopology({"events": ["*"]}))
        try:
            assert bus.delivery == DeliveryPolicy()
            assert bus.durability is DurabilityMode.RELAXED
            _, fanout_synchronous = bus._get_native().pragma_settings()
            subscription = bus._open_subscription_queue("events")
            try:
                assert subscription.delivery is bus.delivery
                assert subscription.durability is bus.durability
                assert subscription.diagnostics().synchronous == 1
            finally:
                subscription.close()
            assert fanout_synchronous == 1
        finally:
            bus.close()

    @pytest.mark.parametrize(
        ("durability", "synchronous"),
        [
            (DurabilityMode.RELAXED, 1),
            (DurabilityMode.DURABLE, 2),
        ],
    )
    def test_propagates_durability_to_fanout_and_subscription_queues(
        self,
        tmp_path,
        durability: DurabilityMode,
        synchronous: int,
    ) -> None:
        policy = DeliveryPolicy(lease_seconds=30, max_retries=5)
        bus = EventBus(
            str(tmp_path / durability.value),
            topology=BusTopology({"events": ["*"]}),
            delivery=policy,
            durability=durability,
        )
        try:
            _, fanout_synchronous = bus._get_native().pragma_settings()
            subscription = bus._open_subscription_queue("events")
            try:
                assert subscription.delivery is policy
                assert subscription.durability is durability
                assert subscription.diagnostics().lease_seconds == 30.0
                assert subscription.diagnostics().max_retries == 5
                assert subscription.diagnostics().synchronous == synchronous
            finally:
                subscription.close()
            assert fanout_synchronous == synchronous
        finally:
            bus.close()

    def test_old_constructor_parameters_are_not_supported(self) -> None:
        parameters = inspect.signature(EventBus).parameters

        assert "fsync" not in parameters
        assert "lease_seconds" not in parameters
        assert "max_retries" not in parameters

    @pytest.mark.parametrize(
        "legacy", [{"fsync": True}, {"lease_seconds": 30}, {"max_retries": 5}]
    )
    def test_old_constructor_calls_fail_immediately(self, tmp_path, legacy) -> None:
        with pytest.raises(TypeError, match="unexpected keyword"):
            EventBus(
                str(tmp_path),
                topology=BusTopology({}),
                **legacy,
            )

    @pytest.mark.parametrize("durability", ["durable", True, object()])
    def test_rejects_invalid_durability(self, tmp_path, durability: object) -> None:
        with pytest.raises(TypeError, match="durability.*DurabilityMode"):
            EventBus(
                str(tmp_path),
                topology=BusTopology({}),
                durability=durability,  # type: ignore[arg-type]
            )

    def test_rejects_non_policy_delivery(self, tmp_path) -> None:
        with pytest.raises(TypeError, match="delivery.*DeliveryPolicy"):
            EventBus(
                str(tmp_path),
                topology=BusTopology({}),
                delivery=object(),  # type: ignore[arg-type]
            )
