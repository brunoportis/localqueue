import math
import sqlite3
from asyncio import run

import pytest
from localqueue import DeliveryPolicy, Empty, FailureReason, SimpleQueue
from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    EventRegistry,
    Retry,
    RetryPolicy,
)
from localqueue.bus.consumer import _process_delivery
from localqueue.policies import _MAX_DELAY_SECONDS


def test_fixed_policy_is_immutable_and_compares_by_value() -> None:
    policy = RetryPolicy.fixed(max_attempts=3, delay=1)

    assert policy == RetryPolicy.fixed(max_attempts=3, delay=1.0)
    assert policy.max_attempts == 3
    with pytest.raises(AttributeError):
        policy.max_attempts = 4  # type: ignore[misc]


def test_policy_must_be_constructed_through_named_factories() -> None:
    with pytest.raises(TypeError, match=r"RetryPolicy\.fixed"):
        RetryPolicy()


@pytest.mark.parametrize("value", [True, 1.0, "3", object()])
def test_max_attempts_must_be_an_integer(value: object) -> None:
    with pytest.raises(TypeError, match="max_attempts.*integer"):
        RetryPolicy.fixed(max_attempts=value, delay=1)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [0, -1])
def test_max_attempts_must_be_positive(value: int) -> None:
    with pytest.raises(ValueError, match="max_attempts.*at least 1"):
        RetryPolicy.fixed(max_attempts=value, delay=1)


def test_max_attempts_must_fit_storage_integer() -> None:
    with pytest.raises(ValueError, match="max_attempts.*storage"):
        RetryPolicy.fixed(max_attempts=2**63, delay=1)


@pytest.mark.parametrize("value", [True, "1", object()])
def test_delays_must_be_numbers(value: object) -> None:
    with pytest.raises(TypeError, match="delay.*number"):
        RetryPolicy.fixed(max_attempts=2, delay=value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "value", [-1, math.inf, -math.inf, math.nan, _MAX_DELAY_SECONDS + 1]
)
def test_delays_must_be_finite_non_negative_and_storage_safe(value: float) -> None:
    with pytest.raises(ValueError, match="delay"):
        RetryPolicy.fixed(max_attempts=2, delay=value)


@pytest.mark.parametrize("value", [True, "2", object()])
def test_multiplier_must_be_a_number(value: object) -> None:
    with pytest.raises(TypeError, match="multiplier.*number"):
        RetryPolicy.exponential(max_attempts=2, multiplier=value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [1, 0, -1, math.inf, math.nan])
def test_multiplier_must_be_finite_and_greater_than_one(value: float) -> None:
    with pytest.raises(ValueError, match="multiplier.*greater than 1"):
        RetryPolicy.exponential(max_attempts=2, multiplier=value)


def test_jitter_must_be_boolean() -> None:
    with pytest.raises(TypeError, match="jitter.*boolean"):
        RetryPolicy.exponential(max_attempts=2, jitter=1)  # type: ignore[arg-type]


def test_max_delay_must_not_be_less_than_initial_delay() -> None:
    with pytest.raises(ValueError, match="max_delay.*initial_delay"):
        RetryPolicy.exponential(max_attempts=2, initial_delay=2, max_delay=1)


def test_fixed_delay_is_constant() -> None:
    policy = RetryPolicy.fixed(max_attempts=5, delay=10)

    assert [policy._delay_for(attempt) for attempt in range(1, 5)] == [10.0] * 4


def test_exponential_delay_uses_current_one_based_attempt() -> None:
    policy = RetryPolicy.exponential(
        max_attempts=8,
        initial_delay=0.5,
        multiplier=2,
        max_delay=60,
        jitter=False,
    )

    assert [policy._delay_for(attempt) for attempt in range(1, 5)] == [
        0.5,
        1.0,
        2.0,
        4.0,
    ]


def test_exponential_delay_saturates_without_computing_a_huge_power() -> None:
    policy = RetryPolicy.exponential(
        max_attempts=2**31,
        initial_delay=0.5,
        multiplier=2,
        max_delay=60,
        jitter=False,
    )

    assert policy._delay_for(2**30) == 60.0


def test_exponential_delay_saturates_for_subnormal_initial_delay() -> None:
    policy = RetryPolicy.exponential(
        max_attempts=3000,
        initial_delay=1e-320,
        multiplier=2,
        max_delay=60,
        jitter=False,
    )

    assert policy._delay_for(2000) == 60.0


def test_exponential_delay_uses_logarithmic_path_without_saturating() -> None:
    policy = RetryPolicy.exponential(
        max_attempts=2000,
        initial_delay=1e-320,
        multiplier=2,
        max_delay=60,
        jitter=False,
    )

    delay = policy._delay_for(1041)

    assert 0 < delay < 60


def test_zero_initial_delay_keeps_exponential_retries_immediate() -> None:
    policy = RetryPolicy.exponential(
        max_attempts=3,
        initial_delay=0,
        jitter=False,
    )

    assert policy._delay_for(1) == 0
    assert policy._delay_for(2) == 0


def test_full_jitter_uses_single_injectable_uniform_draw(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    draws: list[tuple[float, float]] = []

    def deterministic_uniform(lower: float, upper: float) -> float:
        draws.append((lower, upper))
        return upper / 4

    monkeypatch.setattr("localqueue.bus.retry._uniform", deterministic_uniform)
    policy = RetryPolicy.exponential(
        max_attempts=3,
        initial_delay=2,
        multiplier=2,
        max_delay=10,
        jitter=True,
    )

    assert policy._delay_for(2) == 1.0
    assert draws == [(0.0, 4.0)]


class RetryEventA(BaseEvent):
    value: int


class RetryEventB(BaseEvent):
    value: int


def test_retry_policy_belongs_to_subscription_and_omission_inherits(
    tmp_path,
) -> None:
    policy = RetryPolicy.fixed(max_attempts=3, delay=1)
    bus = EventBus(str(tmp_path), topology=BusTopology({"workers": ["*"]}))
    try:
        bus.on(RetryEventA, lambda event: None, subscription="workers", retry=policy)
        bus.on(RetryEventB, lambda event: None, subscription="workers")

        assert bus._retry_for("workers") == policy
    finally:
        bus.close()


def test_registration_order_does_not_change_subscription_policy(tmp_path) -> None:
    policy = RetryPolicy.fixed(max_attempts=3, delay=1)
    bus = EventBus(str(tmp_path), topology=BusTopology({"workers": ["*"]}))
    try:
        bus.on(RetryEventA, lambda event: None, subscription="workers")
        bus.on(RetryEventB, lambda event: None, subscription="workers", retry=policy)

        assert bus._retry_for("workers") == policy
    finally:
        bus.close()


def test_structurally_equal_policies_are_compatible(tmp_path) -> None:
    bus = EventBus(str(tmp_path), topology=BusTopology({"workers": ["*"]}))
    try:
        bus.on(
            RetryEventA,
            lambda event: None,
            subscription="workers",
            retry=RetryPolicy.fixed(max_attempts=3, delay=1),
        )
        bus.on(
            RetryEventB,
            lambda event: None,
            subscription="workers",
            retry=RetryPolicy.fixed(max_attempts=3, delay=1),
        )
    finally:
        bus.close()


def test_conflicting_policy_fails_without_partial_registration(tmp_path) -> None:
    original = RetryPolicy.fixed(max_attempts=3, delay=1)
    registry = EventRegistry()
    bus = EventBus(str(tmp_path), registry=registry)
    bus.handler(
        RetryEventA,
        lambda event: None,
        subscription="workers",
        concurrency=2,
        retry=original,
    )
    topology = bus.topology
    handlers = dict(bus._handlers)
    concurrency = dict(bus._subscription_concurrency)
    try:
        with pytest.raises(ValueError, match="retry policy"):
            bus.handler(
                RetryEventB,
                lambda event: None,
                subscription="workers",
                concurrency=4,
                retry=RetryPolicy.exponential(max_attempts=8),
            )

        assert bus.topology is topology
        assert bus._handlers == handlers
        assert bus._subscription_concurrency == concurrency
        assert bus._retry_for("workers") == original
        assert registry.resolve("RetryEventB") is None
    finally:
        bus.close()


def test_registry_conflict_does_not_commit_subscription_configuration(
    tmp_path,
) -> None:
    class EventClassA(BaseEvent):
        event_name = "shared.event-type"

    class EventClassB(BaseEvent):
        event_name = "shared.event-type"

    class ValidEvent(BaseEvent):
        event_name = "valid.event-type"

    registry = EventRegistry()
    registry.register(EventClassA)
    bus = EventBus(str(tmp_path), concurrency=2, registry=registry)
    original_topology = bus.topology
    failed_retry = RetryPolicy.fixed(max_attempts=3, delay=1)

    try:
        with pytest.raises(ValueError, match="already registered"):
            bus.handler(
                EventClassB,
                lambda event: None,
                subscription="workers",
                concurrency=7,
                retry=failed_retry,
            )

        assert bus.topology is original_topology
        assert bus._handlers == {}
        assert bus._concurrency_for("workers") == 2
        assert bus._retry_for("workers") is None

        valid_retry = RetryPolicy.exponential(max_attempts=4)
        bus.handler(
            ValidEvent,
            lambda event: None,
            subscription="workers",
            concurrency=5,
            retry=valid_retry,
        )
        assert bus.subscription("workers").concurrency == 5
        assert bus.subscription("workers").config.retry == valid_retry
        assert registry.resolve(ValidEvent.event_name) is ValidEvent
    finally:
        bus.close()


def test_retry_parameter_rejects_invalid_values_without_mutation(tmp_path) -> None:
    bus = EventBus(str(tmp_path))
    try:
        with pytest.raises(TypeError, match="retry.*RetryPolicy"):
            bus.handler(RetryEventA, lambda event: None, retry="fixed")  # type: ignore[arg-type]
        assert bus.topology.subscription_names == ()
        assert bus._handlers == {}
    finally:
        bus.close()


def test_exact_and_wildcard_handlers_share_subscription_policy(tmp_path) -> None:
    policy = RetryPolicy.exponential(max_attempts=4)
    bus = EventBus(str(tmp_path), topology=BusTopology({"workers": ["*"]}))
    try:
        bus.on("*", lambda event: None, subscription="workers", retry=policy)
        bus.on(RetryEventA, lambda event: None, subscription="workers", retry=policy)
        assert bus._retry_for("workers") == policy
    finally:
        bus.close()


def _message_row(tmp_path, message_id: int) -> tuple[int, int, int, str | None]:
    with sqlite3.connect(tmp_path / "localqueue.db") as connection:
        return connection.execute(
            "SELECT attempts, max_attempts, available_at, failure_reason "
            "FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()


def _process_once(bus: EventBus[None], subscription: str) -> int:
    queue = bus._open_subscription_queue(subscription)
    try:
        policy = bus._retry_for(subscription)
        assert policy is not None
        job = queue._get_with_max_attempts(
            max_attempts=policy.max_attempts, block=False
        )
        run(_process_delivery(bus, subscription, queue, job))
        return job.id
    finally:
        queue.close()


def test_fixed_backoff_persists_available_at(tmp_path) -> None:
    bus = EventBus(
        str(tmp_path),
        delivery=DeliveryPolicy(lease_seconds=10, max_retries=1),
    )

    @bus.handler(RetryEventA, retry=RetryPolicy.fixed(max_attempts=3, delay=10))
    def handle(event):
        raise RuntimeError("transient")

    try:
        receipt = bus.dispatch(RetryEventA(value=1))
        message_id = _process_once(bus, "RetryEventA")
        attempts, max_attempts, available_at, reason = _message_row(
            tmp_path, message_id
        )
        assert message_id == receipt.message_ids[0]
        assert attempts == 1
        assert max_attempts == 3
        assert 9_900 <= available_at - int(__import__("time").time() * 1000) <= 10_000
        assert reason is None
    finally:
        bus.close()


def test_retry_after_zero_overrides_policy_backoff(tmp_path) -> None:
    bus = EventBus(str(tmp_path))

    @bus.handler(RetryEventA, retry=RetryPolicy.fixed(max_attempts=2, delay=60))
    def handle(event):
        raise Retry("now", after=0)

    try:
        bus.dispatch(RetryEventA(value=1))
        message_id = _process_once(bus, "RetryEventA")
        _, _, available_at, _ = _message_row(tmp_path, message_id)
        assert abs(available_at - int(__import__("time").time() * 1000)) < 1_000
    finally:
        bus.close()


def test_policy_allows_exactly_max_attempts_and_preserves_last_error(tmp_path) -> None:
    bus = EventBus(str(tmp_path))
    attempts: list[int] = []

    @bus.handler(RetryEventA, retry=RetryPolicy.fixed(max_attempts=3, delay=0))
    def handle(event, ctx):
        attempts.append(ctx.attempt)
        raise RuntimeError(f"failure {ctx.attempt}")

    try:
        bus.dispatch(RetryEventA(value=1))
        run(bus.run(idle_timeout=0.05))
        failed = bus.subscription("RetryEventA").list_failed()
        assert attempts == [1, 2, 3]
        assert failed[0].reason is FailureReason.RETRIES_EXHAUSTED
        assert failed[0].last_error == "failure 3"
    finally:
        bus.close()


def test_max_attempts_one_does_not_retry(tmp_path) -> None:
    bus = EventBus(str(tmp_path))
    calls = 0

    @bus.handler(RetryEventA, retry=RetryPolicy.fixed(max_attempts=1, delay=0))
    def handle(event):
        nonlocal calls
        calls += 1
        raise Retry("no budget")

    try:
        bus.dispatch(RetryEventA(value=1))
        run(bus.run(idle_timeout=0.05))
        assert calls == 1
        assert (
            bus.subscription("RetryEventA").list_failed()[0].reason
            is FailureReason.RETRIES_EXHAUSTED
        )
    finally:
        bus.close()


def test_expired_lease_reclaim_uses_larger_override(tmp_path) -> None:
    queue = SimpleQueue(
        str(tmp_path),
        delivery=DeliveryPolicy(lease_seconds=10, max_retries=1),
    )
    try:
        queue.put({"value": 1})
        first = queue._get_with_max_attempts(max_attempts=5, block=False)
        with sqlite3.connect(tmp_path / "localqueue.db") as connection:
            connection.execute(
                "UPDATE messages SET lease_until = 0 WHERE id = ?", (first.id,)
            )
        second = queue._get_with_max_attempts(max_attempts=5, block=False)
        attempts, max_attempts, _, _ = _message_row(tmp_path, second.id)
        assert attempts == 2
        assert max_attempts == 5
    finally:
        queue.close()


def test_expired_lease_reclaim_uses_smaller_override(tmp_path) -> None:
    queue = SimpleQueue(
        str(tmp_path),
        delivery=DeliveryPolicy(lease_seconds=10, max_retries=8),
    )
    try:
        queue.put({"value": 1})
        job = queue._get_with_max_attempts(max_attempts=1, block=False)
        with sqlite3.connect(tmp_path / "localqueue.db") as connection:
            connection.execute(
                "UPDATE messages SET lease_until = 0 WHERE id = ?", (job.id,)
            )
        with pytest.raises(Exception, match="empty"):
            queue._get_with_max_attempts(max_attempts=1, block=False)
        assert _message_row(tmp_path, job.id)[3] == "retries_exhausted"
    finally:
        queue.close()


def test_ready_message_exhausted_by_smaller_override_fails_without_claim(
    tmp_path,
) -> None:
    queue = SimpleQueue(
        str(tmp_path),
        delivery=DeliveryPolicy(lease_seconds=10, max_retries=7),
    )
    try:
        message_id = queue.put({"value": 1})
        with sqlite3.connect(tmp_path / "localqueue.db") as connection:
            connection.execute(
                "UPDATE messages SET attempts = 3 WHERE id = ?", (message_id,)
            )

        with pytest.raises(Empty):
            queue._get_with_max_attempts(max_attempts=2, block=False)

        attempts, max_attempts, _, reason = _message_row(tmp_path, message_id)
        assert attempts == 3
        assert max_attempts == 2
        assert reason == "retries_exhausted"
    finally:
        queue.close()


def test_smaller_policy_does_not_run_handler_after_ready_budget_is_exhausted(
    tmp_path,
) -> None:
    bus = EventBus(str(tmp_path))
    calls = 0

    @bus.handler(RetryEventA, retry=RetryPolicy.fixed(max_attempts=2, delay=0))
    def handle(event):
        nonlocal calls
        calls += 1

    try:
        receipt = bus.dispatch(RetryEventA(value=1))
        message_id = receipt.message_ids[0]
        with sqlite3.connect(tmp_path / "localqueue.db") as connection:
            connection.execute(
                "UPDATE messages SET attempts = 3, max_attempts = 8 WHERE id = ?",
                (message_id,),
            )

        run(bus.run(idle_timeout=0.05))

        attempts, max_attempts, _, reason = _message_row(tmp_path, message_id)
        assert calls == 0
        assert attempts == 3
        assert max_attempts == 2
        assert reason == "retries_exhausted"
    finally:
        bus.close()


def test_ready_message_below_smaller_override_allows_exact_final_attempt(
    tmp_path,
) -> None:
    queue = SimpleQueue(
        str(tmp_path),
        delivery=DeliveryPolicy(lease_seconds=10, max_retries=7),
    )
    try:
        message_id = queue.put({"value": 1})
        with sqlite3.connect(tmp_path / "localqueue.db") as connection:
            connection.execute(
                "UPDATE messages SET attempts = 1 WHERE id = ?", (message_id,)
            )

        job = queue._get_with_max_attempts(max_attempts=2, block=False)

        attempts, max_attempts, _, reason = _message_row(tmp_path, message_id)
        assert job.id == message_id
        assert job.attempts + 1 == 2
        assert attempts == 2
        assert max_attempts == 2
        assert reason is None
    finally:
        queue.close()


def test_claim_skips_ready_message_exhausted_by_override(tmp_path) -> None:
    queue = SimpleQueue(
        str(tmp_path),
        delivery=DeliveryPolicy(lease_seconds=10, max_retries=7),
    )
    try:
        exhausted_id = queue.put({"value": 1})
        eligible_id = queue.put({"value": 2})
        with sqlite3.connect(tmp_path / "localqueue.db") as connection:
            connection.execute(
                "UPDATE messages SET attempts = 3 WHERE id = ?", (exhausted_id,)
            )

        job = queue._get_with_max_attempts(max_attempts=2, block=False)

        assert job.id == eligible_id
        attempts, max_attempts, _, reason = _message_row(tmp_path, exhausted_id)
        assert attempts == 3
        assert max_attempts == 2
        assert reason == "retries_exhausted"
    finally:
        queue.close()
