import math

import pytest

from localqueue.bus import RetryPolicy
from localqueue.policies import _MAX_DELAY_SECONDS


def test_fixed_policy_is_immutable_and_compares_by_value() -> None:
    policy = RetryPolicy.fixed(max_attempts=3, delay=1)

    assert policy == RetryPolicy.fixed(max_attempts=3, delay=1.0)
    assert policy.max_attempts == 3
    with pytest.raises(AttributeError):
        policy.max_attempts = 4  # type: ignore[misc]


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
        RetryPolicy.exponential(
            max_attempts=2, initial_delay=2, max_delay=1
        )


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


def test_full_jitter_uses_single_injectable_uniform_draw(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    draws: list[tuple[float, float]] = []

    def deterministic_uniform(lower: float, upper: float) -> float:
        draws.append((lower, upper))
        return upper / 4

    monkeypatch.setattr(
        "localqueue.bus.retry._uniform", deterministic_uniform
    )
    policy = RetryPolicy.exponential(
        max_attempts=3,
        initial_delay=2,
        multiplier=2,
        max_delay=10,
        jitter=True,
    )

    assert policy._delay_for(2) == 1.0
    assert draws == [(0.0, 4.0)]
