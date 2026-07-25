"""Declarative retry policies for EventBus subscriptions."""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Literal

from localqueue.policies import _delay_to_milliseconds

_MAX_STORAGE_INTEGER = 2**63 - 1


def _uniform(lower: float, upper: float) -> float:
    """Draw full jitter through one small, monkeypatchable boundary."""
    return random.uniform(lower, upper)


def _validate_max_attempts(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError("'max_attempts' must be an integer")
    if value < 1:
        raise ValueError("'max_attempts' must be at least 1")
    if value > _MAX_STORAGE_INTEGER:
        raise ValueError("'max_attempts' must fit the storage integer type")
    return value


def _validate_delay(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"'{field}' must be a number")
    delay = float(value)
    _delay_to_milliseconds(delay, field=field)
    return delay


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Immutable delivery retry budget and backoff for one subscription."""

    max_attempts: int
    _kind: Literal["fixed", "exponential"]
    _initial_delay: float
    _multiplier: float
    _max_delay: float
    _jitter: bool

    @classmethod
    def fixed(cls, *, max_attempts: int, delay: float) -> RetryPolicy:
        """Create a policy whose retries always use ``delay`` seconds."""
        attempts = _validate_max_attempts(max_attempts)
        fixed_delay = _validate_delay(delay, field="delay")
        return cls(
            max_attempts=attempts,
            _kind="fixed",
            _initial_delay=fixed_delay,
            _multiplier=1.0,
            _max_delay=fixed_delay,
            _jitter=False,
        )

    @classmethod
    def exponential(
        cls,
        *,
        max_attempts: int,
        initial_delay: float = 0.5,
        multiplier: float = 2.0,
        max_delay: float = 60.0,
        jitter: bool = True,
    ) -> RetryPolicy:
        """Create a capped exponential policy with optional full jitter."""
        attempts = _validate_max_attempts(max_attempts)
        initial = _validate_delay(initial_delay, field="initial_delay")
        maximum = _validate_delay(max_delay, field="max_delay")
        if isinstance(multiplier, bool) or not isinstance(multiplier, (int, float)):
            raise TypeError("'multiplier' must be a number")
        factor = float(multiplier)
        if not math.isfinite(factor) or factor <= 1:
            raise ValueError("'multiplier' must be finite and greater than 1")
        if not isinstance(jitter, bool):
            raise TypeError("'jitter' must be a boolean")
        if maximum < initial:
            raise ValueError("'max_delay' must be at least 'initial_delay'")
        return cls(
            max_attempts=attempts,
            _kind="exponential",
            _initial_delay=initial,
            _multiplier=factor,
            _max_delay=maximum,
            _jitter=jitter,
        )

    def _delay_for(self, attempt: int) -> float:
        """Return the delay after the current one-based ``attempt``."""
        if self._kind == "fixed":
            return self._initial_delay
        if self._initial_delay == 0 or self._max_delay == 0:
            base_delay = 0.0
        else:
            exponent = attempt - 1
            saturation_exponent = math.log(
                self._max_delay / self._initial_delay, self._multiplier
            )
            if exponent >= saturation_exponent:
                base_delay = self._max_delay
            else:
                base_delay = self._initial_delay * self._multiplier**exponent
        if self._jitter:
            return _uniform(0.0, base_delay)
        return base_delay


__all__ = ["RetryPolicy"]
