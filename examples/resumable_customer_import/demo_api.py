"""Self-contained demo customer API for the worker example.

No real server, no dependencies: ``DemoCustomerApi`` is an in-memory,
deterministic implementation of the small ``CustomerApi`` protocol. It
models the outcomes a real external customer service exhibits so the worker
can map them cleanly to ``Reject`` and ``Retry``:

- normal customers are created successfully;
- one designated email fails validation (permanent);
- one designated email is temporarily unavailable for the first two
  attempts, then succeeds;
- one designated email is rate limited once with ``retry_after``, then
  succeeds;
- a repeated idempotency key returns the exact same successful result;
- an existing external ID under a *different* idempotency key returns the
  existing customer instead of creating another.

The process-local in-memory stores are fine for the demo. A real external
service MUST persist idempotency keys durably — see the README.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

__all__ = [
    "CustomerApi",
    "CustomerApiError",
    "CustomerApiUnavailable",
    "CustomerRateLimited",
    "CustomerResult",
    "CustomerValidationError",
    "DemoCustomerApi",
]

#: Designated emails driving the deterministic demo outcomes.
VALIDATION_FAILURE_EMAIL = "invalid@example.com"
TEMPORARY_FAILURE_EMAIL = "flaky@example.com"
RATE_LIMITED_EMAIL = "throttled@example.com"


class CustomerApiError(Exception):
    """Base class for modeled customer API failures."""


class CustomerValidationError(CustomerApiError):
    """The customer payload is invalid; retrying will never help."""


class CustomerApiUnavailable(CustomerApiError):
    """The service is temporarily unavailable; retry later."""


class CustomerRateLimited(CustomerApiError):
    """The service rejected the call with rate limiting."""

    def __init__(self, message: str, *, retry_after: float) -> None:
        super().__init__(message)
        self.retry_after = retry_after


@dataclass(frozen=True, slots=True)
class CustomerResult:
    """Outcome of one create call.

    ``created`` is ``False`` when the API returned a pre-existing customer
    (same external ID under a different idempotency key) instead of creating
    a new one.
    """

    customer_id: str
    external_id: str
    created: bool


class CustomerApi(Protocol):
    """Minimal async customer-service contract used by the worker."""

    async def create_customer(
        self,
        *,
        idempotency_key: str,
        external_id: str,
        name: str,
        email: str,
        phone: str,
    ) -> CustomerResult:
        """Create one customer idempotently, or raise a modeled failure."""
        ...


class DemoCustomerApi:
    """Deterministic in-memory ``CustomerApi`` for demos and tests."""

    def __init__(
        self,
        *,
        temporary_failure_attempts: int = 2,
        rate_limit_retry_after: float = 0.5,
    ) -> None:
        self._temporary_failure_attempts = temporary_failure_attempts
        self._rate_limit_retry_after = rate_limit_retry_after
        self._by_idempotency_key: dict[str, CustomerResult] = {}
        self._by_external_id: dict[str, CustomerResult] = {}
        self._attempts: dict[str, int] = {}
        self._sequence = 0

    async def create_customer(
        self,
        *,
        idempotency_key: str,
        external_id: str,
        name: str,
        email: str,
        phone: str,
    ) -> CustomerResult:
        """Create one customer with deterministic, idempotent outcomes."""
        remembered = self._by_idempotency_key.get(idempotency_key)
        if remembered is not None:
            # Same key, same result: the idempotency contract.
            return remembered

        attempts = self._attempts.get(idempotency_key, 0) + 1
        self._attempts[idempotency_key] = attempts

        if email == VALIDATION_FAILURE_EMAIL:
            raise CustomerValidationError(
                f"customer {external_id!r} rejected: invalid payload"
            )
        if email == TEMPORARY_FAILURE_EMAIL and attempts <= (
            self._temporary_failure_attempts
        ):
            raise CustomerApiUnavailable(
                f"service temporarily unavailable (attempt {attempts})"
            )
        if email == RATE_LIMITED_EMAIL and attempts == 1:
            raise CustomerRateLimited(
                "rate limit exceeded",
                retry_after=self._rate_limit_retry_after,
            )

        existing = self._by_external_id.get(external_id)
        if existing is not None:
            # Same external ID under a new key: return the existing customer
            # instead of creating a duplicate.
            result = CustomerResult(
                customer_id=existing.customer_id,
                external_id=existing.external_id,
                created=False,
            )
        else:
            self._sequence += 1
            result = CustomerResult(
                customer_id=f"cus-{self._sequence:04d}",
                external_id=external_id,
                created=True,
            )
            self._by_external_id[external_id] = result
        self._by_idempotency_key[idempotency_key] = result
        return result
