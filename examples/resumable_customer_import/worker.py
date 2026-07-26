"""Worker: consumes customer creation requests against the demo API.

Registers local handlers for the shared topology subscriptions. The creator
handler calls the injected ``CustomerApi`` with ``idempotency_key=
ctx.event_id`` and maps modeled API failures to ``Reject``/``Retry``; on
success it returns ``CustomerCreated``, which the bus fans out atomically
with the local ACK (no manual dispatch). The audit handler prints a compact
line per created customer.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Sequence

from localqueue.bus import (
    EventBus,
    HandlerContext,
    Reject,
    Retry,
    RetryPolicy,
    RuntimeContext,
)

from examples.resumable_customer_import.demo_api import (
    CustomerApi,
    CustomerApiUnavailable,
    CustomerRateLimited,
    CustomerValidationError,
    DemoCustomerApi,
)
from examples.resumable_customer_import.events import (
    CustomerCreated,
    CustomerCreationRequested,
)
from examples.resumable_customer_import.topology import (
    BUS_NAME,
    CUSTOMER_AUDIT,
    CUSTOMER_CREATOR,
    TOPOLOGY,
)

EXAMPLE_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = EXAMPLE_DIR / "data"

RETRY_POLICY = RetryPolicy.exponential(
    max_attempts=8,
    initial_delay=0.5,
    multiplier=2.0,
    max_delay=30.0,
    jitter=True,
)


class CustomerWorkerContext(HandlerContext):
    """Typed handler context injecting the customer API adapter per attempt."""

    def __init__(self, runtime: RuntimeContext, api: CustomerApi) -> None:
        super().__init__(runtime)
        self.api = api


async def create_customer(
    event: CustomerCreationRequested, ctx: CustomerWorkerContext
) -> CustomerCreated:
    """Create one customer via the injected API.

    Only modeled API failures are caught: validation maps to ``Reject``,
    temporary unavailability to ``Retry``, and rate limiting to
    ``Retry(after=retry_after)``. Anything else propagates to the bus.
    """
    try:
        result = await ctx.api.create_customer(
            idempotency_key=ctx.event_id,
            external_id=event.external_id,
            name=event.name,
            email=event.email,
            phone=event.phone,
        )
    except CustomerValidationError as error:
        raise Reject(str(error), category="validation") from error
    except CustomerRateLimited as error:
        raise Retry(str(error), after=error.retry_after) from error
    except CustomerApiUnavailable as error:
        raise Retry(str(error)) from error
    return CustomerCreated(
        import_id=event.import_id,
        external_id=event.external_id,
        customer_id=result.customer_id,
    )


async def audit_customer_created(
    event: CustomerCreated, ctx: CustomerWorkerContext
) -> None:
    """Print a compact audit line for each created customer."""
    print(
        f"audit import={event.import_id} "
        f"external_id={event.external_id} "
        f"customer_id={event.customer_id}"
    )


#: Process-local concurrency bound for the creator subscription. Customer
#: creation calls are independent, so up to this many deliveries are handled
#: concurrently within one worker process. The audit subscription keeps the
#: default concurrency and is unaffected.
CREATOR_CONCURRENCY = 20


def build_bus(data_dir: Path, api: CustomerApi) -> EventBus[CustomerWorkerContext]:
    """Build the worker bus with its context factory and local handlers."""
    bus: EventBus[CustomerWorkerContext] = EventBus(
        str(data_dir),
        name=BUS_NAME,
        topology=TOPOLOGY,
        context_factory=lambda runtime: CustomerWorkerContext(runtime, api),
    )
    bus.subscription(CUSTOMER_CREATOR, concurrency=CREATOR_CONCURRENCY).handler(
        CustomerCreationRequested,
        create_customer,
        retry=RETRY_POLICY,
    )
    bus.subscription(CUSTOMER_AUDIT).handler(CustomerCreated, audit_customer_created)
    return bus


async def run_worker(data_dir: Path, api: CustomerApi) -> None:
    """Consume all registered subscriptions until cancelled."""
    bus = build_bus(data_dir, api)
    try:
        await bus.run()
    finally:
        bus.close()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the worker command line."""
    parser = argparse.ArgumentParser(
        prog="python -m examples.resumable_customer_import.worker",
        description="Consume customer creation requests from the event bus.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="queue data directory (default: the example's data/ directory)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    try:
        asyncio.run(run_worker(args.data_dir, DemoCustomerApi()))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
