"""Run the complete durable customer import in one process."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Sequence

from localqueue.bus import CsvRow, CsvSource, ExecutionResult

from examples.resumable_customer_import.demo_api import DemoCustomerApi
from examples.resumable_customer_import.events import CustomerCreationRequested
from examples.resumable_customer_import.producer import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_CSV_PATH,
    DEFAULT_DATA_DIR,
    DEFAULT_IMPORT_ID,
    DEFAULT_MAX_PENDING,
    default_checkpoint_name,
    to_customer_creation_requested,
)
from examples.resumable_customer_import.worker import build_bus


async def run_operation(
    csv_path: Path,
    data_dir: Path,
    *,
    import_id: str,
    batch_size: int,
    max_pending: int,
    checkpoint_name: str,
) -> ExecutionResult:
    """Declare, execute, and fully await one customer import."""
    bus = build_bus(data_dir, DemoCustomerApi())
    try:

        @bus.source(CsvSource(csv_path), checkpoint=checkpoint_name)
        def customer_source(row: CsvRow) -> CustomerCreationRequested:
            return to_customer_creation_requested(row, import_id=import_id)

        customer_source.config.batch_size = batch_size
        customer_source.config.max_pending = max_pending
        return await bus.execute(customer_source)
    finally:
        bus.close()


def print_report(result: ExecutionResult) -> None:
    """Print durable cumulative execution state."""
    print(f"execution ID:              {result.execution_id}")
    print(f"resumed:                   {result.resumed}")
    print(f"source items committed:    {result.items_committed}")
    print(f"events dispatched:         {result.events_dispatched}")
    print(f"events unrouted:           {result.events_unrouted}")
    print(f"deliveries inserted:       {result.deliveries_inserted}")
    print(f"deliveries deduplicated:   {result.deliveries_deduplicated}")
    print(f"deliveries total:          {result.deliveries_total}")
    print(f"acknowledged:              {result.deliveries_acknowledged}")
    print(f"failed:                    {result.deliveries_failed}")
    print(f"completed timestamp:       {result.completed_at.isoformat()}")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m examples.resumable_customer_import.run",
        description="Run and await the complete resumable customer import.",
    )
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV_PATH)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--import-id", default=DEFAULT_IMPORT_ID)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-pending", type=int, default=DEFAULT_MAX_PENDING)
    parser.add_argument("--checkpoint", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = asyncio.run(
        run_operation(
            args.csv,
            args.data_dir,
            import_id=args.import_id,
            batch_size=args.batch_size,
            max_pending=args.max_pending,
            checkpoint_name=args.checkpoint or default_checkpoint_name(args.import_id),
        )
    )
    print_report(result)
    result.raise_for_failures()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
