"""Producer/waiter for the durable finite customer-import execution.

Reads ``customers.csv`` with ``CsvSource``, transforms each row exactly once
into ``CustomerCreationRequested``, and waits for workers sharing the database
to make every execution-owned delivery terminal.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Sequence

from localqueue.bus import (
    CsvRow,
    CsvSource,
    EventBus,
    ExecutionResult,
    SourceDefinition,
)

from examples.resumable_customer_import.events import CustomerCreationRequested
from examples.resumable_customer_import.topology import BUS_NAME, TOPOLOGY

EXAMPLE_DIR = Path(__file__).resolve().parent
DEFAULT_CSV_PATH = EXAMPLE_DIR / "customers.csv"
DEFAULT_DATA_DIR = EXAMPLE_DIR / "data"
DEFAULT_IMPORT_ID = "demo-v1"
DEFAULT_BATCH_SIZE = 1_000
DEFAULT_MAX_PENDING = 50_000


def default_checkpoint_name(import_id: str) -> str:
    """Return the operation-scoped default checkpoint name."""
    return f"customer-import:{import_id}"


def to_customer_creation_requested(
    row: CsvRow, *, import_id: str
) -> CustomerCreationRequested:
    """Transform one CSV row into the creation event, normalizing fields.

    The CSV carries no ``import_id`` column: the logical import identity is
    the ``--import-id`` CLI value, injected here into every event. Whitespace
    is stripped everywhere and the email is lowercased, so rows that differ
    only in padding or case collapse to the same durable identity and
    payload (and are deduplicated on re-ingestion).
    """
    return CustomerCreationRequested(
        import_id=import_id,
        external_id=row["external_id"].strip(),
        name=row["name"].strip(),
        email=row["email"].strip().lower(),
        phone=row["phone"].strip(),
    )


def build_customer_source(
    bus: EventBus,
    csv_path: Path,
    *,
    import_id: str,
    batch_size: int,
    max_pending: int,
    checkpoint_name: str,
) -> SourceDefinition[CsvRow, CustomerCreationRequested]:
    """Declare the finite CSV source shared by both example entry points."""

    @bus.source(CsvSource(csv_path), checkpoint=checkpoint_name)
    def customer_source(row: CsvRow) -> CustomerCreationRequested:
        return to_customer_creation_requested(row, import_id=import_id)

    customer_source.config.batch_size = batch_size
    customer_source.config.max_pending = max_pending
    return customer_source


async def run_import(
    csv_path: Path,
    data_dir: Path,
    *,
    import_id: str,
    batch_size: int,
    max_pending: int,
    checkpoint_name: str,
) -> ExecutionResult:
    """Execute and await one import using workers in another process."""
    bus = EventBus(str(data_dir), name=BUS_NAME, topology=TOPOLOGY)
    try:
        source = build_customer_source(
            bus,
            csv_path,
            import_id=import_id,
            batch_size=batch_size,
            max_pending=max_pending,
            checkpoint_name=checkpoint_name,
        )
        return await bus.execute(source)
    finally:
        bus.close()


def print_report(result: ExecutionResult) -> None:
    """Print cumulative durable execution state."""
    print(f"execution ID:             {result.execution_id}")
    print(f"resumed:                  {result.resumed}")
    print(f"source items committed:   {result.items_committed}")
    print(f"events dispatched:        {result.events_dispatched}")
    print(f"events unrouted:          {result.events_unrouted}")
    print(f"deliveries inserted:      {result.deliveries_inserted}")
    print(f"deliveries deduplicated:  {result.deliveries_deduplicated}")
    print(f"deliveries total:         {result.deliveries_total}")
    print(f"acknowledged:             {result.deliveries_acknowledged}")
    print(f"failed:                   {result.deliveries_failed}")
    print(f"completed timestamp:      {result.completed_at.isoformat()}")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the producer command line."""
    parser = argparse.ArgumentParser(
        prog="python -m examples.resumable_customer_import.producer",
        description="Ingest a customer CSV into the event bus, resumably.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV_PATH,
        help=f"CSV file to import (default: {DEFAULT_CSV_PATH})",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="queue data directory (default: the example's data/ directory)",
    )
    parser.add_argument(
        "--import-id",
        default=DEFAULT_IMPORT_ID,
        help=(
            "logical import operation ID; injected into every event as part "
            "of its durable identity and used for the default checkpoint "
            f"name (default: {DEFAULT_IMPORT_ID})"
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"source items per atomic batch (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--max-pending",
        type=int,
        default=DEFAULT_MAX_PENDING,
        help=(
            "per-subscription pending bound for this run "
            f"(default: {DEFAULT_MAX_PENDING})"
        ),
    )
    parser.add_argument(
        "--checkpoint",
        default=None,
        help="checkpoint name (default: customer-import:<import-id>)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    checkpoint_name = args.checkpoint or default_checkpoint_name(args.import_id)
    result = asyncio.run(
        run_import(
            args.csv,
            args.data_dir,
            import_id=args.import_id,
            batch_size=args.batch_size,
            max_pending=args.max_pending,
            checkpoint_name=checkpoint_name,
        )
    )
    print_report(result)
    result.raise_for_failures()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
