"""Producer: resumable ingestion of a customer CSV into the event bus.

Reads ``customers.csv`` with ``CsvSource``, transforms each row exactly once
into ``CustomerCreationRequested``, and ingests with a durable checkpoint so
the run can be interrupted and resumed without duplicate deliveries.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Sequence

from localqueue.bus import CsvRow, CsvSource, EventBus, IngestionResult

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


def to_customer_creation_requested(row: CsvRow) -> CustomerCreationRequested:
    """Transform one CSV row into the creation event, normalizing fields.

    Whitespace is stripped everywhere and the email is lowercased, so rows
    that differ only in padding or case collapse to the same durable
    identity and payload (and are deduplicated on re-ingestion).
    """
    return CustomerCreationRequested(
        import_id=row["import_id"].strip(),
        external_id=row["external_id"].strip(),
        name=row["name"].strip(),
        email=row["email"].strip().lower(),
        phone=row["phone"].strip(),
    )


async def run_import(
    csv_path: Path,
    data_dir: Path,
    *,
    batch_size: int,
    max_pending: int,
    checkpoint_name: str,
) -> IngestionResult:
    """Run one resumable ingestion, always closing the bus."""
    bus = EventBus(str(data_dir), name=BUS_NAME, topology=TOPOLOGY)
    try:
        return await bus.ingest(
            CsvSource(csv_path),
            checkpoint=checkpoint_name,
            transform=to_customer_creation_requested,
            batch_size=batch_size,
            max_pending=max_pending,
        )
    finally:
        bus.close()


def print_report(result: IngestionResult) -> None:
    """Print the ingestion counters and checkpoint progress."""
    print(f"items read:               {result.items_read}")
    print(f"events dispatched:        {result.events_dispatched}")
    print(f"events unrouted:          {result.events_unrouted}")
    print(f"deliveries inserted:      {result.deliveries_inserted}")
    print(f"deliveries deduplicated:  {result.deliveries_deduplicated}")
    print(f"batches committed:        {result.batches_committed}")
    if result.checkpoint is not None:
        print(f"checkpoint name:          {result.checkpoint.name}")
        print(f"checkpoint start cursor:  {result.checkpoint.start_cursor}")
        print(f"checkpoint end cursor:    {result.checkpoint.end_cursor}")
        print(f"resumed:                  {result.checkpoint.resumed}")
    print(f"elapsed:                  {result.elapsed_seconds:.3f}s")


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
            "import operation ID; only used for the default checkpoint name "
            f"(default: {DEFAULT_IMPORT_ID})"
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
            batch_size=args.batch_size,
            max_pending=args.max_pending,
            checkpoint_name=checkpoint_name,
        )
    )
    print_report(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
