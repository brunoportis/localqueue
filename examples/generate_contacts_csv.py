"""Generate deterministic synthetic input for ``contact_import.py``.

Install the optional generator dependency first:

    uv add faker

For example, generate five thousand contacts with:

    uv run python -m examples.generate_contacts_csv --count 5000
"""

from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path
from typing import Protocol, Sequence

FIELDNAMES = ("cnpj", "nome", "banco", "conta", "agencia")
BANKS = (
    "Banco do Brasil",
    "Bradesco",
    "Caixa Econômica Federal",
    "Itaú Unibanco",
    "Santander",
)
DEFAULT_COUNT = 5_000
DEFAULT_SEED = 20_260_727


class ContactFaker(Protocol):
    """Subset of Faker used to create contact identity and display data."""

    def cnpj(self) -> str: ...

    def name(self) -> str: ...


def write_contacts_csv(
    output: Path,
    *,
    count: int,
    faker: ContactFaker,
    seed: int,
) -> None:
    """Write synthetic contacts in the exact schema consumed by the importer."""
    if count < 0:
        raise ValueError("'count' must be non-negative")

    generator = random.Random(seed)
    with output.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        writer.writeheader()
        for _ in range(count):
            writer.writerow(
                {
                    "cnpj": faker.cnpj(),
                    "nome": faker.name(),
                    "banco": generator.choice(BANKS),
                    "conta": f"{generator.randrange(1, 100_000_000):08d}",
                    "agencia": f"{generator.randrange(1, 10_000):04d}",
                }
            )


def generate_contacts_csv(output: Path, *, count: int, seed: int) -> None:
    """Create a seeded Brazilian Faker provider and write the CSV file."""
    try:
        from faker import Faker
    except ImportError as error:
        raise ImportError(
            "Install the generator dependency with:\n\n    uv add faker"
        ) from error

    faker = Faker("pt_BR")
    faker.seed_instance(seed)
    write_contacts_csv(output, count=count, faker=faker, seed=seed)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line options for the synthetic CSV generator."""
    parser = argparse.ArgumentParser(
        prog="python -m examples.generate_contacts_csv",
        description="Generate synthetic contacts.csv data using Faker.",
    )
    parser.add_argument("--output", type=Path, default=Path("contacts.csv"))
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Generate the requested CSV and print its location."""
    args = parse_args(argv)
    generate_contacts_csv(args.output, count=args.count, seed=args.seed)
    print(f"generated {args.count} contacts at {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
