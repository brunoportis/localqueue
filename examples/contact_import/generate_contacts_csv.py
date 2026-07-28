"""Generate deterministic synthetic CSV input for ``import_contacts.py``."""

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
    def cnpj(self) -> str: ...

    def name(self) -> str: ...


def write_contacts_csv(
    output: Path, *, count: int, faker: ContactFaker, seed: int
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
    from faker import Faker

    faker = Faker("pt_BR")
    faker.seed_instance(seed)
    write_contacts_csv(output, count=count, faker=faker, seed=seed)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic contacts.csv data."
    )
    parser.add_argument("--output", type=Path, default=Path("contacts.csv"))
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    generate_contacts_csv(args.output, count=args.count, seed=args.seed)
    print(f"generated {args.count} contacts at {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
