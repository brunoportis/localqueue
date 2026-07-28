import csv
from pathlib import Path

import generate_contacts_csv


class FakeFaker:
    def __init__(self) -> None:
        self.index = 0

    def cnpj(self) -> str:
        self.index += 1
        return f"12.345.678/0001-{self.index:02d}"

    def name(self) -> str:
        return f"Contato {self.index}"


def test_write_contacts_csv_uses_the_import_schema(tmp_path: Path):
    output = tmp_path / "contacts.csv"

    generate_contacts_csv.write_contacts_csv(
        output,
        count=2,
        faker=FakeFaker(),
        seed=42,
    )

    with output.open(encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))

    assert len(rows) == 2
    assert list(rows[0]) == ["cnpj", "nome", "banco", "conta", "agencia"]
    assert rows[0]["cnpj"] == "12.345.678/0001-01"
    assert rows[0]["nome"] == "Contato 1"
    assert rows[0]["conta"].isdigit()
    assert rows[0]["agencia"].isdigit()
