import ast
import asyncio
from pathlib import Path

import httpx
import pytest
from api import app
from import_contacts import ContactCreationRequested, run_import
from localqueue.bus import EventBus
from localqueue.bus.execution import ExecutionFailed

EXAMPLE = Path(__file__).parents[1] / "import_contacts.py"


def test_importer_has_the_expected_retry_and_idempotency_contract():
    source = EXAMPLE.read_text(encoding="utf-8")

    ast.parse(source, filename=str(EXAMPLE))

    assert (
        'identity=("import_id", "cnpj", "nome", "banco", "conta", "agencia")' in source
    )
    assert "raise Reject(" in source
    assert 'category="validation"' in source
    assert 'raise Retry("rate limited", after=retry_after)' in source
    assert "await bus.execute(contacts, timeout=None)" in source


def test_ingests_different_rows_with_the_same_cnpj(tmp_path: Path):
    first = ContactCreationRequested(
        import_id="2026-07",
        cnpj="18439256000165",
        nome="Vinicius Almeida",
        banco="Itaú Unibanco",
        conta="08903073",
        agencia="2932",
    )
    duplicate_cnpj = ContactCreationRequested(
        import_id="2026-07",
        cnpj="18439256000165",
        nome="Rael Costa",
        banco="Caixa Econômica Federal",
        conta="26319536",
        agencia="4083",
    )
    bus = EventBus(tmp_path, require_subscribers=False)

    try:
        result = asyncio.run(bus.ingest([first, duplicate_cnpj]))
    finally:
        bus.close()

    assert result.events_unrouted == 2


def test_import_runs_csv_through_api_and_records_terminal_duplicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    csv_path = tmp_path / "contacts.csv"
    csv_path.write_text(
        "cnpj,nome,banco,conta,agencia\n"
        "12345678000190,Ana Silva,Bank A,12345678,1234\n"
        "12345678000190,Other Contact,Bank B,87654321,4321\n"
        "98765432000198,Beatriz Costa,Bank C,00001111,1111\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CONTACTS_DB", str(tmp_path / "contacts.lmdb"))

    async def run() -> None:
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://contacts.test"
            ) as client:
                result = await run_import(
                    csv_path, tmp_path / "queue", "e2e-import", client
                )

                assert result.deliveries_acknowledged == 2
                assert result.deliveries_failed == 1
                assert result.deliveries_total == 3
                with pytest.raises(ExecutionFailed, match="1 failed delivery"):
                    result.raise_for_failures()

    asyncio.run(run())
