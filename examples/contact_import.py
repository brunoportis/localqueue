"""Import contacts from a CSV file through an idempotent HTTP API.

Install this example's optional dependencies before running it from the
repository root:

    uv add "localqueue[bus]" httpx

Replace ``https://api.example.com`` with the target API and provide a
``contacts.csv`` file with ``cnpj,nome,banco,conta,agencia`` headers.
Generate synthetic input with ``python -m examples.generate_contacts_csv``
after installing ``faker``.
"""

from __future__ import annotations

import asyncio

import httpx
from localqueue.bus import (
    BaseEvent,
    CsvRow,
    CsvSource,
    EventBus,
    HandlerContext,
    Reject,
    Retry,
    RetryPolicy,
    RuntimeContext,
    event,
)


def normalize_cnpj(value: str) -> str:
    """Retain the digits used as the durable CNPJ identity."""
    return "".join(character for character in value if character.isdigit())


@event(identity=("import_id", "cnpj"))
class ContactCreationRequested(BaseEvent):
    """One idempotent contact-creation operation from the CSV."""

    event_name = "contact.creation-requested"

    import_id: str
    cnpj: str
    nome: str
    banco: str
    conta: str
    agencia: str


class AppContext(HandlerContext):
    """Delivery runtime information plus the application HTTP client."""

    def __init__(
        self,
        runtime: RuntimeContext,
        *,
        http: httpx.AsyncClient,
    ) -> None:
        super().__init__(runtime)
        self.http = http


async def main() -> None:
    async with httpx.AsyncClient(
        base_url="https://api.example.com",
        timeout=30,
    ) as http:

        async def create_context(runtime: RuntimeContext) -> AppContext:
            return AppContext(runtime, http=http)

        bus: EventBus[AppContext] = EventBus(
            "./contacts",
            context_factory=create_context,
        )

        @bus.handler(ContactCreationRequested)
        async def create_contact(
            event: ContactCreationRequested,
            ctx: AppContext,
        ) -> None:
            response = await ctx.http.post(
                "/contacts",
                json={
                    "cnpj": event.cnpj,
                    "nome": event.nome,
                    "banco": event.banco,
                    "conta": event.conta,
                    "agencia": event.agencia,
                },
                headers={"Idempotency-Key": ctx.event_id},
            )

            if response.status_code == 422:
                raise Reject(response.text, category="validation")

            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", "1"))
                raise Retry("rate limited", after=retry_after)

            if response.status_code == 409:
                # Confirm this contract with the real API: it can mean the
                # desired resource already exists.
                return

            response.raise_for_status()

        creator = bus.subscription(ContactCreationRequested.event_name)
        creator.config.concurrency = 100
        creator.config.retry = RetryPolicy.exponential(max_attempts=8)

        @bus.source(
            CsvSource("contacts.csv"),
            checkpoint="contacts:2026-07",
        )
        def contacts(row: CsvRow) -> ContactCreationRequested:
            return ContactCreationRequested(
                import_id="2026-07",
                cnpj=normalize_cnpj(row["cnpj"]),
                nome=row["nome"].strip(),
                banco=row["banco"].strip(),
                conta=row["conta"].strip(),
                agencia=row["agencia"].strip(),
            )

        contacts.config.batch_size = 5_000
        contacts.config.max_pending = 100_000

        try:
            result = await bus.execute(contacts, timeout=None)
            print(
                f"execution={result.execution_id} "
                f"items={result.items_committed} "
                f"acked={result.deliveries_acknowledged} "
                f"failed={result.deliveries_failed}"
            )
            result.raise_for_failures()
        finally:
            bus.close()


if __name__ == "__main__":
    asyncio.run(main())
