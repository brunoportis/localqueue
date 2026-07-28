"""Import contacts from ``contacts.csv`` through the local demo API."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

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


# Full-row identity lets conflicting rows for one CNPJ reach the API, which
# performs the resource-level idempotency check. Exact duplicate rows collapse.
@event(identity=("import_id", "cnpj", "nome", "banco", "conta", "agencia"))
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

    def __init__(self, runtime: RuntimeContext, *, http: httpx.AsyncClient) -> None:
        super().__init__(runtime)
        self.http = http


async def run_import(
    csv_path: str | Path,
    queue_path: str | Path,
    import_id: str,
    http: httpx.AsyncClient,
):
    """Import one CSV through an injected client and return its durable result."""

    async def create_context(runtime: RuntimeContext) -> AppContext:
        return AppContext(runtime, http=http)

    bus: EventBus[AppContext] = EventBus(
        str(queue_path), context_factory=create_context
    )
    try:

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
                raise Reject(response.text, category="duplicate_cnpj")
            response.raise_for_status()

        creator = bus.subscription(ContactCreationRequested.event_name)
        creator.config.concurrency = 20
        creator.config.retry = RetryPolicy.exponential(max_attempts=8)

        @bus.source(CsvSource(csv_path), checkpoint=f"contacts:{import_id}")
        def contacts(row: CsvRow) -> ContactCreationRequested:
            return ContactCreationRequested(
                import_id=import_id,
                cnpj=normalize_cnpj(row["cnpj"]),
                nome=row["nome"].strip(),
                banco=row["banco"].strip(),
                conta=row["conta"].strip(),
                agencia=row["agencia"].strip(),
            )

        contacts.config.batch_size = 500
        contacts.config.max_pending = 10_000
        return await bus.execute(contacts, timeout=None)
    finally:
        bus.close()


async def main() -> None:
    async with httpx.AsyncClient(
        base_url=os.environ.get("CONTACTS_API_URL", "http://127.0.0.1:8000"),
        timeout=30,
    ) as http:
        result = await run_import("contacts.csv", "data/queue", "2026-07", http)
        print(
            f"execution={result.execution_id} "
            f"items={result.items_committed} "
            f"acked={result.deliveries_acknowledged} "
            f"failed={result.deliveries_failed}"
        )
        result.raise_for_failures()


if __name__ == "__main__":
    asyncio.run(main())
