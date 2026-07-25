# Custom handler contexts

`EventBus` can construct an application-specific context for every delivery
attempt. The bus owns only delivery capabilities; applications explicitly add
their own HTTP clients, repositories, metrics, or configuration.

```python
import httpx

from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    HandlerContext,
    RuntimeContext,
)


class ContactCreationRequested(BaseEvent):
    cnpj: str
    name: str


class AppContext(HandlerContext):
    def __init__(self, runtime: RuntimeContext, *, http: httpx.AsyncClient):
        super().__init__(runtime)
        self.http = http


TOPOLOGY = BusTopology({"contacts": [ContactCreationRequested]})


async def main() -> None:
    async with httpx.AsyncClient(timeout=30) as http:

        def create_context(runtime: RuntimeContext) -> AppContext:
            return AppContext(runtime, http=http)

        AppEventBus = EventBus[AppContext]
        bus = AppEventBus(
            "./data",
            topology=TOPOLOGY,
            context_factory=create_context,
        )

        @bus.subscription("contacts").handler(ContactCreationRequested)
        async def create_contact(
            event: ContactCreationRequested,
            ctx: AppContext,
        ) -> None:
            response = await ctx.http.post(
                "http://api.example.com/contacts",
                json=event.model_dump(),
                headers={"Idempotency-Key": ctx.event_id},
            )
            response.raise_for_status()

        await bus.run()
```

`HandlerContext` provides three reserved, read-only runtime capabilities:
`event_id`, `attempt` (starting at 1), and `handler_name`. Do not redefine
these names in custom contexts.

The factory receives a read-only `RuntimeContext` and may be synchronous or
async. It runs once for each delivery attempt, before the handler. A factory
exception prevents handler execution and follows the subscription's existing
retry and dead-letter policy. Context instances are not shared between
attempts, while dependencies captured by the factory may be shared deliberately.

```python
async def create_context(runtime: RuntimeContext) -> AppContext:
    connection = await acquire_connection()
    return AppContext(runtime, http=http)
```

Without `context_factory`, handlers may still accept the built-in
`HandlerContext`; existing one-argument handlers continue to work unchanged.
The EventBus does not perform dependency resolution or automatically close
resources supplied by an application. It also does not offer contextual event
publication: publishing a new event and acknowledging the current delivery are
separate durable operations until a transactional outbox-style primitive exists.
