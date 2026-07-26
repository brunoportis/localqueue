"""Tests for the resumable customer import example.

All state goes to pytest's ``tmp_path``; nothing is written into the repo.
"""

from __future__ import annotations

import asyncio
import functools
import importlib
from pathlib import Path

import pytest
from localqueue.bus import CsvRow, CsvSource, EventBus, Reject, Retry, RuntimeContext

from examples.resumable_customer_import import (
    demo_api,
    events,
    producer,
    worker,
)
from examples.resumable_customer_import.demo_api import (
    RATE_LIMITED_EMAIL,
    TEMPORARY_FAILURE_EMAIL,
    VALIDATION_FAILURE_EMAIL,
    DemoCustomerApi,
)
from examples.resumable_customer_import.events import (
    CustomerCreated,
    CustomerCreationRequested,
)
from examples.resumable_customer_import.topology import (
    BUS_NAME,
    CUSTOMER_AUDIT,
    CUSTOMER_CREATOR,
    TOPOLOGY,
)

CSV_HEADER = "external_id,name,email,phone\n"


def run(coro):
    return asyncio.run(coro)


def write_csv(path: Path, rows: list[str]) -> Path:
    path.write_text(CSV_HEADER + "\n".join(rows) + "\n", encoding="utf-8")
    return path


def make_bus(path: Path) -> EventBus:
    return EventBus(str(path), name=BUS_NAME, topology=TOPOLOGY)


def make_transform(import_id: str = "imp-1"):
    return functools.partial(
        producer.to_customer_creation_requested, import_id=import_id
    )


def make_context(api, *, event_id: str = "key-1") -> worker.CustomerWorkerContext:
    runtime = RuntimeContext(event_id=event_id, attempt=1, handler_name="test")
    return worker.CustomerWorkerContext(runtime, api)


def make_event(email: str, *, external_id: str = "EXT-1") -> CustomerCreationRequested:
    return CustomerCreationRequested(
        import_id="imp-1",
        external_id=external_id,
        name="Some Name",
        email=email,
        phone="+1 555 010 0000",
    )


def test_transform_normalizes_fields_and_carries_import_id():
    row = CsvRow(
        {
            "external_id": " EXT-9 ",
            "name": "  Alice Almeida  ",
            "email": " ALICE@EXAMPLE.COM ",
            "phone": " +1 555 010 0009 ",
        },
        record_number=1,
        line_number=2,
    )
    event = producer.to_customer_creation_requested(row, import_id="imp-42")
    assert event.import_id == "imp-42"
    assert event.external_id == "EXT-9"
    assert event.name == "Alice Almeida"
    assert event.email == "alice@example.com"
    assert event.phone == "+1 555 010 0009"


def test_topology_routes_both_event_types():
    assert TOPOLOGY.subscriptions_for("customer.creation-requested") == (
        CUSTOMER_CREATOR,
    )
    assert TOPOLOGY.subscriptions_for("customer.created") == (CUSTOMER_AUDIT,)
    assert TOPOLOGY.has_subscription(CUSTOMER_CREATOR)
    assert TOPOLOGY.has_subscription(CUSTOMER_AUDIT)


def test_identical_rows_deduplicate_under_identity(tmp_path):
    row = "EXT-1,Alice,alice@example.com,+1 555 010 0001"
    csv_path = write_csv(tmp_path / "dupes.csv", [row, row])
    bus = make_bus(tmp_path)
    try:
        result = run(
            bus.ingest(
                CsvSource(csv_path),
                checkpoint="c1",
                transform=make_transform(),
            )
        )
    finally:
        bus.close()
    assert result.items_read == 2
    assert result.deliveries_inserted == 1
    assert result.deliveries_deduplicated == 1


def test_different_import_id_permits_same_external_id(tmp_path):
    csv_path = write_csv(
        tmp_path / "customers.csv",
        ["EXT-1,Alice,alice@example.com,+1 555 010 0001"],
    )
    bus = make_bus(tmp_path)
    try:
        first = run(
            bus.ingest(
                CsvSource(csv_path),
                checkpoint="import-imp-1",
                transform=make_transform("imp-1"),
            )
        )
        second = run(
            bus.ingest(
                CsvSource(csv_path),
                checkpoint="import-imp-2",
                transform=make_transform("imp-2"),
            )
        )
    finally:
        bus.close()
    # Same external_id under a different import_id is a new operation-scoped
    # identity, so the second run inserts instead of deduplicating.
    assert first.deliveries_inserted == 1
    assert second.deliveries_inserted == 1
    assert second.deliveries_deduplicated == 0


def test_ingestion_creates_and_advances_checkpoint(tmp_path):
    csv_path = write_csv(
        tmp_path / "customers.csv",
        ["EXT-1,Alice,alice@example.com,+1 555 010 0001"],
    )
    bus = make_bus(tmp_path)
    try:
        assert bus.checkpoint("c1").inspect() is None
        result = run(
            bus.ingest(
                CsvSource(csv_path),
                checkpoint="c1",
                transform=make_transform(),
            )
        )
        assert result.checkpoint is not None
        assert result.checkpoint.start_cursor is None
        assert result.checkpoint.end_cursor is not None
        assert result.checkpoint.resumed is False
        state = bus.checkpoint("c1").inspect()
        assert state is not None
        assert state.cursor == result.checkpoint.end_cursor
        assert state.items_committed == 1
    finally:
        bus.close()


def test_rerun_resumes_without_duplicate_deliveries(tmp_path):
    csv_path = write_csv(
        tmp_path / "customers.csv",
        [
            "EXT-1,Alice,alice@example.com,+1 555 010 0001",
            "EXT-2,Bob,bob@example.com,+1 555 010 0002",
        ],
    )
    bus = make_bus(tmp_path)
    try:
        first = run(
            bus.ingest(
                CsvSource(csv_path),
                checkpoint="c1",
                transform=make_transform(),
            )
        )
        second = run(
            bus.ingest(
                CsvSource(csv_path),
                checkpoint="c1",
                transform=make_transform(),
            )
        )
    finally:
        bus.close()
    assert first.deliveries_inserted == 2
    assert second.checkpoint is not None
    assert second.checkpoint.resumed is True
    assert second.items_read == 0
    assert second.deliveries_inserted == 0
    assert second.deliveries_deduplicated == 0
    assert second.checkpoint.end_cursor == first.checkpoint.end_cursor


def test_demo_api_repeats_result_for_one_idempotency_key():
    api = DemoCustomerApi()

    async def scenario():
        first = await api.create_customer(
            idempotency_key="k1",
            external_id="EXT-1",
            name="Alice",
            email="alice@example.com",
            phone="+1 555 010 0001",
        )
        second = await api.create_customer(
            idempotency_key="k1",
            external_id="EXT-1",
            name="Alice",
            email="alice@example.com",
            phone="+1 555 010 0001",
        )
        return first, second

    first, second = run(scenario())
    assert first.created is True
    assert second == first


def test_demo_api_temporary_and_rate_limit_are_deterministic():
    api = DemoCustomerApi(temporary_failure_attempts=2, rate_limit_retry_after=0.25)

    async def call(key: str, email: str):
        return await api.create_customer(
            idempotency_key=key,
            external_id=f"EXT-{key}",
            name="Name",
            email=email,
            phone="+1 555 010 0000",
        )

    async def scenario():
        outcomes = []
        for _ in range(2):
            with pytest.raises(demo_api.CustomerApiUnavailable):
                await call("flaky", TEMPORARY_FAILURE_EMAIL)
        outcomes.append(await call("flaky", TEMPORARY_FAILURE_EMAIL))
        with pytest.raises(demo_api.CustomerRateLimited) as exc_info:
            await call("throttled", RATE_LIMITED_EMAIL)
        assert exc_info.value.retry_after == 0.25
        outcomes.append(await call("throttled", RATE_LIMITED_EMAIL))
        return outcomes

    flaky_result, throttled_result = run(scenario())
    assert flaky_result.created is True
    assert throttled_result.created is True


def test_handler_maps_validation_to_reject():
    api = DemoCustomerApi()
    ctx = make_context(api)
    with pytest.raises(Reject) as exc_info:
        run(worker.create_customer(make_event(VALIDATION_FAILURE_EMAIL), ctx))
    assert exc_info.value.category == "validation"


def test_handler_maps_temporary_failure_and_rate_limit_to_retry():
    api = DemoCustomerApi(temporary_failure_attempts=2, rate_limit_retry_after=0.25)
    with pytest.raises(Retry) as temporary:
        run(
            worker.create_customer(
                make_event(TEMPORARY_FAILURE_EMAIL),
                make_context(api, event_id="k-flaky"),
            )
        )
    assert temporary.value.after is None
    with pytest.raises(Retry) as rate_limited:
        run(
            worker.create_customer(
                make_event(RATE_LIMITED_EMAIL, external_id="EXT-2"),
                make_context(api, event_id="k-throttled"),
            )
        )
    assert rate_limited.value.after == 0.25


def test_successful_handler_returns_customer_created():
    api = DemoCustomerApi()
    created = run(
        worker.create_customer(make_event("alice@example.com"), make_context(api))
    )
    assert isinstance(created, CustomerCreated)
    assert created.import_id == "imp-1"
    assert created.external_id == "EXT-1"
    assert created.customer_id == "cus-0001"


def test_worker_configures_creator_concurrency(tmp_path):
    bus = worker.build_bus(tmp_path, DemoCustomerApi())
    try:
        assert bus.subscription(CUSTOMER_CREATOR).concurrency == (
            worker.CREATOR_CONCURRENCY
        )
        assert worker.CREATOR_CONCURRENCY == 20
        # The audit subscription keeps the default process-local concurrency.
        assert bus.subscription(CUSTOMER_AUDIT).concurrency == 1
    finally:
        bus.close()


def queue_stats(bus: EventBus, subscription: str) -> dict[str, int]:
    queue = bus._open_subscription_queue(subscription)
    try:
        return queue.stats()
    finally:
        queue.close()


def test_end_to_end_smoke(tmp_path, capsys):
    """Exercise the real example wiring: ingest, create, retry/reject, audit."""
    csv_path = write_csv(
        tmp_path / "customers.csv",
        [
            "EXT-1,Alice,alice@example.com,+1 555 010 0001",
            "EXT-2,Bob,bob@example.com,+1 555 010 0002",
            "EXT-3,Carol,invalid@example.com,+1 555 010 0003",
            "EXT-4,Dana,flaky@example.com,+1 555 010 0004",
            "EXT-5,Erik,throttled@example.com,+1 555 010 0005",
        ],
    )
    data_dir = tmp_path / "data"
    api = DemoCustomerApi(temporary_failure_attempts=2, rate_limit_retry_after=0.05)

    async def wait_for(bus, subscription, predicate, description):
        deadline = asyncio.get_running_loop().time() + 30.0
        while True:
            stats = await asyncio.to_thread(queue_stats, bus, subscription)
            if predicate(stats):
                return stats
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"{description} did not settle: {stats}")
            await asyncio.sleep(0.05)

    async def scenario():
        ingestion = await producer.run_import(
            csv_path,
            data_dir,
            import_id="smoke-v1",
            batch_size=100,
            max_pending=1_000,
            checkpoint_name="customer-import:smoke-v1",
        )
        bus = worker.build_bus(data_dir, api)
        runner = asyncio.create_task(bus.run())
        try:
            # Wait until every creator delivery reaches a terminal state:
            # 4 acked (flaky/throttled included, after retries) + 1 failed.
            await wait_for(
                bus,
                CUSTOMER_CREATOR,
                lambda s: s.get("acked", 0) == 4 and s.get("failed", 0) == 1,
                "creator",
            )
            await wait_for(
                bus,
                CUSTOMER_AUDIT,
                lambda s: s.get("acked", 0) == 4,
                "audit",
            )
        finally:
            runner.cancel()
            try:
                await runner
            except asyncio.CancelledError:
                pass
            bus.close()
        return ingestion

    ingestion = run(scenario())
    assert ingestion.items_read == 5
    assert ingestion.deliveries_inserted == 5

    # Validation row landed rejected with the modeled category.
    bus = make_bus(data_dir)
    try:
        failed = bus.subscription(CUSTOMER_CREATOR).list_failed()
        assert len(failed) == 1
        assert failed[0].failure_category == "validation"
        assert failed[0].event_type == "customer.creation-requested"
    finally:
        bus.close()

    # Transactional CustomerCreated emission reached the audit handler once
    # per created customer, and the API saw no duplicate side effects.
    audit_lines = [
        line
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("audit ")
    ]
    assert len(audit_lines) == 4
    assert len(set(audit_lines)) == 4
    assert all("import=smoke-v1" in line for line in audit_lines)
    assert len(api._by_external_id) == 4
    assert api._sequence == 4
    assert set(api._by_external_id) == {"EXT-1", "EXT-2", "EXT-4", "EXT-5"}


def test_example_modules_importable_without_side_effects(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    for module in (
        "examples.resumable_customer_import",
        "examples.resumable_customer_import.events",
        "examples.resumable_customer_import.topology",
        "examples.resumable_customer_import.demo_api",
        "examples.resumable_customer_import.producer",
        "examples.resumable_customer_import.worker",
    ):
        importlib.import_module(module)
    # Importing must not create the example data directory in the cwd.
    assert not (tmp_path / "data").exists()
    assert events.CustomerCreationRequested is not None
