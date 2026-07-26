"""Tests for the resumable customer import example.

All state goes to pytest's ``tmp_path``; nothing is written into the repo.
"""

from __future__ import annotations

import asyncio
import importlib
from pathlib import Path

import pytest
from localqueue.bus import CsvRow, CsvSource, EventBus, Reject, Retry, RuntimeContext

from examples.resumable_customer_import import (
    demo_api,
    events,
    producer,
    topology,
    worker,
)
from examples.resumable_customer_import.demo_api import (
    DemoCustomerApi,
    RATE_LIMITED_EMAIL,
    TEMPORARY_FAILURE_EMAIL,
    VALIDATION_FAILURE_EMAIL,
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

CSV_HEADER = "import_id,external_id,name,email,phone\n"


def run(coro):
    return asyncio.run(coro)


def write_csv(path: Path, rows: list[str]) -> Path:
    path.write_text(CSV_HEADER + "\n".join(rows) + "\n", encoding="utf-8")
    return path


def make_bus(path: Path) -> EventBus:
    return EventBus(str(path), name=BUS_NAME, topology=TOPOLOGY)


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
            "import_id": "  imp-42 ",
            "external_id": " EXT-9 ",
            "name": "  Alice Almeida  ",
            "email": " ALICE@EXAMPLE.COM ",
            "phone": " +1 555 010 0009 ",
        },
        record_number=1,
        line_number=2,
    )
    event = producer.to_customer_creation_requested(row)
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
    row = "imp-1,EXT-1,Alice,alice@example.com,+1 555 010 0001"
    csv_path = write_csv(tmp_path / "dupes.csv", [row, row])
    bus = make_bus(tmp_path)
    try:
        result = run(
            bus.ingest(
                CsvSource(csv_path),
                checkpoint="c1",
                transform=producer.to_customer_creation_requested,
            )
        )
    finally:
        bus.close()
    assert result.items_read == 2
    assert result.deliveries_inserted == 1
    assert result.deliveries_deduplicated == 1


def test_different_import_id_permits_same_external_id(tmp_path):
    csv_path = write_csv(
        tmp_path / "two-imports.csv",
        [
            "imp-1,EXT-1,Alice,alice@example.com,+1 555 010 0001",
            "imp-2,EXT-1,Alice,alice@example.com,+1 555 010 0001",
        ],
    )
    bus = make_bus(tmp_path)
    try:
        result = run(
            bus.ingest(
                CsvSource(csv_path),
                checkpoint="c1",
                transform=producer.to_customer_creation_requested,
            )
        )
    finally:
        bus.close()
    assert result.deliveries_inserted == 2
    assert result.deliveries_deduplicated == 0


def test_ingestion_creates_and_advances_checkpoint(tmp_path):
    csv_path = write_csv(
        tmp_path / "customers.csv",
        ["imp-1,EXT-1,Alice,alice@example.com,+1 555 010 0001"],
    )
    bus = make_bus(tmp_path)
    try:
        assert bus.checkpoint("c1").inspect() is None
        result = run(
            bus.ingest(
                CsvSource(csv_path),
                checkpoint="c1",
                transform=producer.to_customer_creation_requested,
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
            "imp-1,EXT-1,Alice,alice@example.com,+1 555 010 0001",
            "imp-1,EXT-2,Bob,bob@example.com,+1 555 010 0002",
        ],
    )
    bus = make_bus(tmp_path)
    try:
        first = run(
            bus.ingest(
                CsvSource(csv_path),
                checkpoint="c1",
                transform=producer.to_customer_creation_requested,
            )
        )
        second = run(
            bus.ingest(
                CsvSource(csv_path),
                checkpoint="c1",
                transform=producer.to_customer_creation_requested,
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
                make_event(TEMPORARY_FAILURE_EMAIL), make_context(api, event_id="k-flaky")
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
