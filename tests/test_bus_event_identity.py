from __future__ import annotations

import sqlite3
from multiprocessing import get_context
from uuid import uuid4

import pytest
from localqueue import DeduplicationConflict, SimpleQueue
from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    InvalidEventIdentity,
    event,
)


@event(identity="user_id")
class DurableUserCreated(BaseEvent):
    user_id: str
    name: str


def test_same_identity_and_payload_reuses_message(tmp_path):
    bus = EventBus(
        tmp_path,
        name="identity",
        topology=BusTopology({"users": [DurableUserCreated]}),
    )
    try:
        first = bus.dispatch(DurableUserCreated(user_id="1", name="Alice"))
        duplicate = bus.dispatch(DurableUserCreated(user_id="1", name="Alice"))

        assert first.message_ids == duplicate.message_ids
        assert first.inserted == (True,)
        assert duplicate.inserted == (False,)
        assert first.inserted_subscriptions == ("users",)
        assert duplicate.deduplicated_subscriptions == ("users",)
        assert first.inserted_count == 1
        assert duplicate.deduplicated_count == 1
    finally:
        bus.close()


def test_event_without_identity_still_deduplicates_only_by_event_id(tmp_path):
    class Plain(BaseEvent):
        value: str

    bus = EventBus(
        tmp_path,
        name="plain",
        topology=BusTopology({"plain": [Plain]}),
    )
    try:
        first = bus.dispatch(Plain(value="same"))
        second = bus.dispatch(Plain(value="same"))
        assert first.message_ids != second.message_ids
        assert first.inserted == second.inserted == (True,)
    finally:
        bus.close()


@event(identity=("tenant_id", "external_id"))
class DurableCompositeEvent(BaseEvent):
    tenant_id: str
    external_id: str
    value: str


@event(identity="identity")
class DurableInvalidIdentityEvent(BaseEvent):
    identity: object


def _dispatch_identity_process(path, barrier, results, name):
    bus = EventBus(
        path,
        name="multiprocess",
        topology=BusTopology({"target": [DurableUserCreated]}),
    )
    try:
        barrier.wait()
        receipt = bus.dispatch(DurableUserCreated(user_id="1", name=name))
        results.put(("ok", receipt.message_ids[0], receipt.inserted[0]))
    except Exception as error:  # noqa: BLE001 - child reports its public outcome
        results.put((type(error).__name__, None, None))
    finally:
        bus.close()


def test_composite_identity_is_order_independent_and_namespaced():
    from localqueue.bus.identity import business_payload, prepare_persistence_identity

    first = prepare_persistence_identity(
        DurableCompositeEvent(tenant_id="acme", external_id="1", value="x"),
        {"tenant_id": "acme", "external_id": "1", "value": "x"},
    )

    class ReverseComposite(BaseEvent):
        event_name = "DurableCompositeEvent"
        tenant_id: str
        external_id: str
        value: str

    decorated = event(identity=("external_id", "tenant_id"))(ReverseComposite)
    second_event = decorated(tenant_id="acme", external_id="1", value="x")
    second = prepare_persistence_identity(second_event, business_payload(second_event))
    assert first.dedup_key == second.dedup_key


def test_same_identity_with_different_payload_is_conflict_and_atomic(tmp_path):
    bus = EventBus(
        tmp_path,
        name="conflict",
        topology=BusTopology(
            {"existing": [DurableUserCreated], "new": [DurableUserCreated]}
        ),
    )
    try:
        first = DurableUserCreated(user_id="1", name="Alice")
        bus.dispatch(first)
        with pytest.raises(DeduplicationConflict):
            bus.dispatch(DurableUserCreated(user_id="1", name="Bob"))
        connection = sqlite3.connect(tmp_path / "localqueue.db")
        rows = connection.execute(
            "SELECT queue, COUNT(*) FROM messages GROUP BY queue ORDER BY queue"
        ).fetchall()
        connection.close()
        assert rows == [
            ("__bus__:conflict:existing", 1),
            ("__bus__:conflict:new", 1),
        ]
    finally:
        bus.close()


def test_mixed_fanout_reports_each_subscription(tmp_path):
    first_bus = EventBus(
        tmp_path,
        name="mixed",
        topology=BusTopology({"existing": [DurableUserCreated]}),
    )
    event_value = DurableUserCreated(user_id="1", name="Alice")
    first_bus.dispatch(event_value)
    first_bus.close()

    bus = EventBus(
        tmp_path,
        name="mixed",
        topology=BusTopology(
            {"existing": [DurableUserCreated], "new": [DurableUserCreated]}
        ),
    )
    try:
        receipt = bus.dispatch(DurableUserCreated(user_id="1", name="Alice"))
        assert receipt.subscriptions == ("existing", "new")
        assert receipt.inserted == (False, True)
        assert receipt.deduplicated_subscriptions == ("existing",)
        assert receipt.inserted_subscriptions == ("new",)
    finally:
        bus.close()


@pytest.mark.parametrize(
    "identity",
    [123, [], set(), ("",), ("id", "id"), ("id", None)],
)
def test_decorator_rejects_invalid_contract(identity):
    class Candidate(BaseEvent):
        id: str

    with pytest.raises((TypeError, ValueError)):
        event(identity=identity)(Candidate)  # type: ignore[arg-type]


@pytest.mark.parametrize("field", ["missing", "event_id"])
def test_decorator_rejects_invalid_field(field):
    class Candidate(BaseEvent):
        id: str

    with pytest.raises(ValueError, match=field):
        event(identity=field)(Candidate)


def test_identity_is_opt_in_per_concrete_class():
    @event(identity="id")
    class Parent(BaseEvent):
        id: str

    class Child(Parent):
        pass

    assert "__event_identity_fields__" in Parent.__dict__
    assert "__event_identity_fields__" not in Child.__dict__


@pytest.mark.parametrize("invalid", [None, "", "   ", float("nan"), float("inf")])
def test_invalid_identity_fails_before_any_insert(tmp_path, invalid):
    bus = EventBus(
        tmp_path,
        topology=BusTopology({"target": [DurableInvalidIdentityEvent]}),
    )
    try:
        with pytest.raises(InvalidEventIdentity):
            bus.dispatch(DurableInvalidIdentityEvent(identity=invalid))
        connection = sqlite3.connect(tmp_path / "localqueue.db")
        count = connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        connection.close()
        assert count == 0
    finally:
        bus.close()


def test_same_event_id_with_incompatible_identity_conflicts(tmp_path):
    bus = EventBus(
        tmp_path,
        topology=BusTopology({"target": [DurableUserCreated]}),
    )
    occurrence = uuid4()
    try:
        bus.dispatch(DurableUserCreated(event_id=occurrence, user_id="1", name="Alice"))
        with pytest.raises(DeduplicationConflict):
            bus.dispatch(
                DurableUserCreated(event_id=occurrence, user_id="2", name="Alice")
            )
    finally:
        bus.close()


@pytest.mark.parametrize("state", ["key_only", "columns_without_index", "complete"])
def test_event_identity_migration_recovers_partial_states(tmp_path, state):
    queue = SimpleQueue(str(tmp_path))
    queue.close()
    database = tmp_path / "localqueue.db"
    connection = sqlite3.connect(database)
    if state != "complete":
        connection.execute("DROP INDEX idx_messages_dedup_key")
    if state == "key_only":
        connection.execute("ALTER TABLE messages DROP COLUMN dedup_fingerprint")
    connection.commit()
    connection.close()

    reopened = SimpleQueue(str(tmp_path))
    reopened.close()
    connection = sqlite3.connect(database)
    columns = {row[1] for row in connection.execute("PRAGMA table_info(messages)")}
    indexes = {row[1] for row in connection.execute("PRAGMA index_list(messages)")}
    connection.close()
    assert {"dedup_key", "dedup_fingerprint"} <= columns
    assert "idx_messages_dedup_key" in indexes


@pytest.mark.parametrize(
    ("names", "expected_kinds"),
    [
        (("Alice", "Alice"), {"ok"}),
        (("Alice", "Bob"), {"ok", "DeduplicationConflict"}),
    ],
)
def test_multiprocess_identity_is_enforced_by_sqlite(tmp_path, names, expected_kinds):
    context = get_context("spawn")
    barrier = context.Barrier(2)
    results = context.Queue()
    processes = [
        context.Process(
            target=_dispatch_identity_process,
            args=(str(tmp_path), barrier, results, name),
        )
        for name in names
    ]
    for process in processes:
        process.start()
    outcomes = [results.get(timeout=15) for _ in processes]
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0

    assert {outcome[0] for outcome in outcomes} == expected_kinds
    successful = [outcome for outcome in outcomes if outcome[0] == "ok"]
    if names[0] == names[1]:
        assert len({outcome[1] for outcome in successful}) == 1
        assert sorted(outcome[2] for outcome in successful) == [False, True]
    connection = sqlite3.connect(tmp_path / "localqueue.db")
    count = connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    connection.close()
    assert count == 1
