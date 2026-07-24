import asyncio
import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest
from localqueue import DeliveryPolicy
from localqueue.bus import BaseEvent, BusTopology, EventBus, NoSubscribers
from pydantic import ValidationError


class CausalityRoot(BaseEvent):
    user_id: str


class WelcomeEmailRequested(BaseEvent):
    event_name = "welcome-email-requested"
    schema_version = 2

    user_id: str


class EmailDelivered(BaseEvent):
    user_id: str


class TypedPayloadEvent(BaseEvent):
    count: int


def run(coro):
    return asyncio.run(coro)


def make_bus(tmp_path, topology=None, **kwargs):
    return EventBus(
        str(tmp_path / "bus"),
        name="causality",
        topology=topology or BusTopology({"events": ["*"]}),
        delivery=DeliveryPolicy(lease_seconds=0.5, max_retries=1),
        **kwargs,
    )


def put_envelope(bus, envelope, subscription="events"):
    queue = bus._open_subscription_queue(subscription)
    try:
        queue.put(envelope)
    finally:
        queue.close()


def failed_deliveries(bus, subscription="events"):
    queue = bus._open_subscription_queue(subscription)
    try:
        return queue.list_failed()
    finally:
        queue.close()


class RecordingSerializer:
    def __init__(self):
        self.dumped: list[Any] = []

    def dumps(self, obj: Any) -> bytes:
        self.dumped.append(obj)
        return json.dumps(obj).encode("utf-8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))


class EnvelopeSerializer:
    def __init__(self):
        self.dumped: list[dict[str, object]] = []

    def dumps(self, obj: dict[str, object], /) -> bytes:
        self.dumped.append(obj)
        return json.dumps(obj).encode("utf-8")

    def loads(self, data: bytes, /) -> object:
        return json.loads(data.decode("utf-8"))


class TestRootMetadata:
    def test_root_reuses_automatic_event_id_as_correlation_id(self):
        event = CausalityRoot(user_id="42")

        assert isinstance(event.event_id, UUID)
        assert event.correlation_id == event.event_id
        assert event.causation_id is None

    def test_explicit_uuid_metadata_is_preserved(self):
        event_id = UUID("11111111-1111-1111-1111-111111111111")
        correlation_id = UUID("22222222-2222-2222-2222-222222222222")
        causation_id = UUID("33333333-3333-3333-3333-333333333333")

        event = CausalityRoot(
            user_id="42",
            event_id=str(event_id),
            correlation_id=str(correlation_id),
            causation_id=str(causation_id),
        )

        assert event.event_id == event_id
        assert event.correlation_id == correlation_id
        assert event.causation_id == causation_id

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("event_id", True),
            ("event_id", ""),
            ("correlation_id", object()),
            ("correlation_id", "not-a-uuid"),
            ("causation_id", True),
            ("causation_id", ""),
        ],
    )
    def test_invalid_uuid_metadata_is_rejected(self, field, value):
        with pytest.raises(ValidationError) as error:
            CausalityRoot(user_id="42", **{field: value})

        assert field in str(error.value)

    @pytest.mark.parametrize("field", ["event_id", "correlation_id", "causation_id"])
    def test_metadata_is_immutable_after_construction(self, field):
        event = CausalityRoot(user_id="42")

        with pytest.raises(ValidationError, match="frozen"):
            setattr(event, field, uuid4())

    def test_business_payload_fields_remain_mutable(self):
        event = CausalityRoot(user_id="42")

        event.user_id = "43"

        assert event.user_id == "43"


class TestDerivedEvents:
    def test_child_gets_new_identity_timestamp_and_direct_causation(self):
        parent = CausalityRoot(user_id="42")

        child = WelcomeEmailRequested.from_parent(parent, user_id=parent.user_id)

        assert isinstance(child, WelcomeEmailRequested)
        assert child.event_id != parent.event_id
        assert child.event_created_at != parent.event_created_at
        assert child.correlation_id == parent.correlation_id
        assert child.causation_id == parent.event_id
        assert child.user_id == "42"
        assert child.event_type == "welcome-email-requested"
        assert child.event_schema == "welcome-email-requested@2"

    def test_child_business_payload_is_validated(self):
        parent = CausalityRoot(user_id="42")

        with pytest.raises(ValidationError) as error:
            WelcomeEmailRequested.from_parent(parent, user_id=object())

        assert "user_id" in str(error.value)

    def test_invalid_parent_is_rejected(self):
        with pytest.raises(TypeError, match="parent.*BaseEvent"):
            WelcomeEmailRequested.from_parent(object(), user_id="42")

    @pytest.mark.parametrize(
        "reserved",
        ["event_id", "correlation_id", "causation_id", "event_created_at"],
    )
    def test_reserved_helper_kwargs_are_rejected(self, reserved):
        parent = CausalityRoot(user_id="42")

        with pytest.raises(TypeError, match=reserved):
            WelcomeEmailRequested.from_parent(
                parent, user_id="42", **{reserved: uuid4()}
            )

    def test_grandchild_keeps_root_correlation_and_points_to_child(self):
        root = CausalityRoot(user_id="42")
        child = WelcomeEmailRequested.from_parent(root, user_id="42")

        grandchild = EmailDelivered.from_parent(child, user_id="42")

        assert grandchild.event_id not in {root.event_id, child.event_id}
        assert grandchild.correlation_id == root.event_id
        assert grandchild.causation_id == child.event_id


class TestEnvelopeSerialization:
    def test_root_metadata_is_top_level_and_absent_from_payload(self, tmp_path):
        bus = make_bus(tmp_path)
        event = CausalityRoot(user_id="42")
        try:
            envelope = json.loads(bus.serialize_envelope(event))
        finally:
            bus.close()

        assert envelope == {
            "event_id": str(event.event_id),
            "correlation_id": str(event.event_id),
            "causation_id": None,
            "event_type": "CausalityRoot",
            "event_schema": "CausalityRoot@1",
            "event_created_at": event.event_created_at.isoformat(),
            "payload": {"user_id": "42"},
        }

    def test_child_uses_canonical_uuid_strings_and_custom_schema(self, tmp_path):
        bus = make_bus(tmp_path)
        root = CausalityRoot(user_id="42")
        child = WelcomeEmailRequested.from_parent(root, user_id="42")
        try:
            envelope = json.loads(bus.serialize_envelope(child))
        finally:
            bus.close()

        assert envelope["event_id"] == str(child.event_id)
        assert envelope["correlation_id"] == str(root.event_id)
        assert envelope["causation_id"] == str(root.event_id)
        assert envelope["event_type"] == "welcome-email-requested"
        assert envelope["event_schema"] == "welcome-email-requested@2"
        assert envelope["payload"] == {"user_id": "42"}

    def test_dispatch_serializes_once_and_fans_out_the_same_envelope(
        self, tmp_path, monkeypatch
    ):
        serializer = RecordingSerializer()
        topology = BusTopology(
            {
                "audit": [CausalityRoot],
                "email": [CausalityRoot],
                "wildcard": ["*"],
            }
        )
        bus = make_bus(tmp_path, topology=topology, serializer=serializer)
        event = CausalityRoot(user_id="42")
        native = bus._native_queue
        fanout_calls = []

        class SpyNative:
            def fanout(self, payload, targets):
                fanout_calls.append((payload, targets))
                return native.fanout(payload, targets)

            def close(self):
                return native.close()

        monkeypatch.setattr(bus, "_native_queue", SpyNative())
        try:
            receipt = bus.dispatch(event)
        finally:
            bus.close()

        assert len(serializer.dumped) == 1
        assert len(fanout_calls) == 1
        payload, targets = fanout_calls[0]
        assert payload == json.dumps(serializer.dumped[0]).encode("utf-8")
        assert targets == [
            ("__bus__:causality:audit", str(event.event_id)),
            ("__bus__:causality:email", str(event.event_id)),
            ("__bus__:causality:wildcard", str(event.event_id)),
        ]
        assert receipt.event_id == event.event_id

    def test_custom_serializer_roundtrips_the_bus_envelope(self, tmp_path):
        serializer = EnvelopeSerializer()
        bus = make_bus(tmp_path, serializer=serializer)
        seen = []
        bus.on(CausalityRoot, lambda event: seen.append(event), subscription="events")

        try:
            event = CausalityRoot(user_id="42")
            bus.dispatch(event)
            run(bus.run_subscription("events", idle_timeout=0.2))
        finally:
            bus.close()

        assert len(serializer.dumped) == 1
        assert serializer.dumped[0]["event_type"] == "CausalityRoot"
        assert [item.user_id for item in seen] == ["42"]


class TestRoundtrip:
    def test_root_child_and_grandchild_roundtrip_exact_metadata(self, tmp_path):
        bus = make_bus(tmp_path)
        root = CausalityRoot(user_id="42")
        child = WelcomeEmailRequested.from_parent(root, user_id="42")
        grandchild = EmailDelivered.from_parent(child, user_id="42")
        expected = {
            event.event_type: (
                event.event_id,
                event.correlation_id,
                event.causation_id,
                type(event),
            )
            for event in (root, child, grandchild)
        }
        seen = []
        bus.on("*", lambda event: seen.append(event), subscription="events")
        try:
            for event in (root, child, grandchild):
                bus.dispatch(event)
            run(bus.run_subscription("events", idle_timeout=0.2))
        finally:
            bus.close()

        assert len(seen) == 3
        for event in seen:
            event_id, correlation_id, causation_id, event_class = expected[
                event.event_type
            ]
            assert isinstance(event, event_class)
            assert event.event_id == event_id
            assert event.correlation_id == correlation_id
            assert event.causation_id == causation_id

    def test_two_subscriptions_receive_identical_causality_metadata(self, tmp_path):
        topology = BusTopology({"audit": [CausalityRoot], "email": [CausalityRoot]})
        bus = make_bus(tmp_path, topology=topology)
        event = CausalityRoot(user_id="42")
        seen = []
        bus.on(CausalityRoot, lambda item: seen.append(item), subscription="audit")
        bus.on(CausalityRoot, lambda item: seen.append(item), subscription="email")
        try:
            bus.dispatch(event)
            run(bus.run(idle_timeout=0.2))
        finally:
            bus.close()

        assert [
            (item.event_id, item.correlation_id, item.causation_id) for item in seen
        ] == [
            (event.event_id, event.correlation_id, event.causation_id),
            (event.event_id, event.correlation_id, event.causation_id),
        ]


class TestBackwardCompatibility:
    def test_historical_envelope_without_metadata_is_reconstructed_as_root(
        self, tmp_path
    ):
        bus = make_bus(tmp_path)
        event_id = UUID("11111111-1111-1111-1111-111111111111")
        seen = []
        bus.on(CausalityRoot, lambda event: seen.append(event), subscription="events")
        put_envelope(
            bus,
            {
                "event_id": str(event_id),
                "event_type": "CausalityRoot",
                "event_schema": "CausalityRoot@1",
                "event_created_at": "2026-01-01T00:00:00+00:00",
                "payload": {"user_id": "42"},
            },
        )
        try:
            run(bus.run_subscription("events", idle_timeout=0.2))
        finally:
            bus.close()

        assert len(seen) == 1
        assert seen[0].event_id == event_id
        assert seen[0].correlation_id == event_id
        assert seen[0].causation_id is None

    @pytest.mark.parametrize("include_schema", [False, True])
    def test_event_schema_presence_does_not_change_reconstruction(
        self, tmp_path, include_schema
    ):
        bus = make_bus(tmp_path)
        seen = []
        envelope = {
            "event_id": "11111111-1111-1111-1111-111111111111",
            "event_type": "TypedPayloadEvent",
            "event_created_at": "2026-01-01T00:00:00+00:00",
            "payload": {"count": 7},
        }
        if include_schema:
            envelope["event_schema"] = "TypedPayloadEvent@1"
        bus.on(
            TypedPayloadEvent, lambda event: seen.append(event), subscription="events"
        )
        put_envelope(bus, envelope)

        try:
            run(bus.run_subscription("events", idle_timeout=0.2))
            queue = bus._open_subscription_queue("events")
            try:
                assert queue.stats()["acked"] == 1
                assert queue.stats()["failed"] == 0
            finally:
                queue.close()
        finally:
            bus.close()

        assert [event.count for event in seen] == [7]

    @pytest.mark.parametrize(
        ("omitted", "expected_correlation", "expected_causation"),
        [
            (
                "correlation_id",
                UUID("11111111-1111-1111-1111-111111111111"),
                UUID("33333333-3333-3333-3333-333333333333"),
            ),
            (
                "causation_id",
                UUID("22222222-2222-2222-2222-222222222222"),
                None,
            ),
        ],
    )
    def test_each_missing_metadata_field_uses_its_backward_compatible_default(
        self, tmp_path, omitted, expected_correlation, expected_causation
    ):
        bus = make_bus(tmp_path)
        seen = []
        envelope = {
            "event_id": "11111111-1111-1111-1111-111111111111",
            "correlation_id": "22222222-2222-2222-2222-222222222222",
            "causation_id": "33333333-3333-3333-3333-333333333333",
            "event_type": "CausalityRoot",
            "event_schema": "CausalityRoot@1",
            "event_created_at": "2026-01-01T00:00:00+00:00",
            "payload": {"user_id": "42"},
        }
        del envelope[omitted]
        bus.on(CausalityRoot, lambda event: seen.append(event), subscription="events")
        put_envelope(bus, envelope)
        try:
            run(bus.run_subscription("events", idle_timeout=0.2))
        finally:
            bus.close()

        assert seen[0].correlation_id == expected_correlation
        assert seen[0].causation_id == expected_causation

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("correlation_id", "not-a-uuid"),
            ("causation_id", "not-a-uuid"),
            ("causation_id", ""),
        ],
    )
    def test_invalid_new_metadata_is_a_permanent_failure(self, tmp_path, field, value):
        bus = make_bus(tmp_path)
        handled = []
        envelope = {
            "event_id": "11111111-1111-1111-1111-111111111111",
            "correlation_id": "22222222-2222-2222-2222-222222222222",
            "causation_id": None,
            "event_type": "CausalityRoot",
            "event_schema": "CausalityRoot@1",
            "event_created_at": "2026-01-01T00:00:00+00:00",
            "payload": {"user_id": "42"},
        }
        envelope[field] = value
        bus.on(
            CausalityRoot, lambda event: handled.append(event), subscription="events"
        )
        put_envelope(bus, envelope)
        try:
            run(bus.run_subscription("events", idle_timeout=0.2))
            failed = failed_deliveries(bus)
        finally:
            bus.close()

        assert handled == []
        assert len(failed) == 1
        assert "invalid payload" in failed[0].last_error
        assert field in failed[0].last_error
        assert failed[0].attempts == 1


class TestDeduplicationAndRegressions:
    def test_shared_correlation_does_not_deduplicate_distinct_events(
        self, tmp_path, monkeypatch
    ):
        bus = make_bus(tmp_path)
        root = CausalityRoot(user_id="root")
        first = WelcomeEmailRequested.from_parent(root, user_id="first")
        second = WelcomeEmailRequested.from_parent(root, user_id="second")
        native = bus._native_queue
        targets_seen = []

        class SpyNative:
            def fanout(self, payload, targets):
                targets_seen.extend(targets)
                return native.fanout(payload, targets)

            def close(self):
                return native.close()

        monkeypatch.setattr(bus, "_native_queue", SpyNative())
        try:
            first_receipt = bus.dispatch(first)
            second_receipt = bus.dispatch(second)
            queue = bus._open_subscription_queue("events")
            try:
                ready = queue.stats()["ready"]
            finally:
                queue.close()
        finally:
            bus.close()

        assert first.correlation_id == second.correlation_id == root.event_id
        assert first.event_id != second.event_id
        assert first_receipt.event_id == first.event_id
        assert second_receipt.event_id == second.event_id
        assert ready == 2
        assert [job_id for _queue, job_id in targets_seen] == [
            str(first.event_id),
            str(second.event_id),
        ]

    def test_exact_and_wildcard_handlers_still_prefer_exact(self, tmp_path):
        bus = make_bus(tmp_path)
        seen = []
        bus.on(CausalityRoot, lambda event: seen.append("exact"), subscription="events")
        bus.on("*", lambda event: seen.append("wildcard"), subscription="events")
        try:
            bus.dispatch(CausalityRoot(user_id="42"))
            run(bus.run_subscription("events", idle_timeout=0.2))
        finally:
            bus.close()

        assert seen == ["exact"]

    def test_no_subscribers_and_async_dispatch_contracts_are_unchanged(self, tmp_path):
        empty_bus = make_bus(tmp_path, topology=BusTopology({}))
        try:
            with pytest.raises(NoSubscribers):
                empty_bus.dispatch(CausalityRoot(user_id="42"))
        finally:
            empty_bus.close()

        async_bus = EventBus(
            str(tmp_path / "async-bus"),
            name="async",
            topology=BusTopology({"events": [CausalityRoot]}),
        )
        try:
            event = CausalityRoot(user_id="42")
            receipt = run(async_bus.dispatch_async(event))
        finally:
            async_bus.close()

        assert receipt.event_id == event.event_id
        assert receipt.subscriptions == ("events",)

    def test_payload_remains_typed_and_explicit_timestamp_is_preserved(self):
        timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)

        event = TypedPayloadEvent(count="42", event_created_at=timestamp)

        assert event.count == 42
        assert isinstance(event.count, int)
        assert event.event_created_at == timestamp
