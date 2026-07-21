import asyncio
import multiprocessing

import pytest

from localqueue.bus import (
    BaseEvent,
    BusTopology,
    EventBus,
    EventRegistry,
    NoSubscribers,
)


class UserCreated(BaseEvent):
    event_name = "user.created"

    user_id: str


class OrderPlaced(BaseEvent):
    event_name = "order.placed"

    order_id: str


PROCESS_TOPOLOGY = BusTopology({"email": [UserCreated]})


def _produce_without_handlers(path):
    bus = EventBus(path, name="app", topology=PROCESS_TOPOLOGY)
    try:
        bus.dispatch(UserCreated(user_id="123"))
    finally:
        bus.close()


def _consume_in_separate_process(path, result):
    bus = EventBus(path, name="app", topology=PROCESS_TOPOLOGY)
    email = bus.subscription("email")

    @email.handler(UserCreated)
    def handle(event):
        result.put(event.user_id)

    try:
        asyncio.run(bus.run_subscription("email", idle_timeout=0.5))
    finally:
        bus.close()


def _run(coro):
    return asyncio.run(coro)


class TestStaticRouting:
    def test_producer_dispatches_without_registered_handlers(self, tmp_path):
        bus = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology(
                {
                    "email": [UserCreated],
                    "analytics": [UserCreated],
                    "audit": ["*"],
                }
            ),
        )
        try:
            receipt = bus.dispatch(UserCreated(user_id="123"))

            assert bus._handlers == {}
            assert receipt.subscriptions == ("analytics", "audit", "email")
            for subscription in receipt.subscriptions:
                queue = bus._open_subscription_queue(subscription)
                try:
                    assert queue.stats()["ready"] == 1
                finally:
                    queue.close()
        finally:
            bus.close()

    def test_handler_registration_does_not_add_a_route(self, tmp_path):
        topology = BusTopology(
            {"email": [UserCreated], "orders": [OrderPlaced]}
        )
        bus = EventBus(str(tmp_path / "bus"), topology=topology)
        try:
            bus.subscription("orders").handler("*", lambda event: None)

            receipt = bus.dispatch(UserCreated(user_id="123"))

            assert receipt.subscriptions == ("email",)
            assert topology.subscriptions_for("user.created") == ("email",)
        finally:
            bus.close()

    def test_wildcard_handler_does_not_expand_routing(self, tmp_path):
        topology = BusTopology({"email": [UserCreated]})
        bus = EventBus(str(tmp_path / "bus"), topology=topology)
        try:
            bus.subscription("email").handler("*", lambda event: None)

            with pytest.raises(NoSubscribers):
                bus.dispatch(OrderPlaced(order_id="456"))
        finally:
            bus.close()

    def test_no_subscribers_still_obeys_configuration(self, tmp_path):
        topology = BusTopology({"email": [UserCreated]})
        strict = EventBus(str(tmp_path / "strict"), topology=topology)
        permissive = EventBus(
            str(tmp_path / "permissive"),
            topology=topology,
            require_subscribers=False,
        )
        try:
            with pytest.raises(NoSubscribers):
                strict.dispatch(OrderPlaced(order_id="1"))

            receipt = permissive.dispatch(OrderPlaced(order_id="1"))
            assert receipt.subscriptions == ()
            assert receipt.message_ids == ()
        finally:
            strict.close()
            permissive.close()

    def test_same_event_id_is_deduplicated_per_subscription(self, tmp_path):
        topology = BusTopology(
            {"email": [UserCreated], "analytics": [UserCreated]}
        )
        bus = EventBus(str(tmp_path / "bus"), topology=topology)
        event = UserCreated(user_id="123")
        try:
            bus.dispatch(event)
            bus.dispatch(event)

            for subscription in topology.subscription_names:
                queue = bus._open_subscription_queue(subscription)
                try:
                    assert queue.stats()["ready"] == 1
                finally:
                    queue.close()
        finally:
            bus.close()


class TestSubscriptionBinder:
    def test_supports_decorator_and_direct_registration(self, tmp_path):
        topology = BusTopology(
            {"email": [UserCreated], "analytics": [UserCreated]}
        )
        bus = EventBus(str(tmp_path / "bus"), topology=topology)
        email = bus.subscription("email")

        @email.handler(UserCreated)
        def decorated(event):
            return None

        def direct(event):
            return None

        bus.subscription("analytics").handler(UserCreated, direct)
        try:
            assert bus._handlers[("email", "user.created")].handler is decorated
            assert bus._handlers[("analytics", "user.created")].handler is direct
        finally:
            bus.close()

    def test_rejects_undeclared_subscription_registration(self, tmp_path):
        bus = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology({"email": [UserCreated]}),
        )
        try:
            with pytest.raises(ValueError, match="not declared"):
                bus.subscription("orders")
            with pytest.raises(ValueError, match="not declared"):
                bus.on(UserCreated, lambda event: None, subscription="orders")
        finally:
            bus.close()

    def test_rejects_event_not_routed_to_subscription(self, tmp_path):
        bus = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology({"email": [UserCreated]}),
        )
        try:
            with pytest.raises(ValueError, match="does not route"):
                bus.subscription("email").handler(
                    OrderPlaced, lambda event: None
                )
        finally:
            bus.close()

    def test_failed_handler_registration_does_not_mutate_registry(self, tmp_path):
        registry = EventRegistry()
        bus = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology({"email": [UserCreated]}),
            registry=registry,
        )
        try:
            with pytest.raises(ValueError, match="does not route"):
                bus.subscription("email").handler(
                    OrderPlaced, lambda event: None
                )

            assert registry.resolve("order.placed") is None
        finally:
            bus.close()

    def test_accepts_exact_handler_under_wildcard_topology(self, tmp_path):
        bus = EventBus(
            str(tmp_path / "bus"), topology=BusTopology({"audit": ["*"]})
        )
        try:
            handler = bus.subscription("audit").handler(
                UserCreated, lambda event: None
            )
            assert handler is not None
        finally:
            bus.close()

    def test_rejects_duplicate_handler_registration(self, tmp_path):
        bus = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology({"email": [UserCreated]}),
        )
        email = bus.subscription("email")
        try:
            email.handler(UserCreated, lambda event: None)
            with pytest.raises(ValueError, match="already registered"):
                email.handler("user.created", lambda event: None)
        finally:
            bus.close()

    def test_exact_handler_wins_over_wildcard_handler(self, tmp_path):
        seen = []
        bus = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology({"email": [UserCreated]}),
        )
        email = bus.subscription("email")
        email.handler("*", lambda event: seen.append("wildcard"))
        email.handler(UserCreated, lambda event: seen.append("exact"))
        try:
            bus.dispatch(UserCreated(user_id="123"))
            _run(bus.run_subscription("email", idle_timeout=0.5))
            assert seen == ["exact"]
        finally:
            bus.close()

    def test_event_bus_on_delegates_without_changing_topology(self, tmp_path):
        topology = BusTopology({"email": [UserCreated]})
        bus = EventBus(str(tmp_path / "bus"), topology=topology)
        try:
            bus.on("*", lambda event: None, subscription="email")

            assert topology.subscription_names == ("email",)
            assert topology.subscriptions_for("order.placed") == ()
        finally:
            bus.close()


class TestLocalConsumption:
    def test_run_consumes_only_locally_registered_subscriptions(self, tmp_path):
        topology = BusTopology(
            {"email": [UserCreated], "analytics": [UserCreated]}
        )
        bus = EventBus(str(tmp_path / "bus"), topology=topology)
        seen = []
        bus.subscription("email").handler(
            UserCreated, lambda event: seen.append(event.user_id)
        )
        try:
            bus.dispatch(UserCreated(user_id="123"))
            _run(bus.run(idle_timeout=0.5))

            assert seen == ["123"]
            email = bus._open_subscription_queue("email")
            analytics = bus._open_subscription_queue("analytics")
            try:
                assert email.stats()["acked"] == 1
                assert analytics.stats()["ready"] == 1
                assert analytics.stats()["failed"] == 0
            finally:
                email.close()
                analytics.close()
        finally:
            bus.close()

    def test_run_subscription_fails_without_local_handler(self, tmp_path):
        bus = EventBus(
            str(tmp_path / "bus"),
            topology=BusTopology({"email": [UserCreated]}),
        )
        try:
            with pytest.raises(RuntimeError, match="no handler is registered"):
                _run(bus.run_subscription("email", idle_timeout=0.1))
            with pytest.raises(ValueError, match="not declared"):
                _run(bus.run_subscription("orders", idle_timeout=0.1))
        finally:
            bus.close()


def test_producer_and_consumer_work_in_separate_processes(tmp_path):
    path = str(tmp_path / "bus")
    context = multiprocessing.get_context("spawn")
    producer = context.Process(target=_produce_without_handlers, args=(path,))
    result = context.Queue()
    consumer = context.Process(
        target=_consume_in_separate_process, args=(path, result)
    )
    started_processes = []
    try:
        producer.start()
        started_processes.append(producer)
        producer.join(timeout=30)
        assert producer.exitcode == 0

        consumer.start()
        started_processes.append(consumer)
        consumer.join(timeout=30)
        assert consumer.exitcode == 0
        assert result.get(timeout=5) == "123"
    finally:
        for process in started_processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=5)
        result.close()
        result.join_thread()
