import pytest

from localqueue.bus import BaseEvent, BusTopology


class UserCreated(BaseEvent):
    event_name = "user.created"

    user_id: str


class OrderPlaced(BaseEvent):
    event_name = "order.placed"

    order_id: str


class TestBusTopology:
    def test_normalizes_event_classes_and_strings(self):
        topology = BusTopology(
            {
                "email": [UserCreated, "user.created"],
                "analytics": [UserCreated, OrderPlaced],
            }
        )

        assert topology.subscription_names == ("analytics", "email")
        assert topology.routes("email", "user.created")
        assert topology.routes("analytics", "order.placed")
        assert not topology.routes("email", "order.placed")

    def test_returns_subscriptions_in_deterministic_order(self):
        topology = BusTopology(
            {
                "email": [UserCreated],
                "audit": ["*"],
                "analytics": [UserCreated],
            }
        )

        assert topology.subscriptions_for("user.created") == (
            "analytics",
            "audit",
            "email",
        )

    def test_wildcard_routes_every_event_type(self):
        topology = BusTopology({"audit": ["*"]})

        assert topology.subscriptions_for("user.created") == ("audit",)
        assert topology.subscriptions_for("anything.else") == ("audit",)

    @pytest.mark.parametrize(
        "name", ["", " ", "-email", "email:primary", "email primary", 42]
    )
    def test_rejects_invalid_subscription_names(self, name):
        with pytest.raises(ValueError, match="invalid 'subscription'"):
            BusTopology({name: [UserCreated]})

    @pytest.mark.parametrize(
        "pattern",
        ["", " ", "**", "user.*", object(), BaseEvent()],
    )
    def test_rejects_invalid_event_patterns(self, pattern):
        error = ValueError if isinstance(pattern, str) else TypeError
        with pytest.raises(error):
            BusTopology({"email": [pattern]})

    def test_rejects_empty_event_pattern_iterable(self):
        with pytest.raises(ValueError, match="at least one event pattern"):
            BusTopology({"email": []})

    def test_copies_caller_owned_mapping_and_iterables(self):
        patterns = [UserCreated]
        subscriptions = {"email": patterns}
        topology = BusTopology(subscriptions)

        patterns.append(OrderPlaced)
        subscriptions["audit"] = ["*"]

        assert topology.subscription_names == ("email",)
        assert topology.subscriptions_for("order.placed") == ()
