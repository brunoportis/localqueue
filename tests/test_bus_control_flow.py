import asyncio
import sqlite3
import time
from pathlib import Path

import pytest
from localqueue import DeliveryPolicy, FailureReason
from localqueue.bus import BaseEvent, BusTopology, EventBus, Reject, Retry
from localqueue.policies import _MAX_DELAY_SECONDS


class WorkRequested(BaseEvent):
    value: str


def run(coro):
    return asyncio.run(coro)


def make_bus(path: Path, *, max_retries: int = 1, context_factory=None):
    return EventBus(
        str(path),
        topology=BusTopology({"work": [WorkRequested]}),
        delivery=DeliveryPolicy(lease_seconds=1, max_retries=max_retries),
        context_factory=context_factory,
    )


def subscription_stats(bus):
    queue = bus._open_subscription_queue("work")
    try:
        return queue.stats()
    finally:
        queue.close()


@pytest.mark.parametrize("after", [-1, float("inf"), float("-inf"), float("nan")])
def test_retry_rejects_non_finite_or_negative_after(after):
    with pytest.raises(ValueError, match="after.*non-negative finite"):
        Retry(after=after)


def test_retry_rejects_bool_after():
    with pytest.raises(TypeError, match="after.*number or None"):
        Retry(after=True)


def test_retry_rejects_invalid_reason_and_after_types():
    with pytest.raises(TypeError, match="reason.*string or None"):
        Retry(reason=123)
    with pytest.raises(TypeError, match="after.*number or None"):
        Retry(after="30")


def test_retry_accepts_maximum_persistable_delay():
    assert Retry(after=_MAX_DELAY_SECONDS).after == _MAX_DELAY_SECONDS


@pytest.mark.parametrize("after", [_MAX_DELAY_SECONDS + 1, 1e20, 1e308])
def test_retry_rejects_delay_above_persistable_limit(after):
    with pytest.raises(ValueError, match="after.*supported maximum"):
        Retry(after=after)


@pytest.mark.parametrize(
    ("args", "kwargs", "error", "message"),
    [
        (("",), {}, ValueError, "reason.*non-empty"),
        ((" ",), {}, ValueError, "reason.*non-empty"),
        (("reason",), {"category": ""}, ValueError, "category.*non-empty"),
        (("reason",), {"category": " "}, ValueError, "category.*non-empty"),
        ((1,), {}, TypeError, "reason.*string"),
        (("reason",), {"category": 1}, TypeError, "category.*string or None"),
    ],
)
def test_reject_validates_reason_and_category(args, kwargs, error, message):
    with pytest.raises(error, match=message):
        Reject(*args, **kwargs)


def test_reject_allows_an_omitted_category():
    assert Reject("invalid input").category is None


def test_retry_supports_sync_handler_and_retries_immediately_by_default(tmp_path):
    bus = make_bus(tmp_path)
    attempts = []

    @bus.subscription("work").handler(WorkRequested)
    def handle(event):
        attempts.append(event.value)
        if len(attempts) == 1:
            raise Retry()

    bus.dispatch(WorkRequested(value="sync"))
    run(bus.run(idle_timeout=0.2))

    assert attempts == ["sync", "sync"]
    assert subscription_stats(bus)["acked"] == 1
    bus.close()


def test_retry_supports_async_handler_and_respects_max_retries(tmp_path):
    bus = make_bus(tmp_path, max_retries=1)
    attempts = []

    @bus.subscription("work").handler(WorkRequested, permanent_errors=(Retry,))
    async def handle(event):
        attempts.append(event.value)
        raise Retry("not ready")

    bus.dispatch(WorkRequested(value="async"))
    run(bus.run(idle_timeout=0.2))

    failed = bus.subscription("work").list_failed()
    assert attempts == ["async", "async"]
    assert len(failed) == 1
    assert failed[0].reason is FailureReason.RETRIES_EXHAUSTED
    assert failed[0].last_error == "not ready"
    bus.close()


def test_retry_from_context_factory_uses_persistent_delay(tmp_path):
    calls = []

    def create_context(runtime):
        calls.append(runtime.attempt)
        raise Retry("context unavailable", after=0.5)

    bus = make_bus(tmp_path, context_factory=create_context)

    @bus.subscription("work").handler(WorkRequested)
    def handle(event):
        raise AssertionError("the context factory must run first")

    bus.dispatch(WorkRequested(value="context"))
    started = time.time()
    run(bus.run(idle_timeout=0.1))

    assert calls == [1]
    assert subscription_stats(bus)["ready"] == 1
    with sqlite3.connect(tmp_path / "localqueue.db") as connection:
        available_at = connection.execute(
            "SELECT available_at FROM messages WHERE queue = ?",
            (bus._queue_name("work"),),
        ).fetchone()[0]
    assert available_at >= int((started + 0.4) * 1000)
    bus.close()


def test_reject_precedes_permanent_errors_and_never_retries(tmp_path):
    bus = make_bus(tmp_path, max_retries=3)
    attempts = []

    @bus.subscription("work").handler(
        WorkRequested,
        permanent_errors=(Reject,),
    )
    def handle(event):
        attempts.append(event.value)
        raise Reject("invalid input", category="validation")

    bus.dispatch(WorkRequested(value="bad"))
    run(bus.run(idle_timeout=0.2))

    failed = bus.subscription("work").list_failed()
    assert attempts == ["bad"]
    assert len(failed) == 1
    assert failed[0].reason is FailureReason.REJECTED
    assert failed[0].last_error == "invalid input"
    assert failed[0].failure_category == "validation"
    bus.close()


def test_reject_from_async_context_factory_goes_directly_to_dlq(tmp_path):
    async def create_context(runtime):
        raise Reject("missing tenant", category="context")

    bus = make_bus(tmp_path, max_retries=3, context_factory=create_context)

    @bus.subscription("work").handler(WorkRequested)
    async def handle(event):
        raise AssertionError("the context factory must run first")

    bus.dispatch(WorkRequested(value="bad context"))
    run(bus.run(idle_timeout=0.2))

    failed = bus.subscription("work").list_failed()
    assert len(failed) == 1
    assert failed[0].attempts == 1
    assert failed[0].reason is FailureReason.REJECTED
    assert failed[0].last_error == "missing tenant"
    assert failed[0].failure_category == "context"
    bus.close()


def test_reject_survives_reopen_and_remains_replayable(tmp_path):
    bus = make_bus(tmp_path)

    @bus.subscription("work").handler(WorkRequested)
    async def reject(event):
        raise Reject("invalid input", category="validation")

    bus.dispatch(WorkRequested(value="bad"))
    run(bus.run(idle_timeout=0.2))
    failed_id = bus.subscription("work").list_failed()[0].id
    bus.close()

    reopened = make_bus(tmp_path)
    failed = reopened.subscription("work").list_failed()[0]
    assert failed.id == failed_id
    assert failed.last_error == "invalid input"
    assert failed.failure_category == "validation"

    observed = []

    @reopened.subscription("work").handler(WorkRequested)
    def accept(event):
        observed.append(event.value)

    reopened.subscription("work").retry_failed(failed_id)
    run(reopened.run(idle_timeout=0.2))

    assert observed == ["bad"]
    assert subscription_stats(reopened)["acked"] == 1
    reopened.close()


def test_opening_older_database_adds_failure_category_idempotently(tmp_path):
    bus = make_bus(tmp_path)
    bus.close()
    database = tmp_path / "localqueue.db"
    with sqlite3.connect(database) as connection:
        connection.execute("ALTER TABLE messages DROP COLUMN failure_category")

    first = make_bus(tmp_path)
    first.close()
    second = make_bus(tmp_path)
    second.close()

    with sqlite3.connect(database) as connection:
        columns = [row[1] for row in connection.execute("PRAGMA table_info(messages)")]
    assert columns.count("failure_category") == 1
