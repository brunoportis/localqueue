from __future__ import annotations

import argparse
import json
import multiprocessing as mp
from pathlib import Path

import pytest
from localqueue import Empty, LeaseExpired, LocalQueueError

from stress import run_soak


class _StopAfterTransition:
    def __init__(self) -> None:
        self.stopped = False

    def is_set(self) -> bool:
        return self.stopped

    def set(self) -> None:
        self.stopped = True


class _Events:
    def __init__(self, stop: _StopAfterTransition | None = None) -> None:
        self.items: list[dict[str, object]] = []
        self.stop = stop

    def put(self, item: dict[str, object]) -> None:
        self.items.append(item)
        if item["kind"] == "lease_lost" and self.stop is not None:
            self.stop.set()


class _LeaseLossQueue:
    def __init__(self, operation: str, stop: _StopAfterTransition) -> None:
        self.operation = operation
        self.stop = stop
        self.claimed = False
        self.closed = False

    def get(self, *, block: bool):
        if self.claimed:
            raise Empty("empty")
        self.claimed = True
        return object()

    def ack(self, job: object) -> None:
        self._transition("ack")

    def nack(self, job: object, *, delay: float, last_error: str) -> None:
        self._transition("nack")

    def fail(self, job: object, *, last_error: str) -> None:
        self._transition("fail")

    def _transition(self, operation: str) -> None:
        assert operation == self.operation
        self.stop.set()
        raise LeaseExpired("lease has expired")

    def close(self) -> None:
        self.closed = True


def soak_args(tmp_path: Path, **overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "messages": 40,
        "duration": 15.0,
        "producers": 2,
        "consumers": 2,
        "seed": 123,
        "nack_rate": 0.0,
        "fail_rate": 0.0,
        "crash_rate": 0.0,
        "max_restarts": 2,
        "lease_seconds": 5.0,
        "transition_delay_seconds": 0.0,
        "sqlite_retry_initial_delay": 0.01,
        "sqlite_retry_max_delay": 0.1,
        "sqlite_retry_attempts": 8,
        "path": tmp_path / "database",
        "output": None,
        "markdown_output": None,
        "candidate_sha": None,
        "candidate_ref": None,
        "candidate_version": None,
        "require_wheel": False,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


@pytest.mark.parametrize(
    ("operation", "nack_rate", "fail_rate"),
    [("ack", 0.0, 0.0), ("nack", 1.0, 0.0), ("fail", 0.0, 1.0)],
)
def test_consumer_records_lease_loss_and_keeps_running(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    nack_rate: float,
    fail_rate: float,
) -> None:
    stop = _StopAfterTransition()
    queue = _LeaseLossQueue(operation, stop)
    events = _Events(stop)
    counters = run_soak.shared_counters(mp.get_context("spawn"))
    monkeypatch.setattr(run_soak, "SimpleQueue", lambda *args, **kwargs: queue)

    run_soak.consume(
        "unused",
        "stress",
        stop,
        0,
        nack_rate,
        fail_rate,
        0.0,
        5.0,
        3,
        counters,
        events,
    )

    assert queue.closed
    assert run_soak.counter_snapshot(counters)["claims"] == 1
    assert run_soak.counter_snapshot(counters)[f"lease_losses_{operation}"] == 1
    assert {item["kind"] for item in events.items} == {"claim", "lease_lost"}
    assert events.items[-1]["operation"] == operation


def test_exit_code_17_is_an_intentional_restart() -> None:
    restarts = [0]

    classification, should_restart, error = run_soak.handle_consumer_exit(
        17, 0, restarts, 2, stopping=False
    )

    assert classification == "intentional_crash"
    assert should_restart
    assert error is None
    assert restarts == [1]


def test_unexpected_consumer_exit_fails_without_restart() -> None:
    restarts = [0]

    classification, should_restart, error = run_soak.handle_consumer_exit(
        9, 0, restarts, 100, stopping=False
    )

    assert classification == "unexpected_consumer_exit"
    assert not should_restart
    assert error == "unexpected_consumer_exit: consumer 0 exited with code 9"
    assert restarts == [0]


def test_normal_consumer_exit_during_shutdown_is_not_a_restart() -> None:
    restarts = [0]

    classification, should_restart, error = run_soak.handle_consumer_exit(
        0, 0, restarts, 100, stopping=True
    )

    assert classification == "normal_shutdown"
    assert not should_restart
    assert error is None
    assert restarts == [0]


def test_normal_consumer_exit_before_shutdown_fails() -> None:
    restarts = [0]

    classification, should_restart, error = run_soak.handle_consumer_exit(
        0, 0, restarts, 100, stopping=False
    )

    assert classification == "premature_normal_exit"
    assert not should_restart
    assert error == "premature_normal_exit: consumer 0 exited with code 0"


def test_max_restarts_only_limits_intentional_crash_loops() -> None:
    restarts = [1]

    classification, should_restart, error = run_soak.handle_consumer_exit(
        17, 0, restarts, 1, stopping=False
    )

    assert classification == "intentional_crash"
    assert not should_restart
    assert error == "consumer 0 exceeded --max-restarts"
    assert restarts == [2]


def test_supervisor_failure_always_writes_reports(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = tmp_path / "stress-report.json"
    markdown = tmp_path / "stress-report.md"
    args = argparse.Namespace(
        messages=1,
        duration=1.0,
        producers=1,
        consumers=1,
        seed=1,
        nack_rate=0.0,
        fail_rate=0.0,
        crash_rate=0.0,
        max_restarts=1,
        lease_seconds=5.0,
        transition_delay_seconds=0.0,
        sqlite_retry_initial_delay=0.01,
        sqlite_retry_max_delay=0.1,
        sqlite_retry_attempts=8,
        path=tmp_path / "database",
        output=output,
        markdown_output=markdown,
        candidate_sha="a" * 40,
        candidate_ref="refs/heads/fix",
        candidate_version="1.2.0",
        require_wheel=False,
    )
    monkeypatch.setattr(
        run_soak,
        "run",
        lambda _args: (_ for _ in ()).throw(RuntimeError("supervisor exploded")),
    )

    result = run_soak.execute(args)

    assert result["status"] == "failed"
    assert "supervisor exploded" in str(result["failure_reason"])
    assert output.is_file()
    assert markdown.is_file()
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["subject"]["candidate_sha"] == "a" * 40
    assert "configuration" in report
    assert "counters" in report
    assert "exits" in report
    assert "database" in report
    assert report["counters"]["sqlite_busy_get"] == 0
    assert "sqlite_busy_get" in markdown.read_text(encoding="utf-8")


def test_real_spawn_soak_drains_with_integrity(tmp_path: Path) -> None:
    args = soak_args(tmp_path)

    result = run_soak.execute(args)

    assert result["status"] == "passed"
    assert result["database"]["integrity"] == "ok"
    assert result["database"]["rows"] == 40
    assert {
        (item["role"], item["id"], item["exit_code"], item["classification"])
        for item in result["exits"]
    } == {
        ("producer", 0, 0, "normal_shutdown"),
        ("producer", 1, 0, "normal_shutdown"),
        ("consumer", 0, 0, "normal_shutdown"),
        ("consumer", 1, 0, "normal_shutdown"),
    }
    assert not run_soak.report_invariant_errors(
        result["counters"], result["last_stats"], result["exits"], result["restarts"]
    )


def test_real_crash_after_claim_is_reported_from_shared_memory(tmp_path: Path) -> None:
    result = run_soak.execute(
        soak_args(
            tmp_path,
            messages=1,
            producers=1,
            consumers=1,
            crash_rate=1.0,
            max_restarts=0,
        )
    )

    assert result["status"] == "failed"
    assert result["counters"]["claims"] == 1
    assert result["counters"]["intentional_crashes"] == 1
    assert [item["classification"] for item in result["exits"]].count(
        "intentional_crash"
    ) == 1


def test_many_spawn_consumers_preserve_operation_counters(tmp_path: Path) -> None:
    result = run_soak.execute(
        soak_args(
            tmp_path,
            messages=180,
            producers=4,
            consumers=8,
            seed=456,
            nack_rate=0.2,
            fail_rate=0.2,
        )
    )

    assert result["status"] == "passed"
    assert result["counters"]["acks"] > 0
    assert result["counters"]["nacks"] > 0
    assert result["counters"]["fails"] > 0
    assert not run_soak.report_invariant_errors(
        result["counters"], result["last_stats"], result["exits"], result["restarts"]
    )


def test_counter_divergence_fails_and_writes_report(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = tmp_path / "stress-report.json"
    markdown = tmp_path / "stress-report.md"
    original_snapshot = run_soak.counter_snapshot

    def divergent_snapshot(counters: dict[str, object]) -> dict[str, int]:
        snapshot = original_snapshot(counters)
        snapshot["claims"] += 1
        return snapshot

    monkeypatch.setattr(run_soak, "counter_snapshot", divergent_snapshot)
    result = run_soak.execute(
        soak_args(tmp_path, messages=2, output=output, markdown_output=markdown)
    )

    assert result["status"] == "failed"
    assert "claims=" in str(result["failure_reason"])
    assert output.is_file()
    assert markdown.is_file()


def test_real_lease_expiry_is_recorded_without_killing_consumer(tmp_path: Path) -> None:
    queue_path = tmp_path / "database"
    queue = run_soak.SimpleQueue(str(queue_path), name="stress", lease_seconds=0.01)
    queue.put({"id": "expired"}, job_id="expired")
    queue.close()
    stop = _StopAfterTransition()
    events = _Events(stop)
    counters = run_soak.shared_counters(mp.get_context("spawn"))

    run_soak.consume(
        str(queue_path),
        "stress",
        stop,
        0,
        0.0,
        0.0,
        0.0,
        0.01,
        0,
        counters,
        events,
        transition_delay_seconds=0.03,
    )

    assert events.items[-1] == {
        "kind": "lease_lost",
        "consumer_id": 0,
        "operation": "ack",
    }
    assert run_soak.counter_snapshot(counters)["lease_losses_ack"] == 1


@pytest.mark.parametrize(
    "message",
    ["database is locked", " DATABASE   IS LOCKED ", "database is busy"],
)
def test_classifies_only_normalized_transient_sqlite_contention(message: str) -> None:
    assert run_soak.is_transient_sqlite_contention(LocalQueueError(message))


@pytest.mark.parametrize(
    "message",
    [
        "database disk image is malformed",
        "file is not a database",
        "database or disk is full",
        "attempt to write a readonly database",
        "queue is closed",
    ],
)
def test_does_not_classify_other_queue_errors_as_sqlite_contention(
    message: str,
) -> None:
    assert not run_soak.is_transient_sqlite_contention(LocalQueueError(message))


def test_retries_busy_get_then_returns_job() -> None:
    stop = _StopAfterTransition()
    counters = run_soak.shared_counters(mp.get_context("spawn"))
    calls = 0
    job = object()

    def get() -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise LocalQueueError("database is locked")
        return job

    assert (
        run_soak.retry_transient_sqlite_contention(
            "get",
            get,
            stop=stop,
            deadline=None,
            policy=run_soak.SQLiteRetryPolicy(0.0, 0.0, 2),
            counters=counters,
            consumer_id=0,
            events=None,
        )
        is job
    )
    assert run_soak.counter_snapshot(counters)["sqlite_busy_get"] == 1


def test_non_transient_get_error_propagates() -> None:
    stop = _StopAfterTransition()
    counters = run_soak.shared_counters(mp.get_context("spawn"))

    with pytest.raises(LocalQueueError, match="malformed"):
        run_soak.retry_transient_sqlite_contention(
            "get",
            lambda: (_ for _ in ()).throw(
                LocalQueueError("database disk image is malformed")
            ),
            stop=stop,
            deadline=None,
            policy=run_soak.SQLiteRetryPolicy(0.0, 0.0, 2),
            counters=counters,
            consumer_id=0,
            events=None,
        )
    assert run_soak.counter_snapshot(counters)["sqlite_busy_get"] == 0


@pytest.mark.parametrize("operation", ["ack", "nack", "fail"])
def test_retries_busy_transition_then_succeeds(operation: str) -> None:
    stop = _StopAfterTransition()
    counters = run_soak.shared_counters(mp.get_context("spawn"))
    calls = 0

    def transition() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise LocalQueueError("database is busy")

    assert (
        run_soak.retry_transient_sqlite_contention(
            operation,
            transition,
            stop=stop,
            deadline=None,
            policy=run_soak.SQLiteRetryPolicy(0.0, 0.0, 2),
            counters=counters,
            consumer_id=0,
            events=None,
        )
        is None
    )
    assert calls == 2
    assert run_soak.counter_snapshot(counters)[f"sqlite_busy_{operation}"] == 1


def test_busy_ack_followed_by_lease_expired_is_not_retried() -> None:
    stop = _StopAfterTransition()
    counters = run_soak.shared_counters(mp.get_context("spawn"))
    calls = 0

    def ack() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise LocalQueueError("database is locked")
        raise LeaseExpired("lease has expired")

    with pytest.raises(LeaseExpired):
        run_soak.retry_transient_sqlite_contention(
            "ack",
            ack,
            stop=stop,
            deadline=None,
            policy=run_soak.SQLiteRetryPolicy(0.0, 0.0, 3),
            counters=counters,
            consumer_id=0,
            events=None,
        )
    assert calls == 2
    assert run_soak.counter_snapshot(counters)["sqlite_busy_ack"] == 1


def test_busy_retry_respects_stop_event() -> None:
    stop = _StopAfterTransition()
    stop.set()
    counters = run_soak.shared_counters(mp.get_context("spawn"))

    with pytest.raises(run_soak.SQLiteRetryStopped):
        run_soak.retry_transient_sqlite_contention(
            "get",
            lambda: (_ for _ in ()).throw(LocalQueueError("database is locked")),
            stop=stop,
            deadline=None,
            policy=run_soak.SQLiteRetryPolicy(0.0, 0.0, 2),
            counters=counters,
            consumer_id=0,
            events=None,
        )


def test_busy_retry_respects_deadline() -> None:
    stop = _StopAfterTransition()
    counters = run_soak.shared_counters(mp.get_context("spawn"))

    with pytest.raises(run_soak.SQLiteContentionExhausted, match="get after 1"):
        run_soak.retry_transient_sqlite_contention(
            "get",
            lambda: (_ for _ in ()).throw(LocalQueueError("database is locked")),
            stop=stop,
            deadline=0.0,
            policy=run_soak.SQLiteRetryPolicy(1.0, 1.0, 8),
            counters=counters,
            consumer_id=0,
            events=None,
        )


def test_busy_retry_exhaustion_is_reported_with_operation_and_attempts() -> None:
    stop = _StopAfterTransition()
    counters = run_soak.shared_counters(mp.get_context("spawn"))
    events = _Events()

    with pytest.raises(run_soak.SQLiteContentionExhausted, match="ack after 2"):
        run_soak.retry_transient_sqlite_contention(
            "ack",
            lambda: (_ for _ in ()).throw(LocalQueueError("database is locked")),
            stop=stop,
            deadline=None,
            policy=run_soak.SQLiteRetryPolicy(0.0, 0.0, 2),
            counters=counters,
            consumer_id=7,
            events=events,
        )
    assert run_soak.counter_snapshot(counters)["sqlite_busy_exhausted"] == 1
    assert events.items[-1] == {
        "kind": "sqlite_busy_exhausted",
        "consumer_id": 7,
        "operation": "ack",
        "attempts": 2,
    }
