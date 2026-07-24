"""Generated public-contract checks for queue state transitions.

The reference model deliberately tracks only logical jobs, their public
status, delivery attempts, and the current receipt.  It does not reproduce
SQLite rows, native states, SQL queries, or the Rust reclaim algorithm.
"""

from __future__ import annotations

import json
import tempfile
import time
from dataclasses import dataclass
from typing import Any

import pytest
from hypothesis import HealthCheck, note, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule
from localqueue import (
    DeliveryPolicy,
    Empty,
    EnqueueItem,
    Full,
    LeaseExpired,
    SimpleQueue,
)


class FailsBeforeNativeSerializer:
    """A public serializer seam used to fail during batch preparation."""

    def dumps(self, obj: Any) -> bytes:
        if isinstance(obj, dict) and obj.get("__fail__"):
            raise TypeError("intentional serialization failure")
        return json.dumps(obj).encode("utf-8")

    def loads(self, data: bytes) -> Any:
        # The state machine only puts values that the default JSON serializer
        # would decode.  This serializer is used solely by the failure rule.
        return json.loads(data.decode("utf-8"))


@dataclass
class ReferenceJob:
    data: dict[str, int]
    job_id: str | None
    order: int
    status: str = "ready"
    attempts: int = 0
    available_at: float = 0.0
    receipt: str | None = None


@settings(
    max_examples=20,
    stateful_step_count=25,
    deadline=None,
    database=None,
    print_blob=True,
    suppress_health_check=[HealthCheck.too_slow],
)
class QueueStateMachine(RuleBasedStateMachine):
    """Compare a small logical queue model with the public SimpleQueue API."""

    lease_seconds = 30.0
    max_retries = 2
    max_pending_jobs = 4

    def __init__(self) -> None:
        super().__init__()
        self.temp_dir = tempfile.TemporaryDirectory(prefix="localqueue-state-")
        self.path = self.temp_dir.name
        self.queue = SimpleQueue(
            str(self.path),
            delivery=DeliveryPolicy(
                lease_seconds=self.lease_seconds, max_retries=self.max_retries
            ),
            max_pending_jobs=self.max_pending_jobs,
        )
        self.jobs: dict[int, ReferenceJob] = {}
        self.by_job_id: dict[str, int] = {}
        self.current_delivery: dict[int, Any] = {}
        self.order = 0
        self.trace: list[str] = []
        self.above_limit_seeded = False

    def teardown(self) -> None:
        self.queue.close()
        self.temp_dir.cleanup()

    def _record(self, operation: str) -> None:
        self.trace.append(operation)
        note("operation trace: " + " -> ".join(self.trace))

    def _stats(
        self, queue: SimpleQueue, jobs: dict[int, ReferenceJob]
    ) -> dict[str, int]:
        return {
            "ready": sum(job.status == "ready" for job in jobs.values()),
            "processing": sum(job.status == "processing" for job in jobs.values()),
            "acked": sum(job.status == "acked" for job in jobs.values()),
            "failed": sum(job.status == "failed" for job in jobs.values()),
        }

    def _assert_stats(self) -> None:
        assert self.queue.stats() == self._stats(self.queue, self.jobs)

    def _pending(self) -> int:
        return sum(job.status in ("ready", "processing") for job in self.jobs.values())

    def _new_reference(
        self, data: dict[str, int], job_id: str | None, message_id: int
    ) -> None:
        if job_id is not None:
            self.by_job_id[job_id] = message_id
        self.jobs[message_id] = ReferenceJob(data, job_id, self.order)
        self.order += 1

    def _ready_reference(self) -> ReferenceJob | None:
        now = time.monotonic()
        candidates = [
            job
            for job in self.jobs.values()
            if job.status == "ready" and job.available_at <= now
        ]
        return min(candidates, key=lambda job: job.order, default=None)

    @rule(
        value=st.integers(min_value=0, max_value=5),
        job_id=st.one_of(st.none(), st.sampled_from(["a", "b", "c"])),
    )
    def put(self, value: int, job_id: str | None) -> None:
        data = {"value": value}
        self._record(f"put({value}, {job_id!r})")
        duplicate = job_id is not None and job_id in self.by_job_id
        if not duplicate and self._pending() >= self.max_pending_jobs:
            with pytest.raises(Full):
                self.queue.put(data, job_id=job_id, block=False)
            self._assert_stats()
            return
        message_id = self.queue.put(data, job_id=job_id, block=False)
        if duplicate:
            assert message_id == self.by_job_id[job_id]
        else:
            self._new_reference(data, job_id, message_id)
        self._assert_stats()

    @rule(
        items=st.lists(
            st.tuples(
                st.integers(min_value=0, max_value=5),
                st.one_of(st.none(), st.sampled_from(["a", "b", "c"])),
            ),
            min_size=0,
            max_size=4,
        )
    )
    def put_many(self, items: list[tuple[int, str | None]]) -> None:
        public_items = [
            EnqueueItem({"value": value}, job_id)
            if job_id is not None
            else {"value": value}
            for value, job_id in items
        ]
        self._record(f"put_many({items!r})")
        seen_new_job_ids: set[str] = set()
        new_rows = 0
        for _value, job_id in items:
            if job_id is None:
                new_rows += 1
            elif job_id not in self.by_job_id and job_id not in seen_new_job_ids:
                seen_new_job_ids.add(job_id)
                new_rows += 1
        if new_rows > 0 and self._pending() + new_rows > self.max_pending_jobs:
            before = self.queue.stats()
            with pytest.raises(Full):
                self.queue.put_many(public_items, block=False)
            assert self.queue.stats() == before
            self._assert_stats()
            return
        message_ids = self.queue.put_many(public_items, block=False)
        assert len(message_ids) == len(items)
        for (value, job_id), message_id in zip(items, message_ids):
            data = {"value": value}
            if job_id is not None and job_id in self.by_job_id:
                assert message_id == self.by_job_id[job_id]
            elif message_id in self.jobs:
                assert self.jobs[message_id].job_id == job_id
            else:
                self._new_reference(data, job_id, message_id)
        self._assert_stats()

    @precondition(lambda self: not self.above_limit_seeded)
    @rule()
    def reopen_above_limit_and_preserve_duplicate_operations(self) -> None:
        self._record("reopen_above_limit_and_preserve_duplicate_operations()")
        self.queue.close()
        unlimited = SimpleQueue(
            str(self.path),
            delivery=DeliveryPolicy(
                lease_seconds=self.lease_seconds, max_retries=self.max_retries
            ),
        )
        required = self.max_pending_jobs - self._pending() + 1
        for index in range(required):
            job_id = f"above-limit-{self.order}-{index}"
            data = {"value": index}
            message_id = unlimited.put(data, job_id=job_id)
            self._new_reference(data, job_id, message_id)
        unlimited.close()
        self.queue = SimpleQueue(
            str(self.path),
            delivery=DeliveryPolicy(
                lease_seconds=self.lease_seconds, max_retries=self.max_retries
            ),
            max_pending_jobs=self.max_pending_jobs,
        )
        self.above_limit_seeded = True
        assert self._pending() > self.max_pending_jobs
        duplicate_job_id = next(job_id for job_id in self.by_job_id)
        duplicate_id = self.queue.put(
            {"value": 999}, job_id=duplicate_job_id, block=False
        )
        assert duplicate_id == self.by_job_id[duplicate_job_id]
        with pytest.raises(Full):
            self.queue.put({"value": 999}, block=False)
        self._assert_stats()

    @rule(value=st.integers(min_value=0, max_value=5))
    def put_many_serializer_failure(self, value: int) -> None:
        before = self.queue.stats()
        self._record(f"put_many_serializer_failure({value})")
        queue = SimpleQueue(
            str(self.path),
            delivery=DeliveryPolicy(
                lease_seconds=self.lease_seconds, max_retries=self.max_retries
            ),
            max_pending_jobs=self.max_pending_jobs,
            serializer=FailsBeforeNativeSerializer(),
        )
        try:
            with pytest.raises(TypeError):
                queue.put_many([{"value": value}, {"__fail__": True}, {"value": value}])
        finally:
            queue.close()
        assert self.queue.stats() == before
        self._assert_stats()

    def _get(self, method: str) -> None:
        expected = self._ready_reference()
        self._record(f"{method}()")
        getter = (
            self.queue.get_nowait
            if method == "get_nowait"
            else lambda: self.queue.get(block=False)
        )
        if expected is None:
            with pytest.raises(Empty):
                getter()
            self._assert_stats()
            return
        job = getter()
        assert job.data == expected.data
        assert job.attempts == expected.attempts
        expected.status = "processing"
        expected.receipt = job.receipt
        self.current_delivery[job.id] = job
        assert job.id in self.jobs
        self._assert_stats()

    @rule()
    def get(self) -> None:
        self._get("get")

    @rule()
    def get_nowait(self) -> None:
        self._get("get_nowait")

    @precondition(lambda self: bool(self.current_delivery))
    @rule(index=st.integers(min_value=0, max_value=100))
    def ack(self, index: int) -> None:
        message_id = list(self.current_delivery)[index % len(self.current_delivery)]
        job = self.current_delivery[message_id]
        self._record(f"ack({message_id})")
        self.queue.ack(job)
        self.jobs[message_id].status = "acked"
        self.jobs[message_id].receipt = None
        self.current_delivery.pop(message_id)
        self._assert_stats()

    @precondition(lambda self: bool(self.current_delivery))
    @rule(
        index=st.integers(min_value=0, max_value=100),
        delay=st.just(0.0),
    )
    def nack(self, index: int, delay: float) -> None:
        message_id = list(self.current_delivery)[index % len(self.current_delivery)]
        job = self.current_delivery[message_id]
        self._record(f"nack({message_id}, delay={delay})")
        self.queue.nack(job, delay=delay, last_error="transient")
        reference = self.jobs[message_id]
        reference.receipt = None
        if reference.attempts >= self.max_retries:
            reference.status = "failed"
            self.current_delivery.pop(message_id)
        else:
            reference.status = "ready"
            reference.attempts += 1
            reference.available_at = time.monotonic() + delay
            self.current_delivery.pop(message_id)
        self._assert_stats()

    @precondition(lambda self: bool(self.current_delivery))
    @rule(index=st.integers(min_value=0, max_value=100))
    def fail(self, index: int) -> None:
        message_id = list(self.current_delivery)[index % len(self.current_delivery)]
        job = self.current_delivery[message_id]
        self._record(f"fail({message_id})")
        self.queue.fail(job, "permanent")
        reference = self.jobs[message_id]
        reference.status = "failed"
        reference.receipt = None
        self.current_delivery.pop(message_id)
        self._assert_stats()

    @precondition(lambda self: bool(self.current_delivery))
    @rule(
        index=st.integers(min_value=0, max_value=100),
        seconds=st.sampled_from([30.0, 60.0]),
    )
    def extend_lease(self, index: int, seconds: float) -> None:
        message_id = list(self.current_delivery)[index % len(self.current_delivery)]
        job = self.current_delivery[message_id]
        self._record(f"extend_lease({message_id}, {seconds})")
        self.queue.extend_lease(job, seconds)
        self._assert_stats()

    @rule()
    def reopen(self) -> None:
        self._record("close(); reopen()")
        self.queue.close()
        self.queue = SimpleQueue(
            str(self.path),
            delivery=DeliveryPolicy(
                lease_seconds=self.lease_seconds, max_retries=self.max_retries
            ),
            max_pending_jobs=self.max_pending_jobs,
        )
        self._assert_stats()

    @rule(value=st.integers(min_value=0, max_value=5))
    def same_job_id_is_independent_between_queues(self, value: int) -> None:
        cross_index = len(self.trace)
        job_id = f"cross-{cross_index}"
        data = {"value": value}
        self._record(f"cross_queue_put({value}, {job_id!r})")
        queue_a = SimpleQueue(
            str(self.path),
            name=f"cross-a-{cross_index}",
            delivery=DeliveryPolicy(lease_seconds=self.lease_seconds),
        )
        queue_b = SimpleQueue(
            str(self.path),
            name=f"cross-b-{cross_index}",
            delivery=DeliveryPolicy(lease_seconds=self.lease_seconds),
        )
        try:
            id_a = queue_a.put(data, job_id=job_id)
            id_b = queue_b.put(data, job_id=job_id)
            assert id_a != id_b
            job_a = queue_a.get_nowait()
            job_b = queue_b.get_nowait()
            assert job_a.data == job_b.data == data
            queue_a.ack(job_a)
            queue_b.ack(job_b)
        finally:
            queue_a.close()
            queue_b.close()
        self._assert_stats()

    @precondition(
        lambda self: any(job.status == "failed" for job in self.jobs.values())
    )
    @rule()
    def retry_failed(self) -> None:
        reference = next(job for job in self.jobs.values() if job.status == "failed")
        failed = self.queue.list_failed()
        assert any(message["id"] == self._reference_id(reference) for message in failed)
        self._record(f"retry_failed({self._reference_id(reference)})")
        if self._pending() >= self.max_pending_jobs:
            with pytest.raises(Full):
                self.queue.retry_failed(self._reference_id(reference))
            self._assert_stats()
            return
        self.queue.retry_failed(self._reference_id(reference))
        reference.status = "ready"
        reference.attempts = 0
        reference.available_at = time.monotonic()
        self._assert_stats()

    def _reference_id(self, reference: ReferenceJob) -> int:
        return next(
            message_id for message_id, job in self.jobs.items() if job is reference
        )

    @rule()
    def vacuum(self) -> None:
        self._record("vacuum()")
        self.queue.vacuum()
        self._assert_stats()

    @invariant()
    def stats_are_coherent(self) -> None:
        stats = self.queue.stats()
        assert sum(stats.values()) == len(self.jobs)
        report = self.queue.diagnostics()
        assert report.pending_jobs == self._pending()
        assert report.available_slots == max(0, self.max_pending_jobs - self._pending())
        self._assert_stats()


TestQueueStateMachine = QueueStateMachine.TestCase


def test_expiry_reclaim_and_stale_receipt_fencing(tmp_path):
    """Keep the only real-time wait outside generated sequences.

    The state machine above uses a long lease so normal ACK/NACK/fail
    transitions cannot race a slow CI runner. This focused scenario uses one
    deliberately short lease to cover expiration, explicit reclaim, and all
    stale-receipt transitions after redelivery.
    """
    queue = SimpleQueue(
        str(tmp_path), delivery=DeliveryPolicy(lease_seconds=0.2, max_retries=2)
    )
    try:
        queue.put({"task": "expire"})
        first = queue.get_nowait()
        time.sleep(0.6)
        assert queue.reclaim_expired_leases() == 1

        second = queue.get_nowait()
        assert second.data == first.data
        assert second.attempts == 1
        for operation in ("ack", "nack", "fail"):
            with pytest.raises(LeaseExpired):
                getattr(queue, operation)(first)
        with pytest.raises(LeaseExpired):
            queue.extend_lease(first, 1.0)

        queue.ack(second)
        assert queue.stats() == {
            "ready": 0,
            "processing": 0,
            "acked": 1,
            "failed": 0,
        }
    finally:
        queue.close()


def test_delayed_retry_waits_outside_generated_sequences(tmp_path):
    """Cover delayed availability with one isolated, generously-sized wait."""
    queue = SimpleQueue(
        str(tmp_path), delivery=DeliveryPolicy(lease_seconds=30.0, max_retries=2)
    )
    try:
        queue.put({"task": "delayed-retry"})
        job = queue.get_nowait()
        queue.nack(job, delay=0.2, last_error="transient")

        with pytest.raises(Empty):
            queue.get_nowait()

        time.sleep(0.7)
        redelivery = queue.get_nowait()
        assert redelivery.data == job.data
        assert redelivery.attempts == 1
        queue.ack(redelivery)
        assert queue.stats() == {
            "ready": 0,
            "processing": 0,
            "acked": 1,
            "failed": 0,
        }
    finally:
        queue.close()
