from __future__ import annotations

import asyncio
from collections import Counter
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from queue import Empty, Full
from pathlib import Path
from typing import Any, cast
from unittest import mock

import lmdb

from localqueue.retry import MemoryAttemptStore
from localqueue import (
    LMDBQueueStore,
    MemoryQueueStore,
    PersistentQueue,
    PersistentWorkerConfig,
    QueueMessage,
    QueueStoreLockedError,
    SQLiteQueueStore,
    persistent_async_worker,
    persistent_worker,
)
from localqueue.store import (
    _QueueRecord,
    _decode_record,
    _dead_key,
    _encode_record,
    _inflight_key,
    _message_key,
    _ready_key,
    _sequence_from_index_key,
    _timestamp_from_inflight_key,
    _timestamp_from_ready_key,
)


class QueueTests(unittest.TestCase):
    def test_constructor_rejects_invalid_arguments(self) -> None:
        store = MemoryQueueStore()
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(ValueError):
                _ = PersistentQueue("test", store=store, store_path=tmpdir)

        with self.assertRaises(ValueError):
            _ = PersistentQueue("test", store=store, lease_timeout=0)

        with self.assertRaises(ValueError):
            _ = PersistentQueue("test", store=store, maxsize=-1)

    def test_memory_store_basic_ops(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        self.assertTrue(queue.empty())
        _ = queue.put("item1")
        self.assertEqual(queue.qsize(), 1)
        self.assertFalse(queue.empty())
        self.assertEqual(queue.get(), "item1")
        queue.task_done()
        self.assertTrue(queue.empty())

    def test_stats_counts_messages_by_state(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("inflight")
        _ = queue.put("dead")
        _ = queue.put("delayed", delay=10)
        inflight = queue.get_message(leased_by="worker-a")
        self.assertEqual(inflight.value, "inflight")
        dead = queue.get_message(leased_by="worker-b")
        self.assertEqual(dead.value, "dead")
        self.assertTrue(queue.dead_letter(dead, error=RuntimeError("bad")))

        stats = queue.stats()

        self.assertEqual(stats.ready, 0)
        self.assertEqual(stats.delayed, 1)
        self.assertEqual(stats.inflight, 1)
        self.assertEqual(stats.dead, 1)
        self.assertEqual(stats.total, 3)
        self.assertEqual(stats.by_worker_id, {"worker-a": 1})
        self.assertEqual(
            stats.leases_by_worker_id,
            {"worker-a": 1, "worker-b": 1},
        )
        self.assertIsNone(stats.oldest_ready_age_seconds)
        self.assertIsNotNone(stats.oldest_inflight_age_seconds)
        self.assertIsNotNone(stats.average_inflight_age_seconds)

        self.assertTrue(queue.ack(inflight))
        after_ack = queue.stats()
        self.assertEqual(after_ack.ready, 0)
        self.assertEqual(after_ack.delayed, 1)
        self.assertEqual(after_ack.inflight, 0)
        self.assertEqual(after_ack.dead, 1)
        self.assertEqual(after_ack.total, 2)
        self.assertEqual(after_ack.by_worker_id, {})
        self.assertEqual(
            after_ack.leases_by_worker_id,
            {"worker-a": 1, "worker-b": 1},
        )

    def test_stats_reports_queue_and_inflight_age(self) -> None:
        call_times = [100.0, 101.0, 105.0, 110.0]

        def fake_time() -> float:
            return call_times.pop(0) if call_times else 110.0

        with mock.patch("time.time", side_effect=fake_time):
            queue = PersistentQueue("test", store=MemoryQueueStore())
            _ = queue.put("ready")
            _ = queue.put("inflight")
            _ = queue.get_message(leased_by="worker-a")

            stats = queue.stats()

        self.assertEqual(stats.ready, 1)
        self.assertEqual(stats.inflight, 1)
        oldest_ready_age = stats.oldest_ready_age_seconds
        oldest_inflight_age = stats.oldest_inflight_age_seconds
        average_inflight_age = stats.average_inflight_age_seconds
        assert oldest_ready_age is not None
        assert oldest_inflight_age is not None
        assert average_inflight_age is not None
        self.assertGreaterEqual(oldest_ready_age, 0.0)
        self.assertGreaterEqual(oldest_inflight_age, 0.0)
        self.assertGreaterEqual(average_inflight_age, 0.0)

    def test_inspect_returns_message_without_changing_state(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("inflight")
        ready = queue.put("ready")
        inflight = queue.get_message()

        inspected_ready = queue.inspect(ready.id)
        inspected_inflight = queue.inspect(inflight.id)

        assert inspected_ready is not None
        assert inspected_inflight is not None
        self.assertEqual(inspected_ready.value, "ready")
        self.assertEqual(inspected_ready.state, "ready")
        self.assertEqual(inspected_inflight.value, "inflight")
        self.assertEqual(inspected_inflight.state, "inflight")
        self.assertEqual(queue.qsize(), 1)
        self.assertIsNone(queue.inspect("missing"))

    def test_get_message_records_and_clears_lease_owner(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("item")

        message = queue.get_message(leased_by="worker-a")

        self.assertEqual(message.leased_by, "worker-a")
        self.assertEqual(message.attempt_history[0]["type"], "leased")
        self.assertEqual(message.attempt_history[0]["attempt"], 1)
        self.assertEqual(message.attempt_history[0]["leased_by"], "worker-a")
        inspected = queue.inspect(message.id)
        assert inspected is not None
        self.assertEqual(inspected.leased_by, "worker-a")
        self.assertTrue(queue.release(message))
        redelivered = queue.get_message()
        self.assertIsNone(redelivered.leased_by)
        self.assertEqual(redelivered.attempt_history[0]["type"], "leased")
        self.assertEqual(redelivered.attempt_history[1]["type"], "released")
        self.assertEqual(redelivered.attempt_history[2]["type"], "leased")

    def test_dead_letters_lists_dead_messages(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("first")
        _ = queue.put("second")
        first = queue.get_message()
        second = queue.get_message()
        self.assertTrue(queue.dead_letter(first, error=RuntimeError("first failed")))
        self.assertTrue(queue.dead_letter(second, error=RuntimeError("second failed")))

        messages = queue.dead_letters()

        by_value = {message.value: message for message in messages}
        self.assertEqual(set(by_value), {"first", "second"})
        assert by_value["first"].last_error is not None
        assert by_value["second"].last_error is not None
        self.assertEqual(by_value["first"].last_error["message"], "first failed")
        self.assertEqual(by_value["second"].last_error["message"], "second failed")
        self.assertEqual(by_value["first"].attempt_history[0]["type"], "leased")
        self.assertEqual(by_value["first"].attempt_history[1]["type"], "dead_lettered")
        self.assertEqual(len(queue.dead_letters(limit=1)), 1)
        with self.assertRaises(ValueError):
            _ = queue.dead_letters(limit=-1)

    def test_sqlite_default_store_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = f"{tmpdir}/queue.sqlite3"
            queue = PersistentQueue("test", store_path=store_path)
            _ = queue.put("persistent-item")
            self.assertEqual(queue.qsize(), 1)

            # Re-open same path
            queue2 = PersistentQueue("test", store_path=store_path)
            self.assertEqual(queue2.qsize(), 1)
            msg = queue2.get_message()
            self.assertEqual(msg.value, "persistent-item")
            _ = queue2.ack(msg)
            self.assertTrue(queue2.empty())

    def test_sqlite_store_queue_operations(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteQueueStore(f"{tmpdir}/queue.sqlite3")
            now = time.time()

            _ = store.enqueue("jobs", "first", available_at=now)
            second = store.enqueue("jobs", "second", available_at=now + 10)
            third = store.enqueue("jobs", "third", available_at=now + 20)
            self.assertTrue(store.ack("jobs", third.id))

            self.assertEqual(store.qsize("jobs", now=now), 1)
            self.assertFalse(store.empty("jobs", now=now))

            leased = store.dequeue("jobs", lease_timeout=0.1, now=now)
            assert leased is not None
            self.assertEqual(leased.value, "first")
            self.assertEqual(leased.attempts, 1)
            self.assertEqual(store.qsize("jobs", now=now), 0)

            self.assertTrue(store.release(leased.queue, leased.id, available_at=now))
            leased_again = store.dequeue("jobs", lease_timeout=0.1, now=now)
            assert leased_again is not None
            self.assertEqual(leased_again.attempts, 2)
            self.assertTrue(store.dead_letter(leased_again.queue, leased_again.id))

            self.assertEqual(store.qsize("jobs", now=now + 10), 1)
            self.assertEqual(store.purge("jobs"), 2)
            self.assertEqual(store.purge("missing"), 0)
            self.assertIsNone(store.dequeue("jobs", lease_timeout=1, now=now + 10))

            self.assertFalse(store.ack("jobs", "missing"))
            self.assertFalse(store.release("jobs", "missing", available_at=now))
            self.assertFalse(store.dead_letter("jobs", "missing"))
            self.assertFalse(store.requeue_dead("jobs", "missing", available_at=now))
            self.assertEqual(second.value, "second")
            store.close()

    def test_sqlite_store_reclaims_expired_leases(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteQueueStore(f"{tmpdir}/queue.sqlite3")
            now = time.time()

            _ = store.enqueue("jobs", "lease", available_at=now)
            leased = store.dequeue("jobs", lease_timeout=0.1, now=now)
            assert leased is not None

            self.assertEqual(store.qsize("jobs", now=now), 0)
            self.assertEqual(store.qsize("jobs", now=now + 1), 0)

            reclaimed = store.dequeue("jobs", lease_timeout=0.1, now=now + 1)
            assert reclaimed is not None
            self.assertEqual(reclaimed.value, "lease")
            self.assertEqual(reclaimed.attempts, 2)
            store.close()

    def test_sqlite_store_rejects_invalid_queue_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteQueueStore(f"{tmpdir}/queue.sqlite3")

            with self.assertRaises(ValueError):
                _ = store.enqueue("", "item", available_at=time.time())

            with self.assertRaises(ValueError):
                _ = store.enqueue("bad:name", "item", available_at=time.time())

            store.close()

    def test_sqlite_store_creates_parent_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "nested" / "queue.sqlite3"
            store = SQLiteQueueStore(path)

            self.assertTrue(path.exists())
            store.close()

    def test_sqlite_store_handles_concurrent_producers_and_consumers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queue.sqlite3")
            queue_name = "jobs"
            total_messages = 120
            producer_count = 4
            consumer_count = 4
            start = threading.Event()
            producers_done = threading.Event()
            consumed = Counter[str]()
            consumed_lock = threading.Lock()
            errors: list[BaseException] = []
            errors_lock = threading.Lock()
            lock_retries = Counter[str]()

            def record_error(exc: BaseException) -> None:
                with errors_lock:
                    errors.append(exc)

            def retry_sqlite_locked(action: str, fn: Any) -> Any:
                attempts = 0
                while True:
                    try:
                        return fn()
                    except sqlite3.OperationalError as exc:
                        if "database is locked" not in str(exc).lower():
                            raise
                        attempts += 1
                        lock_retries[action] += 1
                        time.sleep(min(0.001 * attempts, 0.05))

            def producer(index: int) -> None:
                try:
                    queue = PersistentQueue(
                        queue_name, store_path=store_path, lease_timeout=0.2
                    )
                    start.wait()
                    for sequence in range(total_messages // producer_count):
                        retry_sqlite_locked(
                            "put",
                            lambda: queue.put(
                                {"producer": index, "sequence": sequence}
                            ),
                        )
                except BaseException as exc:  # pragma: no cover - defensive
                    record_error(exc)

            def consumer(index: int) -> None:
                try:
                    queue = PersistentQueue(
                        queue_name, store_path=store_path, lease_timeout=0.2
                    )
                    start.wait()
                    while True:
                        with consumed_lock:
                            if (
                                producers_done.is_set()
                                and sum(consumed.values()) >= total_messages
                            ):
                                return
                        try:
                            message = retry_sqlite_locked(
                                "get",
                                lambda: queue.get_message(
                                    block=False, leased_by=f"consumer-{index}"
                                ),
                            )
                        except Empty:
                            if producers_done.is_set():
                                with consumed_lock:
                                    if sum(consumed.values()) >= total_messages:
                                        return
                            time.sleep(0.001)
                            continue
                        if not retry_sqlite_locked("ack", lambda: queue.ack(message)):
                            record_error(RuntimeError(f"ack failed for {message.id}"))
                            return
                        with consumed_lock:
                            consumed[message.id] += 1
                except BaseException as exc:  # pragma: no cover - defensive
                    record_error(exc)

            threads = [
                threading.Thread(target=producer, args=(index,))
                for index in range(producer_count)
            ] + [
                threading.Thread(target=consumer, args=(index,))
                for index in range(consumer_count)
            ]

            for thread in threads:
                thread.start()
            start.set()

            for thread in threads[:producer_count]:
                thread.join()
            producers_done.set()
            for thread in threads[producer_count:]:
                thread.join(timeout=5.0)
                self.assertFalse(thread.is_alive())

            self.assertEqual(errors, [])
            self.assertEqual(sum(consumed.values()), total_messages)
            self.assertEqual(len(consumed), total_messages)
            self.assertTrue(all(count == 1 for count in consumed.values()))
            self.assertGreaterEqual(sum(lock_retries.values()), 0)

            queue = PersistentQueue(queue_name, store_path=store_path)
            stats = queue.stats().as_dict()
            self.assertEqual(stats["ready"], 0)
            self.assertEqual(stats["delayed"], 0)
            self.assertEqual(stats["inflight"], 0)
            self.assertEqual(stats["dead"], 0)
            self.assertEqual(stats["total"], 0)
            self.assertEqual(stats["by_worker_id"], {})
            self.assertEqual(sum(stats["leases_by_worker_id"].values()), total_messages)
            self.assertTrue(
                all(
                    worker.startswith("consumer-")
                    for worker in stats["leases_by_worker_id"]
                )
            )

    def test_lmdb_store_queue_operations(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()

            _ = store.enqueue("jobs", "first", available_at=now)
            second = store.enqueue("jobs", "second", available_at=now + 10)
            third = store.enqueue("jobs", "third", available_at=now + 20)
            self.assertTrue(store.ack("jobs", third.id))

            self.assertEqual(store.qsize("jobs", now=now), 1)
            self.assertFalse(store.empty("jobs", now=now))

            leased = store.dequeue("jobs", lease_timeout=0.1, now=now)
            assert leased is not None
            self.assertEqual(leased.value, "first")
            self.assertEqual(leased.attempts, 1)
            self.assertEqual(store.qsize("jobs", now=now), 0)

            self.assertTrue(store.release(leased.queue, leased.id, available_at=now))
            leased_again = store.dequeue("jobs", lease_timeout=0.1, now=now)
            assert leased_again is not None
            self.assertEqual(leased_again.attempts, 2)
            self.assertTrue(store.dead_letter(leased_again.queue, leased_again.id))

            self.assertEqual(store.qsize("jobs", now=now + 10), 1)
            self.assertEqual(store.purge("jobs"), 2)
            self.assertEqual(store.purge("missing"), 0)
            self.assertIsNone(store.dequeue("jobs", lease_timeout=1, now=now + 10))

            self.assertFalse(store.ack("jobs", "missing"))
            self.assertFalse(store.release("jobs", "missing", available_at=now))
            self.assertFalse(store.dead_letter("jobs", "missing"))
            self.assertFalse(store.requeue_dead("jobs", "missing", available_at=now))
            self.assertEqual(second.value, "second")

    def test_sqlite_stats_counts_dead_messages_outside_qsize(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            queue = PersistentQueue("jobs", store_path=f"{tmpdir}/queue.sqlite3")
            _ = queue.put("bad")
            message = queue.get_message()
            self.assertTrue(queue.dead_letter(message, error=RuntimeError("bad")))

            self.assertEqual(queue.qsize(), 0)
            stats = queue.stats()
            self.assertEqual(stats.ready, 0)
            self.assertEqual(stats.delayed, 0)
            self.assertEqual(stats.inflight, 0)
            self.assertEqual(stats.dead, 1)
            self.assertEqual(stats.total, 1)
            self.assertEqual(stats.by_worker_id, {})
            self.assertEqual(stats.leases_by_worker_id, {})
            self.assertIsNone(stats.oldest_ready_age_seconds)
            self.assertIsNone(stats.oldest_inflight_age_seconds)
            self.assertIsNone(stats.average_inflight_age_seconds)

    def test_sqlite_store_tracks_worker_throughput_after_ack(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            queue = PersistentQueue("jobs", store_path=f"{tmpdir}/queue.sqlite3")
            _ = queue.put("item")
            message = queue.get_message(leased_by="worker-a")

            stats = queue.stats()
            self.assertEqual(stats.ready, 0)
            self.assertEqual(stats.delayed, 0)
            self.assertEqual(stats.inflight, 1)
            self.assertEqual(stats.dead, 0)
            self.assertEqual(stats.total, 1)
            self.assertEqual(stats.by_worker_id, {"worker-a": 1})
            self.assertEqual(stats.leases_by_worker_id, {"worker-a": 1})
            self.assertIsNotNone(stats.oldest_inflight_age_seconds)
            self.assertIsNotNone(stats.average_inflight_age_seconds)

            self.assertTrue(queue.ack(message))

            stats = queue.stats()
            self.assertEqual(stats.ready, 0)
            self.assertEqual(stats.delayed, 0)
            self.assertEqual(stats.inflight, 0)
            self.assertEqual(stats.dead, 0)
            self.assertEqual(stats.total, 0)
            self.assertEqual(stats.by_worker_id, {})
            self.assertEqual(stats.leases_by_worker_id, {"worker-a": 1})
            self.assertIsNone(stats.oldest_ready_age_seconds)
            self.assertIsNone(stats.oldest_inflight_age_seconds)
            self.assertIsNone(stats.average_inflight_age_seconds)

    def test_sqlite_dead_letters_persist(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = f"{tmpdir}/queue.sqlite3"
            queue = PersistentQueue("jobs", store_path=store_path)
            _ = queue.put({"id": 1})
            message = queue.get_message()
            self.assertTrue(queue.dead_letter(message, error=TypeError("bad handler")))

            reopened = PersistentQueue("jobs", store_path=store_path)
            messages = reopened.dead_letters()

            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].value, {"id": 1})
            assert messages[0].last_error is not None
            self.assertEqual(messages[0].last_error["type"], "TypeError")
            self.assertEqual(messages[0].last_error["message"], "bad handler")

    def test_lmdb_store_inspects_messages_by_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()
            message = store.enqueue("jobs", {"id": 1}, available_at=now)

            inspected = store.get("jobs", message.id)

            assert inspected is not None
            self.assertEqual(inspected.id, message.id)
            self.assertEqual(inspected.value, {"id": 1})
            self.assertEqual(inspected.state, "ready")
            self.assertIsNone(store.get("jobs", "missing"))

    def test_lmdb_store_persists_and_clears_lease_owner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()
            _ = store.enqueue("jobs", {"id": 1}, available_at=now)
            leased = store.dequeue(
                "jobs", lease_timeout=30, now=now, leased_by="worker-a"
            )
            assert leased is not None
            self.assertEqual(leased.leased_by, "worker-a")

            inspected = store.get("jobs", leased.id)
            assert inspected is not None
            self.assertEqual(inspected.leased_by, "worker-a")
            self.assertTrue(store.release("jobs", leased.id, available_at=now + 1))

            released = store.get("jobs", leased.id)
            assert released is not None
            self.assertIsNone(released.leased_by)

    def test_lmdb_store_tracks_worker_throughput_after_ack(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()
            message = store.enqueue("jobs", {"id": 1}, available_at=now)
            leased = store.dequeue(
                "jobs", lease_timeout=30, now=now, leased_by="worker-a"
            )
            assert leased is not None

            stats = store.stats("jobs", now=now)
            self.assertEqual(stats.ready, 0)
            self.assertEqual(stats.delayed, 0)
            self.assertEqual(stats.inflight, 1)
            self.assertEqual(stats.dead, 0)
            self.assertEqual(stats.total, 1)
            self.assertEqual(stats.by_worker_id, {"worker-a": 1})
            self.assertEqual(stats.leases_by_worker_id, {"worker-a": 1})
            self.assertIsNotNone(stats.oldest_inflight_age_seconds)
            self.assertIsNotNone(stats.average_inflight_age_seconds)

            self.assertTrue(store.ack("jobs", message.id))

            stats = store.stats("jobs", now=now)
            self.assertEqual(stats.ready, 0)
            self.assertEqual(stats.delayed, 0)
            self.assertEqual(stats.inflight, 0)
            self.assertEqual(stats.dead, 0)
            self.assertEqual(stats.total, 0)
            self.assertEqual(stats.by_worker_id, {})
            self.assertEqual(stats.leases_by_worker_id, {"worker-a": 1})
            self.assertIsNone(stats.oldest_ready_age_seconds)
            self.assertIsNone(stats.oldest_inflight_age_seconds)
            self.assertIsNone(stats.average_inflight_age_seconds)

    def test_lmdb_store_requeues_dead_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()
            message = store.enqueue("jobs", {"id": 1}, available_at=now)
            leased = store.dequeue("jobs", lease_timeout=30, now=now)
            assert leased is not None
            self.assertTrue(store.dead_letter("jobs", leased.id))

            self.assertTrue(
                store.requeue_dead("jobs", message.id, available_at=now + 10)
            )

            self.assertEqual(store.dead_letters("jobs"), [])
            self.assertEqual(store.qsize("jobs", now=now), 0)
            self.assertEqual(store.qsize("jobs", now=now + 10), 1)
            redelivered = store.dequeue("jobs", lease_timeout=30, now=now + 10)
            assert redelivered is not None
            self.assertEqual(redelivered.id, message.id)
            self.assertEqual(redelivered.value, {"id": 1})

    def test_lmdb_store_serializes_records_as_versioned_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            message = store.enqueue("jobs", {"kind": "email"}, available_at=time.time())

            with store._env.begin() as txn:
                raw = txn.get(_message_key("jobs", message.id))

            assert raw is not None
            payload = json.loads(bytes(raw).decode("utf-8"))
            self.assertEqual(payload["version"], 2)
            self.assertEqual(payload["value"], {"kind": "email"})
            self.assertIsNone(payload["leased_by"])
            self.assertEqual(payload["attempt_history"], [])

    def test_lmdb_store_rejects_non_json_serializable_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)

            with self.assertRaisesRegex(ValueError, "JSON-serializable"):
                _ = store.enqueue("jobs", object(), available_at=time.time())

    def test_sqlite_store_persists_last_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = f"{tmpdir}/queue.sqlite3"
            queue = PersistentQueue("jobs", store_path=store_path)
            _ = queue.put("bad")
            message = queue.get_message()
            self.assertTrue(queue.release(message, error=TypeError("bad handler")))

            reopened = PersistentQueue("jobs", store_path=store_path)
            failed = reopened.get_message()

            self.assertEqual(
                failed.last_error,
                {
                    "type": "TypeError",
                    "module": "builtins",
                    "message": "bad handler",
                },
            )
            self.assertIsNotNone(failed.failed_at)

    def test_ready_key_parse_error_has_context(self) -> None:
        with self.assertRaises(ValueError) as exc_info:
            _ = _timestamp_from_ready_key(b"queue:jobs:ready")

        self.assertIn("malformed LMDB queue index key", str(exc_info.exception))

        with self.assertRaises(ValueError):
            _ = _timestamp_from_ready_key(b"\xff")

        with self.assertRaises(ValueError):
            _ = _timestamp_from_inflight_key(_ready_key("jobs", time.time(), 1, "id"))

        self.assertIsInstance(
            _timestamp_from_inflight_key(_inflight_key("jobs", time.time(), "id")),
            float,
        )

    def test_sequence_from_index_key_handles_non_ready_keys(self) -> None:
        self.assertEqual(_sequence_from_index_key(b"\xff"), 0)
        self.assertEqual(_sequence_from_index_key(_dead_key("jobs", "id")), 0)

        store = SQLiteQueueStore(":memory:")
        raw = _encode_record(
            _QueueRecord(
                id="id",
                value="value",
                queue="jobs",
                attempts=0,
                created_at=time.time(),
                available_at=time.time(),
                leased_until=None,
                leased_by=None,
                last_error=None,
                failed_at=None,
                state="ready",
                index_key=None,
            )
        ).decode("utf-8")
        self.assertEqual(store._sequence(raw), 0)
        store.close()

    def test_decode_record_rejects_invalid_json_and_versions(self) -> None:
        with self.assertRaises(ValueError):
            _ = _decode_record(b"not-json")

        with self.assertRaises(ValueError):
            _ = _decode_record(b'{"version":999}')

        decoded = _decode_record(
            json.dumps(
                {
                    "version": 1,
                    "id": "job-1",
                    "value": "x",
                    "queue": "jobs",
                    "attempts": 1,
                    "created_at": 1.0,
                    "available_at": 1.0,
                    "leased_until": None,
                    "leased_by": None,
                    "last_error": None,
                    "failed_at": None,
                    "state": "ready",
                    "index_key": None,
                }
            ).encode("utf-8")
        )
        self.assertEqual(decoded.attempt_history, [])

    def test_lmdb_store_reclaims_expired_leases(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()

            _ = store.enqueue("jobs", "lease", available_at=now)
            leased = store.dequeue("jobs", lease_timeout=0.1, now=now)
            assert leased is not None

            self.assertEqual(store.qsize("jobs", now=now), 0)
            self.assertEqual(store.qsize("jobs", now=now + 1), 0)

            reclaimed = store.dequeue("jobs", lease_timeout=0.1, now=now + 1)
            assert reclaimed is not None
            self.assertEqual(reclaimed.value, "lease")
            self.assertEqual(reclaimed.attempts, 2)

    def test_lmdb_store_rejects_invalid_queue_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)

            with self.assertRaises(ValueError):
                _ = store.enqueue("", "item", available_at=time.time())

            with self.assertRaises(ValueError):
                _ = store.enqueue("bad:name", "item", available_at=time.time())

    def test_default_sqlite_store_is_opened_lazily(self) -> None:
        with mock.patch("localqueue.queue.SQLiteQueueStore") as store_cls:
            queue = PersistentQueue("test")
            store_cls.assert_not_called()

            fake_store = mock.Mock()
            fake_store.qsize.return_value = 0
            store_cls.return_value = fake_store

            self.assertTrue(queue.empty())
            store_cls.assert_called_once_with("localqueue_queue.sqlite3")

    def test_lmdb_lock_error_is_reworded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_lmdb = mock.Mock()
            fake_lmdb.LockError = lmdb.LockError
            fake_lmdb.open.side_effect = lmdb.LockError("busy")
            with mock.patch("localqueue.store._import_lmdb", return_value=fake_lmdb):
                with self.assertRaises(QueueStoreLockedError) as exc_info:
                    _ = LMDBQueueStore(tmpdir)

        self.assertIn("locked by another process", str(exc_info.exception))

    def test_lmdb_store_requires_optional_dependency(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict("sys.modules", {"lmdb": None}):
                with self.assertRaisesRegex(RuntimeError, "localqueue\\[lmdb\\]"):
                    _ = LMDBQueueStore(tmpdir)

    def test_lease_expiration_and_reclaim(self) -> None:
        store = MemoryQueueStore()
        queue = PersistentQueue("test", store=store, lease_timeout=0.1)
        _ = queue.put("leased-item")

        msg = queue.get_message()
        self.assertEqual(msg.value, "leased-item")
        self.assertEqual(queue.qsize(), 0)

        time.sleep(0.2)
        # qsize() should reclaim expired leases
        self.assertEqual(queue.qsize(), 1)
        msg2 = queue.get_message()
        self.assertEqual(msg2.value, "leased-item")
        self.assertEqual(msg2.attempts, 2)
        _ = queue.ack(msg2)

    def test_delayed_delivery(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("delayed", delay=0.2)
        self.assertEqual(queue.qsize(), 0)
        with self.assertRaises(Empty):
            queue.get_message(block=False)

        time.sleep(0.3)
        self.assertEqual(queue.qsize(), 1)
        self.assertEqual(queue.get(), "delayed")

    def test_rejects_negative_delay_and_timeout(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        with self.assertRaises(ValueError):
            _ = queue.put("item", delay=-1)

        _ = queue.put("item")
        message = queue.get_message()
        with self.assertRaises(ValueError):
            _ = queue.release(message, delay=-1)

        with self.assertRaises(ValueError):
            _ = queue.get(timeout=-1)

        with self.assertRaises(ValueError):
            _ = queue.put("next", timeout=-1)

    def test_nowait_helpers_raise_when_unavailable(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore(), maxsize=1)

        with self.assertRaises(Empty):
            _ = queue.get_nowait()

        _ = queue.put_nowait("one")
        with self.assertRaises(Full):
            _ = queue.put_nowait("two")

    def test_put_and_get_timeout(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore(), maxsize=1)
        _ = queue.put("one")

        with self.assertRaises(Full):
            _ = queue.put("two", timeout=0)

        self.assertEqual(queue.get_nowait(), "one")
        queue.task_done()

        with self.assertRaises(Empty):
            _ = queue.get(timeout=0)

    def test_maxsize_and_blocking_put(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore(), maxsize=1)
        _ = queue.put("one")
        self.assertTrue(queue.full())

        start = time.time()

        def slow_getter() -> None:
            time.sleep(0.2)
            _ = queue.get()

            queue.task_done()

        t = threading.Thread(target=slow_getter)
        t.start()

        _ = queue.put("two", timeout=1.0)
        self.assertGreaterEqual(time.time() - start, 0.2)
        self.assertEqual(queue.get(), "two")
        t.join()

    def test_release_with_delay(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("item")
        msg = queue.get_message()
        _ = queue.release(msg, delay=0.2)
        self.assertEqual(queue.qsize(), 0)
        time.sleep(0.3)
        self.assertEqual(queue.qsize(), 1)

    def test_ack_release_and_dead_letter_return_false_for_unknown_message(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        unknown = QueueMessage(id="missing", value="x", queue="test")

        self.assertFalse(queue.ack(unknown))
        self.assertFalse(queue.release(unknown))
        self.assertFalse(queue.dead_letter(unknown))
        self.assertFalse(queue.requeue_dead(unknown))

    def test_dead_letter(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("bad-item")
        msg = queue.get_message()
        _ = queue.dead_letter(msg, error=RuntimeError("cannot process"))
        self.assertEqual(queue.qsize(), 0)
        # Should not be in ready queue anymore even after reclaim
        time.sleep(0.1)
        self.assertEqual(queue.qsize(), 0)

    def test_requeue_dead_returns_message_to_ready_queue(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("bad-item")
        msg = queue.get_message()
        self.assertTrue(queue.dead_letter(msg, error=RuntimeError("cannot process")))

        self.assertTrue(queue.requeue_dead(msg))

        self.assertEqual(queue.dead_letters(), [])
        self.assertEqual(queue.qsize(), 1)
        redelivered = queue.get_message()
        self.assertEqual(redelivered.id, msg.id)
        self.assertEqual(redelivered.value, "bad-item")
        assert redelivered.last_error is not None
        self.assertEqual(redelivered.last_error["message"], "cannot process")

    def test_requeue_dead_supports_delay_and_rejects_negative_delay(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("bad-item")
        msg = queue.get_message()
        self.assertTrue(queue.dead_letter(msg))

        with self.assertRaises(ValueError):
            _ = queue.requeue_dead(msg, delay=-1)

        self.assertTrue(queue.requeue_dead(msg, delay=0.2))
        self.assertEqual(queue.qsize(), 0)
        time.sleep(0.3)
        self.assertEqual(queue.qsize(), 1)

    def test_release_preserves_last_error_on_redelivery(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("bad-item")
        msg = queue.get_message()
        self.assertTrue(queue.release(msg, error=TypeError("bad signature")))

        redelivered = queue.get_message()
        self.assertEqual(
            redelivered.last_error,
            {
                "type": "TypeError",
                "module": "builtins",
                "message": "bad signature",
            },
        )
        self.assertIsNotNone(redelivered.failed_at)

    def test_purge(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("1")
        _ = queue.put("2")
        _ = queue.get()
        self.assertEqual(queue.purge(), 2)
        self.assertEqual(queue.qsize(), 0)

    def test_task_done_rejects_extra_calls_and_join_waits(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        with self.assertRaises(ValueError):
            queue.task_done()

        _ = queue.put("item")
        self.assertEqual(queue.get(), "item")

        joined = threading.Event()

        def wait_for_join() -> None:
            queue.join()
            joined.set()

        t = threading.Thread(target=wait_for_join)
        t.start()
        time.sleep(0.1)
        self.assertFalse(joined.is_set())

        queue.task_done()
        self.assertTrue(joined.wait(1.0))
        t.join()

    def test_worker_retries_and_acks_on_success(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("a")

        attempts = {"count": 0}

        @persistent_worker(
            queue, store=MemoryAttemptStore(), max_tries=3, wait=lambda _: 0
        )
        def handle(value: str, *args: Any, **kwargs: Any) -> str:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise RuntimeError("try again")
            return value.upper()

        self.assertEqual(handle("a"), "A")
        self.assertEqual(attempts["count"], 2)
        with self.assertRaises(Empty):
            _ = queue.get_message(block=False)

    def test_async_worker_retries_and_acks_on_success(self) -> None:
        async def scenario() -> None:
            queue = PersistentQueue("test", store=MemoryQueueStore())
            _ = queue.put("a")
            attempts = {"count": 0}

            @persistent_async_worker(
                queue, store=MemoryAttemptStore(), max_tries=3, wait=lambda _: 0
            )
            async def handle(value: str) -> str:
                attempts["count"] += 1
                if attempts["count"] == 1:
                    raise RuntimeError("try again")
                return value.upper()

            self.assertEqual(await cast(Any, handle)(), "A")
            self.assertEqual(attempts["count"], 2)
            with self.assertRaises(Empty):
                _ = queue.get_message(block=False)

        asyncio.run(scenario())

    def test_async_worker_dead_letters_or_releases_on_failure(self) -> None:
        async def dead_letter_scenario() -> None:
            queue = PersistentQueue("test", store=MemoryQueueStore())
            _ = queue.put("bad")

            @persistent_async_worker(
                queue, store=MemoryAttemptStore(), max_tries=1, wait=lambda _: 0
            )
            async def fail(value: str) -> None:
                raise RuntimeError(value)

            with self.assertRaises(RuntimeError):
                await cast(Any, fail)()

            self.assertTrue(queue.empty())

        async def release_scenario() -> None:
            queue = PersistentQueue("test", store=MemoryQueueStore())
            _ = queue.put("retry")

            @persistent_async_worker(
                queue,
                store=MemoryAttemptStore(),
                max_tries=1,
                wait=lambda _: 0,
                dead_letter_on_exhaustion=False,
            )
            async def fail(value: str) -> None:
                raise RuntimeError(value)

            with self.assertRaises(RuntimeError):
                await cast(Any, fail)()

            self.assertEqual(queue.qsize(), 1)

        asyncio.run(dead_letter_scenario())
        asyncio.run(release_scenario())

    def test_worker_dead_letters_or_releases_on_failure(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("bad")

        @persistent_worker(
            queue, store=MemoryAttemptStore(), max_tries=1, wait=lambda _: 0
        )
        def fail(value: str) -> None:
            raise RuntimeError(value)

        with self.assertRaises(RuntimeError):
            cast(Any, fail)()

        self.assertTrue(queue.empty())

        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("retry")

        @persistent_worker(
            queue,
            store=MemoryAttemptStore(),
            max_tries=1,
            wait=lambda _: 0,
            dead_letter_on_exhaustion=False,
        )
        def release(value: str) -> None:
            raise RuntimeError(value)

        with self.assertRaises(RuntimeError):
            cast(Any, release)()

        self.assertEqual(queue.qsize(), 1)
        message = queue.get_message()
        assert message.last_error is not None
        self.assertEqual(message.last_error["type"], "RuntimeError")
        self.assertEqual(message.last_error["message"], "retry")
        self.assertIsNotNone(message.failed_at)

    def test_worker_receives_message_value_when_called_without_arguments(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put({"name": "alice"})

        @persistent_worker(
            queue, store=MemoryAttemptStore(), max_tries=1, wait=lambda _: 0
        )
        def handle(payload: dict[str, str]) -> str:
            return payload["name"].upper()

        self.assertEqual(cast(Any, handle)(), "ALICE")

    def test_worker_config_can_be_shared_and_overridden(self) -> None:
        config = PersistentWorkerConfig(
            store=MemoryAttemptStore(),
            max_tries=1,
            wait=lambda _: 0,
            dead_letter_on_exhaustion=False,
        )

        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("retry")

        @persistent_worker(queue, config=config)
        def release(value: str) -> None:
            raise RuntimeError(value)

        with self.assertRaises(RuntimeError):
            cast(Any, release)()

        self.assertEqual(queue.qsize(), 1)

        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("dead")

        @persistent_worker(queue, config=config, dead_letter_on_exhaustion=True)
        def dead_letter(value: str) -> None:
            raise RuntimeError(value)

        with self.assertRaises(RuntimeError):
            cast(Any, dead_letter)()

        self.assertTrue(queue.empty())

    def test_worker_prefers_dead_letter_on_failure_name(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("retry")

        @persistent_worker(
            queue,
            store=MemoryAttemptStore(),
            max_tries=1,
            wait=lambda _: 0,
            dead_letter_on_failure=False,
        )
        def release(value: str) -> None:
            raise RuntimeError(value)

        with self.assertRaises(RuntimeError):
            cast(Any, release)()

        self.assertEqual(queue.qsize(), 1)

    def test_worker_config_rejects_conflicting_failure_policy_names(self) -> None:
        with self.assertRaises(ValueError):
            _ = PersistentWorkerConfig(
                dead_letter_on_failure=True,
                dead_letter_on_exhaustion=False,
            )

    def test_worker_config_rejects_negative_release_delay(self) -> None:
        with self.assertRaises(ValueError):
            _ = PersistentWorkerConfig(release_delay=-1)

        config = PersistentWorkerConfig(release_delay=0.0)
        with self.assertRaises(ValueError):
            _ = config.with_overrides(release_delay=-1)

    def test_async_worker_accepts_config(self) -> None:
        async def scenario() -> None:
            queue = PersistentQueue("test", store=MemoryQueueStore())
            _ = queue.put("a")
            config = PersistentWorkerConfig(
                store=MemoryAttemptStore(),
                max_tries=2,
                wait=lambda _: 0,
            )
            attempts = {"count": 0}

            @persistent_async_worker(queue, config=config)
            async def handle(value: str) -> str:
                attempts["count"] += 1
                if attempts["count"] == 1:
                    raise RuntimeError("try again")
                return value.upper()

            self.assertEqual(await cast(Any, handle)(), "A")
            self.assertEqual(attempts["count"], 2)

        asyncio.run(scenario())


if __name__ == "__main__":
    _ = unittest.main()
