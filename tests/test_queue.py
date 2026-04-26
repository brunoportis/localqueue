from __future__ import annotations

import asyncio
from collections import Counter
import concurrent.futures
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from dataclasses import dataclass
from queue import Empty, Full
from pathlib import Path
from typing import Any, cast
from unittest import mock

import lmdb

from localqueue.idempotency.stores import _shared as _idempotency_shared
from localqueue.idempotency.stores import lmdb as _idempotency_lmdb
from localqueue.results.stores import _shared as _result_shared
from localqueue.results.stores import lmdb as _result_lmdb
from localqueue.retry import MemoryAttemptStore, RetryRecord, SQLiteAttemptStore
from localqueue import (
    AT_LEAST_ONCE_DELIVERY,
    DEAD_LETTER_QUEUE,
    DEDUPE_KEY_SUPPORT,
    EXPLICIT_ACKNOWLEDGEMENT,
    FIFO_READY_ORDERING,
    FIXED_LEASE_TIMEOUT,
    IdempotencyRecord,
    IdempotencyStoreLockedError,
    LOCAL_ATOMIC_COMMIT,
    LOCAL_QUEUE_PLACEMENT,
    LMDBResultStore,
    LMDBIdempotencyStore,
    POINT_TO_POINT_ROUTING,
    PULL_CONSUMPTION,
    AtLeastOnceDelivery,
    DeadLetterQueue,
    EffectivelyOnceDelivery,
    AtMostOnceDelivery,
    BestEffortOrdering,
    BoundedBackpressure,
    AsyncioNotification,
    CallbackDispatcher,
    CallbackNotification,
    DedupeKeySupport,
    FixedLeaseTimeout,
    FifoReadyOrdering,
    InProcessNotification,
    ExplicitAcknowledgement,
    LocalQueuePlacement,
    LMDBQueueStore,
    LOCAL_AT_LEAST_ONCE,
    MemoryIdempotencyStore,
    MemoryResultStore,
    MemoryQueueStore,
    NO_DISPATCHER,
    NO_NOTIFICATION,
    NO_RESULT_POLICY,
    NO_SUBSCRIPTIONS,
    PersistentQueue,
    PersistentWorkerConfig,
    PointToPointRouting,
    PriorityOrdering,
    PublishSubscribeRouting,
    PullConsumption,
    PushConsumption,
    RejectingBackpressure,
    NoDeduplication,
    NoDispatcher,
    NoNotification,
    QueuePolicySet,
    QueueSemantics,
    QueueMessage,
    ResultStoreLockedError,
    QueueStoreLockedError,
    RETURN_STORED_RESULT,
    ReturnStoredResult,
    RemoteQueuePlacement,
    SagaCommit,
    StaticFanoutSubscriptions,
    SQLiteIdempotencyStore,
    SQLiteResultStore,
    TwoPhaseCommit,
    TransactionalOutboxCommit,
    TWO_PHASE_COMMIT,
    SQLiteQueueStore,
    LocalAtomicCommit,
    WebSocketNotification,
    persistent_async_worker,
    persistent_worker,
)
from localqueue.worker import (
    WorkerPolicyState,
    _get_message_async,
    _record_failure,
    _record_success,
    _commit_outbox,
    _commit_two_phase,
    _commit_saga,
    _result_key,
    _resolve_dead_letter_on_failure,
    _UNSET,
    _validate_circuit_breaker,
    _validate_min_interval,
    _validate_release_delay,
    _sleep_for_policy,
    _sleep_for_policy_async,
)
from localqueue.stores._shared import (
    QueueRecord as _QueueRecord,
    decode_record as _decode_record,
    encode_worker_heartbeats as _encode_worker_heartbeats,
    dead_key as _dead_key,
    dedupe_key_key as _dedupe_key_key,
    dedupe_token as _dedupe_token,
    encode_record as _encode_record,
    inflight_key as _inflight_key,
    message_key as _message_key,
    ready_key as _ready_key,
    last_leased_at as _last_leased_at,
    replace_record as _replace_record,
    sequence_from_index_key as _sequence_from_index_key,
    timestamp_from_inflight_key as _timestamp_from_inflight_key,
    timestamp_from_ready_key as _timestamp_from_ready_key,
)
from localqueue.queue import _deadline, _remaining, _wait_time, _validate_retry_defaults


@dataclass(slots=True)
class _ThreadedQueueScenarioResult:
    errors: list[BaseException]
    consumed: Counter[str]
    lock_retries: Counter[str]
    stats: dict[str, Any]


def _run_sqlite_threaded_queue_scenario(
    *, store_path: str, queue_name: str, total_messages: int
) -> _ThreadedQueueScenarioResult:
    producer_count = 4
    consumer_count = 4
    start = threading.Event()
    producers_done = threading.Event()
    consumed = Counter[str]()
    consumed_lock = threading.Lock()
    errors: list[BaseException] = []
    errors_lock = threading.Lock()
    lock_retries = Counter[str]()

    threads = [
        threading.Thread(
            target=_threaded_sqlite_producer,
            args=(
                index,
                queue_name,
                store_path,
                total_messages // producer_count,
                start,
                lock_retries,
                errors,
                errors_lock,
            ),
        )
        for index in range(producer_count)
    ] + [
        threading.Thread(
            target=_threaded_sqlite_consumer,
            args=(
                index,
                queue_name,
                store_path,
                start,
                producers_done,
                consumed,
                consumed_lock,
                total_messages,
                lock_retries,
                errors,
                errors_lock,
            ),
        )
        for index in range(consumer_count)
    ]
    _start_threaded_scenario(threads, producer_count, start, producers_done)

    queue = PersistentQueue(queue_name, store_path=store_path)
    return _ThreadedQueueScenarioResult(
        errors=errors,
        consumed=consumed,
        lock_retries=lock_retries,
        stats=queue.stats().as_dict(),
    )


def _record_threaded_error(
    exc: BaseException, errors: list[BaseException], errors_lock: threading.Lock
) -> None:
    with errors_lock:
        errors.append(exc)


def _retry_threaded_sqlite_locked(
    action: str, fn: Any, lock_retries: Counter[str]
) -> Any:
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


def _threaded_sqlite_producer(
    index: int,
    queue_name: str,
    store_path: str,
    message_count: int,
    start: threading.Event,
    lock_retries: Counter[str],
    errors: list[BaseException],
    errors_lock: threading.Lock,
) -> None:
    try:
        queue = PersistentQueue(queue_name, store_path=store_path, lease_timeout=1.0)
        start.wait()
        for sequence in range(message_count):
            _retry_threaded_sqlite_locked(
                "put",
                lambda sequence=sequence: queue.put(
                    {"producer": index, "sequence": sequence}
                ),
                lock_retries,
            )
    except Exception as exc:  # pragma: no cover - defensive
        _record_threaded_error(exc, errors, errors_lock)


def _threaded_sqlite_consumer(
    index: int,
    queue_name: str,
    store_path: str,
    start: threading.Event,
    producers_done: threading.Event,
    consumed: Counter[str],
    consumed_lock: threading.Lock,
    total_messages: int,
    lock_retries: Counter[str],
    errors: list[BaseException],
    errors_lock: threading.Lock,
) -> None:
    try:
        queue = PersistentQueue(queue_name, store_path=store_path, lease_timeout=1.0)
        start.wait()
        while not _threaded_scenario_complete(
            producers_done, consumed, consumed_lock, total_messages
        ):
            try:
                message = _retry_threaded_sqlite_locked(
                    "get",
                    lambda: queue.get_message(
                        block=False, leased_by=f"consumer-{index}"
                    ),
                    lock_retries,
                )
            except Empty:
                time.sleep(0.001)
                continue
            if not _retry_threaded_sqlite_locked(
                "ack", lambda message=message: queue.ack(message), lock_retries
            ):
                _record_threaded_error(
                    RuntimeError(f"ack failed for {message.id}"), errors, errors_lock
                )
                return
            with consumed_lock:
                consumed[message.id] += 1
    except Exception as exc:  # pragma: no cover - defensive
        _record_threaded_error(exc, errors, errors_lock)


def _threaded_scenario_complete(
    producers_done: threading.Event,
    consumed: Counter[str],
    consumed_lock: threading.Lock,
    total_messages: int,
) -> bool:
    with consumed_lock:
        return producers_done.is_set() and sum(consumed.values()) >= total_messages


def _start_threaded_scenario(
    threads: list[threading.Thread],
    producer_count: int,
    start: threading.Event,
    producers_done: threading.Event,
) -> None:
    for thread in threads:
        thread.start()
    start.set()

    for thread in threads[:producer_count]:
        thread.join()
    producers_done.set()
    for thread in threads[producer_count:]:
        thread.join(timeout=5.0)
        assert not thread.is_alive()


class QueueTests(unittest.TestCase):
    def test_constructor_rejects_invalid_arguments(self) -> None:
        store = MemoryQueueStore()
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(
                ValueError, "pass either store= or store_path=, not both"
            ):
                _ = PersistentQueue("test", store=store, store_path=tmpdir)

        with self.assertRaisesRegex(
            ValueError, "lease_timeout must be greater than zero"
        ):
            _ = PersistentQueue("test", store=store, lease_timeout=0)

        with self.assertRaisesRegex(
            ValueError, "lease timeout must be greater than zero"
        ):
            _ = FixedLeaseTimeout(0)

        with self.assertRaisesRegex(ValueError, "maxsize cannot be negative"):
            _ = PersistentQueue("test", store=store, maxsize=-1)

        with self.assertRaisesRegex(ValueError, "maxsize cannot be negative"):
            _ = BoundedBackpressure(-1)

        with self.assertRaisesRegex(
            ValueError, "backpressure overflow must be 'block' or 'reject'"
        ):
            _ = BoundedBackpressure(1, overflow=cast("Any", "drop-oldest"))

        with self.assertRaisesRegex(ValueError, "maxsize cannot be negative"):
            _ = RejectingBackpressure(-1)

        with self.assertRaisesRegex(
            ValueError, "rejecting backpressure overflow must be 'reject'"
        ):
            _ = RejectingBackpressure(1, overflow=cast("Any", "block"))

        with self.assertRaisesRegex(ValueError, "subscribers cannot be empty"):
            _ = StaticFanoutSubscriptions(())

        with self.assertRaisesRegex(ValueError, "subscriber names cannot be empty"):
            _ = StaticFanoutSubscriptions(("billing", ""))

        with self.assertRaisesRegex(ValueError, "subscriber names must be unique"):
            _ = StaticFanoutSubscriptions(("billing", "billing"))

        with self.assertRaisesRegex(ValueError, "handlers cannot be empty"):
            _ = CallbackDispatcher(())

        with self.assertRaisesRegex(ValueError, "listeners cannot be empty"):
            _ = CallbackNotification(())

        queue = PersistentQueue("test", store=store)
        with self.assertRaisesRegex(TypeError, "priority must be an integer"):
            _ = queue.put("item", priority=cast("Any", 1.5))
        with self.assertRaisesRegex(TypeError, "priority must be an integer"):
            _ = queue.put("item", priority=cast("Any", True))
        with self.assertRaisesRegex(ValueError, "priority cannot be negative"):
            _ = queue.put("item", priority=-1)
        with self.assertRaisesRegex(ValueError, "priority requires PriorityOrdering"):
            _ = queue.put("item", priority=1)

        effectively_once_queue = PersistentQueue(
            "test",
            store=store,
            delivery_policy=EffectivelyOnceDelivery(),
        )
        with self.assertRaisesRegex(
            ValueError, "dedupe_key is required by the active delivery_policy"
        ):
            _ = effectively_once_queue.put("item")

        with self.assertRaisesRegex(
            ValueError, "semantics locality must match locality_policy locality"
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                semantics=QueueSemantics(locality="remote"),
                locality_policy=LocalQueuePlacement(),
            )

        with self.assertRaisesRegex(
            ValueError, "semantics leases must match lease_policy uses_leases"
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                semantics=QueueSemantics(leases=False),
                lease_policy=FixedLeaseTimeout(),
            )

        with self.assertRaisesRegex(
            ValueError,
            "semantics acknowledgements must match acknowledgement_policy acknowledgements",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                semantics=QueueSemantics(acknowledgements=False),
                acknowledgement_policy=ExplicitAcknowledgement(),
            )

        with self.assertRaisesRegex(
            ValueError,
            "semantics dead_letters must match dead_letter_policy dead_letters",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                semantics=QueueSemantics(dead_letters=False),
                dead_letter_policy=DeadLetterQueue(),
            )

        with self.assertRaisesRegex(
            ValueError,
            "semantics deduplication must match deduplication_policy deduplication",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                semantics=QueueSemantics(deduplication=False),
                deduplication_policy=DedupeKeySupport(),
            )

        with self.assertRaisesRegex(
            ValueError, "dedupe_key is not supported by the active deduplication_policy"
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                deduplication_policy=NoDeduplication(),
            ).put("item", dedupe_key="job-1")

        with self.assertRaisesRegex(
            ValueError, "semantics delivery must match delivery_policy guarantee"
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                semantics=QueueSemantics(delivery="at-most-once"),
                delivery_policy=AtLeastOnceDelivery(),
            )

        with self.assertRaisesRegex(
            ValueError, "semantics ordering must match ordering_policy guarantee"
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                semantics=QueueSemantics(ordering="priority"),
                ordering_policy=FifoReadyOrdering(),
            )

        with self.assertRaisesRegex(
            ValueError, "semantics consumption must match consumption_policy pattern"
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                semantics=QueueSemantics(consumption="push"),
                consumption_policy=PullConsumption(),
            )

        with self.assertRaisesRegex(
            ValueError, "semantics routing must match routing_policy pattern"
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                semantics=QueueSemantics(routing="publish-subscribe"),
                routing_policy=PointToPointRouting(),
            )

        with self.assertRaisesRegex(
            ValueError,
            "semantics subscriptions must match subscription_policy subscriptions",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                semantics=QueueSemantics(subscriptions=True),
                subscription_policy=NO_SUBSCRIPTIONS,
            )

        with self.assertRaisesRegex(
            ValueError,
            "semantics notifications must match notification_policy notifies",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                semantics=QueueSemantics(notifications=True),
                notification_policy=NO_NOTIFICATION,
            )

        with self.assertRaisesRegex(
            ValueError, "pass either maxsize= or backpressure=, not both"
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                maxsize=1,
                backpressure=BoundedBackpressure(1),
            )

        with self.assertRaisesRegex(
            ValueError, "pass either lease_timeout= or lease_policy=, not both"
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                lease_timeout=10,
                lease_policy=FixedLeaseTimeout(20),
            )

        with self.assertRaisesRegex(
            ValueError,
            "pass either delivery_policy= or policy_set.delivery_policy, not both",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                delivery_policy=AtLeastOnceDelivery(),
                policy_set=QueuePolicySet(delivery_policy=AtMostOnceDelivery()),
            )

        with self.assertRaisesRegex(
            ValueError,
            "pass either locality_policy= or policy_set.locality_policy, not both",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                locality_policy=LocalQueuePlacement(),
                policy_set=QueuePolicySet(locality_policy=RemoteQueuePlacement()),
            )

        with self.assertRaisesRegex(
            ValueError,
            "pass either lease_policy= or policy_set.lease_policy, not both",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                lease_policy=FixedLeaseTimeout(20),
                policy_set=QueuePolicySet(lease_policy=FixedLeaseTimeout(10)),
            )

        with self.assertRaisesRegex(
            ValueError,
            "pass either acknowledgement_policy= or "
            "policy_set.acknowledgement_policy, not both",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                acknowledgement_policy=ExplicitAcknowledgement(),
                policy_set=QueuePolicySet(
                    acknowledgement_policy=ExplicitAcknowledgement()
                ),
            )

        with self.assertRaisesRegex(
            ValueError,
            "pass either dead_letter_policy= or policy_set.dead_letter_policy, not both",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                dead_letter_policy=DeadLetterQueue(),
                policy_set=QueuePolicySet(dead_letter_policy=DeadLetterQueue()),
            )

        with self.assertRaisesRegex(
            ValueError,
            "pass either deduplication_policy= or policy_set.deduplication_policy, not both",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                deduplication_policy=DedupeKeySupport(),
                policy_set=QueuePolicySet(deduplication_policy=NoDeduplication()),
            )

        with self.assertRaisesRegex(
            ValueError,
            "pass either subscription_policy= or policy_set.subscription_policy, not both",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                subscription_policy=NO_SUBSCRIPTIONS,
                policy_set=QueuePolicySet(
                    subscription_policy=StaticFanoutSubscriptions(("billing",))
                ),
            )

        with self.assertRaisesRegex(
            ValueError,
            "pass either dispatch_policy= or policy_set.dispatch_policy, not both",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                dispatch_policy=NO_DISPATCHER,
                policy_set=QueuePolicySet(
                    dispatch_policy=CallbackDispatcher((lambda message: message,))
                ),
            )

        with self.assertRaisesRegex(
            ValueError,
            "pass either notification_policy= or policy_set.notification_policy, not both",
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                notification_policy=NO_NOTIFICATION,
                policy_set=QueuePolicySet(
                    notification_policy=CallbackNotification((lambda message: message,))
                ),
            )

        with self.assertRaisesRegex(
            ValueError, "pass either maxsize= or policy_set.backpressure, not both"
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                maxsize=1,
                policy_set=QueuePolicySet(backpressure=BoundedBackpressure(1)),
            )

        with self.assertRaisesRegex(TypeError, "retry_defaults must be a mapping"):
            _ = PersistentQueue("test", store=store, retry_defaults=cast("Any", []))

        with self.assertRaisesRegex(
            ValueError, "retry_defaults cannot set both max_tries and stop"
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                retry_defaults={"max_tries": 1, "stop": object()},
            )

        with self.assertRaisesRegex(
            ValueError, "retry_defaults max_tries must be a positive integer"
        ):
            _ = PersistentQueue(
                "test",
                store=store,
                retry_defaults={"max_tries": 0},
            )

    def test_constructor_copies_retry_defaults(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            retry_defaults={"max_tries": 3},
        )

        self.assertEqual(queue.retry_defaults, {"max_tries": 3})

        queue.retry_defaults["max_tries"] = 7

        self.assertEqual(queue.retry_defaults, {"max_tries": 7})

    def test_constructor_uses_default_maxsize_and_store_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            queue = PersistentQueue("test", store_path=f"{tmpdir}/queue.sqlite3")

            self.assertEqual(queue.lease_timeout, 30.0)
            self.assertEqual(queue.lease_policy, FIXED_LEASE_TIMEOUT)
            self.assertEqual(queue.acknowledgement_policy, EXPLICIT_ACKNOWLEDGEMENT)
            self.assertEqual(queue.dead_letter_policy, DEAD_LETTER_QUEUE)
            self.assertEqual(queue.deduplication_policy, DEDUPE_KEY_SUPPORT)
            self.assertEqual(
                queue.lease_policy.as_dict(),
                {
                    "type": "fixed-timeout",
                    "timeout": 30.0,
                    "uses_leases": True,
                    "expires_inflight": True,
                },
            )
            self.assertEqual(
                queue.acknowledgement_policy.as_dict(),
                {
                    "acknowledgements": True,
                    "explicit_ack": True,
                    "removes_on_ack": True,
                },
            )
            self.assertEqual(
                queue.dead_letter_policy.as_dict(),
                {
                    "dead_letters": True,
                    "stores_failures": True,
                    "supports_requeue": True,
                },
            )
            self.assertEqual(
                queue.deduplication_policy.as_dict(),
                {
                    "deduplication": True,
                    "accepts_dedupe_key": True,
                },
            )
            self.assertEqual(queue.maxsize, 0)
            self.assertEqual(queue.backpressure, BoundedBackpressure())
            self.assertEqual(queue.semantics, LOCAL_AT_LEAST_ONCE)
            self.assertEqual(queue.locality_policy, LOCAL_QUEUE_PLACEMENT)
            self.assertEqual(
                queue.locality_policy.as_dict(),
                {
                    "locality": "local",
                    "co_located_state": True,
                    "crosses_network_boundary": False,
                },
            )
            self.assertEqual(queue.consumption_policy, PULL_CONSUMPTION)
            self.assertEqual(queue.delivery_policy, AT_LEAST_ONCE_DELIVERY)
            self.assertEqual(queue.dispatch_policy, NO_DISPATCHER)
            queue.dispatch_policy.dispatch(
                QueueMessage(id="message-1", queue="test", value="item")
            )
            self.assertEqual(
                queue.dispatch_policy.as_dict(),
                {
                    "dispatches": False,
                    "dispatches_on_put": False,
                    "handler_count": 0,
                },
            )
            self.assertEqual(queue.notification_policy, NO_NOTIFICATION)
            queue.notification_policy.notify(
                QueueMessage(id="message-2", queue="test", value="item")
            )
            self.assertEqual(
                queue.notification_policy.as_dict(),
                {
                    "notifies": False,
                    "notifies_on_put": False,
                    "listener_count": 0,
                },
            )
            self.assertEqual(queue.ordering_policy, FIFO_READY_ORDERING)
            self.assertEqual(queue.routing_policy, POINT_TO_POINT_ROUTING)
            self.assertEqual(queue.subscription_policy, NO_SUBSCRIPTIONS)
            self.assertEqual(
                queue.subscription_policy.as_dict(),
                {
                    "subscriptions": False,
                    "fanout": False,
                    "subscriber_count": 0,
                },
            )
            self.assertIsNone(queue._store)
            self.assertEqual(queue._store_path, Path(tmpdir) / "queue.sqlite3")

    def test_constructor_accepts_lease_policy(self) -> None:
        lease_policy = FixedLeaseTimeout(12.5)
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            lease_policy=lease_policy,
        )

        self.assertIs(queue.lease_policy, lease_policy)
        self.assertEqual(queue.lease_timeout, 12.5)
        self.assertTrue(queue.semantics.leases)

    def test_constructor_accepts_deduplication_policy(self) -> None:
        deduplication_policy = NoDeduplication()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            deduplication_policy=deduplication_policy,
        )

        self.assertIs(queue.deduplication_policy, deduplication_policy)
        self.assertFalse(queue.semantics.deduplication)
        self.assertEqual(
            queue.deduplication_policy.as_dict(),
            {
                "deduplication": False,
                "accepts_dedupe_key": False,
            },
        )

    def test_constructor_accepts_remote_locality_policy(self) -> None:
        locality_policy = RemoteQueuePlacement()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            locality_policy=locality_policy,
        )

        self.assertIs(queue.locality_policy, locality_policy)
        self.assertEqual(queue.semantics.locality, "remote")
        self.assertEqual(
            queue.locality_policy.as_dict(),
            {
                "locality": "remote",
                "co_located_state": False,
                "crosses_network_boundary": True,
            },
        )

    def test_constructor_accepts_explicit_queue_semantics(self) -> None:
        semantics = QueueSemantics(
            locality="local",
            delivery="at-least-once",
        )
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            semantics=semantics,
        )

        self.assertIs(queue.semantics, semantics)
        self.assertEqual(
            queue.semantics.as_dict(),
            {
                "locality": "local",
                "delivery": "at-least-once",
                "routing": "point-to-point",
                "consumption": "pull",
                "ordering": "fifo-ready",
                "leases": True,
                "acknowledgements": True,
                "dead_letters": True,
                "deduplication": True,
                "subscriptions": False,
                "notifications": False,
            },
        )

    def test_constructor_accepts_explicit_routing_policy(self) -> None:
        routing_policy = PointToPointRouting()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            routing_policy=routing_policy,
        )

        self.assertIs(queue.routing_policy, routing_policy)
        self.assertEqual(
            queue.routing_policy.as_dict(),
            {
                "pattern": "point-to-point",
                "single_consumer_per_message": True,
                "fanout": False,
            },
        )

    def test_constructor_accepts_publish_subscribe_routing_policy(self) -> None:
        routing_policy = PublishSubscribeRouting()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            routing_policy=routing_policy,
        )

        self.assertIs(queue.routing_policy, routing_policy)
        self.assertEqual(queue.semantics.routing, "publish-subscribe")
        self.assertEqual(
            queue.routing_policy.as_dict(),
            {
                "pattern": "publish-subscribe",
                "single_consumer_per_message": False,
                "fanout": True,
            },
        )

    def test_constructor_accepts_static_fanout_subscriptions(self) -> None:
        subscription_policy = StaticFanoutSubscriptions(("billing", "audit"))
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            subscription_policy=subscription_policy,
        )

        self.assertIs(queue.subscription_policy, subscription_policy)
        self.assertTrue(queue.semantics.subscriptions)
        self.assertEqual(
            queue.subscription_policy.as_dict(),
            {
                "subscriptions": True,
                "fanout": True,
                "subscriber_count": 2,
                "subscribers": ["billing", "audit"],
            },
        )

    def test_constructor_accepts_explicit_consumption_policy(self) -> None:
        consumption_policy = PullConsumption()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            consumption_policy=consumption_policy,
        )

        self.assertIs(queue.consumption_policy, consumption_policy)
        self.assertEqual(
            queue.consumption_policy.as_dict(),
            {
                "pattern": "pull",
                "consumer_requests_messages": True,
                "producer_invokes_handler": False,
            },
        )

    def test_constructor_accepts_callback_dispatch_policy(self) -> None:
        dispatched: list[QueueMessage] = []
        dispatch_policy = CallbackDispatcher((dispatched.append,))
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            dispatch_policy=dispatch_policy,
        )

        message = queue.put("item")

        self.assertIs(queue.dispatch_policy, dispatch_policy)
        self.assertEqual(dispatched, [message])
        self.assertEqual(
            queue.dispatch_policy.as_dict(),
            {
                "type": "callback",
                "dispatches": True,
                "dispatches_on_put": True,
                "handler_count": 1,
            },
        )

    def test_constructor_accepts_callback_notification_policy(self) -> None:
        notified: list[QueueMessage] = []
        notification_policy = CallbackNotification(notified.append)
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            notification_policy=notification_policy,
        )

        message = queue.put("item")

        self.assertIs(queue.notification_policy, notification_policy)
        self.assertEqual(notified, [message])
        self.assertEqual(
            queue.notification_policy.as_dict(),
            {
                "type": "callback",
                "scope": "in-process",
                "notifies": True,
                "notifies_on_put": True,
                "listener_count": 1,
            },
        )

    def test_constructor_accepts_in_process_notification_policy(self) -> None:
        notification_policy = InProcessNotification()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            notification_policy=notification_policy,
        )

        self.assertFalse(notification_policy.wait(timeout=0))
        message = queue.put("item")

        self.assertIs(queue.notification_policy, notification_policy)
        self.assertTrue(notification_policy.wait(timeout=0))
        self.assertEqual(queue.inspect(message.id), message)
        self.assertEqual(
            queue.notification_policy.as_dict(),
            {
                "type": "thread-event",
                "scope": "in-process",
                "notifies": True,
                "notifies_on_put": True,
                "listener_count": 1,
            },
        )

        notification_policy.clear()
        self.assertFalse(notification_policy.wait(timeout=0))

    def test_constructor_accepts_asyncio_notification_policy(self) -> None:
        async def scenario() -> None:
            notification_policy = AsyncioNotification()
            queue = PersistentQueue(
                "test",
                store=MemoryQueueStore(),
                notification_policy=notification_policy,
            )

            waiter = asyncio.create_task(notification_policy.wait_async(timeout=0.1))
            await asyncio.sleep(0)
            message = queue.put("item")

            self.assertIs(queue.notification_policy, notification_policy)
            self.assertTrue(await waiter)
            self.assertEqual(queue.inspect(message.id), message)
            self.assertEqual(
                queue.notification_policy.as_dict(),
                {
                    "type": "asyncio-event",
                    "scope": "in-process",
                    "notifies": True,
                    "notifies_on_put": True,
                    "listener_count": 1,
                },
            )

            notification_policy.clear()
            self.assertFalse(await notification_policy.wait_async(timeout=0.01))

        asyncio.run(scenario())

    def test_constructor_accepts_websocket_notification_policy(self) -> None:
        class _FakeWebSocket:
            def __init__(self) -> None:
                self.sent_json: list[object] = []

            async def send_json(self, data: object) -> None:
                self.sent_json.append(data)

        async def scenario() -> None:
            websocket = _FakeWebSocket()
            notification_policy = WebSocketNotification(websocket)
            queue = PersistentQueue(
                "test",
                store=MemoryQueueStore(),
                notification_policy=notification_policy,
            )

            message = queue.put({"kind": "signup"})
            await asyncio.sleep(0)

            self.assertIs(queue.notification_policy, notification_policy)
            self.assertEqual(
                websocket.sent_json,
                [
                    {
                        "id": message.id,
                        "queue": "test",
                        "value": {"kind": "signup"},
                        "state": "ready",
                        "attempts": 0,
                        "created_at": message.created_at,
                        "available_at": message.available_at,
                        "leased_until": None,
                        "leased_by": None,
                        "last_error": None,
                        "failed_at": None,
                        "attempt_history": [],
                        "dedupe_key": None,
                        "priority": 0,
                    }
                ],
            )
            self.assertEqual(
                queue.notification_policy.as_dict(),
                {
                    "type": "websocket",
                    "scope": "network",
                    "notifies": True,
                    "notifies_on_put": True,
                    "listener_count": 1,
                },
            )

        asyncio.run(scenario())

    def test_asyncio_notification_without_running_loop_uses_local_event(self) -> None:
        notification_policy = AsyncioNotification()

        self.assertIsNone(notification_policy.loop)
        self.assertFalse(notification_policy.event.is_set())

        notification_policy.notify(
            QueueMessage(id="message-1", queue="test", value="item")
        )

        self.assertTrue(notification_policy.event.is_set())

        async def scenario() -> None:
            notification_policy.clear()
            waiter = asyncio.create_task(notification_policy.wait_async())
            await asyncio.sleep(0)
            notification_policy.notify(
                QueueMessage(id="message-2", queue="test", value="item")
            )
            self.assertTrue(await waiter)

        asyncio.run(scenario())

    def test_websocket_notification_validates_and_uses_runtime_branches(self) -> None:
        with self.assertRaisesRegex(ValueError, "connections cannot be empty"):
            _ = WebSocketNotification(())

        notification_policy = WebSocketNotification((cast("Any", object()),))
        self.assertIsNone(notification_policy.loop)

        with self.assertRaisesRegex(
            RuntimeError, "WebSocketNotification requires an active event loop or loop="
        ):
            notification_policy.notify(
                QueueMessage(id="message-1", queue="test", value="item")
            )

    def test_websocket_notification_supports_send_text_send_and_threadsafe_loop(
        self,
    ) -> None:
        class _TextWebSocket:
            def __init__(self) -> None:
                self.sent_text: list[str] = []

            async def send_text(self, data: str) -> None:
                self.sent_text.append(data)

        class _RawWebSocket:
            def __init__(self) -> None:
                self.sent: list[object] = []

            async def send(self, data: object) -> None:
                self.sent.append(data)

        explicit_loop = asyncio.new_event_loop()
        text_socket = _TextWebSocket()
        raw_socket = _RawWebSocket()
        recorded_loops: list[asyncio.AbstractEventLoop] = []

        def run_threadsafe(
            coroutine: Any, loop: asyncio.AbstractEventLoop
        ) -> concurrent.futures.Future[object]:
            recorded_loops.append(loop)
            _ = asyncio.run(coroutine)
            future: concurrent.futures.Future[object] = concurrent.futures.Future()
            future.set_result(None)
            return future

        with mock.patch(
            "localqueue.adapters.notification.asyncio.run_coroutine_threadsafe",
            side_effect=run_threadsafe,
        ):
            WebSocketNotification(
                (text_socket,),
                serializer=lambda message: f"text:{message.id}",
                loop=explicit_loop,
            ).notify(QueueMessage(id="message-text", queue="test", value="item"))
            WebSocketNotification(
                (raw_socket,),
                serializer=lambda message: {"message_id": message.id},
                loop=explicit_loop,
            ).notify(QueueMessage(id="message-raw", queue="test", value="item"))

        self.assertEqual(recorded_loops, [explicit_loop, explicit_loop])
        self.assertEqual(text_socket.sent_text, ["text:message-text"])
        self.assertEqual(raw_socket.sent, [{"message_id": "message-raw"}])
        explicit_loop.close()

    def test_websocket_notification_uses_running_loop_without_bound_loop(self) -> None:
        class _TextWebSocket:
            def __init__(self) -> None:
                self.sent_text: list[str] = []

            async def send_text(self, data: str) -> None:
                self.sent_text.append(data)

        websocket = _TextWebSocket()
        notification_policy = WebSocketNotification(
            (websocket,),
            serializer=lambda message: f"text:{message.id}",
        )

        async def scenario() -> None:
            notification_policy.notify(
                QueueMessage(id="message-1", queue="test", value="item")
            )
            await asyncio.sleep(0)

            self.assertEqual(websocket.sent_text, ["text:message-1"])

        asyncio.run(scenario())

    def test_constructor_accepts_push_consumption_policy(self) -> None:
        consumption_policy = PushConsumption()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            consumption_policy=consumption_policy,
        )

        self.assertIs(queue.consumption_policy, consumption_policy)
        self.assertEqual(queue.semantics.consumption, "push")
        self.assertEqual(
            queue.consumption_policy.as_dict(),
            {
                "pattern": "push",
                "consumer_requests_messages": False,
                "producer_invokes_handler": True,
            },
        )

    def test_constructor_accepts_explicit_ordering_policy(self) -> None:
        ordering_policy = FifoReadyOrdering()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            ordering_policy=ordering_policy,
        )

        self.assertIs(queue.ordering_policy, ordering_policy)
        self.assertEqual(
            queue.ordering_policy.as_dict(),
            {
                "guarantee": "fifo-ready",
                "ready_before_delayed": True,
                "stable_for_same_timestamp": True,
                "priority_before_sequence": False,
            },
        )

    def test_constructor_accepts_priority_ordering_policy(self) -> None:
        ordering_policy = PriorityOrdering()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            ordering_policy=ordering_policy,
        )

        self.assertEqual(queue.semantics.ordering, "priority")
        self.assertIs(queue.ordering_policy, ordering_policy)
        self.assertEqual(
            queue.ordering_policy.as_dict(),
            {
                "guarantee": "priority",
                "ready_before_delayed": True,
                "stable_for_same_timestamp": True,
                "priority_before_sequence": True,
            },
        )

    def test_constructor_accepts_best_effort_ordering_policy(self) -> None:
        ordering_policy = BestEffortOrdering()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            ordering_policy=ordering_policy,
        )

        self.assertEqual(queue.semantics.ordering, "best-effort")
        self.assertIs(queue.ordering_policy, ordering_policy)
        self.assertEqual(
            queue.ordering_policy.as_dict(),
            {
                "guarantee": "best-effort",
                "ready_before_delayed": False,
                "stable_for_same_timestamp": False,
                "priority_before_sequence": False,
            },
        )

    def test_constructor_accepts_explicit_delivery_policy(self) -> None:
        delivery_policy = AtLeastOnceDelivery()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=delivery_policy,
        )

        self.assertIs(queue.delivery_policy, delivery_policy)
        self.assertEqual(
            queue.delivery_policy.as_dict(),
            {
                "guarantee": "at-least-once",
                "ack_timing": "after-success",
                "uses_leases": True,
                "redelivers_expired_leases": True,
            },
        )

    def test_constructor_accepts_at_most_once_delivery_policy(self) -> None:
        delivery_policy = AtMostOnceDelivery()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=delivery_policy,
        )

        self.assertEqual(queue.semantics.delivery, "at-most-once")
        self.assertIs(queue.delivery_policy, delivery_policy)
        self.assertEqual(
            queue.delivery_policy.as_dict(),
            {
                "guarantee": "at-most-once",
                "ack_timing": "before-delivery",
                "uses_leases": False,
                "redelivers_expired_leases": False,
            },
        )

    def test_constructor_accepts_effectively_once_delivery_policy(self) -> None:
        delivery_policy = EffectivelyOnceDelivery()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=delivery_policy,
        )

        self.assertEqual(queue.semantics.delivery, "effectively-once")
        self.assertIs(queue.delivery_policy, delivery_policy)
        self.assertEqual(
            queue.delivery_policy.as_dict(),
            {
                "guarantee": "effectively-once",
                "ack_timing": "after-success",
                "uses_leases": True,
                "redelivers_expired_leases": True,
                "requires_dedupe_key": True,
                "idempotency_store": None,
                "result_policy": NO_RESULT_POLICY.as_dict(),
                "commit_policy": LOCAL_ATOMIC_COMMIT.as_dict(),
            },
        )

    def test_constructor_accepts_effectively_once_delivery_policy_with_store(
        self,
    ) -> None:
        delivery_policy = EffectivelyOnceDelivery(
            idempotency_store=MemoryIdempotencyStore()
        )
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=delivery_policy,
        )

        self.assertIs(queue.delivery_policy, delivery_policy)
        self.assertEqual(
            queue.delivery_policy.as_dict(),
            {
                "guarantee": "effectively-once",
                "ack_timing": "after-success",
                "uses_leases": True,
                "redelivers_expired_leases": True,
                "requires_dedupe_key": True,
                "idempotency_store": "MemoryIdempotencyStore",
                "result_policy": NO_RESULT_POLICY.as_dict(),
                "commit_policy": LOCAL_ATOMIC_COMMIT.as_dict(),
            },
        )

    def test_constructor_accepts_policy_set(self) -> None:
        delivery_policy = EffectivelyOnceDelivery(
            idempotency_store=MemoryIdempotencyStore(),
            result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
            commit_policy=SagaCommit(saga_store=MemoryResultStore()),
        )
        ordering_policy = BestEffortOrdering()
        locality_policy = RemoteQueuePlacement()
        lease_policy = FixedLeaseTimeout(15)
        acknowledgement_policy = ExplicitAcknowledgement()
        dead_letter_policy = DeadLetterQueue()
        deduplication_policy = NoDeduplication()
        dispatch_policy = NoDispatcher()
        notification_policy = NoNotification()
        subscription_policy = StaticFanoutSubscriptions(("billing", "audit"))
        backpressure = BoundedBackpressure(5)
        policy_set = QueuePolicySet(
            locality_policy=locality_policy,
            lease_policy=lease_policy,
            acknowledgement_policy=acknowledgement_policy,
            dead_letter_policy=dead_letter_policy,
            deduplication_policy=deduplication_policy,
            delivery_policy=delivery_policy,
            dispatch_policy=dispatch_policy,
            notification_policy=notification_policy,
            ordering_policy=ordering_policy,
            subscription_policy=subscription_policy,
            backpressure=backpressure,
        )

        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            policy_set=policy_set,
        )

        self.assertIs(queue.locality_policy, locality_policy)
        self.assertIs(queue.lease_policy, lease_policy)
        self.assertIs(queue.acknowledgement_policy, acknowledgement_policy)
        self.assertIs(queue.dead_letter_policy, dead_letter_policy)
        self.assertIs(queue.deduplication_policy, deduplication_policy)
        self.assertIs(queue.delivery_policy, delivery_policy)
        self.assertIs(queue.dispatch_policy, dispatch_policy)
        self.assertIs(queue.notification_policy, notification_policy)
        self.assertIs(queue.ordering_policy, ordering_policy)
        self.assertIs(queue.subscription_policy, subscription_policy)
        self.assertIs(queue.backpressure, backpressure)
        self.assertEqual(queue.maxsize, 5)
        self.assertEqual(queue.lease_timeout, 15)
        self.assertEqual(queue.semantics.locality, "remote")
        self.assertFalse(queue.semantics.deduplication)
        self.assertTrue(queue.semantics.subscriptions)
        self.assertFalse(queue.semantics.notifications)
        self.assertEqual(queue.semantics.delivery, "effectively-once")
        self.assertEqual(queue.semantics.ordering, "best-effort")

    def test_effectively_once_policy_set_factory(self) -> None:
        idempotency_store = MemoryIdempotencyStore()
        result_policy = ReturnStoredResult(result_store=MemoryResultStore())
        commit_policy = SagaCommit(saga_store=MemoryResultStore())
        locality_policy = RemoteQueuePlacement()
        lease_policy = FixedLeaseTimeout(45)
        acknowledgement_policy = ExplicitAcknowledgement()
        dead_letter_policy = DeadLetterQueue()
        deduplication_policy = DedupeKeySupport()
        consumption_policy = PushConsumption()
        dispatch_policy = NoDispatcher()
        notification_policy = NoNotification()
        ordering_policy = BestEffortOrdering()
        routing_policy = PointToPointRouting()
        subscription_policy = StaticFanoutSubscriptions(("billing", "audit"))
        backpressure = BoundedBackpressure(5)

        policy_set = QueuePolicySet.effectively_once(
            idempotency_store=idempotency_store,
            result_policy=result_policy,
            commit_policy=commit_policy,
            locality_policy=locality_policy,
            lease_policy=lease_policy,
            acknowledgement_policy=acknowledgement_policy,
            dead_letter_policy=dead_letter_policy,
            deduplication_policy=deduplication_policy,
            consumption_policy=consumption_policy,
            dispatch_policy=dispatch_policy,
            notification_policy=notification_policy,
            ordering_policy=ordering_policy,
            routing_policy=routing_policy,
            subscription_policy=subscription_policy,
            backpressure=backpressure,
        )

        self.assertIs(policy_set.locality_policy, locality_policy)
        self.assertIs(policy_set.lease_policy, lease_policy)
        self.assertIs(policy_set.acknowledgement_policy, acknowledgement_policy)
        self.assertIs(policy_set.dead_letter_policy, dead_letter_policy)
        self.assertIs(policy_set.deduplication_policy, deduplication_policy)
        self.assertIs(policy_set.consumption_policy, consumption_policy)
        self.assertIs(policy_set.dispatch_policy, dispatch_policy)
        self.assertIs(policy_set.notification_policy, notification_policy)
        self.assertIs(policy_set.ordering_policy, ordering_policy)
        self.assertIs(policy_set.routing_policy, routing_policy)
        self.assertIs(policy_set.subscription_policy, subscription_policy)
        self.assertIs(policy_set.backpressure, backpressure)
        self.assertEqual(
            policy_set.as_dict(),
            {
                "semantics": None,
                "locality_policy": {
                    "locality": "remote",
                    "co_located_state": False,
                    "crosses_network_boundary": True,
                },
                "lease_policy": {
                    "type": "fixed-timeout",
                    "timeout": 45,
                    "uses_leases": True,
                    "expires_inflight": True,
                },
                "acknowledgement_policy": {
                    "acknowledgements": True,
                    "explicit_ack": True,
                    "removes_on_ack": True,
                },
                "dead_letter_policy": {
                    "dead_letters": True,
                    "stores_failures": True,
                    "supports_requeue": True,
                },
                "deduplication_policy": {
                    "deduplication": True,
                    "accepts_dedupe_key": True,
                },
                "delivery_policy": {
                    "guarantee": "effectively-once",
                    "ack_timing": "after-success",
                    "uses_leases": True,
                    "redelivers_expired_leases": True,
                    "requires_dedupe_key": True,
                    "idempotency_store": "MemoryIdempotencyStore",
                    "result_policy": {
                        "type": "return-stored",
                        "stores_result": True,
                        "returns_cached_result": True,
                        "result_store": "MemoryResultStore",
                    },
                    "commit_policy": {
                        "mode": "saga",
                        "local_commit": False,
                        "coordinates_effects": True,
                        "saga_store": "MemoryResultStore",
                    },
                },
                "consumption_policy": {
                    "pattern": "push",
                    "consumer_requests_messages": False,
                    "producer_invokes_handler": True,
                },
                "dispatch_policy": {
                    "dispatches": False,
                    "dispatches_on_put": False,
                    "handler_count": 0,
                },
                "notification_policy": {
                    "notifies": False,
                    "notifies_on_put": False,
                    "listener_count": 0,
                },
                "ordering_policy": {
                    "guarantee": "best-effort",
                    "ready_before_delayed": False,
                    "stable_for_same_timestamp": False,
                    "priority_before_sequence": False,
                },
                "routing_policy": {
                    "pattern": "point-to-point",
                    "single_consumer_per_message": True,
                    "fanout": False,
                },
                "subscription_policy": {
                    "subscriptions": True,
                    "fanout": True,
                    "subscriber_count": 2,
                    "subscribers": ["billing", "audit"],
                },
                "backpressure": {
                    "type": "bounded",
                    "maxsize": 5,
                    "overflow": "block",
                },
            },
        )

    def test_policy_set_delivery_factories(self) -> None:
        at_least_once = QueuePolicySet.at_least_once(
            ordering_policy=PriorityOrdering(),
            notification_policy=NO_NOTIFICATION,
            backpressure=BoundedBackpressure(3),
        )
        at_most_once = QueuePolicySet.at_most_once(
            routing_policy=PublishSubscribeRouting(),
            notification_policy=NO_NOTIFICATION,
            backpressure=BoundedBackpressure(1),
        )

        at_least_once_queue = PersistentQueue(
            "at-least-once",
            store=MemoryQueueStore(),
            policy_set=at_least_once,
        )
        at_most_once_queue = PersistentQueue(
            "at-most-once",
            store=MemoryQueueStore(),
            policy_set=at_most_once,
        )

        self.assertEqual(at_least_once_queue.semantics.delivery, "at-least-once")
        self.assertEqual(at_least_once_queue.semantics.ordering, "priority")
        self.assertIs(at_least_once_queue.delivery_policy, AT_LEAST_ONCE_DELIVERY)
        self.assertIs(at_least_once_queue.notification_policy, NO_NOTIFICATION)
        self.assertEqual(at_least_once_queue.maxsize, 3)
        self.assertEqual(at_most_once_queue.semantics.delivery, "at-most-once")
        self.assertEqual(at_most_once_queue.semantics.routing, "publish-subscribe")
        self.assertIs(at_most_once_queue.notification_policy, NO_NOTIFICATION)
        self.assertEqual(at_most_once_queue.maxsize, 1)

    def test_constructor_accepts_effectively_once_delivery_policy_with_result_policy(
        self,
    ) -> None:
        delivery_policy = EffectivelyOnceDelivery(
            idempotency_store=MemoryIdempotencyStore(),
            result_policy=ReturnStoredResult(),
        )
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=delivery_policy,
        )

        self.assertIs(queue.delivery_policy, delivery_policy)
        self.assertEqual(
            queue.delivery_policy.as_dict(),
            {
                "guarantee": "effectively-once",
                "ack_timing": "after-success",
                "uses_leases": True,
                "redelivers_expired_leases": True,
                "requires_dedupe_key": True,
                "idempotency_store": "MemoryIdempotencyStore",
                "result_policy": RETURN_STORED_RESULT.as_dict(),
                "commit_policy": LOCAL_ATOMIC_COMMIT.as_dict(),
            },
        )

    def test_constructor_accepts_effectively_once_delivery_policy_with_result_store(
        self,
    ) -> None:
        delivery_policy = EffectivelyOnceDelivery(
            idempotency_store=MemoryIdempotencyStore(),
            result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
        )
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=delivery_policy,
        )

        self.assertIs(queue.delivery_policy, delivery_policy)
        self.assertEqual(
            queue.delivery_policy.as_dict(),
            {
                "guarantee": "effectively-once",
                "ack_timing": "after-success",
                "uses_leases": True,
                "redelivers_expired_leases": True,
                "requires_dedupe_key": True,
                "idempotency_store": "MemoryIdempotencyStore",
                "result_policy": {
                    "type": "return-stored",
                    "stores_result": True,
                    "returns_cached_result": True,
                    "result_store": "MemoryResultStore",
                },
                "commit_policy": LOCAL_ATOMIC_COMMIT.as_dict(),
            },
        )

    def test_constructor_accepts_explicit_commit_policy(self) -> None:
        commit_policy = TransactionalOutboxCommit()
        delivery_policy = EffectivelyOnceDelivery(
            idempotency_store=MemoryIdempotencyStore(),
            commit_policy=commit_policy,
        )
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=delivery_policy,
        )

        self.assertIs(queue.delivery_policy, delivery_policy)
        self.assertEqual(
            queue.delivery_policy.as_dict(),
            {
                "guarantee": "effectively-once",
                "ack_timing": "after-success",
                "uses_leases": True,
                "redelivers_expired_leases": True,
                "requires_dedupe_key": True,
                "idempotency_store": "MemoryIdempotencyStore",
                "result_policy": NO_RESULT_POLICY.as_dict(),
                "commit_policy": commit_policy.as_dict(),
            },
        )

    def test_commit_policy_variants_have_expected_shape(self) -> None:
        self.assertEqual(
            LocalAtomicCommit().as_dict(),
            {
                "mode": "local-atomic",
                "local_commit": True,
                "coordinates_effects": False,
            },
        )
        self.assertEqual(
            TransactionalOutboxCommit().as_dict(),
            {
                "mode": "transactional-outbox",
                "local_commit": True,
                "coordinates_effects": True,
                "outbox_store": None,
            },
        )
        self.assertEqual(
            TWO_PHASE_COMMIT.as_dict(),
            {
                "mode": "two-phase",
                "local_commit": False,
                "coordinates_effects": True,
                "prepare_store": None,
                "commit_store": None,
            },
        )
        self.assertEqual(
            SagaCommit().as_dict(),
            {
                "mode": "saga",
                "local_commit": False,
                "coordinates_effects": True,
                "saga_store": None,
            },
        )

    def test_two_phase_commit_uses_prepare_and_commit_stores(self) -> None:
        store = MemoryIdempotencyStore()
        prepare_store = MemoryResultStore()
        commit_store = MemoryResultStore()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=store,
                result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
                commit_policy=TwoPhaseCommit(
                    prepare_store=prepare_store,
                    commit_store=commit_store,
                ),
            ),
        )
        message = queue.put("item", dedupe_key="job-1")

        @persistent_worker(queue)
        def handle(value: str) -> dict[str, str]:
            return {"status": value.upper()}

        self.assertEqual(cast("Any", handle)(), {"status": "ITEM"})
        record = store.load("job-1")
        assert record is not None
        self.assertEqual(record.result_key, "dedupe:test:job-1")
        self.assertEqual(
            prepare_store.load("prepare:dedupe:test:job-1"),
            {
                "queue": "test",
                "message_id": message.id,
                "dedupe_key": "job-1",
                "result_key": "dedupe:test:job-1",
                "result": {"status": "ITEM"},
                "phase": "prepare",
            },
        )
        self.assertEqual(
            commit_store.load("commit:dedupe:test:job-1"),
            {
                "queue": "test",
                "message_id": message.id,
                "dedupe_key": "job-1",
                "result_key": "dedupe:test:job-1",
                "result": {"status": "ITEM"},
                "phase": "commit",
            },
        )

    def test_two_phase_commit_failure_prevents_ack(self) -> None:
        store = MemoryIdempotencyStore()

        class FailingCommitStore:
            def load(self, key: str) -> Any | None:
                return None

            def save(self, key: str, value: Any) -> None:
                raise RuntimeError("commit phase failed")

            def delete(self, key: str) -> None:
                return None

        prepare_store = MemoryResultStore()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=store,
                result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
                commit_policy=TwoPhaseCommit(
                    prepare_store=prepare_store,
                    commit_store=FailingCommitStore(),
                ),
            ),
        )
        message = queue.put("item", dedupe_key="job-1")

        @persistent_worker(queue)
        def handle(value: str) -> dict[str, str]:
            return {"status": value.upper()}

        with self.assertRaisesRegex(RuntimeError, "commit phase failed"):
            cast("Any", handle)()

        record = store.load("job-1")
        assert record is not None
        self.assertEqual(record.status, "succeeded")
        self.assertEqual(
            prepare_store.load("prepare:dedupe:test:job-1"),
            {
                "queue": "test",
                "message_id": message.id,
                "dedupe_key": "job-1",
                "result_key": "dedupe:test:job-1",
                "result": {"status": "ITEM"},
                "phase": "prepare",
            },
        )
        self.assertIsNotNone(queue.inspect(message.id))

    def test_saga_commit_uses_saga_store(self) -> None:
        store = MemoryIdempotencyStore()
        saga_store = MemoryResultStore()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=store,
                result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
                commit_policy=SagaCommit(saga_store=saga_store),
            ),
        )
        message = queue.put("item", dedupe_key="job-1")

        @persistent_worker(queue)
        def handle(value: str) -> dict[str, str]:
            return {"status": value.upper()}

        self.assertEqual(cast("Any", handle)(), {"status": "ITEM"})
        self.assertEqual(
            saga_store.load("saga:forward:dedupe:test:job-1"),
            {
                "queue": "test",
                "message_id": message.id,
                "dedupe_key": "job-1",
                "phase": "forward",
                "result": {"status": "ITEM"},
            },
        )

    def test_saga_commit_failure_records_compensation(self) -> None:
        store = MemoryIdempotencyStore()
        saga_store = MemoryResultStore()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=store,
                result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
                commit_policy=SagaCommit(saga_store=saga_store),
            ),
        )
        message = queue.put("item", dedupe_key="job-1")

        @persistent_worker(
            queue,
            config=PersistentWorkerConfig(dead_letter_on_failure=False, max_tries=1),
        )
        def handle(value: str) -> dict[str, str]:
            raise RuntimeError(f"boom: {value}")

        with self.assertRaisesRegex(RuntimeError, "boom: item"):
            cast("Any", handle)()

        self.assertEqual(
            saga_store.load("saga:compensate:dedupe:test:job-1"),
            {
                "queue": "test",
                "message_id": message.id,
                "dedupe_key": "job-1",
                "phase": "compensate",
                "error": {
                    "type": "RuntimeError",
                    "module": "builtins",
                    "message": "boom: item",
                },
            },
        )
        self.assertIsNotNone(queue.inspect(message.id))

    def test_commit_saga_without_store_is_noop(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=MemoryIdempotencyStore(),
                result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
                commit_policy=SagaCommit(),
            ),
        )
        message = QueueMessage(
            id="message-1",
            queue="test",
            value="item",
            dedupe_key="job-1",
        )

        _commit_saga(queue, message, phase="forward", result={"status": "ITEM"})

    def test_commit_saga_without_dedupe_key_is_noop(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=MemoryIdempotencyStore(),
                result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
                commit_policy=SagaCommit(saga_store=MemoryResultStore()),
            ),
        )
        message = QueueMessage(id="message-1", queue="test", value="item")

        _commit_saga(queue, message, phase="forward", result={"status": "ITEM"})

    def test_commit_two_phase_without_stores_is_noop(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=MemoryIdempotencyStore(),
                result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
                commit_policy=TwoPhaseCommit(),
            ),
        )
        message = QueueMessage(
            id="message-1",
            queue="test",
            value="item",
            dedupe_key="job-1",
        )

        _commit_two_phase(queue, message, result={"status": "ITEM"}, result_key=None)

    def test_commit_two_phase_without_dedupe_key_is_noop(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=MemoryIdempotencyStore(),
                result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
                commit_policy=TwoPhaseCommit(
                    prepare_store=MemoryResultStore(),
                    commit_store=MemoryResultStore(),
                ),
            ),
        )
        message = QueueMessage(id="message-1", queue="test", value="item")

        _commit_two_phase(queue, message, result={"status": "ITEM"}, result_key=None)

    def test_commit_two_phase_without_commit_store_is_prepare_only(self) -> None:
        prepare_store = MemoryResultStore()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=MemoryIdempotencyStore(),
                result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
                commit_policy=TwoPhaseCommit(prepare_store=prepare_store),
            ),
        )
        message = QueueMessage(
            id="message-1",
            queue="test",
            value="item",
            dedupe_key="job-1",
        )

        _commit_two_phase(queue, message, result={"status": "ITEM"}, result_key="rk")

        self.assertEqual(
            prepare_store.load("prepare:dedupe:test:job-1"),
            {
                "queue": "test",
                "message_id": "message-1",
                "dedupe_key": "job-1",
                "result_key": "rk",
                "result": {"status": "ITEM"},
                "phase": "prepare",
            },
        )

    def test_transactional_outbox_commit_uses_outbox_store(self) -> None:
        store = MemoryIdempotencyStore()
        outbox_store = MemoryResultStore()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=store,
                result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
                commit_policy=TransactionalOutboxCommit(outbox_store=outbox_store),
            ),
        )
        message = queue.put("item", dedupe_key="job-1")

        @persistent_worker(queue)
        def handle(value: str) -> dict[str, str]:
            return {"status": value.upper()}

        self.assertEqual(cast("Any", handle)(), {"status": "ITEM"})
        record = store.load("job-1")
        assert record is not None
        self.assertEqual(record.metadata, {"queue": "test", "message_id": message.id})
        self.assertEqual(record.result_key, "dedupe:test:job-1")
        self.assertEqual(
            outbox_store.load("outbox:dedupe:test:job-1"),
            {
                "queue": "test",
                "message_id": message.id,
                "dedupe_key": "job-1",
                "result_key": "dedupe:test:job-1",
                "result": {"status": "ITEM"},
            },
        )

    def test_transactional_outbox_commit_failure_prevents_ack(self) -> None:
        store = MemoryIdempotencyStore()

        class FailingOutboxStore:
            def load(self, key: str) -> Any | None:
                return None

            def save(self, key: str, value: Any) -> None:
                raise RuntimeError("outbox down")

            def delete(self, key: str) -> None:
                return None

        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=store,
                result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
                commit_policy=TransactionalOutboxCommit(
                    outbox_store=FailingOutboxStore()
                ),
            ),
        )
        message = queue.put("item", dedupe_key="job-1")

        @persistent_worker(queue)
        def handle(value: str) -> dict[str, str]:
            return {"status": value.upper()}

        with self.assertRaisesRegex(RuntimeError, "outbox down"):
            cast("Any", handle)()

        record = store.load("job-1")
        assert record is not None
        self.assertEqual(record.status, "succeeded")
        self.assertEqual(queue.qsize(), 0)
        self.assertIsNotNone(queue.inspect(message.id))

    def test_transactional_outbox_commit_without_outbox_store_is_noop(self) -> None:
        store = MemoryIdempotencyStore()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=store,
                result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
                commit_policy=TransactionalOutboxCommit(),
            ),
        )
        _ = queue.put("item", dedupe_key="job-1")

        @persistent_worker(queue)
        def handle(value: str) -> dict[str, str]:
            return {"status": value.upper()}

        self.assertEqual(cast("Any", handle)(), {"status": "ITEM"})
        self.assertIsNone(MemoryResultStore().load("outbox:dedupe:test:job-1"))

    def test_commit_outbox_returns_without_dedupe_key(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=MemoryIdempotencyStore(),
                result_policy=ReturnStoredResult(result_store=MemoryResultStore()),
                commit_policy=TransactionalOutboxCommit(
                    outbox_store=MemoryResultStore()
                ),
            ),
        )
        message = QueueMessage(id="message-1", queue="test", value="item")

        _commit_outbox(queue, message, result={"status": "ITEM"}, result_key=None)

    def test_constructor_accepts_backpressure_strategy(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            backpressure=BoundedBackpressure(1),
        )

        self.assertEqual(queue.maxsize, 1)
        self.assertEqual(
            queue.backpressure.as_dict(),
            {"type": "bounded", "maxsize": 1, "overflow": "block"},
        )
        _ = queue.put("one")
        self.assertTrue(queue.full())

    def test_constructor_accepts_rejecting_backpressure_strategy(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            backpressure=RejectingBackpressure(1),
        )

        self.assertEqual(queue.maxsize, 1)
        self.assertEqual(
            queue.backpressure.as_dict(),
            {"type": "rejecting", "maxsize": 1, "overflow": "reject"},
        )
        _ = queue.put("one")
        with self.assertRaises(Full):
            _ = queue.put("two")

    def test_constructor_copies_lease_timeout_and_defaults(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore(), lease_timeout=31.0)

        self.assertEqual(queue.lease_timeout, 31.0)

    def test_wait_time_and_remaining_helpers(self) -> None:
        deadline = time.monotonic() + 1.0

        self.assertIsNone(_remaining(None))
        remaining = _remaining(deadline)
        assert remaining is not None
        self.assertAlmostEqual(remaining, 1.0, delta=0.2)
        self.assertEqual(_wait_time(None), 0.05)
        self.assertEqual(_wait_time(0.0), 0.0)
        self.assertEqual(_wait_time(0.2), 0.05)

    def test_constructor_defaults_blocking_and_capacity(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        self.assertEqual(queue.lease_timeout, 30.0)
        self.assertEqual(queue.maxsize, 0)
        self.assertTrue(queue.put("item"))
        self.assertEqual(queue.get(), "item")
        queue.task_done()

    def test_default_get_and_get_message_block_until_item_arrives(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        def producer() -> None:
            time.sleep(0.2)
            _ = queue.put("item")

        t = threading.Thread(target=producer)
        t.start()

        start = time.time()
        self.assertEqual(queue.get(), "item")
        self.assertGreaterEqual(time.time() - start, 0.2)
        queue.task_done()

        t.join()

    def test_put_default_delay_is_immediate(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        _ = queue.put("item")
        self.assertEqual(queue.qsize(), 1)

    def test_deadline_rejects_negative_timeout_with_exact_message(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "^'timeout' must be a non-negative number$"
        ):
            _ = _deadline(-1)

    def test_validate_retry_defaults_rejects_invalid_values_with_exact_messages(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError, "^retry_defaults cannot set both max_tries and stop$"
        ):
            _validate_retry_defaults({"max_tries": 1, "stop": object()})

        with self.assertRaisesRegex(
            ValueError, "^retry_defaults max_tries must be a positive integer$"
        ):
            _validate_retry_defaults({"max_tries": 0})

    def test_release_and_dead_letter_error_payloads_include_error_details(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("item")
        message = queue.get_message()

        self.assertTrue(queue.release(message, error=RuntimeError("boom")))
        released = queue.inspect(message.id)
        assert released is not None
        self.assertEqual(
            released.last_error,
            {
                "type": "RuntimeError",
                "module": "builtins",
                "message": "boom",
            },
        )
        self.assertIsNotNone(released.failed_at)

        command_error = RuntimeError("boom")
        command_error.command = "run job"  # type: ignore[attr-defined]
        command_error.exit_code = 127  # type: ignore[attr-defined]
        command_error.stdout = "stdout"  # type: ignore[attr-defined]
        command_error.stderr = "stderr"  # type: ignore[attr-defined]
        _ = queue.put("command-item")
        command_message = queue.get_message()
        self.assertTrue(queue.release(command_message, error=command_error))
        command_released = queue.inspect(command_message.id)
        assert command_released is not None
        self.assertEqual(
            command_released.last_error,
            {
                "type": "RuntimeError",
                "module": "builtins",
                "message": "boom",
                "command": "run job",
                "exit_code": 127,
                "stdout": "stdout",
                "stderr": "stderr",
            },
        )

        _ = queue.put("dead-item")
        dead_message = queue.get_message()
        self.assertTrue(queue.dead_letter(dead_message, error="bad handler"))
        dead = queue.inspect(dead_message.id)
        assert dead is not None
        self.assertEqual(
            dead.last_error,
            {
                "type": None,
                "module": None,
                "message": "bad handler",
            },
        )
        self.assertIsNotNone(dead.failed_at)

    def test_dead_letter_removes_unfinished_message(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("item")
        message = queue.get_message()

        self.assertTrue(queue.dead_letter(message))
        with self.assertRaises(ValueError):
            queue.task_done()

    def test_memory_store_basic_ops(self) -> None:
        queue: PersistentQueue[str] = PersistentQueue("test", store=MemoryQueueStore())
        self.assertTrue(queue.empty())
        _ = queue.put("item1")
        self.assertEqual(queue.qsize(), 1)
        self.assertFalse(queue.empty())
        self.assertEqual(queue.get(), "item1")
        queue.task_done()
        self.assertTrue(queue.empty())

    def test_memory_store_deduplicates_enqueued_messages(self) -> None:
        queue: PersistentQueue[str] = PersistentQueue("test", store=MemoryQueueStore())

        first = queue.put("item1", dedupe_key="job-1")
        second = queue.put("item2", dedupe_key="job-1")

        self.assertEqual(first.id, second.id)
        self.assertEqual(first.value, "item1")
        self.assertEqual(second.value, "item1")
        self.assertEqual(queue.qsize(), 1)

        self.assertTrue(queue.ack(first))
        third = queue.put("item3", dedupe_key="job-1")
        self.assertNotEqual(first.id, third.id)
        self.assertEqual(third.value, "item3")

    def test_put_rejects_empty_dedupe_key(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        with self.assertRaisesRegex(ValueError, "^dedupe_key cannot be empty$"):
            _ = queue.put("item", dedupe_key="")

    def test_effectively_once_put_requires_dedupe_key(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(),
        )

        with self.assertRaisesRegex(
            ValueError, "dedupe_key is required by the active delivery_policy"
        ):
            _ = queue.put("item")

        message = queue.put("item", dedupe_key="job-1")
        self.assertEqual(message.dedupe_key, "job-1")

    def test_priority_ordering_delivers_higher_priority_first(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            ordering_policy=PriorityOrdering(),
        )
        with mock.patch("time.time", return_value=100.0):
            low = queue.put("low", priority=1)
            high = queue.put("high", priority=10)
            normal = queue.put("normal")

        self.assertEqual(low.priority, 1)
        self.assertEqual(high.priority, 10)
        self.assertEqual(normal.priority, 0)
        with mock.patch("time.time", return_value=100.0):
            self.assertEqual(queue.get_nowait(), "high")
            queue.task_done()
            self.assertEqual(queue.get_nowait(), "low")
            queue.task_done()
            self.assertEqual(queue.get_nowait(), "normal")
            queue.task_done()

    def test_sqlite_priority_ordering_delivers_higher_priority_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            queue = PersistentQueue(
                "test",
                store_path=f"{tmpdir}/queue.sqlite3",
                ordering_policy=PriorityOrdering(),
            )
            with mock.patch("time.time", return_value=100.0):
                _ = queue.put("low", priority=1)
                _ = queue.put("high", priority=10)
                _ = queue.put("normal")

            with mock.patch("time.time", return_value=100.0):
                self.assertEqual(queue.get_nowait(), "high")
                queue.task_done()
                self.assertEqual(queue.get_nowait(), "low")
                queue.task_done()
                self.assertEqual(queue.get_nowait(), "normal")
                queue.task_done()

    def test_lmdb_priority_ordering_delivers_higher_priority_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            queue = PersistentQueue(
                "test",
                store=LMDBQueueStore(tmpdir),
                ordering_policy=PriorityOrdering(),
            )
            with mock.patch("time.time", return_value=100.0):
                _ = queue.put("low", priority=1)
                _ = queue.put("high", priority=10)
                _ = queue.put("normal")

            with mock.patch("time.time", return_value=100.0):
                self.assertEqual(queue.get_nowait(), "high")
                queue.task_done()
                self.assertEqual(queue.get_nowait(), "low")
                queue.task_done()
                self.assertEqual(queue.get_nowait(), "normal")
                queue.task_done()

    def test_default_put_blocks_until_capacity_frees(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore(), maxsize=1)
        _ = queue.put("one")
        put_finished = threading.Event()

        def producer() -> None:
            _ = queue.put("two")
            put_finished.set()

        t = threading.Thread(target=producer)
        t.start()

        self.assertFalse(put_finished.wait(0.05))
        self.assertEqual(queue.get_nowait(), "one")
        queue.task_done()
        self.assertTrue(put_finished.wait(1.0))
        t.join()

    def test_at_most_once_get_message_removes_message_before_handling(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=AtMostOnceDelivery(),
        )
        original = queue.put("item")

        message = queue.get_message()

        self.assertEqual(message.id, original.id)
        self.assertEqual(message.value, "item")
        self.assertIsNone(queue.inspect(message.id))
        self.assertTrue(queue.empty())
        self.assertFalse(queue.ack(message))
        self.assertFalse(queue.release(message))
        self.assertFalse(queue.dead_letter(message))

    def test_at_most_once_get_does_not_track_unfinished_tasks(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=AtMostOnceDelivery(),
        )
        _ = queue.put("item")

        self.assertEqual(queue.get_nowait(), "item")
        with self.assertRaisesRegex(
            ValueError, "task_done\\(\\) called too many times"
        ):
            queue.task_done()

    def test_default_get_message_blocks_until_value_is_available(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        def producer() -> None:
            time.sleep(0.2)
            _ = queue.put("item")

        t = threading.Thread(target=producer)
        t.start()

        start = time.time()
        message = queue.get_message()
        self.assertGreaterEqual(time.time() - start, 0.2)
        self.assertEqual(message.value, "item")
        t.join()

    def test_put_rejects_negative_delay_with_message(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        with self.assertRaisesRegex(ValueError, "delay cannot be negative"):
            _ = queue.put("item", delay=-1)

    def test_stats_counts_messages_by_state(self) -> None:
        queue: PersistentQueue[str] = PersistentQueue("test", store=MemoryQueueStore())
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
        self.assertEqual(stats.last_seen_by_worker_id, {})
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
        self.assertEqual(after_ack.last_seen_by_worker_id, {})

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

    def test_record_worker_heartbeat_updates_memory_stats(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        queue.record_worker_heartbeat("worker-a")

        stats = queue.stats()
        self.assertEqual(set(stats.last_seen_by_worker_id), {"worker-a"})
        self.assertGreaterEqual(stats.last_seen_by_worker_id["worker-a"], 0.0)

        with self.assertRaises(ValueError):
            queue.record_worker_heartbeat("")

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

    def test_dead_letter_prune_removes_old_messages(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        with mock.patch(
            "time.time",
            side_effect=[100.0, 100.0, 100.0, 100.0, 100.0],
        ):
            _ = queue.put("old")
            message = queue.get_message()
            self.assertTrue(queue.dead_letter(message, error=RuntimeError("bad")))

        with mock.patch("time.time", return_value=200.0):
            self.assertEqual(queue.prune_dead_letters(older_than=50), 1)

        self.assertEqual(queue.dead_letters(), [])

    def test_sqlite_dead_letter_prune_removes_old_messages(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            queue = PersistentQueue("test", store_path=f"{tmpdir}/queue.sqlite3")

            with mock.patch(
                "time.time",
                side_effect=[100.0, 100.0, 100.0, 100.0, 100.0],
            ):
                _ = queue.put("old")
                message = queue.get_message()
                self.assertTrue(queue.dead_letter(message, error=RuntimeError("bad")))

            with mock.patch("time.time", return_value=200.0):
                self.assertEqual(queue.prune_dead_letters(older_than=50), 1)

            self.assertEqual(queue.dead_letters(), [])

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
            _ = store.enqueue("jobs", "second", available_at=now + 10)
            third = store.enqueue("jobs", "third", available_at=now + 20)
            self.assertTrue(store.ack("jobs", third.id))

            self.assertEqual(store.qsize("jobs", now=now), 1)
            self.assertFalse(store.empty("jobs", now=now))

            leased = store.dequeue("jobs", lease_timeout=0.1, now=now)
            assert leased is not None
            self.assertEqual(leased.value, "first")
            self.assertEqual(leased.attempts, 1)
            self.assertEqual(store.qsize("jobs", now=now), 0)

    def test_sqlite_store_deduplicates_enqueued_messages(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteQueueStore(f"{tmpdir}/queue.sqlite3")
            now = time.time()

            first = store.enqueue(
                "jobs",
                {"job": 1},
                available_at=now,
                dedupe_key="job-1",
            )
            second = store.enqueue(
                "jobs",
                {"job": 2},
                available_at=now,
                dedupe_key="job-1",
            )

            self.assertEqual(first.id, second.id)
            self.assertEqual(second.value, {"job": 1})
            self.assertEqual(
                store.qsize("jobs", now=now),
                1,
            )

            self.assertTrue(store.ack("jobs", first.id))
            third = store.enqueue(
                "jobs",
                {"job": 3},
                available_at=now,
                dedupe_key="job-1",
            )
            self.assertNotEqual(first.id, third.id)
            self.assertEqual(third.value, {"job": 3})

            with store._transaction() as connection:  # type: ignore[attr-defined]
                _ = connection.execute(
                    "DELETE FROM queue_messages WHERE queue = ? AND id = ?",
                    ("jobs", third.id),
                )

            fourth = store.enqueue(
                "jobs",
                {"job": 4},
                available_at=now,
                dedupe_key="job-1",
            )
            self.assertNotEqual(third.id, fourth.id)
            self.assertEqual(fourth.value, {"job": 4})

            self.assertEqual(store.qsize("jobs", now=now), 1)

    def test_store_helpers_cover_encoding_and_empty_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_store = SQLiteQueueStore(f"{tmpdir}/queue.sqlite3")
            now = time.time()

            self.assertEqual(sqlite_store.qsize("jobs", now=now), 0)
            self.assertEqual(sqlite_store.stats("jobs", now=now).as_dict()["total"], 0)
            self.assertEqual(sqlite_store.dead_letters("jobs"), [])
            self.assertEqual(
                sqlite_store.count_dead_letters_older_than(
                    "jobs", older_than=1, now=now
                ),
                0,
            )
            self.assertFalse(sqlite_store.release("jobs", "missing", available_at=now))
            self.assertFalse(sqlite_store.dead_letter("jobs", "missing"))
            self.assertFalse(
                sqlite_store.requeue_dead("jobs", "missing", available_at=now)
            )
            self.assertEqual(
                sqlite_store.prune_dead_letters("jobs", older_than=1, now=now), 0
            )
            self.assertEqual(sqlite_store.purge("jobs"), 0)
            sqlite_store.close()

            lmdb_store = LMDBQueueStore(Path(tmpdir) / "lmdb")
            self.assertEqual(lmdb_store.qsize("jobs", now=now), 0)
            self.assertEqual(lmdb_store.stats("jobs", now=now).as_dict()["total"], 0)
            self.assertEqual(lmdb_store.dead_letters("jobs"), [])
            self.assertEqual(
                lmdb_store.count_dead_letters_older_than("jobs", older_than=1, now=now),
                0,
            )
            self.assertFalse(lmdb_store.release("jobs", "missing", available_at=now))
            self.assertFalse(lmdb_store.dead_letter("jobs", "missing"))
            self.assertFalse(
                lmdb_store.requeue_dead("jobs", "missing", available_at=now)
            )
            self.assertEqual(
                lmdb_store.prune_dead_letters("jobs", older_than=1, now=now), 0
            )
            self.assertEqual(lmdb_store.purge("jobs"), 0)

    def test_store_helpers_cover_dedupe_encoding_and_last_leased_at(self) -> None:
        record = _QueueRecord.new(
            "jobs",
            {"job": 1},
            123.0,
            dedupe_key="dedupe-1",
        )
        leased_record = _replace_record(
            record,
            attempt_history=[
                {
                    "type": "leased",
                    "at": 42.0,
                    "attempt": 1,
                    "leased_by": "worker-a",
                }
            ],
        )

        encoded = _encode_record(leased_record)
        decoded = _decode_record(encoded)
        self.assertEqual(decoded.dedupe_key, "dedupe-1")
        self.assertEqual(_last_leased_at(decoded), 42.0)
        self.assertEqual(_dedupe_token("abc"), "616263")
        self.assertEqual(
            _dedupe_key_key("jobs", "abc"),
            b"queue:jobs:dedupe:616263",
        )

    def test_store_helpers_cover_invalid_worker_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(Path(tmpdir) / "lmdb")
            with self.assertRaisesRegex(
                ValueError, "^queue worker stats are not valid JSON$"
            ):
                store._decode_worker_stats(b"not-json")

            with self.assertRaisesRegex(
                ValueError, "^queue worker stats payload must be a JSON object$"
            ):
                store._decode_worker_stats(b"[]")

            with self.assertRaisesRegex(
                ValueError, "^queue worker heartbeat payload is not valid JSON$"
            ):
                store._decode_worker_heartbeats(b"not-json")

            with self.assertRaisesRegex(
                ValueError, "^queue worker heartbeat payload must be a JSON object$"
            ):
                store._decode_worker_heartbeats(b"[]")

            self.assertEqual(
                store._decode_worker_stats(b'{"worker-a": 2}'), {"worker-a": 2}
            )
            self.assertEqual(
                store._decode_worker_heartbeats(b'{"worker-a": 2.5}'),
                {"worker-a": 2.5},
            )
            self.assertEqual(
                _encode_worker_heartbeats({"worker-a": 2.5}),
                b'{"worker-a":2.5}',
            )

    def test_lmdb_store_covers_empty_and_dedupe_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(Path(tmpdir) / "lmdb")
            now = time.time()

            self.assertEqual(store.qsize("jobs", now=now), 0)
            self.assertEqual(store.stats("jobs", now=now).as_dict()["total"], 0)
            self.assertEqual(store.dead_letters("jobs"), [])
            self.assertEqual(store.dead_letters("jobs", limit=1), [])
            self.assertEqual(
                store.count_dead_letters_older_than("jobs", older_than=1, now=now), 0
            )
            self.assertFalse(store.release("jobs", "missing", available_at=now))
            self.assertFalse(store.dead_letter("jobs", "missing"))
            self.assertFalse(store.requeue_dead("jobs", "missing", available_at=now))
            self.assertEqual(store.prune_dead_letters("jobs", older_than=1, now=now), 0)
            self.assertEqual(store.purge("jobs"), 0)

            first = store.enqueue(
                "jobs", {"job": 1}, available_at=now, dedupe_key="job-1"
            )
            second = store.enqueue(
                "jobs", {"job": 2}, available_at=now, dedupe_key="job-1"
            )
            self.assertEqual(first.id, second.id)
            self.assertEqual(second.value, {"job": 1})

            with store._env.begin(write=True) as txn:  # type: ignore[attr-defined]
                _ = txn.delete(_message_key("jobs", first.id))

            third = store.enqueue(
                "jobs", {"job": 3}, available_at=now, dedupe_key="job-1"
            )
            self.assertNotEqual(first.id, third.id)

            dead_one = store.enqueue("jobs", {"dead": 1}, available_at=now)
            dead_two = store.enqueue("jobs", {"dead": 2}, available_at=now)
            self.assertTrue(store.dead_letter("jobs", dead_one.id))
            self.assertTrue(store.dead_letter("jobs", dead_two.id))
            self.assertEqual(len(store.dead_letters("jobs", limit=1)), 1)
            self.assertEqual(
                store.count_dead_letters_older_than("jobs", older_than=0, now=now + 1),
                2,
            )
            self.assertEqual(
                store.prune_dead_letters("jobs", older_than=0, now=now + 1), 2
            )
            self.assertEqual(store.dead_letters("jobs"), [])
            self.assertGreaterEqual(store.purge("jobs"), 1)

    def test_memory_store_cleans_stale_dedupe_keys(self) -> None:
        store = MemoryQueueStore()
        now = time.time()

        first = store.enqueue("jobs", {"job": 1}, available_at=now, dedupe_key="job-1")
        store._dedupe_keys["jobs"]["job-1"] = "missing"  # type: ignore[attr-defined]

        second = store.enqueue("jobs", {"job": 2}, available_at=now, dedupe_key="job-1")

        self.assertNotEqual(first.id, second.id)
        self.assertEqual(second.value, {"job": 2})

    def test_memory_store_prunes_dead_letters_and_dedupe_keys(self) -> None:
        store = MemoryQueueStore()
        now = time.time()

        message = store.enqueue(
            "jobs", {"job": 1}, available_at=now, dedupe_key="job-1"
        )
        self.assertTrue(store.dead_letter("jobs", message.id, failed_at=now))
        self.assertEqual(
            store.count_dead_letters_older_than("jobs", older_than=0, now=now), 1
        )
        self.assertEqual(store.prune_dead_letters("jobs", older_than=0, now=now), 1)
        self.assertEqual(store.dead_letters("jobs"), [])

        replacement = store.enqueue(
            "jobs", {"job": 2}, available_at=now, dedupe_key="job-1"
        )
        self.assertNotEqual(message.id, replacement.id)

    def test_lmdb_store_covers_cursor_fallback_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(Path(tmpdir) / "lmdb")
            now = time.time()

            _ = store.enqueue("jobs2", {"job": 1}, available_at=now)
            self.assertIsNone(store.dequeue("jobs", lease_timeout=1, now=now))

            future = store.enqueue("jobs", {"job": 2}, available_at=now + 10)
            self.assertIsNone(store.dequeue("jobs", lease_timeout=1, now=now))

            with store._env.begin(write=True) as txn:  # type: ignore[attr-defined]
                _ = txn.delete(_message_key("jobs", future.id))
            self.assertIsNone(store.dequeue("jobs", lease_timeout=1, now=now + 20))

    def test_lmdb_store_prunes_dead_letters_and_dedupe_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()

            message = store.enqueue(
                "jobs", {"job": 1}, available_at=now, dedupe_key="job-1"
            )
            leased = store.dequeue("jobs", lease_timeout=1, now=now)
            assert leased is not None
            self.assertTrue(store.dead_letter("jobs", leased.id, failed_at=now))
            self.assertEqual(
                store.count_dead_letters_older_than("jobs", older_than=0, now=now),
                1,
            )
            self.assertEqual(store.prune_dead_letters("jobs", older_than=0, now=now), 1)
            self.assertEqual(store.dead_letters("jobs"), [])

            replacement = store.enqueue(
                "jobs", {"job": 2}, available_at=now, dedupe_key="job-1"
            )
            self.assertNotEqual(message.id, replacement.id)

    def test_lmdb_store_dequeue_covers_cursor_edge_cases(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()

            cursor = mock.MagicMock()
            txn = mock.MagicMock()
            txn.cursor.return_value = cursor
            fake_env = mock.MagicMock()
            fake_env.begin.return_value.__enter__.return_value = txn
            fake_env.begin.return_value.__exit__.return_value = False
            store._env = fake_env  # type: ignore[attr-defined]

            cursor.set_range.return_value = True
            cursor.item.return_value = None
            self.assertIsNone(store.dequeue("jobs", lease_timeout=1, now=now))

            cursor.item.return_value = (
                b"queue:jobs2:ready:0000000000000:message-id",
                b"message-id",
            )
            self.assertIsNone(store.dequeue("jobs", lease_timeout=1, now=now))

            cursor.item.return_value = (
                _ready_key("jobs", now + 10, 1, "future-id"),
                b"future-id",
            )
            self.assertIsNone(store.dequeue("jobs", lease_timeout=1, now=now))

            cursor.item.return_value = (
                _ready_key("jobs", now, 1, "missing-id"),
                b"missing-id",
            )
            txn.get.return_value = None
            self.assertIsNone(store.dequeue("jobs", lease_timeout=1, now=now))

    def test_lmdb_store_prune_and_purge_cover_internal_cleanup_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()

            message = store.enqueue(
                "jobs", {"job": 1}, available_at=now, dedupe_key="job-1"
            )
            self.assertTrue(store.dead_letter("jobs", message.id, failed_at=now))
            with store._env.begin() as txn:  # type: ignore[attr-defined]
                doomed = store._get_record(txn, "jobs", message.id)
            assert doomed is not None
            with mock.patch.object(
                store,
                "_get_record",
                side_effect=[doomed, None],
            ):
                self.assertEqual(
                    store.prune_dead_letters("jobs", older_than=0, now=now), 1
                )

            with tempfile.TemporaryDirectory() as other_tmpdir:
                other_store = LMDBQueueStore(other_tmpdir)
                _ = other_store.enqueue("jobs2", {"job": 2}, available_at=now)
                self.assertEqual(other_store.purge("jobs"), 0)

                _ = other_store.enqueue(
                    "jobs", {"job": 3}, available_at=now, dedupe_key="job-3"
                )
                self.assertGreaterEqual(other_store.purge("jobs"), 1)

    def test_lmdb_store_records_worker_heartbeat(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()

            store.record_worker_heartbeat("jobs", "worker-a", now=now)

            stats = store.stats("jobs", now=now)
            self.assertEqual(stats.last_seen_by_worker_id, {"worker-a": now})

    def test_lmdb_store_reclaims_missing_expired_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()

            message = store.enqueue("jobs", {"job": 1}, available_at=now)
            leased = store.dequeue("jobs", lease_timeout=0.1, now=now)
            assert leased is not None

            with store._env.begin(write=True) as txn:  # type: ignore[attr-defined]
                _ = txn.delete(_message_key("jobs", message.id))

            self.assertIsNone(store.dequeue("jobs", lease_timeout=0.1, now=now + 1))

    def test_lmdb_store_iterates_and_deletes_dedupe_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()

            _ = store.enqueue("jobs", {"job": 1}, available_at=now, dedupe_key="job-1")
            with store._env.begin() as txn:  # type: ignore[attr-defined]
                self.assertEqual(
                    list(store._iter_dedupe_keys(txn, "jobs")),
                    [_dedupe_key_key("jobs", "job-1")],
                )

            self.assertGreaterEqual(store.purge("jobs"), 1)

    def test_lmdb_store_purge_breaks_on_non_matching_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()

            _ = store.enqueue("jobsa", {"job": 1}, available_at=now)
            self.assertEqual(store.purge("jobs"), 0)

    def test_lmdb_store_purge_deletes_yielded_dedupe_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)
            now = time.time()

            _ = store.enqueue("jobs", {"job": 1}, available_at=now)
            with mock.patch.object(
                store,
                "_iter_dedupe_keys",
                return_value=iter([_dedupe_key_key("jobs", "fake")]),
            ):
                self.assertEqual(store.purge("jobs"), 1)

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

    def test_sqlite_store_sets_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queue.sqlite3"
            store = SQLiteQueueStore(path)
            store.close()

            connection = sqlite3.connect(path)
            cursor = connection.execute("PRAGMA user_version")
            row = cursor.fetchone()
            connection.close()

            assert row is not None
            self.assertEqual(int(row[0]), 3)

    def test_sqlite_store_rejects_future_schema_versions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "queue.sqlite3"
            connection = sqlite3.connect(path)
            connection.execute("PRAGMA user_version = 999")
            connection.commit()
            connection.close()

            with self.assertRaises(ValueError):
                _ = SQLiteQueueStore(path)

    def test_sqlite_store_handles_concurrent_producers_and_consumers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queue.sqlite3")
            queue_name = "jobs"
            total_messages = 120
            result = _run_sqlite_threaded_queue_scenario(
                store_path=store_path,
                queue_name=queue_name,
                total_messages=total_messages,
            )

            self.assertEqual(result.errors, [])
            self.assertEqual(sum(result.consumed.values()), total_messages)
            self.assertEqual(len(result.consumed), total_messages)
            self.assertTrue(all(count == 1 for count in result.consumed.values()))
            self.assertGreaterEqual(sum(result.lock_retries.values()), 0)
            stats = result.stats
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
            self.assertEqual(stats.last_seen_by_worker_id, {})
            self.assertIsNone(stats.oldest_ready_age_seconds)
            self.assertIsNone(stats.oldest_inflight_age_seconds)
            self.assertIsNone(stats.average_inflight_age_seconds)

    def test_sqlite_store_tracks_worker_heartbeat(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            queue = PersistentQueue("jobs", store_path=f"{tmpdir}/queue.sqlite3")

            queue.record_worker_heartbeat("worker-a")

            stats = queue.stats()
            self.assertEqual(set(stats.last_seen_by_worker_id), {"worker-a"})
            self.assertGreaterEqual(stats.last_seen_by_worker_id["worker-a"], 0.0)

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
            self.assertEqual(stats.last_seen_by_worker_id, {})
            self.assertIsNone(stats.oldest_ready_age_seconds)
            self.assertIsNone(stats.oldest_inflight_age_seconds)
            self.assertIsNone(stats.average_inflight_age_seconds)

    def test_dead_letter_retention_rejects_negative_values(self) -> None:
        queue = PersistentQueue("jobs", store=MemoryQueueStore())

        with self.assertRaises(ValueError):
            _ = queue.prune_dead_letters(older_than=-1)

        with self.assertRaises(ValueError):
            _ = queue.count_dead_letters_older_than(older_than=-1)

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
            self.assertEqual(stats.last_seen_by_worker_id, {})
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
            self.assertEqual(payload["version"], 5)
            self.assertEqual(payload["value"], {"kind": "email"})
            self.assertIsNone(payload["leased_by"])
            self.assertIsNone(payload["dedupe_key"])
            self.assertEqual(payload["priority"], 0)
            self.assertEqual(payload["attempt_history"], [])

    def test_lmdb_store_rejects_non_json_serializable_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBQueueStore(tmpdir)

            with self.assertRaisesRegex(ValueError, "JSON-serializable"):
                _ = store.enqueue("jobs", object(), available_at=time.time())

    def test_retry_store_prunes_exhausted_records(self) -> None:
        store = MemoryAttemptStore()
        store.save(
            "old", RetryRecord(attempts=3, first_attempt_at=100.0, exhausted=True)
        )
        store.save(
            "active", RetryRecord(attempts=1, first_attempt_at=100.0, exhausted=False)
        )

        self.assertEqual(store.prune_exhausted(older_than=50, now=200.0), 1)
        self.assertIsNone(store.load("old"))
        self.assertIsNotNone(store.load("active"))

    def test_sqlite_retry_store_prunes_exhausted_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteAttemptStore(f"{tmpdir}/retries.sqlite3")
            store.save(
                "old", RetryRecord(attempts=3, first_attempt_at=100.0, exhausted=True)
            )
            store.save(
                "active",
                RetryRecord(attempts=1, first_attempt_at=100.0, exhausted=False),
            )

            self.assertEqual(store.prune_exhausted(older_than=50, now=200.0), 1)
            self.assertIsNone(store.load("old"))
            self.assertIsNotNone(store.load("active"))
            store.close()

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
        self.assertEqual(
            _sequence_from_index_key(
                _ready_key("jobs", time.time(), 12, "id", priority=5)
            ),
            12,
        )

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
                dedupe_key=None,
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
        self.assertEqual(decoded.priority, 0)

    def test_sqlite_store_reads_legacy_version_1_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = f"{tmpdir}/queue.sqlite3"
            legacy_record = {
                "version": 1,
                "id": "legacy-job",
                "value": {"kind": "email"},
                "queue": "jobs",
                "attempts": 2,
                "created_at": 100.0,
                "available_at": 100.0,
                "leased_until": None,
                "leased_by": "worker-a",
                "last_error": {"type": "RuntimeError", "message": "boom"},
                "failed_at": 120.0,
                "state": "dead",
                "index_key": _dead_key("jobs", "legacy-job").decode("utf-8"),
            }

            connection = sqlite3.connect(store_path)
            connection.execute(
                "CREATE TABLE queue_messages ("
                "queue TEXT NOT NULL, "
                "id TEXT NOT NULL, "
                "record_json TEXT NOT NULL, "
                "state TEXT NOT NULL, "
                "available_at REAL NOT NULL, "
                "leased_until REAL, "
                "sequence INTEGER NOT NULL, "
                "PRIMARY KEY(queue, id)"
                ")"
            )
            connection.execute(
                "INSERT INTO queue_messages("
                "queue, id, record_json, state, available_at, leased_until, sequence"
                ") VALUES(?, ?, ?, ?, ?, ?, ?)",
                (
                    "jobs",
                    "legacy-job",
                    json.dumps(legacy_record, separators=(",", ":")),
                    "dead",
                    100.0,
                    None,
                    1,
                ),
            )
            connection.execute("PRAGMA user_version = 1")
            connection.commit()
            connection.close()

            queue = PersistentQueue("jobs", store_path=store_path)
            messages = queue.dead_letters()
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0].id, "legacy-job")
            self.assertEqual(messages[0].attempt_history, [])
            self.assertEqual(messages[0].last_error, legacy_record["last_error"])
            self.assertEqual(queue.count_dead_letters_older_than(older_than=50), 1)

            connection = sqlite3.connect(store_path)
            version_row = connection.execute("PRAGMA user_version").fetchone()
            column_rows = connection.execute("PRAGMA table_info(queue_messages)")
            columns = {str(row[1]) for row in column_rows.fetchall()}
            migrated_row = connection.execute(
                "SELECT created_at, failed_at, leased_by, priority "
                "FROM queue_messages WHERE queue = ? AND id = ?",
                ("jobs", "legacy-job"),
            ).fetchone()
            connection.close()

            assert version_row is not None
            self.assertEqual(int(version_row[0]), 3)
            self.assertLessEqual(
                {"created_at", "failed_at", "leased_by", "priority"}, columns
            )
            self.assertEqual(migrated_row, (100.0, 120.0, "worker-a", 0))

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
        default_path = Path("/home/example/.local/share/localqueue/queue.sqlite3")
        with (
            mock.patch(
                "localqueue.queue.default_queue_store_path",
                return_value=default_path,
            ),
            mock.patch("localqueue.queue.SQLiteQueueStore") as store_cls,
        ):
            queue = PersistentQueue("test")
            store_cls.assert_not_called()

            fake_store = mock.Mock()
            fake_store.qsize.return_value = 0
            store_cls.return_value = fake_store

            self.assertTrue(queue.empty())
            store_cls.assert_called_once_with(default_path)

    def test_default_sqlite_store_uses_xdg_data_home(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict("os.environ", {"XDG_DATA_HOME": tmpdir}, clear=False):
                queue = PersistentQueue("test")
                message = queue.put("item")
                expected_path = Path(tmpdir) / "localqueue" / "queue.sqlite3"

                self.assertTrue(expected_path.is_file())
                self.assertEqual(queue.get_nowait(), "item")
                queue.task_done()
                self.assertTrue(queue.empty())
                self.assertIsNotNone(message.id)

                store = queue._get_store()
                close = getattr(store, "close", None)
                if close is not None:
                    close()

    def test_lmdb_lock_error_is_reworded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_lmdb = mock.Mock()
            fake_lmdb.LockError = lmdb.LockError
            fake_lmdb.open.side_effect = lmdb.LockError("busy")
            with mock.patch(
                "localqueue.stores.lmdb.import_lmdb", return_value=fake_lmdb
            ):
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

    def test_get_timeout_zero_on_empty_queue_raises_immediately(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        start = time.time()
        with self.assertRaises(Empty):
            _ = queue.get(timeout=0)
        self.assertLess(time.time() - start, 0.1)

    def test_get_message_timeout_zero_on_empty_queue_raises_immediately(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        start = time.time()
        with self.assertRaises(Empty):
            _ = queue.get_message(timeout=0)
        self.assertLess(time.time() - start, 0.1)

    def test_nowait_helpers_raise_when_unavailable(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore(), maxsize=1)

        with self.assertRaises(Empty):
            _ = queue.get_nowait()

        _ = queue.put_nowait("one")
        with self.assertRaises(Full):
            _ = queue.put_nowait("two")

    def test_default_blocking_put_waits_for_capacity(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore(), maxsize=1)
        _ = queue.put("one")

        released = threading.Event()

        def freer() -> None:
            time.sleep(0.2)
            self.assertEqual(queue.get_nowait(), "one")
            queue.task_done()
            released.set()

        t = threading.Thread(target=freer)
        t.start()

        start = time.time()
        _ = queue.put("two")
        self.assertGreaterEqual(time.time() - start, 0.2)
        self.assertTrue(released.wait(1.0))
        t.join()

    def test_default_get_blocks_until_item_arrives(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        produced = threading.Event()

        def producer() -> None:
            time.sleep(0.2)
            _ = queue.put("item")
            produced.set()

        t = threading.Thread(target=producer)
        t.start()

        start = time.time()
        self.assertEqual(queue.get(block=True), "item")
        self.assertGreaterEqual(time.time() - start, 0.2)
        self.assertTrue(produced.is_set())
        queue.task_done()
        t.join()

    def test_default_get_message_blocks_until_item_arrives(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        produced = threading.Event()

        def producer() -> None:
            time.sleep(0.2)
            _ = queue.put("item")
            produced.set()

        t = threading.Thread(target=producer)
        t.start()

        start = time.time()
        message = queue.get_message()
        self.assertGreaterEqual(time.time() - start, 0.2)
        self.assertEqual(message.value, "item")
        self.assertTrue(produced.is_set())
        self.assertEqual(queue.qsize(), 0)
        t.join()

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

    def test_blocking_put_respects_timeout_when_queue_stays_full(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore(), maxsize=1)
        _ = queue.put("one")

        def slow_getter() -> None:
            time.sleep(0.2)
            _ = queue.get()
            queue.task_done()

        t = threading.Thread(target=slow_getter)
        t.start()

        with self.assertRaises(Full):
            _ = queue.put("two", timeout=0.05)

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

    def test_dead_letter_and_requeue_dead_accept_original_message_identity(
        self,
    ) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("bad-item")
        msg = queue.get_message()
        self.assertTrue(queue.dead_letter(msg))
        self.assertTrue(queue.requeue_dead(msg))
        self.assertEqual(queue.get_message().id, msg.id)

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

    def test_release_supports_delay_zero(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("bad-item")
        msg = queue.get_message()

        self.assertTrue(queue.release(msg, delay=0))

    def test_release_without_delay_makes_message_available_immediately(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("bad-item")
        msg = queue.get_message()

        self.assertTrue(queue.release(msg))
        self.assertEqual(queue.qsize(), 1)
        self.assertEqual(queue.get_message(block=False).id, msg.id)

    def test_put_without_delay_is_available_immediately(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        message = queue.put("item")

        self.assertEqual(queue.qsize(), 1)
        self.assertEqual(queue.get_message(block=False).id, message.id)

    def test_requeue_dead_supports_delay_zero(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("bad-item")
        msg = queue.get_message()
        self.assertTrue(queue.dead_letter(msg))

        self.assertTrue(queue.requeue_dead(msg, delay=0))

    def test_requeue_dead_notifies_waiters(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("bad-item")
        msg = queue.get_message()
        self.assertTrue(queue.dead_letter(msg))

        received: list[QueueMessage] = []

        def waiter() -> None:
            received.append(queue.get_message())

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.05)

        self.assertTrue(queue.requeue_dead(msg))
        t.join()

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].id, msg.id)

    def test_requeue_dead_without_delay_makes_message_available_immediately(
        self,
    ) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("bad-item")
        msg = queue.get_message()
        self.assertTrue(queue.dead_letter(msg))

        self.assertTrue(queue.requeue_dead(msg))
        self.assertEqual(queue.qsize(), 1)

    def test_prune_dead_letters_accepts_zero_and_rejects_negative(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        with self.assertRaisesRegex(ValueError, "^older_than cannot be negative$"):
            _ = queue.prune_dead_letters(older_than=-1)

        self.assertEqual(queue.prune_dead_letters(older_than=0), 0)

    def test_count_dead_letters_older_than_rejects_negative_with_exact_message(
        self,
    ) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        with self.assertRaisesRegex(ValueError, "^older_than cannot be negative$"):
            _ = queue.count_dead_letters_older_than(older_than=-1)

    def test_record_worker_heartbeat_rejects_empty_worker_id_with_exact_message(
        self,
    ) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        with self.assertRaisesRegex(ValueError, "^worker_id cannot be empty$"):
            queue.record_worker_heartbeat("")

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

    def test_release_without_error_does_not_set_failed_at(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("item")
        msg = queue.get_message()

        self.assertTrue(queue.release(msg))

        released = queue.inspect(msg.id)
        assert released is not None
        self.assertIsNone(released.failed_at)
        self.assertIsNone(released.last_error)

    def test_release_and_get_message_notify_waiters(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("item")
        msg = queue.get_message()
        received: list[QueueMessage] = []

        def waiter() -> None:
            received.append(queue.get_message())

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.05)

        self.assertTrue(queue.release(msg))
        t.join()

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].value, "item")

    def test_purge(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("1")
        _ = queue.put("2")
        _ = queue.get()
        self.assertEqual(queue.purge(), 2)
        self.assertEqual(queue.qsize(), 0)

    def test_remove_unfinished_discards_known_message(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        queue._unfinished["message-id"] = QueueMessage(
            id="message-id",
            value="item",
            queue="test",
        )

        queue._remove_unfinished("message-id")

        self.assertNotIn("message-id", queue._unfinished)

    def test_task_done_rejects_extra_calls_and_join_waits(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())

        with self.assertRaisesRegex(
            ValueError, "task_done\\(\\) called too many times"
        ):
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

    def test_internal_deadline_and_wait_time_helpers(self) -> None:
        self.assertIsNone(_deadline(None))
        deadline = _deadline(1.0)
        assert deadline is not None
        self.assertGreater(deadline, time.monotonic())

        self.assertIsNone(_remaining(None))
        remaining = _remaining(time.monotonic() + 1.0)
        assert remaining is not None
        self.assertGreater(remaining, 0.0)

        self.assertEqual(_wait_time(None), 0.05)
        self.assertEqual(_wait_time(-1.0), 0.0)
        self.assertEqual(_wait_time(0.2), 0.05)

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

    def test_worker_propagates_ack_failures_after_success(self) -> None:
        class AckFailingQueue(PersistentQueue):
            def ack(self, message: QueueMessage) -> bool:
                raise RuntimeError("ack failed")

        queue = AckFailingQueue("test", store=MemoryQueueStore())
        queued = queue.put("a")

        @persistent_worker(
            queue, store=MemoryAttemptStore(), max_tries=1, wait=lambda _: 0
        )
        def handle(value: str) -> str:
            return value.upper()

        with self.assertRaises(RuntimeError):
            cast("Any", handle)()

        self.assertEqual(queue.qsize(), 0)
        self.assertIsNotNone(queue.inspect(queued.id))

    def test_worker_e2e_with_sqlite_queue_and_retry_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            queue_path = Path(tmpdir) / "queue.sqlite3"
            retry_path = Path(tmpdir) / "retries.sqlite3"
            queue_store = SQLiteQueueStore(queue_path)
            retry_store = SQLiteAttemptStore(retry_path)
            try:
                queue = PersistentQueue("jobs", store=queue_store)
                queued = queue.put({"id": "job-1"})
                attempts = {"count": 0}

                @persistent_worker(
                    queue,
                    store=retry_store,
                    max_tries=2,
                    wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
                )
                def handle(payload: dict[str, str]) -> str:
                    attempts["count"] += 1
                    if attempts["count"] == 1:
                        raise ConnectionError("temporary failure")
                    return payload["id"]

                self.assertEqual(cast("Any", handle)(), "job-1")
                self.assertEqual(attempts["count"], 2)
                self.assertTrue(queue.empty())
                self.assertIsNone(retry_store.load(queued.id))
                self.assertEqual(queue_store.qsize("jobs", now=time.time()), 0)
                self.assertTrue(queue_path.is_file())
                self.assertTrue(retry_path.is_file())
            finally:
                retry_store.close()
                queue_store.close()

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

            self.assertEqual(await cast("Any", handle)(), "A")
            self.assertEqual(attempts["count"], 2)
            with self.assertRaises(Empty):
                _ = queue.get_message(block=False)

        asyncio.run(scenario())

    def test_async_worker_propagates_ack_failures_after_success(self) -> None:
        class AckFailingQueue(PersistentQueue):
            def ack(self, message: QueueMessage) -> bool:
                raise RuntimeError("ack failed")

        async def scenario() -> None:
            queue = AckFailingQueue("test", store=MemoryQueueStore())
            queued = queue.put("a")

            @persistent_async_worker(
                queue, store=MemoryAttemptStore(), max_tries=1, wait=lambda _: 0
            )
            async def handle(value: str) -> str:
                return value.upper()

            with self.assertRaises(RuntimeError):
                await cast("Any", handle)()

            self.assertEqual(queue.qsize(), 0)
            self.assertIsNotNone(queue.inspect(queued.id))

        asyncio.run(scenario())

    def test_async_queue_polling_does_not_block_event_loop(self) -> None:
        class SlowQueue:
            def get_message(
                self,
                block: bool = True,
                timeout: float | None = None,
                *,
                leased_by: str | None = None,
            ) -> QueueMessage:
                time.sleep(0.2)
                return QueueMessage(
                    id="job-1",
                    queue="test",
                    value="payload",
                )

        async def scenario() -> None:
            queue = SlowQueue()
            marker = asyncio.Event()

            async def tick() -> None:
                await asyncio.sleep(0)
                marker.set()

            task = asyncio.create_task(_get_message_async(cast("Any", queue)))
            marker_task = asyncio.create_task(tick())
            await asyncio.sleep(0.05)
            self.assertTrue(marker.is_set())
            self.assertFalse(task.done())
            await marker_task
            message = await task
            self.assertEqual(message.id, "job-1")

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
                await cast("Any", fail)()

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
                await cast("Any", fail)()

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
            cast("Any", fail)()

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
            cast("Any", release)()

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

        self.assertEqual(cast("Any", handle)(), "ALICE")

    def test_at_most_once_worker_removes_message_before_handler(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=AtMostOnceDelivery(),
        )
        message = queue.put("bad")

        @persistent_worker(
            queue, store=MemoryAttemptStore(), max_tries=1, wait=lambda _: 0
        )
        def fail(value: str) -> None:
            self.assertIsNone(queue.inspect(message.id))
            raise RuntimeError(value)

        with self.assertRaises(RuntimeError):
            cast("Any", fail)()

        self.assertTrue(queue.empty())
        self.assertEqual(queue.dead_letters(), [])

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
            cast("Any", release)()

        self.assertEqual(queue.qsize(), 1)

        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("dead")

        @persistent_worker(queue, config=config, dead_letter_on_exhaustion=True)
        def dead_letter(value: str) -> None:
            raise RuntimeError(value)

        with self.assertRaises(RuntimeError):
            cast("Any", dead_letter)()

        self.assertTrue(queue.empty())

    def test_worker_uses_queue_retry_defaults(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            retry_defaults={"max_tries": 1, "wait": lambda _: 0},
        )
        _ = queue.put("retry")

        @persistent_worker(queue, store=MemoryAttemptStore())
        def release(value: str) -> None:
            raise RuntimeError(value)

        with self.assertRaises(RuntimeError):
            cast("Any", release)()

        self.assertTrue(queue.empty())
        dead_letters = queue.dead_letters()
        self.assertEqual(len(dead_letters), 1)
        self.assertEqual(dead_letters[0].value, "retry")

    def test_worker_retry_defaults_can_be_overridden(self) -> None:
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            retry_defaults={"max_tries": 1, "wait": lambda _: 0},
        )
        _ = queue.put("retry")
        attempts = {"count": 0}

        @persistent_worker(
            queue, store=MemoryAttemptStore(), max_tries=2, wait=lambda _: 0
        )
        def release(value: str) -> str:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise RuntimeError(value)
            return value.upper()

        self.assertEqual(cast("Any", release)(), "RETRY")
        self.assertEqual(attempts["count"], 2)

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
            cast("Any", release)()

        self.assertEqual(queue.qsize(), 1)

    def test_worker_releases_validation_errors_when_release_is_selected(
        self,
    ) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("bad")

        @persistent_worker(
            queue,
            store=MemoryAttemptStore(),
            max_tries=1,
            wait=lambda _: 0,
            dead_letter_on_exhaustion=False,
        )
        def fail(value: str) -> None:
            raise ValueError(value)

        with self.assertRaises(ValueError):
            cast("Any", fail)()

        self.assertEqual(queue.qsize(), 1)
        message = queue.get_message()
        self.assertEqual(message.value, "bad")
        assert message.last_error is not None
        self.assertEqual(message.last_error["type"], "ValueError")

    def test_worker_dead_letters_import_errors_even_when_release_is_selected(
        self,
    ) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("bad")

        @persistent_worker(
            queue,
            store=MemoryAttemptStore(),
            max_tries=1,
            wait=lambda _: 0,
            dead_letter_on_exhaustion=False,
        )
        def fail(value: str) -> None:
            raise ImportError(value)

        with self.assertRaises(ImportError):
            cast("Any", fail)()

        self.assertTrue(queue.empty())
        dead_letters = queue.dead_letters()
        self.assertEqual(len(dead_letters), 1)
        assert dead_letters[0].last_error is not None
        self.assertEqual(dead_letters[0].last_error["type"], "ImportError")

    def test_async_worker_releases_validation_errors_when_release_is_selected(
        self,
    ) -> None:
        async def scenario() -> None:
            queue = PersistentQueue("test", store=MemoryQueueStore())
            _ = queue.put("bad")

            @persistent_async_worker(
                queue,
                store=MemoryAttemptStore(),
                max_tries=1,
                wait=lambda _: 0,
                dead_letter_on_exhaustion=False,
            )
            async def fail(value: str) -> None:
                raise ValueError(value)

            with self.assertRaises(ValueError):
                await cast("Any", fail)()

            self.assertEqual(queue.qsize(), 1)
            message = queue.get_message()
            self.assertEqual(message.value, "bad")
            assert message.last_error is not None
            self.assertEqual(message.last_error["type"], "ValueError")

        asyncio.run(scenario())

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

        with self.assertRaises(ValueError):
            _ = PersistentWorkerConfig(min_interval=-1)
        with self.assertRaises(ValueError):
            _ = config.with_overrides(min_interval=-1)

        with self.assertRaises(ValueError):
            _ = PersistentWorkerConfig(circuit_breaker_failures=1)

        with self.assertRaises(ValueError):
            _ = PersistentWorkerConfig(
                circuit_breaker_failures=1,
                circuit_breaker_cooldown=-1,
            )

        with self.assertRaises(ValueError):
            _ = PersistentWorkerConfig(
                circuit_breaker_failures=0,
                circuit_breaker_cooldown=1,
            )

        config = PersistentWorkerConfig(
            circuit_breaker_failures=1,
            circuit_breaker_cooldown=1,
        )
        with self.assertRaises(ValueError):
            _ = config.with_overrides(circuit_breaker_cooldown=-1)

    def test_worker_policy_helpers_cover_validation_and_sleep_paths(self) -> None:
        self.assertTrue(
            _resolve_dead_letter_on_failure(
                dead_letter_on_failure=_UNSET,
                dead_letter_on_exhaustion=_UNSET,
            )
        )
        self.assertFalse(
            _resolve_dead_letter_on_failure(
                dead_letter_on_failure=False,
                dead_letter_on_exhaustion=_UNSET,
            )
        )
        with self.assertRaises(ValueError):
            _resolve_dead_letter_on_failure(
                dead_letter_on_failure=True,
                dead_letter_on_exhaustion=False,
            )

        _validate_release_delay(0.0)
        _validate_min_interval(0.0)
        _validate_circuit_breaker(0, 0.0)
        with self.assertRaises(ValueError):
            _validate_release_delay(-1)
        with self.assertRaises(ValueError):
            _validate_min_interval(-1)
        with self.assertRaises(ValueError):
            _validate_circuit_breaker(-1, 0.0)
        with self.assertRaises(ValueError):
            _validate_circuit_breaker(1, -1.0)

        state = WorkerPolicyState(last_started_at=None)
        config = PersistentWorkerConfig()
        with mock.patch("localqueue.worker.time.time", return_value=10.0):
            _sleep_for_policy(state, config)
        self.assertEqual(state.last_started_at, 10.0)

        async def async_scenario() -> None:
            state = WorkerPolicyState(last_started_at=None)
            config = PersistentWorkerConfig()
            with mock.patch("localqueue.worker.time.time", return_value=10.0):
                await _sleep_for_policy_async(state, config)
            self.assertEqual(state.last_started_at, 10.0)

            state = WorkerPolicyState(last_started_at=98.0)
            config = PersistentWorkerConfig(min_interval=5.0)
            with mock.patch("localqueue.worker.time.time", return_value=100.0):
                with mock.patch("localqueue.worker.asyncio.sleep") as sleep:
                    await _sleep_for_policy_async(state, config)
            self.assertTrue(sleep.called)

        asyncio.run(async_scenario())

        state = WorkerPolicyState()
        config = PersistentWorkerConfig(
            circuit_breaker_failures=1,
            circuit_breaker_cooldown=1.0,
        )
        with mock.patch("localqueue.worker.time.time", return_value=100.0):
            _record_failure(state, config, permanent=False)
        self.assertEqual(state.consecutive_failures, 0)
        self.assertIsNotNone(state.breaker_open_until)
        _record_success(state)
        self.assertEqual(state.consecutive_failures, 0)

        state = WorkerPolicyState(
            last_started_at=90.0, breaker_open_until=105.0, consecutive_failures=0
        )
        config = PersistentWorkerConfig(min_interval=5.0)
        with mock.patch("localqueue.worker.time.time", return_value=100.0):
            with mock.patch("localqueue.worker.time.sleep") as sleep:
                _sleep_for_policy(state, config)
        self.assertTrue(sleep.called)

        async def async_sleep_scenario() -> None:
            state = WorkerPolicyState(
                last_started_at=90.0, breaker_open_until=105.0, consecutive_failures=0
            )
            config = PersistentWorkerConfig(min_interval=5.0)
            with mock.patch("localqueue.worker.time.time", return_value=100.0):
                with mock.patch("localqueue.worker.asyncio.sleep") as sleep:
                    await _sleep_for_policy_async(state, config)
            self.assertTrue(sleep.called)

        asyncio.run(async_sleep_scenario())

    def test_worker_respects_min_interval_between_calls(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("first")
        _ = queue.put("second")
        config = PersistentWorkerConfig(
            store=MemoryAttemptStore(),
            max_tries=1,
            wait=lambda _: 0,
            min_interval=1.0,
        )

        with mock.patch("localqueue.worker.time") as worker_time:
            worker_time.time.side_effect = [100.0, 100.0, 100.4, 100.4]

            @persistent_worker(queue, config=config)
            def handle(value: str) -> str:
                return value.upper()

            self.assertEqual(cast("Any", handle)(), "FIRST")
            self.assertEqual(cast("Any", handle)(), "SECOND")

        self.assertTrue(worker_time.sleep.called)
        self.assertAlmostEqual(
            cast("Any", worker_time.sleep.call_args.args[0]), 0.6, places=1
        )

    def test_worker_opens_circuit_breaker_after_recoverable_failures(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("first")
        _ = queue.put("second")
        config = PersistentWorkerConfig(
            store=MemoryAttemptStore(),
            max_tries=1,
            wait=lambda _: 0,
            dead_letter_on_failure=False,
            circuit_breaker_failures=1,
            circuit_breaker_cooldown=10.0,
        )

        with mock.patch("localqueue.worker.time") as worker_time:
            worker_time.time.side_effect = [
                100.0,
                100.0,
                100.0,
                100.4,
                100.4,
                100.4,
                100.4,
            ]

            @persistent_worker(queue, config=config)
            def handle(value: str) -> None:
                raise RuntimeError(value)

            with self.assertRaises(RuntimeError):
                cast("Any", handle)()
            with self.assertRaises(RuntimeError):
                cast("Any", handle)()

        self.assertTrue(worker_time.sleep.called)
        self.assertGreater(cast("Any", worker_time.sleep.call_args.args[0]), 0.0)

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

            self.assertEqual(await cast("Any", handle)(), "A")
            self.assertEqual(attempts["count"], 2)

        asyncio.run(scenario())

    def test_async_worker_uses_queue_retry_defaults(self) -> None:
        async def scenario() -> None:
            queue = PersistentQueue(
                "test",
                store=MemoryQueueStore(),
                retry_defaults={"max_tries": 2, "wait": lambda _: 0},
            )
            _ = queue.put("a")
            attempts = {"count": 0}

            @persistent_async_worker(queue, store=MemoryAttemptStore())
            async def handle(value: str) -> str:
                attempts["count"] += 1
                if attempts["count"] == 1:
                    raise RuntimeError("try again")
                return value.upper()

            self.assertEqual(await cast("Any", handle)(), "A")
            self.assertEqual(attempts["count"], 2)

        asyncio.run(scenario())

    def test_worker_heartbeat_and_async_polling_paths(self) -> None:
        queue = PersistentQueue("test", store=MemoryQueueStore())
        _ = queue.put("item")
        heartbeats: list[str] = []

        class HeartbeatQueue(PersistentQueue):
            def record_worker_heartbeat(self, worker_id: str) -> None:
                heartbeats.append(worker_id)
                super().record_worker_heartbeat(worker_id)

        heartbeat_queue = HeartbeatQueue("test", store=MemoryQueueStore())
        _ = heartbeat_queue.put("item")

        @persistent_worker(
            heartbeat_queue,
            store=MemoryAttemptStore(),
            max_tries=1,
            wait=lambda _: 0,
            worker_id="worker-a",
        )
        def handle(value: str) -> str:
            return value.upper()

        self.assertEqual(cast("Any", handle)(), "ITEM")
        self.assertEqual(heartbeats, ["worker-a", "worker-a"])

        async def scenario() -> None:
            calls = {"count": 0}

            class EmptyThenReadyQueue:
                def get_message(
                    self,
                    block: bool = True,
                    timeout: float | None = None,
                    *,
                    leased_by: str | None = None,
                ) -> QueueMessage:
                    calls["count"] += 1
                    if calls["count"] == 1:
                        raise Empty
                    return QueueMessage(id="job-1", queue="test", value="payload")

            with mock.patch("localqueue.worker.asyncio.sleep") as sleep:
                message = await _get_message_async(cast("Any", EmptyThenReadyQueue()))
            self.assertEqual(message.id, "job-1")
            self.assertTrue(sleep.called)

            class TrackingAsyncNotification:
                def __init__(self) -> None:
                    self.event = asyncio.Event()
                    self.wait_calls = 0
                    self.clear_calls = 0
                    self.notifies = True
                    self.notifies_on_put = True
                    self.listener_count = 1

                def notify(self, message: QueueMessage) -> None:
                    _ = message
                    self.event.set()

                async def wait_async(self, timeout: float | None = None) -> bool:
                    self.wait_calls += 1
                    if timeout is None:
                        await self.event.wait()
                        return True
                    try:
                        await asyncio.wait_for(self.event.wait(), timeout)
                    except TimeoutError:
                        return False
                    return True

                def clear(self) -> None:
                    self.clear_calls += 1
                    self.event.clear()

                def as_dict(self) -> dict[str, object]:
                    return {}

            notification = TrackingAsyncNotification()
            queue = PersistentQueue(
                "test",
                store=MemoryQueueStore(),
                notification_policy=cast("Any", notification),
            )
            task = asyncio.create_task(_get_message_async(cast("Any", queue)))
            await asyncio.sleep(0)
            _ = queue.put("payload")
            message = await task

            self.assertEqual(message.value, "payload")
            self.assertGreaterEqual(notification.wait_calls, 1)
            self.assertGreaterEqual(notification.clear_calls, 1)

            heartbeat_queue = PersistentQueue("test", store=MemoryQueueStore())
            _ = heartbeat_queue.put("item")
            heartbeats: list[str] = []

            class AsyncHeartbeatQueue(PersistentQueue):
                def record_worker_heartbeat(self, worker_id: str) -> None:
                    heartbeats.append(worker_id)
                    super().record_worker_heartbeat(worker_id)

            async_heartbeat_queue = AsyncHeartbeatQueue(
                "test", store=MemoryQueueStore()
            )
            _ = async_heartbeat_queue.put("item")

            @persistent_async_worker(
                async_heartbeat_queue,
                store=MemoryAttemptStore(),
                max_tries=1,
                wait=lambda _: 0,
                worker_id="worker-a",
            )
            async def handle(value: str) -> str:
                return value.upper()

            self.assertEqual(await cast("Any", handle)(), "ITEM")
            self.assertEqual(heartbeats, ["worker-a", "worker-a"])

        asyncio.run(scenario())

    def test_effectively_once_worker_records_success_in_idempotency_store(self) -> None:
        store = MemoryIdempotencyStore()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=store,
                result_policy=ReturnStoredResult(),
            ),
        )
        message = queue.put("item", dedupe_key="job-1")

        @persistent_worker(queue)
        def handle(value: str) -> str:
            return value.upper()

        self.assertEqual(cast("Any", handle)(), "ITEM")
        record = store.load("job-1")

        assert record is not None
        self.assertEqual(record.status, "succeeded")
        self.assertIsNotNone(record.completed_at)
        self.assertEqual(record.result_key, "inline")
        self.assertEqual(
            record.metadata,
            {"queue": "test", "message_id": message.id, "result": "ITEM"},
        )
        self.assertTrue(queue.empty())

    def test_effectively_once_worker_records_failure_in_idempotency_store(self) -> None:
        store = MemoryIdempotencyStore()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(idempotency_store=store),
        )
        message = queue.put("item", dedupe_key="job-1")

        @persistent_worker(
            queue,
            config=PersistentWorkerConfig(dead_letter_on_failure=False, max_tries=1),
        )
        def handle(value: str) -> None:
            raise RuntimeError(f"boom: {value}")

        with self.assertRaisesRegex(RuntimeError, "boom: item"):
            cast("Any", handle)()

        record = store.load("job-1")
        assert record is not None
        self.assertEqual(record.status, "failed")
        self.assertIsNotNone(record.completed_at)
        self.assertEqual(record.metadata["queue"], "test")
        self.assertEqual(record.metadata["message_id"], message.id)
        self.assertEqual(record.metadata["last_error"]["type"], "RuntimeError")
        self.assertEqual(record.metadata["last_error"]["message"], "boom: item")
        self.assertIsNotNone(queue.inspect(message.id))

    def test_effectively_once_worker_short_circuits_known_success(self) -> None:
        store = MemoryIdempotencyStore()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(idempotency_store=store),
        )
        message = queue.put("item", dedupe_key="job-1")
        store.save(
            "job-1",
            IdempotencyRecord(
                status="succeeded",
                first_seen_at=100.0,
                completed_at=120.0,
                metadata={"queue": "test", "message_id": "previous"},
            ),
        )
        calls = {"count": 0}

        @persistent_worker(queue)
        def handle(value: str) -> str:
            calls["count"] += 1
            return value.upper()

        self.assertIsNone(cast("Any", handle)())
        self.assertEqual(calls["count"], 0)
        self.assertTrue(queue.empty())
        self.assertIsNone(queue.inspect(message.id))
        record = store.load("job-1")
        assert record is not None
        self.assertEqual(record.metadata["message_id"], "previous")

    def test_effectively_once_worker_returns_cached_result(self) -> None:
        store = MemoryIdempotencyStore()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=store,
                result_policy=ReturnStoredResult(),
            ),
        )
        message = queue.put("item", dedupe_key="job-1")
        store.save(
            "job-1",
            IdempotencyRecord(
                status="succeeded",
                first_seen_at=100.0,
                completed_at=120.0,
                result_key="inline",
                metadata={
                    "queue": "test",
                    "message_id": "previous",
                    "result": "CACHED",
                },
            ),
        )
        calls = {"count": 0}

        @persistent_worker(queue)
        def handle(value: str) -> str:
            calls["count"] += 1
            return value.upper()

        self.assertEqual(cast("Any", handle)(), "CACHED")
        self.assertEqual(calls["count"], 0)
        self.assertTrue(queue.empty())
        self.assertIsNone(queue.inspect(message.id))

    def test_effectively_once_worker_uses_external_result_store(self) -> None:
        store = MemoryIdempotencyStore()
        result_store = MemoryResultStore()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=store,
                result_policy=ReturnStoredResult(result_store=result_store),
            ),
        )
        message = queue.put("item", dedupe_key="job-1")

        @persistent_worker(queue)
        def handle(value: str) -> dict[str, str]:
            return {"status": value.upper()}

        self.assertEqual(cast("Any", handle)(), {"status": "ITEM"})
        record = store.load("job-1")

        assert record is not None
        self.assertEqual(record.result_key, "dedupe:test:job-1")
        self.assertEqual(record.metadata, {"queue": "test", "message_id": message.id})
        self.assertEqual(result_store.load("dedupe:test:job-1"), {"status": "ITEM"})

    def test_effectively_once_worker_returns_cached_result_from_external_store(
        self,
    ) -> None:
        store = MemoryIdempotencyStore()
        result_store = MemoryResultStore()
        queue = PersistentQueue(
            "test",
            store=MemoryQueueStore(),
            delivery_policy=EffectivelyOnceDelivery(
                idempotency_store=store,
                result_policy=ReturnStoredResult(result_store=result_store),
            ),
        )
        message = queue.put("item", dedupe_key="job-1")
        result_store.save("dedupe:test:job-1", {"status": "CACHED"})
        store.save(
            "job-1",
            IdempotencyRecord(
                status="succeeded",
                first_seen_at=100.0,
                completed_at=120.0,
                result_key="dedupe:test:job-1",
                metadata={"queue": "test", "message_id": "previous"},
            ),
        )
        calls = {"count": 0}

        @persistent_worker(queue)
        def handle(value: str) -> dict[str, str]:
            calls["count"] += 1
            return {"status": value.upper()}

        self.assertEqual(cast("Any", handle)(), {"status": "CACHED"})
        self.assertEqual(calls["count"], 0)
        self.assertTrue(queue.empty())
        self.assertIsNone(queue.inspect(message.id))

    def test_effectively_once_async_worker_short_circuits_known_success(self) -> None:
        async def scenario() -> None:
            store = MemoryIdempotencyStore()
            queue = PersistentQueue(
                "test",
                store=MemoryQueueStore(),
                delivery_policy=EffectivelyOnceDelivery(idempotency_store=store),
            )
            message = queue.put("item", dedupe_key="job-1")
            store.save(
                "job-1",
                IdempotencyRecord(
                    status="succeeded",
                    first_seen_at=100.0,
                    completed_at=120.0,
                    metadata={"queue": "test", "message_id": "previous"},
                ),
            )
            calls = {"count": 0}

            @persistent_async_worker(queue)
            async def handle(value: str) -> str:
                calls["count"] += 1
                return value.upper()

            self.assertIsNone(await cast("Any", handle)())
            self.assertEqual(calls["count"], 0)
            self.assertTrue(queue.empty())
            self.assertIsNone(queue.inspect(message.id))

        asyncio.run(scenario())

    def test_effectively_once_async_worker_returns_cached_result(self) -> None:
        async def scenario() -> None:
            store = MemoryIdempotencyStore()
            queue = PersistentQueue(
                "test",
                store=MemoryQueueStore(),
                delivery_policy=EffectivelyOnceDelivery(
                    idempotency_store=store,
                    result_policy=ReturnStoredResult(),
                ),
            )
            message = queue.put("item", dedupe_key="job-1")
            store.save(
                "job-1",
                IdempotencyRecord(
                    status="succeeded",
                    first_seen_at=100.0,
                    completed_at=120.0,
                    result_key="inline",
                    metadata={
                        "queue": "test",
                        "message_id": "previous",
                        "result": {"status": "cached"},
                    },
                ),
            )
            calls = {"count": 0}

            @persistent_async_worker(queue)
            async def handle(value: str) -> dict[str, str]:
                calls["count"] += 1
                return {"status": value.upper()}

            self.assertEqual(await cast("Any", handle)(), {"status": "cached"})
            self.assertEqual(calls["count"], 0)
            self.assertTrue(queue.empty())
            self.assertIsNone(queue.inspect(message.id))

        asyncio.run(scenario())

    def test_effectively_once_async_worker_uses_external_result_store(self) -> None:
        async def scenario() -> None:
            store = MemoryIdempotencyStore()
            result_store = MemoryResultStore()
            queue = PersistentQueue(
                "test",
                store=MemoryQueueStore(),
                delivery_policy=EffectivelyOnceDelivery(
                    idempotency_store=store,
                    result_policy=ReturnStoredResult(result_store=result_store),
                ),
            )
            message = queue.put("item", dedupe_key="job-1")

            @persistent_async_worker(queue)
            async def handle(value: str) -> list[str]:
                return [value.upper()]

            self.assertEqual(await cast("Any", handle)(), ["ITEM"])
            record = store.load("job-1")

            assert record is not None
            self.assertEqual(record.result_key, "dedupe:test:job-1")
            self.assertEqual(
                record.metadata, {"queue": "test", "message_id": message.id}
            )
            self.assertEqual(result_store.load("dedupe:test:job-1"), ["ITEM"])

        asyncio.run(scenario())

    def test_worker_result_key_returns_none_without_dedupe_key(self) -> None:
        self.assertIsNone(
            _result_key(QueueMessage(id="job-1", queue="test", value=None))
        )

    def test_memory_idempotency_store_round_trip_and_prune(self) -> None:
        store = MemoryIdempotencyStore()
        pending = IdempotencyRecord.pending()
        succeeded = IdempotencyRecord(
            status="succeeded",
            first_seen_at=100.0,
            completed_at=120.0,
            result_key="result-1",
            metadata={"kind": "email"},
        )

        store.save("pending", pending)
        store.save("done", succeeded)

        loaded = store.load("done")
        assert loaded is not None
        self.assertEqual(loaded.status, "succeeded")
        self.assertEqual(loaded.result_key, "result-1")
        self.assertEqual(loaded.metadata, {"kind": "email"})
        self.assertEqual(store.count_completed_older_than(older_than=50, now=200.0), 1)
        self.assertEqual(store.prune_completed(older_than=50, now=200.0), 1)
        store.delete("missing")
        store.delete("pending")
        self.assertIsNone(store.load("done"))
        self.assertIsNone(store.load("pending"))

    def test_memory_result_store_round_trip_and_delete(self) -> None:
        store = MemoryResultStore()
        payload = {"status": "ok", "items": [1, 2, 3]}

        store.save("job-1", payload)
        loaded = store.load("job-1")

        self.assertEqual(loaded, payload)
        assert loaded is not None
        loaded["items"].append(4)
        self.assertEqual(store.load("job-1"), payload)
        store.delete("job-1")
        store.delete("missing")
        self.assertIsNone(store.load("job-1"))

    def test_idempotency_store_locked_error_uses_resolved_path(self) -> None:
        error = IdempotencyStoreLockedError("./relative-idempotency")

        self.assertIn("relative-idempotency", str(error))
        self.assertTrue(error.path.endswith("relative-idempotency"))

    def test_result_store_locked_error_uses_resolved_path(self) -> None:
        error = ResultStoreLockedError("./relative-results")

        self.assertIn("relative-results", str(error))
        self.assertTrue(error.path.endswith("relative-results"))

    def test_sqlite_idempotency_store_round_trip_and_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "idempotency.sqlite3"
            store = SQLiteIdempotencyStore(path)
            succeeded = IdempotencyRecord(
                status="succeeded",
                first_seen_at=100.0,
                completed_at=120.0,
                result_key="result-1",
                metadata={"kind": "email"},
            )

            store.save("done", succeeded)
            loaded = store.load("done")
            assert loaded is not None
            self.assertEqual(loaded.metadata, {"kind": "email"})
            self.assertEqual(
                store.count_completed_older_than(older_than=50, now=200.0), 1
            )
            self.assertEqual(store.prune_completed(older_than=50, now=200.0), 1)
            self.assertIsNone(store.load("done"))
            store.close()

            connection = sqlite3.connect(path)
            version = connection.execute("PRAGMA user_version").fetchone()
            connection.close()
            assert version is not None
            self.assertEqual(int(version[0]), 1)

    def test_sqlite_result_store_round_trip_and_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "results.sqlite3"
            store = SQLiteResultStore(path)

            store.save("job-1", {"status": "ok"})
            self.assertEqual(store.load("job-1"), {"status": "ok"})
            store.delete("job-1")
            store.delete("missing")
            self.assertIsNone(store.load("job-1"))
            store.close()

            connection = sqlite3.connect(path)
            version = connection.execute("PRAGMA user_version").fetchone()
            connection.close()
            assert version is not None
            self.assertEqual(int(version[0]), 1)

    def test_sqlite_idempotency_store_handles_missing_keys_and_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "idempotency.sqlite3"
            store = SQLiteIdempotencyStore(path)

            self.assertIsNone(store.load("missing"))
            store.delete("missing")

            store.save("done", IdempotencyRecord.pending())
            store.delete("done")
            self.assertIsNone(store.load("done"))
            store.close()

    def test_sqlite_idempotency_store_migrates_legacy_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "legacy.sqlite3"
            connection = sqlite3.connect(path)
            connection.execute(
                "CREATE TABLE idempotency_records ("
                "key TEXT PRIMARY KEY, "
                "record_json TEXT NOT NULL"
                ")"
            )
            payload = json.dumps(
                {
                    "status": "succeeded",
                    "first_seen_at": 100.0,
                    "completed_at": 120.0,
                    "result_key": "result-1",
                    "metadata": {"kind": "email"},
                }
            )
            connection.execute(
                "INSERT INTO idempotency_records(key, record_json) VALUES(?, ?)",
                ("done", payload),
            )
            connection.execute("PRAGMA user_version = 0")
            connection.commit()
            connection.close()

            store = SQLiteIdempotencyStore(path)
            loaded = store.load("done")

            assert loaded is not None
            self.assertEqual(loaded.status, "succeeded")
            self.assertEqual(loaded.completed_at, 120.0)
            self.assertEqual(
                store.count_completed_older_than(older_than=50, now=200.0), 1
            )
            store.close()

    def test_sqlite_idempotency_store_rejects_newer_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "future.sqlite3"
            connection = sqlite3.connect(path)
            connection.execute("PRAGMA user_version = 999")
            connection.commit()
            connection.close()

            with self.assertRaisesRegex(
                ValueError, "unsupported SQLite idempotency schema version"
            ):
                _ = SQLiteIdempotencyStore(path)

    def test_sqlite_result_store_rejects_newer_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "future-results.sqlite3"
            connection = sqlite3.connect(path)
            connection.execute("PRAGMA user_version = 999")
            connection.commit()
            connection.close()

            with self.assertRaisesRegex(
                ValueError, "unsupported SQLite result schema version"
            ):
                _ = SQLiteResultStore(path)

    def test_sqlite_idempotency_store_closes_connection_on_init_failure(self) -> None:
        connection = mock.Mock()
        connection.execute.side_effect = sqlite3.OperationalError("boom")

        with mock.patch(
            "localqueue.idempotency.stores.sqlite.sqlite3.connect",
            return_value=connection,
        ):
            with self.assertRaises(sqlite3.OperationalError):
                _ = SQLiteIdempotencyStore("idempotency.sqlite3")

        connection.close.assert_called_once_with()

    def test_sqlite_result_store_closes_connection_on_init_failure(self) -> None:
        connection = mock.Mock()
        connection.execute.side_effect = sqlite3.OperationalError("boom")

        with mock.patch(
            "localqueue.results.stores.sqlite.sqlite3.connect",
            return_value=connection,
        ):
            with self.assertRaises(sqlite3.OperationalError):
                _ = SQLiteResultStore("results.sqlite3")

        connection.close.assert_called_once_with()

    def test_lmdb_idempotency_store_round_trip_and_prune(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBIdempotencyStore(tmpdir)
            succeeded = IdempotencyRecord(
                status="succeeded",
                first_seen_at=100.0,
                completed_at=120.0,
                result_key="result-1",
                metadata={"kind": "email"},
            )

            store.save("done", succeeded)
            loaded = store.load("done")
            assert loaded is not None
            self.assertEqual(loaded.result_key, "result-1")
            self.assertEqual(
                store.count_completed_older_than(older_than=50, now=200.0), 1
            )
            self.assertEqual(store.prune_completed(older_than=50, now=200.0), 1)
            self.assertIsNone(store.load("done"))

    def test_lmdb_result_store_round_trip_and_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBResultStore(tmpdir)

            store.save("job-1", {"status": "ok"})
            self.assertEqual(store.load("job-1"), {"status": "ok"})
            store.delete("job-1")
            store.delete("missing")
            self.assertIsNone(store.load("job-1"))

    def test_lmdb_idempotency_store_handles_missing_keys_and_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBIdempotencyStore(tmpdir)

            self.assertIsNone(store.load("missing"))
            store.delete("missing")

    def test_lmdb_idempotency_store_raises_locked_error(self) -> None:
        fake_lmdb = mock.Mock()
        fake_lmdb.LockError = lmdb.LockError
        fake_lmdb.open.side_effect = lmdb.LockError("locked")

        with mock.patch.object(
            _idempotency_lmdb, "import_lmdb", return_value=fake_lmdb
        ):
            with tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(IdempotencyStoreLockedError):
                    _ = LMDBIdempotencyStore(Path(tmpdir) / "locked")

    def test_lmdb_result_store_raises_locked_error(self) -> None:
        fake_lmdb = mock.Mock()
        fake_lmdb.LockError = lmdb.LockError
        fake_lmdb.open.side_effect = lmdb.LockError("locked")

        with mock.patch.object(_result_lmdb, "import_lmdb", return_value=fake_lmdb):
            with tempfile.TemporaryDirectory() as tmpdir:
                with self.assertRaises(ResultStoreLockedError):
                    _ = LMDBResultStore(Path(tmpdir) / "locked")

    def test_import_lmdb_reports_missing_optional_dependency(self) -> None:
        original_import = __import__

        def raising_import(
            name: str,
            globals: dict[str, object] | None = None,
            locals: dict[str, object] | None = None,
            fromlist: tuple[str, ...] = (),
            level: int = 0,
        ) -> object:
            if name == "lmdb":
                raise ModuleNotFoundError("No module named 'lmdb'")
            return original_import(name, globals, locals, fromlist, level)

        with mock.patch("builtins.__import__", side_effect=raising_import):
            with self.assertRaisesRegex(
                RuntimeError, "LMDB support requires the optional dependency"
            ):
                _idempotency_shared.import_lmdb()

    def test_result_store_import_lmdb_reports_missing_optional_dependency(self) -> None:
        original_import = __import__

        def raising_import(
            name: str,
            globals: dict[str, object] | None = None,
            locals: dict[str, object] | None = None,
            fromlist: tuple[str, ...] = (),
            level: int = 0,
        ) -> object:
            if name == "lmdb":
                raise ModuleNotFoundError("No module named 'lmdb'")
            return original_import(name, globals, locals, fromlist, level)

        with mock.patch("builtins.__import__", side_effect=raising_import):
            with self.assertRaisesRegex(
                RuntimeError, "LMDB support requires the optional dependency"
            ):
                _result_shared.import_lmdb()


if __name__ == "__main__":
    _ = unittest.main()
