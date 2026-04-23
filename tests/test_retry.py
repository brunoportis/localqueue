from __future__ import annotations

import asyncio
import importlib
import importlib.metadata
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any, cast
from unittest import mock

import lmdb
from tenacity import Retrying

import localqueue.retry.tenacity
from localqueue.retry import (
    AttemptStoreLockedError,
    LMDBAttemptStore,
    MemoryAttemptStore,
    PersistentAsyncRetrying,
    PersistentRetryExhausted,
    PersistentRetrying,
    RetryRecord,
    SQLiteAttemptStore,
    close_default_store,
    configure_default_store,
    configure_default_store_factory,
    idempotency_key_from_id,
    key_from_argument,
    key_from_attr,
    persistent_async_retry,
    persistent_retry,
)
from localqueue.retry.tenacity import PersistentCallContext, PersistentRetryState
from localqueue.failure import is_permanent_failure
from localqueue.retry.tenacity import _sqlite_default_store_factory
import localqueue


class AbortRun(Exception):
    pass


class CloseTrackingStore(MemoryAttemptStore):
    closed: bool

    def __init__(self) -> None:
        super().__init__()
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _raise_runtime_error(message: str = "fail") -> None:
    raise RuntimeError(message)


def _raise_runtime_error_from_arg(task_id: str) -> None:
    raise RuntimeError(task_id)


def _raise_value_error_from_arg(task_id: str) -> None:
    raise ValueError(task_id)


def json_dumps_record(record: RetryRecord) -> str:
    return json.dumps(
        {
            "attempts": record.attempts,
            "first_attempt_at": record.first_attempt_at,
            "exhausted": record.exhausted,
        },
        separators=(",", ":"),
    )


class RetryTests(unittest.TestCase):
    def test_is_permanent_failure_checks_exit_code_carriers(self) -> None:
        class ExitCodeError(Exception):
            def __init__(self, exit_code: int) -> None:
                self.exit_code = exit_code

        self.assertTrue(is_permanent_failure(ExitCodeError(127)))
        self.assertFalse(is_permanent_failure(ExitCodeError(126)))
        self.assertTrue(is_permanent_failure(ImportError("missing")))
        self.assertTrue(is_permanent_failure(ModuleNotFoundError("missing")))
        self.assertTrue(is_permanent_failure(NameError("missing")))

    def test_constructor_rejects_invalid_store_and_stop_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(ValueError):
                _ = PersistentRetrying(
                    store=MemoryAttemptStore(),
                    store_path=tmpdir,
                    max_tries=1,
                )

        with self.assertRaises(ValueError):
            _ = PersistentRetrying(
                store=MemoryAttemptStore(),
                max_tries=1,
                stop=lambda state: state.attempt_number >= 1,
            )

    def test_key_is_required_when_not_configured_explicitly(self) -> None:
        store = MemoryAttemptStore()
        calls = {"count": 0}

        @persistent_retry(store=store, max_tries=1, wait=lambda _: 0)  # pyright: ignore[reportUnknownLambdaType]
        def by_task_id(*, task_id: str) -> None:
            calls["count"] += 1
            raise RuntimeError(task_id)

        with self.assertRaises(ValueError):
            by_task_id(task_id="kw-task")

        self.assertIsNone(store.load("kw-task"))

        @persistent_retry(store=store, max_tries=1, wait=lambda _: 0)  # pyright: ignore[reportUnknownLambdaType]
        def by_first_arg(value: str) -> None:
            calls["count"] += 1
            raise RuntimeError(value)

        with self.assertRaises(ValueError):
            by_first_arg("first-arg")

        self.assertIsNone(store.load("first-arg"))

        @persistent_retry(store=store, max_tries=1, wait=lambda _: 0)  # pyright: ignore[reportUnknownLambdaType]
        def missing_key() -> None:
            raise RuntimeError("unreachable")

        with self.assertRaises(ValueError):
            missing_key()

        self.assertEqual(calls["count"], 0)

    def test_key_from_argument_factory(self) -> None:
        store = MemoryAttemptStore()

        @persistent_retry(
            store=store,
            max_tries=1,
            wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
            key_fn=key_from_argument("job_id"),
        )
        def by_job_id(*, job_id: str) -> None:
            raise RuntimeError(job_id)

        with self.assertRaises(RuntimeError):
            by_job_id(job_id="explicit-job")

        self.assertIsNotNone(store.load("explicit-job"))

        with self.assertRaises(ValueError):
            _ = key_from_argument("")

        key_fn = key_from_argument("missing")
        with self.assertRaises(ValueError):
            _ = key_fn(lambda value: value, ("value",), {})

    def test_key_from_attr_factory(self) -> None:
        store = MemoryAttemptStore()

        class Job:
            def __init__(self, job_id: str) -> None:
                self.id = job_id

        @persistent_retry(
            store=store,
            max_tries=1,
            wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
            key_fn=key_from_attr("job", "id", prefix="video"),
        )
        def by_job(job: Job) -> None:
            raise RuntimeError(job.id)

        with self.assertRaises(RuntimeError):
            by_job(Job("job-123"))

        self.assertIsNotNone(store.load("video:job-123"))

        with self.assertRaises(ValueError):
            _ = key_from_attr("", "id")

        with self.assertRaises(ValueError):
            _ = key_from_attr("job", "")

        with self.assertRaises(ValueError):
            _ = key_from_attr("job", "id", prefix="")

        missing_arg_key_fn = key_from_attr("missing", "id")
        with self.assertRaises(ValueError):
            _ = missing_arg_key_fn(lambda value: value, (Job("job-123"),), {})

        missing_attr_key_fn = key_from_attr("job", "missing")
        with self.assertRaises(ValueError):
            _ = missing_attr_key_fn(lambda job: job, (Job("job-123"),), {})

        plain_key_fn = key_from_attr("job", "id")
        self.assertEqual(
            plain_key_fn(lambda job: job, (Job("job-123"),), {}), "job-123"
        )

    def test_idempotency_key_from_id_factory(self) -> None:
        store = MemoryAttemptStore()

        class Task:
            def __init__(self, task_id: int) -> None:
                self.id = task_id

        @persistent_retry(
            store=store,
            max_tries=1,
            wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
            key_fn=idempotency_key_from_id("task", prefix="process-video"),
        )
        def by_task(*, task: Task) -> None:
            raise RuntimeError(str(task.id))

        with self.assertRaises(RuntimeError):
            by_task(task=Task(42))

        self.assertIsNotNone(store.load("process-video:42"))

    def test_default_store_can_be_configured(self) -> None:
        original = MemoryAttemptStore()
        replacement = MemoryAttemptStore()

        try:
            configure_default_store(original)
            retryer = PersistentRetrying(
                key="configured", max_tries=1, wait=lambda _: 0
            )

            with self.assertRaises(RuntimeError):
                _ = retryer(_raise_runtime_error)

            self.assertIsNotNone(original.load("configured"))
            self.assertIsNone(replacement.load("configured"))

            configure_default_store_factory(lambda: replacement)
            retryer = PersistentRetrying(key="factory", max_tries=1, wait=lambda _: 0)

            with self.assertRaises(RuntimeError):
                _ = retryer(_raise_runtime_error)

            self.assertIsNotNone(replacement.load("factory"))
        finally:
            configure_default_store_factory(_sqlite_default_store_factory)
            configure_default_store(None)

    def test_default_store_can_be_closed_explicitly(self) -> None:
        store = CloseTrackingStore()

        configure_default_store(store)
        try:
            retryer = PersistentRetrying(
                key="close-current", max_tries=1, wait=lambda _: 0
            )
            with self.assertRaises(RuntimeError):
                _ = retryer(_raise_runtime_error)

            close_default_store()

            self.assertTrue(store.closed)
        finally:
            configure_default_store_factory(_sqlite_default_store_factory)
            close_default_store(all_threads=True)

    def test_default_store_ignores_unhashable_owned_stores(self) -> None:
        class UnhashableStore(MemoryAttemptStore):
            __hash__ = None  # type: ignore[assignment]

        store = UnhashableStore()

        try:
            configure_default_store(store)
            retryer = PersistentRetrying(
                key="unhashable", max_tries=1, wait=lambda _: 0
            )
            with self.assertRaises(RuntimeError):
                _ = retryer(_raise_runtime_error)
            self.assertIsNotNone(store.load("unhashable"))
        finally:
            configure_default_store_factory(_sqlite_default_store_factory)
            close_default_store(all_threads=True)

    def test_get_default_store_ignores_unhashable_store_errors(self) -> None:
        store = CloseTrackingStore()
        with mock.patch(
            "localqueue.retry.tenacity._default_store_local", new=threading.local()
        ):
            with mock.patch(
                "localqueue.retry.tenacity._default_store_factory",
                return_value=store,
            ):
                with mock.patch.object(
                    localqueue.retry.tenacity._default_owned_stores,  # type: ignore[attr-defined]
                    "add",
                    side_effect=TypeError,
                ):
                    self.assertIs(localqueue.retry.tenacity._get_default_store(), store)

    def test_package_version_falls_back_to_unknown(self) -> None:
        with mock.patch(
            "importlib.metadata.version",
            side_effect=importlib.metadata.PackageNotFoundError,
        ):
            reloaded = importlib.reload(localqueue)
        self.assertEqual(reloaded.__version__, "unknown")

    def test_default_sqlite_store_factory_uses_xdg_data_home(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict("os.environ", {"XDG_DATA_HOME": tmpdir}, clear=False):
                store = cast("SQLiteAttemptStore", _sqlite_default_store_factory())
                expected_path = Path(tmpdir) / "localqueue" / "retries.sqlite3"
                try:
                    self.assertIsInstance(store, SQLiteAttemptStore)
                    self.assertEqual(store.path, expected_path)
                    self.assertTrue(expected_path.is_file())
                finally:
                    store.close()

    def test_sqlite_retry_store_handles_legacy_schema_without_first_attempt_field(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = f"{tmpdir}/retries.sqlite3"
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "CREATE TABLE retry_records (key TEXT PRIMARY KEY, record_json TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO retry_records(key, record_json) VALUES(?, ?)",
                    (
                        "legacy",
                        json.dumps(
                            {
                                "attempts": 2,
                                "first_attempt_at": 100.0,
                                "exhausted": True,
                            }
                        ),
                    ),
                )
                connection.commit()
            finally:
                connection.close()

            store = SQLiteAttemptStore(path)
            try:
                self.assertIsNotNone(store.load("legacy"))
            finally:
                store.close()

    def test_sqlite_retry_store_rejects_unsupported_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = f"{tmpdir}/retries.sqlite3"
            connection = sqlite3.connect(path)
            try:
                connection.execute("PRAGMA user_version = 99")
                connection.commit()
            finally:
                connection.close()

            with self.assertRaisesRegex(
                ValueError, "unsupported SQLite retry schema version"
            ):
                _ = SQLiteAttemptStore(path)

    def test_sqlite_retry_store_closes_connection_when_initialization_fails(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = f"{tmpdir}/retries.sqlite3"
            connection = mock.Mock()
            connection.execute.side_effect = [None, None, RuntimeError("boom")]
            connection.close = mock.Mock()
            with mock.patch(
                "localqueue.retry.store.sqlite3.connect", return_value=connection
            ):
                with self.assertRaises(RuntimeError):
                    _ = SQLiteAttemptStore(path)
            connection.close.assert_called_once()

    def test_default_store_can_close_factory_stores_across_threads(self) -> None:
        stores: list[CloseTrackingStore] = []
        stores_lock = threading.Lock()

        def factory() -> CloseTrackingStore:
            store = CloseTrackingStore()
            with stores_lock:
                stores.append(store)
            return store

        def run_retry(key: str) -> None:
            retryer = PersistentRetrying(key=key, max_tries=1, wait=lambda _: 0)
            with self.assertRaises(RuntimeError):
                _ = retryer(_raise_runtime_error)

        try:
            configure_default_store_factory(factory)
            run_retry("main-thread")
            thread = threading.Thread(target=run_retry, args=("worker-thread",))
            thread.start()
            thread.join(timeout=2.0)
            self.assertFalse(thread.is_alive())

            close_default_store(all_threads=True)

            self.assertEqual(len(stores), 2)
            self.assertTrue(all(store.closed for store in stores))
        finally:
            configure_default_store_factory(_sqlite_default_store_factory)
            close_default_store(all_threads=True)

    def test_attempts_resume_after_interrupted_run(self) -> None:
        store = MemoryAttemptStore()
        calls = {"count": 0}

        def fail(task_id: str) -> None:
            calls["count"] += 1
            raise ValueError(task_id)

        def aborting_sleep(_: float) -> None:
            raise AbortRun()

        retryer = PersistentRetrying(
            store=store,
            key="job-1",
            max_tries=3,
            wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
            sleep=aborting_sleep,
        )

        with self.assertRaises(AbortRun):
            _ = retryer(fail, "job-1")

        record = retryer.get_record("job-1")
        assert record is not None
        self.assertEqual(record.attempts, 1)
        self.assertFalse(record.exhausted)

        retryer = PersistentRetrying(
            store=store,
            key="job-1",
            max_tries=3,
            wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
        )

        with self.assertRaises(ValueError):
            _ = retryer(fail, "job-1")

        self.assertEqual(calls["count"], 3)
        record = retryer.get_record("job-1")
        assert record is not None
        self.assertEqual(record.attempts, 3)
        self.assertTrue(record.exhausted)

        with self.assertRaises(PersistentRetryExhausted):
            _ = retryer(fail, "job-1")

    def test_success_clears_retry_record(self) -> None:
        store = MemoryAttemptStore()
        calls = {"count": 0}

        @persistent_retry(
            store=store,
            max_tries=3,
            wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
            key_fn=key_from_argument("task_id"),
        )
        def flaky(task_id: str) -> str:
            calls["count"] += 1
            if calls["count"] < 2:
                raise ValueError("not yet")
            return "ok"

        self.assertEqual(flaky("job-2"), "ok")
        self.assertEqual(calls["count"], 2)
        self.assertIsNone(store.load("job-2"))

    def test_success_can_keep_record_and_reset_can_clear_it(self) -> None:
        store = MemoryAttemptStore()

        retryer = PersistentRetrying(
            store=store,
            key="job-keep",
            max_tries=2,
            clear_on_success=False,
            wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
        )

        calls = {"count": 0}

        def flaky() -> str:
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("not yet")
            return "ok"

        self.assertEqual(retryer(flaky), "ok")
        record = retryer.get_record("job-keep")
        assert record is not None
        self.assertEqual(record.attempts, 1)
        self.assertFalse(record.exhausted)

        retryer.reset("job-keep")
        self.assertIsNone(store.load("job-keep"))

    def test_callbacks_receive_persistent_retry_state(self) -> None:
        store = MemoryAttemptStore()
        seen: dict[str, Any] = {}

        def before(state: Any) -> None:
            seen["before_attempt"] = state.attempt_number
            seen["start_time"] = state.start_time

        def after(state: Any) -> None:
            seen["after_attempt"] = state.attempt_number
            seen["seconds"] = state.seconds_since_start

        def before_sleep(state: Any) -> None:
            seen["before_sleep_attempt"] = state.attempt_number

        retryer = PersistentRetrying(
            store=store,
            key="job-callback",
            max_tries=2,
            wait=lambda state: state.attempt_number * 0,
            before=before,
            after=after,
            before_sleep=before_sleep,
        )

        calls = {"count": 0}

        def flaky() -> str:
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("try again")
            return "ok"

        self.assertEqual(retryer(flaky), "ok")
        self.assertEqual(seen["before_attempt"], 2)
        self.assertEqual(seen["after_attempt"], 1)
        self.assertEqual(seen["before_sleep_attempt"], 1)
        self.assertIsInstance(seen["start_time"], float)
        self.assertIsInstance(seen["seconds"], float)

    def test_async_retrying_persists_attempts(self) -> None:
        async def scenario() -> None:
            store = MemoryAttemptStore()
            calls = {"count": 0}

            async def fail(task_id: str) -> None:
                await asyncio.sleep(0)
                calls["count"] += 1
                raise ValueError(task_id)

            async def aborting_sleep(_: float) -> None:
                await asyncio.sleep(0)
                raise AbortRun()

            retryer = PersistentAsyncRetrying(
                store=store,
                key="job-3",
                max_tries=2,
                wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
                sleep=aborting_sleep,
            )

            with self.assertRaises(AbortRun):
                await retryer(fail, "job-3")

            retryer = PersistentAsyncRetrying(
                store=store,
                key="job-3",
                max_tries=2,
                wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
            )

            with self.assertRaises(ValueError):
                await retryer(fail, "job-3")

            self.assertEqual(calls["count"], 2)
            record = store.load("job-3")
            assert record is not None
            self.assertTrue(record.exhausted)

        asyncio.run(scenario())

    def test_async_retrying_supports_decorator_and_async_callbacks(self) -> None:
        async def scenario() -> None:
            store = MemoryAttemptStore()
            seen: dict[str, Any] = {}

            async def retry(state: Any) -> bool:
                await asyncio.sleep(0)
                seen["retry_attempt"] = state.attempt_number
                return True

            async def wait(state: Any) -> float:
                await asyncio.sleep(0)
                seen["wait_attempt"] = state.attempt_number
                return 0.0

            async def stop(state: Any) -> bool:
                await asyncio.sleep(0)
                seen["stop_attempt"] = state.attempt_number
                return state.attempt_number >= 2

            async def before(state: Any) -> None:
                await asyncio.sleep(0)
                seen["before_attempt"] = state.attempt_number

            async def after(state: Any) -> None:
                await asyncio.sleep(0)
                seen["after_attempt"] = state.attempt_number

            async def before_sleep(state: Any) -> None:
                await asyncio.sleep(0)
                seen["before_sleep_attempt"] = state.attempt_number

            @persistent_async_retry(
                store=store,
                key="async-decorator",
                retry=retry,
                wait=wait,
                stop=stop,
                before=before,
                after=after,
                before_sleep=before_sleep,
            )
            async def fail() -> None:
                await asyncio.sleep(0)
                raise RuntimeError("async")

            with self.assertRaises(RuntimeError):
                await fail()

            self.assertEqual(seen["retry_attempt"], 2)
            self.assertEqual(seen["wait_attempt"], 2)
            self.assertEqual(seen["stop_attempt"], 2)
            self.assertEqual(seen["before_attempt"], 2)
            self.assertEqual(seen["after_attempt"], 2)
            self.assertEqual(seen["before_sleep_attempt"], 1)

        asyncio.run(scenario())

    def test_retry_error_callback_marks_record_exhausted(self) -> None:
        store = MemoryAttemptStore()

        def on_error(state: Any) -> str:
            return f"fallback-{state.attempt_number}"

        retryer = PersistentRetrying(
            store=store,
            key="job-fallback",
            max_tries=1,
            wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
            retry_error_callback=on_error,
        )

        self.assertEqual(
            retryer(_raise_runtime_error),
            "fallback-1",
        )

        record = store.load("job-fallback")
        assert record is not None
        self.assertTrue(record.exhausted)

    def test_async_retry_error_callback_marks_record_exhausted(self) -> None:
        async def scenario() -> None:
            store = MemoryAttemptStore()

            async def on_error(state: Any) -> str:
                await asyncio.sleep(0)
                return f"fallback-{state.attempt_number}"

            retryer = PersistentAsyncRetrying(
                store=store,
                key="async-fallback",
                max_tries=1,
                wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
                retry_error_callback=on_error,
            )

            async def fail() -> None:
                await asyncio.sleep(0)
                raise RuntimeError("fail")

            self.assertEqual(await retryer(fail), "fallback-1")

            record = store.load("async-fallback")
            assert record is not None
            self.assertTrue(record.exhausted)

        asyncio.run(scenario())

    def test_async_retrying_persists_without_blocking_event_loop(self) -> None:
        class SlowMemoryStore(MemoryAttemptStore):
            def load(self, key: str) -> RetryRecord | None:
                time.sleep(0.2)
                return super().load(key)

            def save(self, key: str, record: RetryRecord) -> None:
                time.sleep(0.2)
                super().save(key, record)

        async def scenario() -> None:
            store = SlowMemoryStore()
            marker = asyncio.Event()

            async def tick() -> None:
                await asyncio.sleep(0)
                marker.set()

            retryer = PersistentAsyncRetrying(
                store=store,
                key="async-slow",
                max_tries=1,
                wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
            )

            async def fail() -> None:
                raise RuntimeError("fail")

            task = asyncio.create_task(retryer(fail))
            marker_task = asyncio.create_task(tick())
            await asyncio.sleep(0.05)
            self.assertTrue(marker.is_set())
            self.assertFalse(task.done())
            await marker_task
            with self.assertRaises(RuntimeError):
                await task

        asyncio.run(scenario())

    def test_lmdb_store_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBAttemptStore(tmpdir)
            retryer = PersistentRetrying(
                store=store,
                key="job-4",
                max_tries=1,
                wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
            )

            with self.assertRaises(ValueError):
                _ = retryer(
                    _raise_value_error_from_arg,
                    "job-4",  # pyright: ignore[reportUnknownArgumentType, reportUnknownLambdaType]
                )

            record = store.load("job-4")
            assert record is not None
            self.assertEqual(record.attempts, 1)
            self.assertTrue(record.exhausted)

            store.delete("job-4")
            self.assertIsNone(store.load("job-4"))

    def test_sqlite_store_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteAttemptStore(f"{tmpdir}/retries.sqlite3")
            store.save(
                "job-sqlite",
                RetryRecord(attempts=2, first_attempt_at=123.0, exhausted=True),
            )

            reopened = SQLiteAttemptStore(f"{tmpdir}/retries.sqlite3")
            record = reopened.load("job-sqlite")
            assert record is not None
            self.assertEqual(record.attempts, 2)
            self.assertTrue(record.exhausted)

            reopened.delete("job-sqlite")
            self.assertIsNone(store.load("job-sqlite"))
            store.close()
            reopened.close()

    def test_memory_store_returns_record_copies(self) -> None:
        store = MemoryAttemptStore()
        store.save("job-copy", RetryRecord(attempts=1))

        loaded = store.load("job-copy")
        assert loaded is not None
        loaded.attempts = 99

        loaded_again = store.load("job-copy")
        assert loaded_again is not None
        self.assertEqual(loaded_again.attempts, 1)

    def test_memory_store_prunes_and_counts_exhausted_records(self) -> None:
        store = MemoryAttemptStore()
        store.save(
            "old", RetryRecord(attempts=3, first_attempt_at=100.0, exhausted=True)
        )
        store.save(
            "new", RetryRecord(attempts=1, first_attempt_at=190.0, exhausted=True)
        )
        store.save(
            "active",
            RetryRecord(attempts=1, first_attempt_at=100.0, exhausted=False),
        )

        self.assertEqual(store.count_exhausted_older_than(older_than=50, now=200.0), 1)
        self.assertEqual(store.prune_exhausted(older_than=50, now=200.0), 1)
        self.assertIsNone(store.load("old"))
        self.assertIsNotNone(store.load("new"))
        self.assertIsNotNone(store.load("active"))
        store.delete("new")
        self.assertIsNone(store.load("new"))

    def test_sqlite_store_prunes_and_counts_exhausted_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SQLiteAttemptStore(f"{tmpdir}/retries.sqlite3")
            store.save(
                "old", RetryRecord(attempts=3, first_attempt_at=100.0, exhausted=True)
            )
            store.save(
                "new", RetryRecord(attempts=1, first_attempt_at=190.0, exhausted=True)
            )
            store.save(
                "active",
                RetryRecord(attempts=1, first_attempt_at=100.0, exhausted=False),
            )

            self.assertEqual(
                store.count_exhausted_older_than(older_than=50, now=200.0), 1
            )
            self.assertEqual(store.prune_exhausted(older_than=50, now=200.0), 1)
            self.assertIsNone(store.load("old"))
            self.assertIsNotNone(store.load("new"))
            self.assertIsNotNone(store.load("active"))
            store.delete("new")
            self.assertIsNone(store.load("new"))
            store.close()

    def test_sqlite_store_sets_schema_version_and_indexes_retention(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = f"{tmpdir}/retries.sqlite3"
            store = SQLiteAttemptStore(path)
            store.close()

            connection = sqlite3.connect(path)
            try:
                version_row = connection.execute("PRAGMA user_version").fetchone()
                columns = {
                    str(row[1])
                    for row in connection.execute("PRAGMA table_info(retry_records)")
                }
                indexes = {
                    str(row[1])
                    for row in connection.execute("PRAGMA index_list(retry_records)")
                }
            finally:
                connection.close()

            assert version_row is not None
            self.assertEqual(int(version_row[0]), 1)
            self.assertLessEqual({"first_attempt_at", "exhausted"}, columns)
            self.assertIn("retry_records_exhausted_idx", indexes)

    def test_sqlite_store_migrates_legacy_records_for_retention(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = f"{tmpdir}/retries.sqlite3"
            connection = sqlite3.connect(path)
            legacy = RetryRecord(attempts=3, first_attempt_at=100.0, exhausted=True)
            active = RetryRecord(attempts=1, first_attempt_at=100.0, exhausted=False)
            connection.execute(
                "CREATE TABLE retry_records (key TEXT PRIMARY KEY, record_json TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO retry_records(key, record_json) VALUES(?, ?)",
                ("old", json_dumps_record(legacy)),
            )
            connection.execute(
                "INSERT INTO retry_records(key, record_json) VALUES(?, ?)",
                ("active", json_dumps_record(active)),
            )
            connection.commit()
            connection.close()

            store = SQLiteAttemptStore(path)
            try:
                self.assertEqual(
                    store.count_exhausted_older_than(older_than=50, now=200.0),
                    1,
                )
                self.assertEqual(store.prune_exhausted(older_than=50, now=200.0), 1)
                self.assertIsNone(store.load("old"))
                self.assertIsNotNone(store.load("active"))
            finally:
                store.close()

    def test_lmdb_store_prunes_and_counts_exhausted_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = LMDBAttemptStore(tmpdir)
            store.save(
                "old", RetryRecord(attempts=3, first_attempt_at=100.0, exhausted=True)
            )
            store.save(
                "new", RetryRecord(attempts=1, first_attempt_at=190.0, exhausted=True)
            )

            self.assertEqual(
                store.count_exhausted_older_than(older_than=50, now=200.0), 1
            )
            self.assertEqual(store.prune_exhausted(older_than=50, now=200.0), 1)
            self.assertIsNone(store.load("old"))
            self.assertIsNotNone(store.load("new"))
            store.delete("new")
            self.assertIsNone(store.load("new"))

    def test_store_path_is_opened_lazily(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = f"{tmpdir}/retries.sqlite3"
            with mock.patch(
                "localqueue.retry.tenacity.SQLiteAttemptStore"
            ) as store_cls:
                retryer = PersistentRetrying(
                    store_path=path,
                    key="job-lazy",
                    max_tries=1,
                    wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
                )
                store_cls.assert_not_called()

                fake_store = mock.Mock()
                fake_store.load.return_value = None
                store_cls.return_value = fake_store

                with self.assertRaises(RuntimeError):
                    _ = retryer(
                        _raise_runtime_error_from_arg,
                        "job-lazy",
                    )

                store_cls.assert_called_once()
                fake_store.save.assert_called_once()

    def test_store_path_uses_sqlite_attempt_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = f"{tmpdir}/retries.sqlite3"
            retryer = PersistentRetrying(
                store_path=path,
                key="job-sqlite-path",
                max_tries=1,
                wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType]
            )

            with self.assertRaises(RuntimeError):
                _ = retryer(
                    _raise_runtime_error_from_arg,
                    "job-sqlite-path",
                )

            store = SQLiteAttemptStore(path)
            try:
                self.assertIsNotNone(store.load("job-sqlite-path"))
            finally:
                store.close()

    def test_build_effective_tenacity_kwargs_rejects_unknown_arguments(self) -> None:
        with self.assertRaisesRegex(TypeError, "unexpected Tenacity arguments"):
            _ = localqueue.retry.tenacity._build_effective_tenacity_kwargs(
                Retrying, {"unknown": 1}
            )

    def test_build_effective_tenacity_kwargs_keeps_kwargs_for_var_keyword_bases(
        self,
    ) -> None:
        class BaseWithVarKeyword:
            def __init__(self, *, known: int = 1, **kwargs: Any) -> None:
                self.known = known
                self.kwargs = kwargs

        effective = localqueue.retry.tenacity._build_effective_tenacity_kwargs(
            BaseWithVarKeyword, {"unknown": 1}
        )
        self.assertEqual(effective["unknown"], 1)

    def test_persistent_retry_state_reports_seconds_since_start(self) -> None:
        retry_state = mock.Mock()
        retry_state.attempt_number = 1
        retry_state.outcome_timestamp = None
        record = RetryRecord(
            attempts=1, first_attempt_at=time.time() - 5, exhausted=False
        )
        state = PersistentRetryState(retry_state, record, starting_attempts=0)

        self.assertIsNone(state.seconds_since_start)

        retry_state.outcome_timestamp = time.time()
        self.assertGreaterEqual(state.seconds_since_start or 0.0, 0.0)

    def test_persistent_retrying_requires_context_before_translation(self) -> None:
        retryer = PersistentRetrying(
            store=MemoryAttemptStore(), key="ctx", max_tries=1, wait=lambda _: 0
        )
        with self.assertRaises(RuntimeError):
            _ = retryer._current_context()

    def test_key_from_attr_without_prefix_returns_raw_key(self) -> None:
        class Job:
            def __init__(self, job_id: str) -> None:
                self.id = job_id

        key_fn = key_from_attr("job", "id")
        self.assertEqual(key_fn(lambda job: job, (Job("job-123"),), {}), "job-123")

    def test_persistent_retrying_wait_none_returns_zero(self) -> None:
        retryer = PersistentRetrying(
            store=MemoryAttemptStore(), key="job", max_tries=1, wait=None
        )
        wait = retryer._wrap_wait()
        self.assertEqual(wait(mock.Mock(outcome_timestamp=time.time())), 0.0)

    def test_persistent_async_retrying_wait_none_returns_zero(self) -> None:
        retryer = PersistentAsyncRetrying(
            store=MemoryAttemptStore(), key="job", max_tries=1, wait=None
        )
        wait = retryer._wrap_wait()

        async def run() -> float:
            return await wait(mock.Mock(outcome_timestamp=time.time()))

        self.assertEqual(asyncio.run(run()), 0.0)

    def test_persistent_retrying_raises_exhausted_error_for_exhausted_record(
        self,
    ) -> None:
        store = MemoryAttemptStore()
        store.save(
            "job", RetryRecord(attempts=3, first_attempt_at=100.0, exhausted=True)
        )
        retryer = PersistentRetrying(
            store=store, key="job", max_tries=1, wait=lambda _: 0
        )

        with self.assertRaises(PersistentRetryExhausted) as exc_info:
            _ = retryer(lambda: None)

        self.assertEqual(exc_info.exception.key, "job")
        self.assertEqual(exc_info.exception.attempts, 3)

    def test_persistent_retrying_set_exception_raises_when_exc_info_is_missing(
        self,
    ) -> None:
        retryer = PersistentRetrying(
            store=MemoryAttemptStore(), key="job", max_tries=1, wait=lambda _: 0
        )

        with mock.patch(
            "localqueue.retry.tenacity.sys.exc_info", return_value=(None, None, None)
        ):
            with self.assertRaises(RuntimeError):
                _ = retryer(lambda: _raise_runtime_error("boom"))

    def test_persistent_retrying_drops_context_after_completion(self) -> None:
        store = MemoryAttemptStore()
        retryer = PersistentRetrying(
            store=store, key="job", max_tries=1, wait=lambda _: 0
        )

        def succeed() -> str:
            return "ok"

        self.assertEqual(retryer(succeed), "ok")
        self.assertFalse(hasattr(retryer._local, "persistent_context"))

    def test_persistent_async_retrying_set_exception_raises_when_exc_info_is_missing(
        self,
    ) -> None:
        retryer = PersistentAsyncRetrying(
            store=MemoryAttemptStore(), key="job", max_tries=1, wait=lambda _: 0
        )

        async def fail() -> None:
            await asyncio.sleep(0)
            raise RuntimeError("boom")

        with mock.patch(
            "localqueue.retry.tenacity.sys.exc_info", return_value=(None, None, None)
        ):
            with self.assertRaises(RuntimeError):
                _ = asyncio.run(retryer(fail))

    def test_persistent_async_retrying_drops_context_after_completion(self) -> None:
        retryer = PersistentAsyncRetrying(
            store=MemoryAttemptStore(), key="job", max_tries=1, wait=lambda _: 0
        )

        async def succeed() -> str:
            await asyncio.sleep(0)
            return "ok"

        self.assertEqual(asyncio.run(retryer(succeed)), "ok")
        self.assertFalse(hasattr(retryer._local, "persistent_context"))

    def test_persistent_retry_error_callback_persists_attempts_before_returning(
        self,
    ) -> None:
        store = MemoryAttemptStore()
        retryer = PersistentRetrying(
            store=store,
            key="job",
            max_tries=1,
            wait=lambda _: 0,
            retry_error_callback=lambda state: "fallback",
        )
        context = PersistentCallContext(
            key="job",
            record=RetryRecord(attempts=1, first_attempt_at=100.0, exhausted=False),
            starting_attempts=1,
        )
        retryer._local.persistent_context = context
        try:
            retry_state = mock.Mock()
            retry_state.attempt_number = 1
            retry_state.outcome = mock.Mock(failed=True)
            callback = retryer._wrap_retry_error_callback()
            assert callback is not None
            result = callback(retry_state)
        finally:
            del retryer._local.persistent_context

        self.assertEqual(result, "fallback")
        record = store.load("job")
        assert record is not None
        self.assertTrue(record.exhausted)

    def test_persistent_async_retry_error_callback_persists_attempts_before_returning(
        self,
    ) -> None:
        async def scenario() -> None:
            await asyncio.sleep(0)
            store = MemoryAttemptStore()

            async def on_error(state: Any) -> str:
                await asyncio.sleep(0)
                return "fallback"

            retryer = PersistentAsyncRetrying(
                store=store,
                key="job",
                max_tries=1,
                wait=lambda _: 0,
                retry_error_callback=on_error,
            )
            context = PersistentCallContext(
                key="job",
                record=RetryRecord(attempts=1, first_attempt_at=100.0, exhausted=False),
                starting_attempts=1,
            )
            retryer._local.persistent_context = context
            try:
                retry_state = mock.Mock()
                retry_state.attempt_number = 1
                retry_state.outcome = mock.Mock(failed=True)
                callback = retryer._wrap_retry_error_callback()
                assert callback is not None
                result = await callback(retry_state)
            finally:
                del retryer._local.persistent_context

            self.assertEqual(result, "fallback")
            record = store.load("job")
            assert record is not None
            self.assertTrue(record.exhausted)

        asyncio.run(scenario())

    def test_persistent_async_retry_error_callback_supports_sync_callback(self) -> None:
        store = MemoryAttemptStore()

        def on_error(state: Any) -> str:
            return f"fallback-{state.attempt_number}"

        retryer = PersistentAsyncRetrying(
            store=store,
            key="job",
            max_tries=1,
            wait=lambda _: 0,
            retry_error_callback=on_error,
        )
        context = PersistentCallContext(
            key="job",
            record=RetryRecord(attempts=1, first_attempt_at=100.0, exhausted=False),
            starting_attempts=1,
        )
        retryer._local.persistent_context = context
        try:
            retry_state = mock.Mock()
            retry_state.attempt_number = 1
            retry_state.outcome = mock.Mock(failed=True)
            callback = retryer._wrap_retry_error_callback()
            assert callback is not None
            result = callback(retry_state)
        finally:
            del retryer._local.persistent_context

        self.assertEqual(result, "fallback-2")
        record = store.load("job")
        assert record is not None
        self.assertTrue(record.exhausted)

    def test_persistent_async_retrying_raises_exhausted_error_for_exhausted_record(
        self,
    ) -> None:
        async def scenario() -> None:
            store = MemoryAttemptStore()
            store.save(
                "job", RetryRecord(attempts=3, first_attempt_at=100.0, exhausted=True)
            )
            retryer = PersistentAsyncRetrying(
                store=store, key="job", max_tries=1, wait=lambda _: 0
            )

            with self.assertRaises(PersistentRetryExhausted) as exc_info:
                _ = await retryer(lambda: None)

            self.assertEqual(exc_info.exception.key, "job")
            self.assertEqual(exc_info.exception.attempts, 3)

        asyncio.run(scenario())

    def test_persistent_async_retrying_set_exception_raises_when_exc_info_is_missing_sync(
        self,
    ) -> None:
        async def scenario() -> None:
            retryer = PersistentAsyncRetrying(
                store=MemoryAttemptStore(), key="job", max_tries=1, wait=lambda _: 0
            )

            with mock.patch(
                "localqueue.retry.tenacity.sys.exc_info",
                return_value=(None, None, None),
            ):
                with self.assertRaises(RuntimeError):
                    _ = await retryer(lambda: _raise_runtime_error("boom"))

        asyncio.run(scenario())

    def test_retry_with_max_tries_overrides_existing_stop(self) -> None:
        store = MemoryAttemptStore()

        @persistent_retry(
            store=store,
            stop=lambda state: state.attempt_number >= 5,
            wait=lambda _: 0,  # pyright: ignore[reportUnknownLambdaType, reportUnknownMemberType]
            key_fn=key_from_argument("task_id"),
        )
        def fail(task_id: str) -> None:
            raise RuntimeError(task_id)

        with self.assertRaises(RuntimeError):
            _ = cast("Any", fail).retry_with(max_tries=1)("job-5")

        record = store.load("job-5")
        assert record is not None
        self.assertEqual(record.attempts, 1)
        self.assertTrue(record.exhausted)

    def test_lmdb_lock_error_is_reworded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_lmdb = mock.Mock()
            fake_lmdb.LockError = lmdb.LockError
            fake_lmdb.open.side_effect = lmdb.LockError("busy")
            with mock.patch(
                "localqueue.retry.store._import_lmdb", return_value=fake_lmdb
            ):
                with self.assertRaises(AttemptStoreLockedError) as exc_info:
                    _ = LMDBAttemptStore(tmpdir)

        self.assertIn("locked by another process", str(exc_info.exception))

    def test_lmdb_store_requires_optional_dependency(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict("sys.modules", {"lmdb": None}):
                with self.assertRaisesRegex(RuntimeError, "localqueue\\[lmdb\\]"):
                    _ = LMDBAttemptStore(tmpdir)


if __name__ == "__main__":
    _ = unittest.main()
