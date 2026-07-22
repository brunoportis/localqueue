from __future__ import annotations

import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, fields
from importlib import metadata
from pathlib import Path

import pytest
from localqueue import LocalQueueError, QueueDiagnostics, SimpleQueue


def database_rows(path: Path) -> list[tuple[object, ...]]:
    with sqlite3.connect(path) as connection:
        return connection.execute(
            "SELECT id, status, receipt, updated_at, lease_until, attempts "
            "FROM messages ORDER BY id"
        ).fetchall()


def test_empty_queue_returns_versioned_immutable_json_report(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path / "empty"))

    report = queue.diagnostics()

    assert isinstance(report, QueueDiagnostics)
    assert report.schema_version == 1
    assert report.package_version
    assert report.package_version != "unknown"
    assert report.sqlite_version
    assert report.queue_name == "default"
    assert report.serializer_identity == "localqueue.JsonSerializer"
    assert report.lease_seconds == 60.0
    assert report.max_retries == 3
    assert report.journal_mode == "wal"
    assert report.synchronous == 1
    assert report.durability_mode == "normal"
    assert report.busy_timeout_ms == 5000
    assert report.database_size_bytes is not None
    assert report.database_size_bytes > 0
    assert report.page_count > 0
    assert report.page_size > 0
    assert report.freelist_count >= 0
    assert (report.ready, report.processing, report.acked, report.failed) == (
        0,
        0,
        0,
        0,
    )
    assert report.oldest_available_age_seconds is None
    assert report.oldest_processing_updated_age_seconds is None
    assert report.active_leases == 0
    assert report.expired_leases == 0
    assert report.oldest_expired_lease_age_seconds is None
    assert report.observed_at > 0
    assert json.loads(json.dumps(report.to_dict())) == report.to_dict()

    with pytest.raises(FrozenInstanceError):
        report.ready = 1  # type: ignore[misc]

    queue.close()


def test_to_dict_matches_the_public_dataclass_schema(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path))
    report = queue.diagnostics()
    serialized = report.to_dict()

    public_fields = {field.name for field in fields(QueueDiagnostics)}

    assert set(serialized) == public_fields
    for field in fields(QueueDiagnostics):
        assert serialized[field.name] == getattr(report, field.name)
    json.dumps(serialized)
    queue.close()


@pytest.mark.parametrize(
    ("fsync", "synchronous", "durability_mode"),
    [(False, 1, "normal"), (True, 2, "full")],
)
def test_effective_durability_comes_from_native_connection(
    tmp_path: Path,
    fsync: bool,
    synchronous: int,
    durability_mode: str,
) -> None:
    queue = SimpleQueue(str(tmp_path / str(fsync)), fsync=fsync)
    database = tmp_path / str(fsync) / "localqueue.db"

    with sqlite3.connect(database) as external:
        external.execute("PRAGMA synchronous=OFF")
        external.execute("PRAGMA busy_timeout=17")
        assert external.execute("PRAGMA synchronous").fetchone() == (0,)
        assert external.execute("PRAGMA busy_timeout").fetchone() == (17,)

    report = queue.diagnostics()

    assert report.synchronous == synchronous
    assert report.durability_mode == durability_mode
    assert report.busy_timeout_ms == 5000
    queue.close()


def test_custom_serializer_identity_is_deterministic(tmp_path: Path) -> None:
    class BytesSerializer:
        def dumps(self, obj: object) -> bytes:
            return str(obj).encode()

        def loads(self, data: bytes) -> str:
            return data.decode()

    serializer = BytesSerializer()
    queue = SimpleQueue(str(tmp_path), serializer=serializer)

    identity = queue.diagnostics().serializer_identity

    assert identity == f"{BytesSerializer.__module__}.{BytesSerializer.__qualname__}"
    assert "0x" not in identity
    assert repr(serializer) not in identity
    queue.close()


def test_closed_queue_uses_existing_error_contract(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path))
    queue.close()

    with pytest.raises(LocalQueueError, match="^queue is closed$"):
        queue.diagnostics()


def test_populated_queue_reports_each_state_and_defensible_ages(
    tmp_path: Path,
) -> None:
    queue = SimpleQueue(str(tmp_path), lease_seconds=30)
    database = tmp_path / "localqueue.db"

    queue.put({"state": "acked"})
    queue.ack(queue.get_nowait())
    queue.put({"state": "failed"})
    queue.fail(queue.get_nowait(), "permanent")
    queue.put({"state": "processing"})
    processing = queue.get_nowait()
    ready_id = queue.put({"state": "ready"})

    now_ms = int(time.time() * 1000)
    with sqlite3.connect(database) as external:
        external.execute(
            "UPDATE messages SET available_at = ? WHERE id = ?",
            (now_ms - 2_000, ready_id),
        )
        external.execute(
            "UPDATE messages SET updated_at = ? WHERE id = ?",
            (now_ms - 3_000, processing.id),
        )

    report = queue.diagnostics()

    assert (report.ready, report.processing, report.acked, report.failed) == (
        1,
        1,
        1,
        1,
    )
    assert report.oldest_available_age_seconds is not None
    assert 1.5 <= report.oldest_available_age_seconds < 10
    assert report.oldest_processing_updated_age_seconds is not None
    assert 2.5 <= report.oldest_processing_updated_age_seconds < 10
    assert report.active_leases == 1
    assert report.expired_leases == 0
    assert report.oldest_expired_lease_age_seconds is None
    queue.close()


def test_reports_only_the_selected_logical_queue(tmp_path: Path) -> None:
    alpha = SimpleQueue(str(tmp_path), name="alpha")
    beta = SimpleQueue(str(tmp_path), name="beta")
    alpha.put({"queue": "alpha"})
    alpha.put({"queue": "alpha"})
    beta.put({"queue": "beta"})

    alpha_report = alpha.diagnostics()
    beta_report = beta.diagnostics()

    assert alpha_report.queue_name == "alpha"
    assert (alpha_report.ready, alpha_report.processing) == (2, 0)
    assert beta_report.queue_name == "beta"
    assert (beta_report.ready, beta_report.processing) == (1, 0)
    assert alpha_report.database_size_bytes == beta_report.database_size_bytes
    assert alpha_report.page_count == beta_report.page_count
    assert alpha_report.page_size == beta_report.page_size
    alpha.close()
    beta.close()


def test_delayed_ready_job_is_not_reported_as_already_available(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path))
    queue.put({"delayed": True})
    job = queue.get_nowait()
    queue.nack(job, delay=60)

    report = queue.diagnostics()

    assert report.ready == 1
    assert report.oldest_available_age_seconds is None
    queue.close()


def test_active_lease_is_reported_without_changing_it(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), lease_seconds=30)
    queue.put({"lease": "active"})
    job = queue.get_nowait()

    report = queue.diagnostics()

    assert report.processing == 1
    assert report.lease_seconds == 30.0
    assert isinstance(report.lease_seconds, float)
    assert report.active_leases == 1
    assert report.expired_leases == 0
    assert report.oldest_processing_updated_age_seconds is not None
    assert report.oldest_processing_updated_age_seconds >= 0
    queue.ack(job)
    queue.close()


def test_expired_lease_is_observed_but_not_reclaimed(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), lease_seconds=30)
    database = tmp_path / "localqueue.db"
    queue.put({"lease": "expired"})
    job = queue.get_nowait()
    now_ms = int(time.time() * 1000)
    with sqlite3.connect(database) as external:
        external.execute(
            "UPDATE messages SET lease_until = ?, updated_at = ? WHERE id = ?",
            (now_ms - 1_000, now_ms - 2_000, job.id),
        )
    before = database_rows(database)

    report = queue.diagnostics()

    assert report.processing == 1
    assert report.active_leases == 0
    assert report.expired_leases == 1
    assert report.oldest_expired_lease_age_seconds is not None
    assert 0.5 <= report.oldest_expired_lease_age_seconds < 10
    assert database_rows(database) == before
    assert queue.stats()["processing"] == 1
    queue.close()


def test_wal_is_reported_when_active(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path))
    queue.put({"wal": True})

    report = queue.diagnostics()

    assert report.journal_mode == "wal"
    assert report.wal_size_bytes is not None
    assert report.wal_size_bytes > 0
    assert report.shm_size_bytes is not None
    assert report.shm_size_bytes > 0
    queue.close()


def test_relative_database_path_remains_stable_after_cwd_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_cwd = tmp_path / "original cwd"
    changed_cwd = tmp_path / "changed cwd"
    original_cwd.mkdir()
    changed_cwd.mkdir()
    monkeypatch.chdir(original_cwd)
    queue = SimpleQueue("./relative data-\u00e5")
    queue.put({"path": "stable"})
    database = original_cwd / "relative data-\u00e5" / "localqueue.db"
    expected_database_size = database.stat().st_size
    expected_wal_size = Path(f"{database}-wal").stat().st_size
    expected_shm_size = Path(f"{database}-shm").stat().st_size

    try:
        monkeypatch.chdir(changed_cwd)
        report = queue.diagnostics()

        assert queue.path == Path("relative data-\u00e5")
        assert report.database_size_bytes == expected_database_size
        assert report.wal_size_bytes == expected_wal_size
        assert report.shm_size_bytes == expected_shm_size
        assert not (changed_cwd / "relative data-\u00e5").exists()
    finally:
        queue.close()


def test_absolute_database_path_with_spaces_and_unicode_is_reported(
    tmp_path: Path,
) -> None:
    queue_directory = tmp_path / "absolute data-\u00e5"
    queue = SimpleQueue(str(queue_directory))
    queue.put({"path": "absolute"})

    report = queue.diagnostics()
    database = queue_directory / "localqueue.db"

    assert report.database_size_bytes == database.stat().st_size
    assert report.wal_size_bytes == Path(f"{database}-wal").stat().st_size
    assert report.shm_size_bytes == Path(f"{database}-shm").stat().st_size
    queue.close()


def test_sidecar_sizes_are_optional_across_database_lifecycle(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path))
    queue.close()
    assert not (tmp_path / "localqueue.db-wal").exists()
    assert not (tmp_path / "localqueue.db-shm").exists()

    reopened = SimpleQueue(str(tmp_path))
    report = reopened.diagnostics()

    assert report.wal_size_bytes is None or report.wal_size_bytes >= 0
    assert report.shm_size_bytes is None or report.shm_size_bytes >= 0
    reopened.close()


def test_package_metadata_fallback_only_handles_missing_distribution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import localqueue.diagnostics as diagnostics_module

    def missing_distribution(name: str) -> str:
        raise metadata.PackageNotFoundError(name)

    monkeypatch.setattr(diagnostics_module.metadata, "version", missing_distribution)
    queue = SimpleQueue(str(tmp_path))

    assert queue.diagnostics().package_version == "development"
    queue.close()


def test_package_metadata_does_not_mask_other_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import localqueue.diagnostics as diagnostics_module

    def broken_metadata(name: str) -> str:
        raise RuntimeError(f"metadata failure for {name}")

    monkeypatch.setattr(diagnostics_module.metadata, "version", broken_metadata)
    queue = SimpleQueue(str(tmp_path))

    with pytest.raises(RuntimeError, match="metadata failure for localqueue"):
        queue.diagnostics()
    queue.close()


def test_processing_age_is_since_latest_lease_update(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), lease_seconds=30)
    database = tmp_path / "localqueue.db"
    queue.put({"lease": "extended"})
    job = queue.get_nowait()
    with sqlite3.connect(database) as external:
        external.execute(
            "UPDATE messages SET updated_at = ? WHERE id = ?",
            (int(time.time() * 1000) - 60_000, job.id),
        )

    job.extend_lease(30)
    report = queue.diagnostics()

    assert report.oldest_processing_updated_age_seconds is not None
    assert 0 <= report.oldest_processing_updated_age_seconds < 5
    queue.ack(job)
    queue.close()


def test_repeated_diagnostics_does_not_mutate_queue_or_wal(tmp_path: Path) -> None:
    queue = SimpleQueue(str(tmp_path), lease_seconds=30)
    database = tmp_path / "localqueue.db"
    queue.put({"state": "ready"})
    queue.put({"state": "expired"})
    expired = queue.get_nowait()
    now_ms = int(time.time() * 1000)
    with sqlite3.connect(database) as external:
        external.execute(
            "UPDATE messages SET lease_until = ? WHERE id = ?",
            (now_ms - 1_000, expired.id),
        )
    before_rows = database_rows(database)

    first = queue.diagnostics()
    second = queue.diagnostics()

    assert database_rows(database) == before_rows
    assert first.wal_size_bytes == second.wal_size_bytes
    assert first.to_dict().keys() == second.to_dict().keys()
    assert (first.ready, first.processing, first.acked, first.failed) == (
        second.ready,
        second.processing,
        second.acked,
        second.failed,
    )
    assert first.expired_leases == second.expired_leases == 1
    queue.close()


def test_existing_schema_database_requires_no_migration(tmp_path: Path) -> None:
    database = tmp_path / "localqueue.db"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                payload BLOB NOT NULL,
                status INTEGER NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                available_at INTEGER NOT NULL,
                lease_until INTEGER,
                receipt TEXT,
                last_error TEXT,
                job_id TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE UNIQUE INDEX idx_messages_job_id
                ON messages(queue, job_id) WHERE job_id IS NOT NULL;
            CREATE INDEX idx_messages_queue_status
                ON messages(queue, status, available_at, lease_until);
            """
        )
        now_ms = int(time.time() * 1000)
        connection.execute(
            "INSERT INTO messages (queue, payload, status, attempts, max_attempts, "
            "available_at, created_at, updated_at) VALUES (?, ?, 0, 0, 4, ?, ?, ?)",
            ("legacy", b"{}", now_ms, now_ms, now_ms),
        )

    queue = SimpleQueue(str(tmp_path), name="legacy")

    assert queue.diagnostics().ready == 1
    queue.close()


def test_concurrent_activity_produces_bounded_valid_snapshots(tmp_path: Path) -> None:
    observer = SimpleQueue(str(tmp_path), name="concurrent", lease_seconds=5)
    start = threading.Event()

    def produce() -> None:
        producer = SimpleQueue(str(tmp_path), name="concurrent", lease_seconds=5)
        start.wait(timeout=2)
        try:
            for index in range(40):
                producer.put({"index": index})
                if index % 2 == 0:
                    producer.ack(producer.get_nowait())
        finally:
            producer.close()

    reports: list[QueueDiagnostics] = []
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(produce)
        start.set()
        for _ in range(50):
            reports.append(observer.diagnostics())
        future.result(timeout=10)

    assert reports
    for report in reports:
        counts = (report.ready, report.processing, report.acked, report.failed)
        assert all(count >= 0 for count in counts)
        assert report.active_leases >= 0
        assert report.expired_leases >= 0
        assert report.active_leases + report.expired_leases == report.processing
        for age in (
            report.oldest_available_age_seconds,
            report.oldest_processing_updated_age_seconds,
            report.oldest_expired_lease_age_seconds,
        ):
            assert age is None or age >= 0
        assert report.wal_size_bytes is None or report.wal_size_bytes >= 0
        assert report.shm_size_bytes is None or report.shm_size_bytes >= 0
    observer.close()
