use pyo3::prelude::*;
use rusqlite::params;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use crate::error::{QueueError, Result};
use crate::queue::{STATUS_ACKED, STATUS_FAILED, STATUS_LEASED, STATUS_READY};
use crate::storage::{now_ms, Storage};

pub const DIAGNOSTICS_SCHEMA_VERSION: i64 = 1;

#[derive(Debug, Clone)]
#[pyclass(skip_from_py_object)]
pub struct DiagnosticsSnapshot {
    #[pyo3(get)]
    pub schema_version: i64,
    #[pyo3(get)]
    pub sqlite_version: String,
    #[pyo3(get)]
    pub observed_at_ms: i64,
    #[pyo3(get)]
    pub journal_mode: String,
    #[pyo3(get)]
    pub synchronous: i64,
    #[pyo3(get)]
    pub durability_mode: String,
    #[pyo3(get)]
    pub busy_timeout_ms: i64,
    #[pyo3(get)]
    pub database_size_bytes: Option<u64>,
    #[pyo3(get)]
    pub wal_size_bytes: Option<u64>,
    #[pyo3(get)]
    pub shm_size_bytes: Option<u64>,
    #[pyo3(get)]
    pub page_count: i64,
    #[pyo3(get)]
    pub page_size: i64,
    #[pyo3(get)]
    pub freelist_count: i64,
    #[pyo3(get)]
    pub ready: i64,
    #[pyo3(get)]
    pub processing: i64,
    #[pyo3(get)]
    pub acked: i64,
    #[pyo3(get)]
    pub failed: i64,
    #[pyo3(get)]
    pub oldest_available_age_ms: Option<i64>,
    #[pyo3(get)]
    pub oldest_processing_updated_age_ms: Option<i64>,
    #[pyo3(get)]
    pub active_leases: i64,
    #[pyo3(get)]
    pub expired_leases: i64,
    #[pyo3(get)]
    pub oldest_expired_lease_age_ms: Option<i64>,
}

pub fn collect(storage: &Storage, queue: &str) -> Result<DiagnosticsSnapshot> {
    collect_after_transaction(storage, queue, now_ms, || {})
}

fn collect_after_transaction<F, H>(
    storage: &Storage,
    queue: &str,
    clock: F,
    snapshot_started: H,
) -> Result<DiagnosticsSnapshot>
where
    F: FnOnce() -> i64,
    H: FnOnce(),
{
    let mut guard = storage.connection();
    let conn = guard.as_mut().ok_or(QueueError::Closed)?;
    let tx = conn.transaction()?;

    // Establish SQLite's read snapshot before taking the wall-clock boundary.
    // The timestamp is then immediately used by the aggregate below, so rows
    // committed after it cannot enter this report with a newer state.
    let _: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM messages)", [], |row| {
        row.get(0)
    })?;
    snapshot_started();
    let observed_at_ms = clock();

    let (
        ready,
        processing,
        acked,
        failed,
        oldest_available_at,
        oldest_processing_updated_at,
        active_leases,
        expired_leases,
        oldest_expired_lease_until,
    ) = tx.query_row(
        "SELECT
            COALESCE(SUM(status = ?2), 0),
            COALESCE(SUM(status = ?3), 0),
            COALESCE(SUM(status = ?4), 0),
            COALESCE(SUM(status = ?5), 0),
            MIN(CASE WHEN status = ?2 AND available_at <= ?6 THEN available_at END),
            MIN(CASE WHEN status = ?3 THEN updated_at END),
            COALESCE(SUM(status = ?3 AND lease_until > ?6), 0),
            COALESCE(SUM(status = ?3 AND lease_until <= ?6), 0),
            MIN(CASE WHEN status = ?3 AND lease_until <= ?6 THEN lease_until END)
         FROM messages
         WHERE queue = ?1",
        params![
            queue,
            STATUS_READY,
            STATUS_LEASED,
            STATUS_ACKED,
            STATUS_FAILED,
            observed_at_ms
        ],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get::<_, Option<i64>>(8)?,
            ))
        },
    )?;

    let journal_mode: String = tx.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    let synchronous: i64 = tx.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
    let busy_timeout_ms: i64 = tx.query_row("PRAGMA busy_timeout", [], |row| row.get(0))?;
    let page_count: i64 = tx.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let page_size: i64 = tx.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    let freelist_count: i64 = tx.query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
    tx.commit()?;
    drop(guard);

    let database_path = storage.path();
    Ok(DiagnosticsSnapshot {
        schema_version: DIAGNOSTICS_SCHEMA_VERSION,
        sqlite_version: rusqlite::version().to_owned(),
        observed_at_ms,
        journal_mode: journal_mode.to_lowercase(),
        synchronous,
        durability_mode: durability_mode(synchronous).to_owned(),
        busy_timeout_ms,
        database_size_bytes: best_effort_size(database_path),
        wal_size_bytes: best_effort_size(&sidecar_path(database_path, "-wal")),
        shm_size_bytes: best_effort_size(&sidecar_path(database_path, "-shm")),
        page_count,
        page_size,
        freelist_count,
        ready,
        processing,
        acked,
        failed,
        oldest_available_age_ms: age_ms(observed_at_ms, oldest_available_at),
        oldest_processing_updated_age_ms: age_ms(observed_at_ms, oldest_processing_updated_at),
        active_leases,
        expired_leases,
        oldest_expired_lease_age_ms: age_ms(observed_at_ms, oldest_expired_lease_until),
    })
}

fn durability_mode(synchronous: i64) -> &'static str {
    match synchronous {
        1 => "normal",
        2 => "full",
        _ => "unknown",
    }
}

fn age_ms(observed_at_ms: i64, timestamp_ms: Option<i64>) -> Option<i64> {
    timestamp_ms.map(|timestamp| observed_at_ms.saturating_sub(timestamp).max(0))
}

fn sidecar_path(database_path: &Path, suffix: &str) -> PathBuf {
    let mut path = OsString::from(database_path.as_os_str());
    path.push(suffix);
    PathBuf::from(path)
}

fn best_effort_size(path: &Path) -> Option<u64> {
    std::fs::metadata(path).ok().map(|metadata| metadata.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn age_is_never_negative() {
        assert_eq!(age_ms(10, Some(20)), Some(0));
        assert_eq!(age_ms(20, Some(10)), Some(10));
        assert_eq!(age_ms(20, None), None);
    }

    #[test]
    fn missing_sidecar_has_no_size() {
        let path =
            std::env::temp_dir().join(format!("localqueue-missing-sidecar-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        assert_eq!(best_effort_size(&path), None);
    }

    #[test]
    fn sidecar_metadata_race_is_best_effort() {
        let directory = std::env::temp_dir().join(format!(
            "localqueue-sidecar-race-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let sidecar = directory.join("localqueue.db-wal");
        let writer_path = sidecar.clone();
        let writer = std::thread::spawn(move || {
            for _ in 0..1_000 {
                std::fs::write(&writer_path, b"wal").unwrap();
                std::fs::remove_file(&writer_path).unwrap();
            }
        });

        while !writer.is_finished() {
            assert!(matches!(best_effort_size(&sidecar), None | Some(0..=3)));
        }
        writer.join().unwrap();
        assert_eq!(best_effort_size(&sidecar), None);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn observed_at_is_captured_after_waiting_for_the_connection_mutex() {
        use std::sync::{mpsc, Arc};
        use std::time::Duration;

        let directory = std::env::temp_dir().join(format!(
            "localqueue-observed-at-boundary-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let database = directory.join("localqueue.db");
        let storage = Arc::new(Storage::new(database.to_str().unwrap(), false).unwrap());
        let held_connection = storage.connection();
        let (snapshot_started_tx, snapshot_started_rx) = mpsc::channel();
        let (clock_value_tx, clock_value_rx) = mpsc::channel();
        let worker_storage = Arc::clone(&storage);
        let worker = std::thread::spawn(move || {
            collect_after_transaction(
                &worker_storage,
                "queue",
                || clock_value_rx.recv().unwrap(),
                || snapshot_started_tx.send(()).unwrap(),
            )
            .unwrap()
        });

        drop(held_connection);
        snapshot_started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("transaction must start after waiting for the connection mutex");
        clock_value_tx.send(123).unwrap();
        let snapshot = worker.join().unwrap();

        assert_eq!(snapshot.observed_at_ms, 123);
        storage.close().unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }
}
