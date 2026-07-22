use pyo3::prelude::*;
use rusqlite::{params, Connection, TransactionBehavior};
use std::sync::MutexGuard;

use crate::diagnostics::{collect as collect_diagnostics, DiagnosticsSnapshot};
use crate::error::QueueError;
use crate::storage::{now_ms, EnqueueEntry, Storage};

pub const STATUS_READY: i64 = 0;
pub const STATUS_LEASED: i64 = 1;
pub const STATUS_ACKED: i64 = 2;
pub const STATUS_FAILED: i64 = 3;

#[derive(Debug, Clone)]
#[pyclass(skip_from_py_object)]
pub struct Lease {
    #[pyo3(get)]
    pub id: i64,
    #[pyo3(get)]
    pub payload: Vec<u8>,
    #[pyo3(get)]
    pub attempts: i64,
    #[pyo3(get)]
    pub receipt: String,
    #[pyo3(get)]
    pub lease_until: i64,
}

#[derive(Debug, Clone, Default)]
#[pyclass(skip_from_py_object)]
pub struct Stats {
    #[pyo3(get)]
    pub ready: i64,
    #[pyo3(get)]
    pub processing: i64,
    #[pyo3(get)]
    pub acked: i64,
    #[pyo3(get)]
    pub failed: i64,
}

#[derive(Debug, Clone)]
#[pyclass(skip_from_py_object)]
pub struct FailedMessage {
    #[pyo3(get)]
    pub id: i64,
    #[pyo3(get)]
    pub payload: Vec<u8>,
    #[pyo3(get)]
    pub attempts: i64,
    #[pyo3(get)]
    pub last_error: Option<String>,
    #[pyo3(get)]
    pub created_at: i64,
    #[pyo3(get)]
    pub updated_at: i64,
}

#[pyclass]
pub struct NativeQueue {
    storage: Storage,
    queue: String,
    max_attempts: i64,
}

#[pymethods]
impl NativeQueue {
    #[new]
    #[pyo3(signature = (path, queue, max_attempts = 3, fsync = false))]
    pub fn new(path: &str, queue: &str, max_attempts: i64, fsync: bool) -> PyResult<Self> {
        let storage = Storage::new(path, fsync)?;
        Ok(Self {
            storage,
            queue: queue.to_string(),
            max_attempts,
        })
    }

    #[cfg(feature = "__crash_test")]
    #[doc(hidden)]
    pub fn _test_configure_failpoint(&self, name: &str, address: &str) -> PyResult<()> {
        crate::failpoints::configure(name, address).map_err(pyo3::exceptions::PyValueError::new_err)
    }

    /// Apply SQLite's connection-local page limit for the operational chaos harness.
    #[cfg(feature = "__crash_test")]
    #[doc(hidden)]
    pub fn _test_set_max_page_count(&self, pages: i64) -> PyResult<i64> {
        let mut guard = self.storage.connection();
        let conn = guard.as_mut().ok_or(QueueError::Closed)?;
        conn.pragma_update(None, "max_page_count", pages)
            .map_err(QueueError::from)?;
        conn.query_row("PRAGMA max_page_count", [], |row| row.get(0))
            .map_err(QueueError::from)
            .map_err(Into::into)
    }

    /// Read the connection-local busy timeout for the operational chaos harness.
    #[cfg(feature = "__crash_test")]
    #[doc(hidden)]
    pub fn _test_busy_timeout(&self) -> PyResult<i64> {
        let mut guard = self.storage.connection();
        let conn = guard.as_mut().ok_or(QueueError::Closed)?;
        conn.query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .map_err(QueueError::from)
            .map_err(Into::into)
    }

    pub fn put(&self, py: Python<'_>, payload: Vec<u8>, job_id: Option<&str>) -> PyResult<i64> {
        let job_id = job_id.map(str::to_owned);
        py.detach(move || {
            let entries = [EnqueueEntry {
                queue_name: &self.queue,
                payload: &payload,
                job_id: job_id.as_deref(),
            }];
            let ids = self.storage.enqueue_batch(&entries, self.max_attempts)?;
            Ok(ids[0])
        })
    }

    /// Insert multiple messages into the queue in one transaction.
    pub fn put_many(
        &self,
        py: Python<'_>,
        payloads: Vec<Vec<u8>>,
        job_ids: Option<Vec<Option<String>>>,
    ) -> PyResult<Vec<i64>> {
        if let Some(ids) = &job_ids {
            if ids.len() != payloads.len() {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "'job_ids' must have the same length as 'payloads'",
                ));
            }
        }
        py.detach(move || {
            let entries: Vec<EnqueueEntry<'_>> = payloads
                .iter()
                .enumerate()
                .map(|(index, payload)| EnqueueEntry {
                    queue_name: &self.queue,
                    payload,
                    job_id: job_ids.as_ref().and_then(|ids| ids[index].as_deref()),
                })
                .collect();
            Ok(self.storage.enqueue_batch(&entries, self.max_attempts)?)
        })
    }

    /// Internal fan-out of one payload to multiple queues in one transaction.
    ///
    /// `targets` is a list of (queue_name, job_id) pairs. This is used by the
    /// event bus and is not part of the public Python facade.
    pub fn fanout(
        &self,
        py: Python<'_>,
        payload: Vec<u8>,
        targets: Vec<(String, Option<String>)>,
    ) -> PyResult<Vec<i64>> {
        py.detach(move || {
            let entries: Vec<EnqueueEntry<'_>> = targets
                .iter()
                .map(|(queue_name, job_id)| EnqueueEntry {
                    queue_name,
                    payload: &payload,
                    job_id: job_id.as_deref(),
                })
                .collect();
            Ok(self.storage.enqueue_batch(&entries, self.max_attempts)?)
        })
    }

    pub fn get(&self, py: Python<'_>, lease_ms: i64) -> PyResult<Option<Lease>> {
        py.detach(move || {
            let now = now_ms();
            let lease_until = now + lease_ms;
            let receipt = generate_receipt();
            let mut guard = self.conn()?;
            let conn = guard.as_mut().unwrap();

            // Keep a genuinely idle queue on the read-only path. Besides
            // avoiding two unnecessary UPDATEs, this prevents every poll from
            // trying to acquire SQLite's global writer lock.
            let has_deliverable: bool = conn
                .query_row(
                    "SELECT
                        EXISTS(
                            SELECT 1 FROM messages
                            WHERE queue = ?1 AND status = ?2
                                AND available_at <= ?3
                        )
                        OR EXISTS(
                            SELECT 1 FROM messages
                            WHERE queue = ?1 AND status = ?4
                                AND lease_until <= ?3
                        )",
                    params![self.queue, STATUS_READY, now, STATUS_LEASED],
                    |row| row.get(0),
                )
                .map_err(QueueError::from)?;
            if !has_deliverable {
                return Ok(None);
            }

            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(QueueError::from)?;

            // Reclaim expired leases into ready or failed in one pass.
            tx.execute(
                "UPDATE messages SET
                status = ?1,
                available_at = ?2,
                receipt = NULL,
                lease_until = NULL,
                updated_at = ?3
             WHERE queue = ?4 AND status = ?5 AND lease_until <= ?6
                AND attempts < max_attempts",
                params![STATUS_READY, now, now, self.queue, STATUS_LEASED, now],
            )
            .map_err(QueueError::from)?;
            tx.execute(
                "UPDATE messages SET
                status = ?1,
                receipt = NULL,
                lease_until = NULL,
                updated_at = ?2
             WHERE queue = ?3 AND status = ?4 AND lease_until <= ?5
                AND attempts >= max_attempts",
                params![STATUS_FAILED, now, self.queue, STATUS_LEASED, now],
            )
            .map_err(QueueError::from)?;

            let row: Option<(i64, Vec<u8>, i64)> = tx
                .query_row(
                    "SELECT id, payload, attempts FROM messages
                 WHERE queue = ?1 AND status = ?2 AND available_at <= ?3
                 ORDER BY id LIMIT 1",
                    params![self.queue, STATUS_READY, now],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .map_err(QueueError::from)?;

            let (id, payload, attempts) = match row {
                Some(r) => r,
                None => {
                    tx.commit().map_err(QueueError::from)?;
                    return Ok(None);
                }
            };

            let new_attempts = attempts + 1;
            let changed = tx
                .execute(
                    "UPDATE messages SET
                    status = ?1,
                    receipt = ?2,
                    lease_until = ?3,
                    attempts = ?4,
                    updated_at = ?5
                 WHERE id = ?6 AND queue = ?7 AND status = ?8 AND available_at <= ?9",
                    params![
                        STATUS_LEASED,
                        receipt,
                        lease_until,
                        new_attempts,
                        now,
                        id,
                        self.queue,
                        STATUS_READY,
                        now,
                    ],
                )
                .map_err(QueueError::from)?;

            if changed == 0 {
                tx.commit().map_err(QueueError::from)?;
                return Ok(None);
            }

            #[cfg(feature = "__crash_test")]
            crate::failpoints::hit(crate::failpoints::Failpoint::ClaimBeforeCommit);
            tx.commit().map_err(QueueError::from)?;
            Ok(Some(Lease {
                id,
                payload,
                attempts: new_attempts,
                receipt,
                lease_until,
            }))
        })
    }

    pub fn ack(&self, py: Python<'_>, id: i64, receipt: &str) -> PyResult<()> {
        let receipt = receipt.to_owned();
        py.detach(move || {
            let now = now_ms();
            let mut guard = self.conn()?;
            let conn = guard.as_mut().unwrap();
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(QueueError::from)?;
            let changed = tx
                .execute(
                    "UPDATE messages SET
                    status = ?1,
                    receipt = NULL,
                    lease_until = NULL,
                    updated_at = ?2
                 WHERE id = ?3 AND queue = ?4 AND status = ?5
                    AND receipt = ?6 AND lease_until > ?7",
                    params![
                        STATUS_ACKED,
                        now,
                        id,
                        self.queue,
                        STATUS_LEASED,
                        receipt,
                        now
                    ],
                )
                .map_err(QueueError::from)?;
            if changed == 0 {
                return Err(QueueError::LeaseExpired.into());
            }
            #[cfg(feature = "__crash_test")]
            crate::failpoints::hit(crate::failpoints::Failpoint::AckBeforeCommit);
            tx.commit().map_err(QueueError::from)?;
            Ok(())
        })
    }

    #[pyo3(signature = (id, receipt, delay_ms = 0, last_error = None))]
    pub fn nack(
        &self,
        py: Python<'_>,
        id: i64,
        receipt: &str,
        delay_ms: i64,
        last_error: Option<&str>,
    ) -> PyResult<()> {
        let receipt = receipt.to_owned();
        let last_error = last_error.map(str::to_owned);
        py.detach(move || {
            let now = now_ms();
            let mut guard = self.conn()?;
            let conn = guard.as_mut().unwrap();

            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(QueueError::from)?;
            let attempt_limits: Option<(i64, i64)> = tx
                .query_row(
                    "SELECT attempts, max_attempts FROM messages
                 WHERE id = ?1 AND queue = ?2 AND status = ?3
                    AND receipt = ?4 AND lease_until > ?5",
                    params![id, self.queue, STATUS_LEASED, receipt, now],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(QueueError::from)?;
            let (attempts, max_attempts) = match attempt_limits {
                Some(limits) => limits,
                None => return Err(QueueError::LeaseExpired.into()),
            };

            let new_status = if attempts >= max_attempts {
                STATUS_FAILED
            } else {
                STATUS_READY
            };
            let available_at = if new_status == STATUS_READY {
                now + delay_ms
            } else {
                now
            };

            let changed = tx
                .execute(
                    "UPDATE messages SET
                    status = ?1,
                    available_at = ?2,
                    receipt = NULL,
                    lease_until = NULL,
                    last_error = ?3,
                    updated_at = ?4
                 WHERE id = ?5 AND queue = ?6 AND status = ?7
                    AND receipt = ?8 AND lease_until > ?9",
                    params![
                        new_status,
                        available_at,
                        last_error,
                        now,
                        id,
                        self.queue,
                        STATUS_LEASED,
                        receipt,
                        now,
                    ],
                )
                .map_err(QueueError::from)?;

            #[cfg(feature = "__crash_test")]
            crate::failpoints::hit(crate::failpoints::Failpoint::NackBeforeCommit);
            tx.commit().map_err(QueueError::from)?;
            if changed == 0 {
                return Err(QueueError::LeaseExpired.into());
            }
            Ok(())
        })
    }

    #[pyo3(signature = (id, receipt, last_error = None))]
    pub fn fail(
        &self,
        py: Python<'_>,
        id: i64,
        receipt: &str,
        last_error: Option<&str>,
    ) -> PyResult<()> {
        let receipt = receipt.to_owned();
        let last_error = last_error.map(str::to_owned);
        py.detach(move || {
            let now = now_ms();
            let mut guard = self.conn()?;
            let conn = guard.as_mut().unwrap();
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(QueueError::from)?;
            let changed = tx
                .execute(
                    "UPDATE messages SET
                    status = ?1,
                    receipt = NULL,
                    lease_until = NULL,
                    last_error = ?2,
                    updated_at = ?3
                 WHERE id = ?4 AND queue = ?5 AND status = ?6
                    AND receipt = ?7 AND lease_until > ?8",
                    params![
                        STATUS_FAILED,
                        last_error,
                        now,
                        id,
                        self.queue,
                        STATUS_LEASED,
                        receipt,
                        now
                    ],
                )
                .map_err(QueueError::from)?;
            if changed == 0 {
                return Err(QueueError::LeaseExpired.into());
            }
            #[cfg(feature = "__crash_test")]
            crate::failpoints::hit(crate::failpoints::Failpoint::FailBeforeCommit);
            tx.commit().map_err(QueueError::from)?;
            Ok(())
        })
    }

    pub fn extend_lease(
        &self,
        py: Python<'_>,
        id: i64,
        receipt: &str,
        extend_ms: i64,
    ) -> PyResult<i64> {
        let receipt = receipt.to_owned();
        py.detach(move || {
            let now = now_ms();
            let new_lease_until = now + extend_ms;
            let mut guard = self.conn()?;
            let conn = guard.as_mut().unwrap();
            let changed = conn
                .execute(
                    "UPDATE messages SET
                    lease_until = ?1,
                    updated_at = ?2
                 WHERE id = ?3 AND queue = ?4 AND status = ?5
                    AND receipt = ?6 AND lease_until > ?7",
                    params![
                        new_lease_until,
                        now,
                        id,
                        self.queue,
                        STATUS_LEASED,
                        receipt,
                        now
                    ],
                )
                .map_err(QueueError::from)?;
            if changed == 0 {
                return Err(QueueError::LeaseExpired.into());
            }
            Ok(new_lease_until)
        })
    }

    pub fn reclaim_expired(&self, py: Python<'_>, now: Option<i64>) -> PyResult<i64> {
        py.detach(move || {
            let now = now.unwrap_or_else(now_ms);
            let mut guard = self.conn()?;
            let conn = guard.as_mut().unwrap();

            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(QueueError::from)?;
            let to_ready = tx
                .execute(
                    "UPDATE messages SET
                    status = ?1,
                    available_at = ?2,
                    receipt = NULL,
                    lease_until = NULL,
                    updated_at = ?3
                 WHERE queue = ?4 AND status = ?5 AND lease_until <= ?6
                    AND attempts < max_attempts",
                    params![STATUS_READY, now, now, self.queue, STATUS_LEASED, now],
                )
                .map_err(QueueError::from)?;
            let to_failed = tx
                .execute(
                    "UPDATE messages SET
                    status = ?1,
                    receipt = NULL,
                    lease_until = NULL,
                    updated_at = ?2
                 WHERE queue = ?3 AND status = ?4 AND lease_until <= ?5
                    AND attempts >= max_attempts",
                    params![STATUS_FAILED, now, self.queue, STATUS_LEASED, now],
                )
                .map_err(QueueError::from)?;
            tx.commit().map_err(QueueError::from)?;
            Ok((to_ready + to_failed) as i64)
        })
    }

    pub fn stats(&self, py: Python<'_>) -> PyResult<Stats> {
        py.detach(move || {
            let mut guard = self.conn()?;
            let conn = guard.as_mut().unwrap();
            let mut stmt = conn
                .prepare("SELECT status, COUNT(*) FROM messages WHERE queue = ?1 GROUP BY status")
                .map_err(QueueError::from)?;
            let rows = stmt
                .query_map(params![self.queue], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(QueueError::from)?;

            let mut stats = Stats::default();
            for row in rows {
                let (status, count) = row.map_err(QueueError::from)?;
                match status {
                    STATUS_READY => stats.ready = count,
                    STATUS_LEASED => stats.processing = count,
                    STATUS_ACKED => stats.acked = count,
                    STATUS_FAILED => stats.failed = count,
                    _ => {}
                }
            }
            Ok(stats)
        })
    }

    /// Capture a bounded, read-only operational snapshot.
    pub fn diagnostics(&self, py: Python<'_>) -> PyResult<DiagnosticsSnapshot> {
        py.detach(move || collect_diagnostics(&self.storage, &self.queue).map_err(Into::into))
    }

    /// Return the SQLite pragmas used by the active connection.
    pub fn pragma_settings(&self, py: Python<'_>) -> PyResult<(String, i64)> {
        py.detach(move || {
            let mut guard = self.conn()?;
            let conn = guard.as_mut().unwrap();
            let journal_mode = conn
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))
                .map_err(QueueError::from)?;
            let synchronous = conn
                .query_row("PRAGMA synchronous", [], |row| row.get(0))
                .map_err(QueueError::from)?;
            Ok((journal_mode, synchronous))
        })
    }

    /// Remove `acked` or `failed` messages older than `older_than_ms`.
    #[pyo3(signature = (older_than_ms, status = None))]
    pub fn purge(&self, py: Python<'_>, older_than_ms: i64, status: Option<i64>) -> PyResult<i64> {
        py.detach(|| {
            let now = now_ms();
            let cutoff = now - older_than_ms;
            let mut guard = self.conn()?;
            let conn = guard.as_mut().unwrap();

            let status_filter = status.unwrap_or(STATUS_ACKED);
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(QueueError::from)?;
            let changed = tx
                .execute(
                    "DELETE FROM messages
                     WHERE queue = ?1 AND status = ?2 AND updated_at < ?3",
                    params![self.queue, status_filter, cutoff],
                )
                .map_err(QueueError::from)?;
            #[cfg(feature = "__crash_test")]
            crate::failpoints::hit(crate::failpoints::Failpoint::PurgeBeforeCommit);
            tx.commit().map_err(QueueError::from)?;
            Ok(changed as i64)
        })
    }

    /// List dead-letter messages (status = failed).
    #[pyo3(signature = (limit = 100, offset = 0))]
    pub fn list_failed(
        &self,
        py: Python<'_>,
        limit: i64,
        offset: i64,
    ) -> PyResult<Vec<FailedMessage>> {
        py.detach(|| {
            let mut guard = self.conn()?;
            let conn = guard.as_mut().unwrap();
            let mut stmt = conn
                .prepare(
                    "SELECT id, payload, attempts, last_error, created_at, updated_at
                     FROM messages
                     WHERE queue = ?1 AND status = ?2
                     ORDER BY id
                     LIMIT ?3 OFFSET ?4",
                )
                .map_err(QueueError::from)?;

            let rows = stmt
                .query_map(params![self.queue, STATUS_FAILED, limit, offset], |row| {
                    Ok(FailedMessage {
                        id: row.get(0)?,
                        payload: row.get(1)?,
                        attempts: row.get(2)?,
                        last_error: row.get(3)?,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                    })
                })
                .map_err(QueueError::from)?;

            let mut result = Vec::new();
            for row in rows {
                result.push(row.map_err(QueueError::from)?);
            }
            Ok(result)
        })
    }

    /// Move a `failed` message back to `ready`.
    pub fn retry_failed(&self, py: Python<'_>, id: i64) -> PyResult<()> {
        py.detach(move || {
            let now = now_ms();
            let mut guard = self.conn()?;
            let conn = guard.as_mut().unwrap();
            let changed = conn
                .execute(
                    "UPDATE messages SET
                    status = ?1,
                    available_at = ?2,
                    attempts = 0,
                    receipt = NULL,
                    lease_until = NULL,
                    last_error = NULL,
                    updated_at = ?3
                 WHERE id = ?4 AND queue = ?5 AND status = ?6",
                    params![STATUS_READY, now, now, id, self.queue, STATUS_FAILED],
                )
                .map_err(QueueError::from)?;
            if changed == 0 {
                return Err(QueueError::NotFound.into());
            }
            Ok(())
        })
    }

    /// Run VACUUM to compact the database.
    pub fn vacuum(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            let mut guard = self.conn()?;
            let conn = guard.as_mut().unwrap();
            conn.execute("VACUUM", params![])
                .map_err(QueueError::from)?;
            Ok(())
        })
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            self.storage.close()?;
            Ok(())
        })
    }
}

impl NativeQueue {
    fn conn(&self) -> PyResult<MutexGuard<'_, Option<Connection>>> {
        let guard = self.storage.connection();
        if guard.is_none() {
            return Err(QueueError::Closed.into());
        }
        Ok(guard)
    }
}

trait OptionalExt<T> {
    fn optional(self) -> std::result::Result<Option<T>, rusqlite::Error>;
}

impl<T> OptionalExt<T> for std::result::Result<T, rusqlite::Error> {
    fn optional(self) -> std::result::Result<Option<T>, rusqlite::Error> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

fn generate_receipt() -> String {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    format!("{}-{}", pid, nanos)
}
