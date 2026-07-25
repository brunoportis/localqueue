use pyo3::prelude::*;
use rusqlite::{params, Connection, TransactionBehavior};
use std::sync::MutexGuard;

use crate::backup::{create as create_backup, BackupSnapshot};
use crate::diagnostics::{collect as collect_diagnostics, DiagnosticsSnapshot};
use crate::error::QueueError;
use crate::integrity::{check as check_integrity, IntegrityCheckSnapshot};
use crate::storage::{now_ms, CapacityPolicy, EnqueueEntry, Storage};

pub const STATUS_READY: i64 = 0;
pub const STATUS_LEASED: i64 = 1;
pub const STATUS_ACKED: i64 = 2;
pub const STATUS_FAILED: i64 = 3;
type IdentityTarget = (String, Option<String>, Option<String>, Option<String>);
type EnqueueIdentityEntry = (
    String,
    Vec<u8>,
    Option<String>,
    Option<String>,
    Option<String>,
);

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
    pub failure_reason: Option<String>,
    #[pyo3(get)]
    pub failure_category: Option<String>,
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
    max_pending_jobs: Option<i64>,
}

#[pymethods]
impl NativeQueue {
    #[new]
    #[pyo3(signature = (path, queue, max_attempts = 3, fsync = false, max_pending_jobs = None))]
    pub fn new(
        path: &str,
        queue: &str,
        max_attempts: i64,
        fsync: bool,
        max_pending_jobs: Option<i64>,
    ) -> PyResult<Self> {
        if matches!(max_pending_jobs, Some(limit) if limit <= 0) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "'max_pending_jobs' must be positive",
            ));
        }
        let storage = Storage::new(path, fsync)?;
        Ok(Self {
            storage,
            queue: queue.to_string(),
            max_attempts,
            max_pending_jobs,
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

    /// Limit the next backup's private destination database for disk-full tests.
    #[cfg(feature = "__crash_test")]
    #[doc(hidden)]
    pub fn _test_set_backup_max_page_count(&self, pages: i64) -> PyResult<()> {
        if pages < 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "pages must be at least 1",
            ));
        }
        crate::backup::set_test_backup_max_page_count(pages);
        Ok(())
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

    #[pyo3(signature = (payload, job_id = None, busy_timeout_ms = None))]
    pub fn put(
        &self,
        py: Python<'_>,
        payload: Vec<u8>,
        job_id: Option<&str>,
        busy_timeout_ms: Option<u64>,
    ) -> PyResult<i64> {
        let job_id = job_id.map(str::to_owned);
        py.detach(move || {
            let entries = [EnqueueEntry {
                queue_name: &self.queue,
                payload: &payload,
                job_id: job_id.as_deref(),
                dedup_key: None,
                dedup_fingerprint: None,
            }];
            let capacity = self.capacity_policy();
            let outcomes = self.storage.enqueue_batch_outcomes(
                &entries,
                self.max_attempts,
                capacity.as_slice(),
                busy_timeout_ms,
            )?;
            Ok(outcomes[0].id)
        })
    }

    /// Insert multiple messages into the queue in one transaction.
    #[pyo3(signature = (payloads, job_ids = None, busy_timeout_ms = None))]
    pub fn put_many(
        &self,
        py: Python<'_>,
        payloads: Vec<Vec<u8>>,
        job_ids: Option<Vec<Option<String>>>,
        busy_timeout_ms: Option<u64>,
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
                    dedup_key: None,
                    dedup_fingerprint: None,
                })
                .collect();
            let capacity = self.capacity_policy();
            Ok(self
                .storage
                .enqueue_batch_outcomes(
                    &entries,
                    self.max_attempts,
                    capacity.as_slice(),
                    busy_timeout_ms,
                )?
                .into_iter()
                .map(|outcome| outcome.id)
                .collect())
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
                    dedup_key: None,
                    dedup_fingerprint: None,
                })
                .collect();
            Ok(self
                .storage
                .enqueue_batch(&entries, self.max_attempts, &[], None)?)
        })
    }

    #[pyo3(name = "_fanout_with_identity")]
    pub fn fanout_with_identity(
        &self,
        py: Python<'_>,
        payload: Vec<u8>,
        targets: Vec<IdentityTarget>,
    ) -> PyResult<Vec<(i64, bool)>> {
        py.detach(move || {
            let entries: Vec<EnqueueEntry<'_>> = targets
                .iter()
                .map(
                    |(queue_name, job_id, dedup_key, dedup_fingerprint)| EnqueueEntry {
                        queue_name,
                        payload: &payload,
                        job_id: job_id.as_deref(),
                        dedup_key: dedup_key.as_deref(),
                        dedup_fingerprint: dedup_fingerprint.as_deref(),
                    },
                )
                .collect();
            // EventBus fanout deliberately remains unlimited in issue #25.
            Ok(self
                .storage
                .enqueue_batch_outcomes(&entries, self.max_attempts, &[], None)?
                .into_iter()
                .map(|outcome| (outcome.id, outcome.inserted))
                .collect())
        })
    }

    /// Ingest a heterogeneous batch of (queue, payload, identity) events in a
    /// single transaction, honoring the given per-queue pending limits.
    ///
    /// Each entry carries its own payload, unlike `_fanout_with_identity`.
    /// `capacity` is a list of (queue_name, max_pending) pairs; `None` means
    /// unlimited. Outcomes are aligned 1:1 with the input entries.
    #[pyo3(name = "_enqueue_batch_with_identity")]
    pub fn enqueue_batch_with_identity(
        &self,
        py: Python<'_>,
        entries: Vec<EnqueueIdentityEntry>,
        capacity: Option<Vec<(String, i64)>>,
    ) -> PyResult<Vec<(i64, bool)>> {
        if let Some(policies) = &capacity {
            for (_, max_pending) in policies {
                if *max_pending < 1 {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "capacity limits must be at least 1",
                    ));
                }
            }
        }
        py.detach(move || {
            let entries: Vec<EnqueueEntry<'_>> = entries
                .iter()
                .map(
                    |(queue_name, payload, job_id, dedup_key, dedup_fingerprint)| EnqueueEntry {
                        queue_name,
                        payload,
                        job_id: job_id.as_deref(),
                        dedup_key: dedup_key.as_deref(),
                        dedup_fingerprint: dedup_fingerprint.as_deref(),
                    },
                )
                .collect();
            let policies: Vec<CapacityPolicy<'_>> = capacity
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|(queue_name, max_pending_jobs)| CapacityPolicy {
                    queue_name,
                    max_pending_jobs: *max_pending_jobs,
                })
                .collect();
            Ok(self
                .storage
                .enqueue_batch_outcomes(&entries, self.max_attempts, &policies, None)?
                .into_iter()
                .map(|outcome| (outcome.id, outcome.inserted))
                .collect())
        })
    }

    /// Atomically acknowledge this queue's leased message and fan one payload
    /// out to all target queues.
    pub fn ack_and_fanout(
        &self,
        py: Python<'_>,
        id: i64,
        receipt: &str,
        payload: Vec<u8>,
        targets: Vec<(String, Option<String>)>,
    ) -> PyResult<Vec<i64>> {
        let receipt = receipt.to_owned();
        py.detach(move || {
            let entries: Vec<EnqueueEntry<'_>> = targets
                .iter()
                .map(|(queue_name, job_id)| EnqueueEntry {
                    queue_name,
                    payload: &payload,
                    job_id: job_id.as_deref(),
                    dedup_key: None,
                    dedup_fingerprint: None,
                })
                .collect();
            Ok(self.storage.ack_and_fanout(
                &self.queue,
                id,
                &receipt,
                &entries,
                self.max_attempts,
            )?)
        })
    }

    #[pyo3(name = "_ack_and_fanout_with_identity")]
    pub fn ack_and_fanout_with_identity(
        &self,
        py: Python<'_>,
        id: i64,
        receipt: &str,
        payload: Vec<u8>,
        targets: Vec<IdentityTarget>,
    ) -> PyResult<Vec<(i64, bool)>> {
        let receipt = receipt.to_owned();
        py.detach(move || {
            let entries: Vec<EnqueueEntry<'_>> = targets
                .iter()
                .map(
                    |(queue_name, job_id, dedup_key, dedup_fingerprint)| EnqueueEntry {
                        queue_name,
                        payload: &payload,
                        job_id: job_id.as_deref(),
                        dedup_key: dedup_key.as_deref(),
                        dedup_fingerprint: dedup_fingerprint.as_deref(),
                    },
                )
                .collect();
            Ok(self
                .storage
                .ack_and_fanout_outcomes(&self.queue, id, &receipt, &entries, self.max_attempts)?
                .into_iter()
                .map(|outcome| (outcome.id, outcome.inserted))
                .collect())
        })
    }

    #[pyo3(signature = (lease_ms, max_attempts = None))]
    pub fn get(
        &self,
        py: Python<'_>,
        lease_ms: i64,
        max_attempts: Option<i64>,
    ) -> PyResult<Option<Lease>> {
        if matches!(max_attempts, Some(attempts) if attempts < 1) {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "'max_attempts' must be at least 1",
            ));
        }
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

            // An EventBus retry policy owns the subscription budget. Apply
            // it to only the expired leases processed by this reclaim before
            // deciding whether each lease is ready or exhausted.
            if let Some(override_attempts) = max_attempts {
                tx.execute(
                    "UPDATE messages SET max_attempts = ?1
                     WHERE queue = ?2 AND status = ?3 AND lease_until <= ?4",
                    params![override_attempts, self.queue, STATUS_LEASED, now],
                )
                .map_err(QueueError::from)?;
            }

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
                failure_reason = 'retries_exhausted',
                updated_at = ?2
             WHERE queue = ?3 AND status = ?4 AND lease_until <= ?5
                AND attempts >= max_attempts",
                params![STATUS_FAILED, now, self.queue, STATUS_LEASED, now],
            )
            .map_err(QueueError::from)?;

            // Fail every currently deliverable READY message that has already
            // exhausted a reduced EventBus budget in one set-based statement.
            if let Some(override_attempts) = max_attempts {
                tx.execute(
                    "UPDATE messages SET
                        status = ?1,
                        max_attempts = ?2,
                        failure_reason = 'retries_exhausted',
                        updated_at = ?3
                     WHERE queue = ?4 AND status = ?5 AND available_at <= ?6
                        AND attempts >= ?7",
                    params![
                        STATUS_FAILED,
                        override_attempts,
                        now,
                        self.queue,
                        STATUS_READY,
                        now,
                        override_attempts
                    ],
                )
                .map_err(QueueError::from)?;
            }

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
                Some(row) => row,
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
                    max_attempts = COALESCE(?5, max_attempts),
                    updated_at = ?6
                 WHERE id = ?7 AND queue = ?8 AND status = ?9 AND available_at <= ?10",
                    params![
                        STATUS_LEASED,
                        receipt,
                        lease_until,
                        new_attempts,
                        max_attempts,
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

    #[pyo3(signature = (id, receipt, delay_ms = 0, last_error = None, failure_reason = None))]
    pub fn nack(
        &self,
        py: Python<'_>,
        id: i64,
        receipt: &str,
        delay_ms: i64,
        last_error: Option<&str>,
        failure_reason: Option<&str>,
    ) -> PyResult<()> {
        let receipt = receipt.to_owned();
        let last_error = last_error.map(str::to_owned);
        let failure_reason = failure_reason.map(str::to_owned);
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
                checked_available_at(now, delay_ms)?
            } else {
                now
            };
            let terminal_reason = if new_status == STATUS_FAILED {
                Some(failure_reason.as_deref().unwrap_or("retries_exhausted"))
            } else {
                None
            };

            let changed = tx
                .execute(
                    "UPDATE messages SET
                    status = ?1,
                    available_at = ?2,
                    receipt = NULL,
                    lease_until = NULL,
                    last_error = ?3,
                    failure_reason = ?4,
                    failure_category = NULL,
                    updated_at = ?5
                 WHERE id = ?6 AND queue = ?7 AND status = ?8
                    AND receipt = ?9 AND lease_until > ?10",
                    params![
                        new_status,
                        available_at,
                        last_error,
                        terminal_reason,
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

    #[pyo3(signature = (id, receipt, last_error = None, failure_reason = None, failure_category = None))]
    pub fn fail(
        &self,
        py: Python<'_>,
        id: i64,
        receipt: &str,
        last_error: Option<&str>,
        failure_reason: Option<&str>,
        failure_category: Option<&str>,
    ) -> PyResult<()> {
        let receipt = receipt.to_owned();
        let last_error = last_error.map(str::to_owned);
        let failure_reason = failure_reason.map(str::to_owned);
        let failure_category = failure_category.map(str::to_owned);
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
                    failure_reason = ?3,
                    failure_category = ?4,
                    updated_at = ?5
                 WHERE id = ?6 AND queue = ?7 AND status = ?8
                    AND receipt = ?9 AND lease_until > ?10",
                    params![
                        STATUS_FAILED,
                        last_error,
                        failure_reason,
                        failure_category,
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
                    failure_reason = 'retries_exhausted',
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
        py.detach(move || {
            collect_diagnostics(&self.storage, &self.queue, self.max_pending_jobs)
                .map_err(Into::into)
        })
    }

    /// Run a read-only SQLite full or quick integrity check.
    #[pyo3(signature = (quick = false, max_errors = 100))]
    pub fn check_integrity(
        &self,
        py: Python<'_>,
        quick: bool,
        max_errors: u16,
    ) -> PyResult<IntegrityCheckSnapshot> {
        py.detach(move || check_integrity(&self.storage, quick, max_errors).map_err(Into::into))
    }

    /// Create a consistent SQLite online backup at `destination`.
    pub fn backup(&self, py: Python<'_>, destination: &str) -> PyResult<BackupSnapshot> {
        let destination = destination.to_owned();
        py.detach(move || create_backup(&self.storage, &destination).map_err(Into::into))
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
                    "SELECT id, payload, attempts, last_error, failure_reason,
                            failure_category, created_at, updated_at
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
                        failure_reason: row.get(4)?,
                        failure_category: row.get(5)?,
                        created_at: row.get(6)?,
                        updated_at: row.get(7)?,
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
            self.storage
                .retry_failed(&self.queue, id, self.max_pending_jobs)
                .map_err(Into::into)
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
    fn capacity_policy(&self) -> Option<CapacityPolicy<'_>> {
        self.max_pending_jobs
            .map(|max_pending_jobs| CapacityPolicy {
                queue_name: &self.queue,
                max_pending_jobs,
            })
    }

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

fn checked_available_at(now: i64, delay_ms: i64) -> Result<i64, QueueError> {
    if delay_ms < 0 {
        return Err(QueueError::InvalidDelay);
    }
    now.checked_add(delay_ms).ok_or(QueueError::InvalidDelay)
}

#[cfg(test)]
mod tests {
    use super::checked_available_at;
    use crate::error::QueueError;

    #[test]
    fn available_at_rejects_negative_and_overflowing_delays() {
        assert!(matches!(
            checked_available_at(1, -1),
            Err(QueueError::InvalidDelay)
        ));
        assert!(matches!(
            checked_available_at(i64::MAX, 1),
            Err(QueueError::InvalidDelay)
        ));
        assert_eq!(checked_available_at(10, 20).unwrap(), 30);
    }
}
