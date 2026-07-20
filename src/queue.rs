use pyo3::prelude::*;
use rusqlite::{params, TransactionBehavior};

use crate::error::QueueError;
use crate::storage::{now_ms, Storage};

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

    pub fn put(&self, payload: Vec<u8>, job_id: Option<&str>) -> PyResult<i64> {
        let now = now_ms();
        let conn = self.storage.connection();

        if let Some(jid) = job_id {
            let existing: Option<i64> = conn
                .query_row(
                    "SELECT id FROM messages WHERE queue = ?1 AND job_id = ?2",
                    params![self.queue, jid],
                    |row| row.get(0),
                )
                .optional()
                .map_err(QueueError::from)?;
            if let Some(id) = existing {
                return Ok(id);
            }
        }

        conn.execute(
            "INSERT INTO messages (
                queue, payload, status, attempts, max_attempts,
                available_at, lease_until, receipt, job_id,
                created_at, updated_at
            ) VALUES (?1, ?2, ?3, 0, ?4, ?5, NULL, NULL, ?6, ?7, ?8)",
            params![
                self.queue,
                payload,
                STATUS_READY,
                self.max_attempts,
                now,
                job_id,
                now,
                now,
            ],
        )
        .map_err(QueueError::from)?;
        Ok(conn.last_insert_rowid())
    }

    pub fn get(&self, lease_ms: i64) -> PyResult<Option<Lease>> {
        let now = now_ms();
        let lease_until = now + lease_ms;
        let receipt = generate_receipt();
        let mut conn = self.storage.connection();

        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(QueueError::from)?;

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
                let expired: Option<(i64, Vec<u8>, i64)> = tx
                    .query_row(
                        "SELECT id, payload, attempts FROM messages
                         WHERE queue = ?1 AND status = ?2 AND lease_until <= ?3
                         ORDER BY lease_until LIMIT 1",
                        params![self.queue, STATUS_LEASED, now],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()
                    .map_err(QueueError::from)?;
                match expired {
                    Some(r) => r,
                    None => {
                        tx.commit().map_err(QueueError::from)?;
                        return Ok(None);
                    }
                }
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
                 WHERE id = ?6 AND (
                    (status = ?7 AND available_at <= ?8) OR
                    (status = ?9 AND lease_until <= ?10)
                 )",
                params![
                    STATUS_LEASED,
                    receipt,
                    lease_until,
                    new_attempts,
                    now,
                    id,
                    STATUS_READY,
                    now,
                    STATUS_LEASED,
                    now,
                ],
            )
            .map_err(QueueError::from)?;

        if changed == 0 {
            tx.commit().map_err(QueueError::from)?;
            return Ok(None);
        }

        tx.commit().map_err(QueueError::from)?;
        Ok(Some(Lease {
            id,
            payload,
            attempts: new_attempts,
            receipt,
            lease_until,
        }))
    }

    pub fn ack(&self, id: i64, receipt: &str) -> PyResult<()> {
        let now = now_ms();
        let conn = self.storage.connection();
        let changed = conn
            .execute(
                "UPDATE messages SET
                    status = ?1,
                    receipt = NULL,
                    lease_until = NULL,
                    updated_at = ?2
                 WHERE id = ?3 AND status = ?4 AND receipt = ?5",
                params![STATUS_ACKED, now, id, STATUS_LEASED, receipt],
            )
            .map_err(QueueError::from)?;
        if changed == 0 {
            return Err(QueueError::LeaseExpired.into());
        }
        Ok(())
    }

    #[pyo3(signature = (id, receipt, delay_ms = 0, last_error = None))]
    pub fn nack(
        &self,
        id: i64,
        receipt: &str,
        delay_ms: i64,
        last_error: Option<&str>,
    ) -> PyResult<()> {
        let now = now_ms();
        let mut conn = self.storage.connection();

        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(QueueError::from)?;
        let (attempts, max_attempts): (i64, i64) = tx
            .query_row(
                "SELECT attempts, max_attempts FROM messages WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(QueueError::from)?;

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
                 WHERE id = ?5 AND status = ?6 AND receipt = ?7",
                params![
                    new_status,
                    available_at,
                    last_error,
                    now,
                    id,
                    STATUS_LEASED,
                    receipt,
                ],
            )
            .map_err(QueueError::from)?;

        tx.commit().map_err(QueueError::from)?;
        if changed == 0 {
            return Err(QueueError::LeaseExpired.into());
        }
        Ok(())
    }

    #[pyo3(signature = (id, receipt, last_error = None))]
    pub fn fail(&self, id: i64, receipt: &str, last_error: Option<&str>) -> PyResult<()> {
        let now = now_ms();
        let conn = self.storage.connection();
        let changed = conn
            .execute(
                "UPDATE messages SET
                    status = ?1,
                    receipt = NULL,
                    lease_until = NULL,
                    last_error = ?2,
                    updated_at = ?3
                 WHERE id = ?4 AND status = ?5 AND receipt = ?6",
                params![STATUS_FAILED, last_error, now, id, STATUS_LEASED, receipt],
            )
            .map_err(QueueError::from)?;
        if changed == 0 {
            return Err(QueueError::LeaseExpired.into());
        }
        Ok(())
    }

    pub fn extend_lease(&self, id: i64, receipt: &str, extend_ms: i64) -> PyResult<i64> {
        let now = now_ms();
        let new_lease_until = now + extend_ms;
        let conn = self.storage.connection();
        let changed = conn
            .execute(
                "UPDATE messages SET
                    lease_until = ?1,
                    updated_at = ?2
                 WHERE id = ?3 AND status = ?4 AND receipt = ?5",
                params![new_lease_until, now, id, STATUS_LEASED, receipt],
            )
            .map_err(QueueError::from)?;
        if changed == 0 {
            return Err(QueueError::LeaseExpired.into());
        }
        Ok(new_lease_until)
    }

    pub fn reclaim_expired(&self, now: Option<i64>) -> PyResult<i64> {
        let now = now.unwrap_or_else(now_ms);
        let mut conn = self.storage.connection();

        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(QueueError::from)?;
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
    }

    pub fn stats(&self) -> PyResult<Stats> {
        let conn = self.storage.connection();
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
