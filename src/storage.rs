use rusqlite::{
    params, Connection, ErrorCode, OpenFlags, OptionalExtension, Transaction, TransactionBehavior,
};
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::error::{QueueError, Result};
use crate::schema::{
    BASE_SCHEMA_SQL, CHECKPOINTS_SCHEMA_SQL, EXECUTION_MEMBERSHIP_SCHEMA_SQL,
    EXECUTION_RUNTIME_SCHEMA_SQL, SCHEMA_SQL,
};

pub(crate) const BUSY_TIMEOUT_MS: u64 = 5_000;

/// An item in a batch insertion. The payload is borrowed to avoid copying data
/// across the PyO3 boundary.
pub struct EnqueueEntry<'a> {
    pub queue_name: &'a str,
    pub payload: &'a [u8],
    pub job_id: Option<&'a str>,
    pub dedup_key: Option<&'a str>,
    pub dedup_fingerprint: Option<&'a str>,
}

#[derive(Clone, Copy)]
pub struct EnqueueOutcome {
    pub id: i64,
    pub inserted: bool,
}
#[derive(Clone, Copy)]
pub struct CapacityPolicy<'a> {
    pub queue_name: &'a str,
    pub max_pending_jobs: i64,
}

/// Durable ingestion checkpoint update applied in the same transaction as the
/// batch inserts. Both expected tokens absent mean the checkpoint must be
/// created; otherwise generation and version compare-and-swap together.
pub struct CheckpointUpdate {
    pub bus_name: String,
    pub checkpoint_name: String,
    /// The immutable incarnation token of an existing checkpoint. `None`
    /// means this update creates a previously absent checkpoint.
    pub expected_generation: Option<String>,
    pub expected_version: Option<i64>,
    pub new_cursor: String,
    pub source_fingerprint: Option<String>,
    pub items_committed: i64,
}

/// Read-only view of a stored ingestion checkpoint row.
pub struct CheckpointSnapshot {
    pub cursor: String,
    pub source_fingerprint: Option<String>,
    pub generation: String,
    pub version: i64,
    pub items_committed: i64,
    pub batches_committed: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Immutable metadata for one finite EventBus execution. This is internal
/// infrastructure; it deliberately contains no event payload data.
pub struct ExecutionSnapshot {
    pub execution_id: String,
    pub bus_name: String,
    pub source_name: String,
    pub checkpoint_name: Option<String>,
    pub source_completed: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Counts are derived from membership joined to `messages`, never persisted.
pub struct ExecutionDeliveryStateSnapshot {
    pub total: i64,
    pub ready: i64,
    pub processing: i64,
    pub acknowledged: i64,
    pub failed: i64,
}

/// Consistent durable lifecycle view used by the private Python handle.
pub struct ExecutionRuntimeSnapshot {
    pub execution_id: String,
    pub source_name: String,
    pub checkpoint_name: String,
    pub source_fingerprint: String,
    pub checkpoint_generation: Option<String>,
    pub source_completed: bool,
    pub source_completed_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub items_committed: i64,
    pub events_dispatched: i64,
    pub events_unrouted: i64,
    pub deliveries_inserted: i64,
    pub deliveries_deduplicated: i64,
    pub batches_committed: i64,
    pub total: i64,
    pub ready: i64,
    pub processing: i64,
    pub acknowledged: i64,
    pub failed: i64,
    pub source_lease_until: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// The checkpoint incarnation observed while acquiring an execution source
/// lease.  All fields come from the same immediate transaction as the claim.
pub struct ExecutionSourceClaim {
    pub claimed: bool,
    pub cursor: Option<String>,
    pub source_fingerprint: Option<String>,
    pub generation: Option<String>,
    pub version: Option<i64>,
}

pub struct Storage {
    conn: Mutex<Option<Connection>>,
    path: PathBuf,
    fsync: bool,
}

impl Storage {
    pub fn new(path: &str, fsync: bool) -> Result<Self> {
        let path = stable_database_path(path)?;
        let mut conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI,
        )?;

        conn.pragma_update(None, "busy_timeout", BUSY_TIMEOUT_MS)?;
        enable_wal(&conn)?;
        conn.pragma_update(None, "synchronous", if fsync { "FULL" } else { "NORMAL" })?;
        conn.pragma_update(None, "foreign_keys", "ON")?;

        let schema = if has_messages_table(&conn)? && !has_column(&conn, "dedup_key")? {
            BASE_SCHEMA_SQL
        } else {
            SCHEMA_SQL
        };
        conn.execute_batch(schema)?;
        migrate_failure_reason(&mut conn)?;
        migrate_failure_category(&mut conn)?;
        migrate_event_identity(&mut conn)?;
        migrate_ingestion_checkpoints(&mut conn)?;
        migrate_execution_membership(&mut conn)?;
        migrate_execution_runtime(&mut conn)?;

        Ok(Self {
            conn: Mutex::new(Some(conn)),
            path,
            fsync,
        })
    }

    pub fn connection(&self) -> std::sync::MutexGuard<'_, Option<Connection>> {
        self.conn.lock().expect("mutex poisoned")
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn close(&self) -> Result<()> {
        let mut guard = self.connection();
        if let Some(conn) = guard.take() {
            conn.close().map_err(|(_, e)| QueueError::Sqlite(e))?;
        }
        Ok(())
    }

    /// Insert a batch of messages in one BEGIN IMMEDIATE transaction.
    ///
    /// Deduplication by (queue, job_id) uses INSERT OR IGNORE followed by a
    /// SELECT of the existing ID. IDs are returned in input order. Any error
    /// rolls the transaction back without a partial write.
    pub fn enqueue_batch(
        &self,
        entries: &[EnqueueEntry<'_>],
        max_attempts: i64,
        capacity: &[CapacityPolicy<'_>],
        busy_timeout_ms: Option<u64>,
    ) -> Result<Vec<i64>> {
        Ok(self
            .enqueue_batch_outcomes(entries, max_attempts, capacity, busy_timeout_ms)?
            .into_iter()
            .map(|outcome| outcome.id)
            .collect())
    }

    pub fn enqueue_batch_outcomes(
        &self,
        entries: &[EnqueueEntry<'_>],
        max_attempts: i64,
        capacity: &[CapacityPolicy<'_>],
        busy_timeout_ms: Option<u64>,
    ) -> Result<Vec<EnqueueOutcome>> {
        Ok(self
            .enqueue_batch_outcomes_with_checkpoint(
                entries,
                max_attempts,
                capacity,
                busy_timeout_ms,
                None,
            )?
            .0)
    }

    /// Same as `enqueue_batch_outcomes`, plus an optional durable ingestion
    /// checkpoint applied in the same transaction. Its CAS precondition is
    /// checked before capacity planning and delivery inserts; a conflict rolls
    /// back the transaction.
    ///
    /// Unlike the checkpoint-less path, an empty `entries` with a checkpoint
    /// still opens the transaction and advances the checkpoint (checkpoint-only
    /// commit). Returns the outcomes and the confirmed checkpoint version.
    pub fn enqueue_batch_outcomes_with_checkpoint(
        &self,
        entries: &[EnqueueEntry<'_>],
        max_attempts: i64,
        capacity: &[CapacityPolicy<'_>],
        busy_timeout_ms: Option<u64>,
        checkpoint: Option<&CheckpointUpdate>,
    ) -> Result<(Vec<EnqueueOutcome>, Option<CheckpointCommit>)> {
        self.enqueue_batch_outcomes_with_checkpoint_and_execution(
            entries,
            max_attempts,
            capacity,
            busy_timeout_ms,
            checkpoint,
            None,
        )
    }

    pub fn enqueue_batch_outcomes_with_checkpoint_and_execution(
        &self,
        entries: &[EnqueueEntry<'_>],
        max_attempts: i64,
        capacity: &[CapacityPolicy<'_>],
        busy_timeout_ms: Option<u64>,
        checkpoint: Option<&CheckpointUpdate>,
        execution_id: Option<&str>,
    ) -> Result<(Vec<EnqueueOutcome>, Option<CheckpointCommit>)> {
        if entries.is_empty() && checkpoint.is_none() {
            return Ok((Vec::new(), None));
        }

        let mut guard = self.connection();
        let primary = guard.as_mut().ok_or(QueueError::Closed)?;

        match busy_timeout_ms {
            Some(timeout) => {
                let mut attempt = self.open_attempt_connection(timeout)?;
                enqueue_batch_on_connection(
                    &mut attempt,
                    entries,
                    max_attempts,
                    capacity,
                    checkpoint,
                    execution_id,
                )
            }
            None => enqueue_batch_on_connection(
                primary,
                entries,
                max_attempts,
                capacity,
                checkpoint,
                execution_id,
            ),
        }
    }

    /// Read one ingestion checkpoint row, or `None` if it does not exist.
    pub fn checkpoint_inspect(
        &self,
        bus_name: &str,
        checkpoint_name: &str,
    ) -> Result<Option<CheckpointSnapshot>> {
        let guard = self.connection();
        let conn = guard.as_ref().ok_or(QueueError::Closed)?;
        conn.query_row(
            "SELECT cursor, source_fingerprint, generation, version,
                    items_committed, batches_committed, created_at, updated_at
             FROM ingestion_checkpoints
             WHERE bus_name = ?1 AND checkpoint_name = ?2",
            params![bus_name, checkpoint_name],
            |row| {
                Ok(CheckpointSnapshot {
                    cursor: row.get(0)?,
                    source_fingerprint: row.get(1)?,
                    generation: row.get(2)?,
                    version: row.get(3)?,
                    items_committed: row.get(4)?,
                    batches_committed: row.get(5)?,
                    created_at: row.get(6)?,
                    updated_at: row.get(7)?,
                })
            },
        )
        .optional()
        .map_err(QueueError::from)
    }

    /// Delete one ingestion checkpoint row. Returns whether it existed.
    /// Never touches the `messages` table.
    pub fn checkpoint_reset(&self, bus_name: &str, checkpoint_name: &str) -> Result<bool> {
        let guard = self.connection();
        let conn = guard.as_ref().ok_or(QueueError::Closed)?;
        let changed = conn.execute(
            "DELETE FROM ingestion_checkpoints
             WHERE bus_name = ?1 AND checkpoint_name = ?2",
            params![bus_name, checkpoint_name],
        )?;
        Ok(changed == 1)
    }

    pub fn execution_create(
        &self,
        execution_id: &str,
        bus_name: &str,
        source_name: &str,
        checkpoint_name: Option<&str>,
    ) -> Result<()> {
        let now = now_ms();
        let guard = self.connection();
        let conn = guard.as_ref().ok_or(QueueError::Closed)?;
        conn.execute(
            "INSERT INTO event_bus_executions (
                execution_id, bus_name, source_name, checkpoint_name,
                source_completed, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, 0, ?5, ?5)",
            params![execution_id, bus_name, source_name, checkpoint_name, now],
        )?;
        Ok(())
    }

    pub fn execution_inspect(&self, execution_id: &str) -> Result<Option<ExecutionSnapshot>> {
        let guard = self.connection();
        let conn = guard.as_ref().ok_or(QueueError::Closed)?;
        conn.query_row(
            "SELECT execution_id, bus_name, source_name, checkpoint_name,
                    source_completed, created_at, updated_at
             FROM event_bus_executions WHERE execution_id = ?1",
            params![execution_id],
            |row| {
                Ok(ExecutionSnapshot {
                    execution_id: row.get(0)?,
                    bus_name: row.get(1)?,
                    source_name: row.get(2)?,
                    checkpoint_name: row.get(3)?,
                    source_completed: row.get::<_, i64>(4)? != 0,
                    created_at: row.get(5)?,
                    updated_at: row.get(6)?,
                })
            },
        )
        .optional()
        .map_err(QueueError::from)
    }

    pub fn execution_mark_source_completed(&self, execution_id: &str) -> Result<bool> {
        let now = now_ms();
        let mut guard = self.connection();
        let conn = guard.as_mut().ok_or(QueueError::Closed)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let has_runtime: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM event_bus_execution_runtime WHERE execution_id=?1)",
            params![execution_id],
            |row| row.get(0),
        )?;
        if has_runtime {
            return Err(QueueError::ExecutionReceiptRequired);
        }
        let changed = tx.execute(
            "UPDATE event_bus_executions SET source_completed = 1, updated_at = ?2
             WHERE execution_id = ?1 AND source_completed = 0",
            params![execution_id, now],
        )?;
        if !execution_exists(&tx, execution_id)? {
            return Err(QueueError::NotFound);
        }
        tx.commit()?;
        Ok(changed == 1)
    }

    pub fn execution_delivery_states(
        &self,
        execution_id: &str,
    ) -> Result<ExecutionDeliveryStateSnapshot> {
        let guard = self.connection();
        let conn = guard.as_ref().ok_or(QueueError::Closed)?;
        if !execution_exists(conn, execution_id)? {
            return Err(QueueError::NotFound);
        }
        conn.query_row(
            "SELECT COUNT(*),
                    COALESCE(SUM(m.status = 0), 0), COALESCE(SUM(m.status = 1), 0),
                    COALESCE(SUM(m.status = 2), 0), COALESCE(SUM(m.status = 3), 0)
             FROM event_bus_execution_deliveries d
             JOIN messages m ON m.id = d.message_id
             WHERE d.execution_id = ?1",
            params![execution_id],
            |row| {
                Ok(ExecutionDeliveryStateSnapshot {
                    total: row.get(0)?,
                    ready: row.get(1)?,
                    processing: row.get(2)?,
                    acknowledged: row.get(3)?,
                    failed: row.get(4)?,
                })
            },
        )
        .map_err(QueueError::from)
    }

    pub fn execution_open(
        &self,
        candidate: &str,
        bus: &str,
        source: &str,
        checkpoint: &str,
        fingerprint: &str,
        expected_fingerprint: &str,
    ) -> Result<(String, bool)> {
        let now = now_ms();
        let mut guard = self.connection();
        let conn = guard.as_mut().ok_or(QueueError::Closed)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let checkpoint_state: Option<(String, Option<String>)> = tx.query_row("SELECT generation, source_fingerprint FROM ingestion_checkpoints WHERE bus_name=?1 AND checkpoint_name=?2", params![bus, checkpoint], |r| Ok((r.get(0)?, r.get(1)?))).optional()?;
        if let Some((generation, source_fingerprint)) = &checkpoint_state {
            if source_fingerprint.as_deref() != Some(expected_fingerprint) {
                return Err(QueueError::CheckpointConflict {
                    checkpoint_name: checkpoint.to_owned(),
                    expected_generation: None,
                    expected_version: None,
                    actual_generation: Some(generation.clone()),
                    actual_version: None,
                });
            }
        }
        let expected_generation = checkpoint_state
            .as_ref()
            .map(|(generation, _)| generation.as_str());
        let existing: Option<String> = if let Some(generation) = expected_generation {
            tx.query_row("SELECT execution_id FROM event_bus_execution_runtime WHERE bus_name=?1 AND checkpoint_name=?2 AND checkpoint_generation=?3", params![bus, checkpoint, generation], |r| r.get(0)).optional()?
        } else {
            tx.query_row("SELECT execution_id FROM event_bus_execution_runtime WHERE bus_name=?1 AND checkpoint_name=?2 AND source_fingerprint=?3 AND checkpoint_generation IS NULL", params![bus, checkpoint, fingerprint], |r| r.get(0)).optional()?
        };
        if let Some(id) = existing {
            let meta: (String, String, String, String) = tx.query_row("SELECT e.bus_name,e.source_name,r.checkpoint_name,r.source_fingerprint FROM event_bus_executions e JOIN event_bus_execution_runtime r USING(execution_id) WHERE e.execution_id=?1", params![id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
            if meta
                != (
                    bus.to_owned(),
                    source.to_owned(),
                    checkpoint.to_owned(),
                    fingerprint.to_owned(),
                )
            {
                return Err(QueueError::InvalidDeduplicationMetadata);
            }
            tx.commit()?;
            return Ok((id, false));
        }
        if expected_generation.is_some() {
            return Err(QueueError::ExecutionRuntimeMissing);
        }
        tx.execute("INSERT INTO event_bus_executions (execution_id,bus_name,source_name,checkpoint_name,source_completed,created_at,updated_at) VALUES (?1,?2,?3,?4,0,?5,?5)", params![candidate,bus,source,checkpoint,now])?;
        tx.execute("INSERT INTO event_bus_execution_runtime (execution_id,bus_name,checkpoint_name,source_fingerprint,checkpoint_generation) VALUES (?1,?2,?3,?4,?5)", params![candidate,bus,checkpoint,fingerprint,expected_generation])?;
        tx.commit()?;
        Ok((candidate.to_owned(), true))
    }

    pub fn execution_claim_source(
        &self,
        id: &str,
        receipt: &str,
        lease_ms: i64,
    ) -> Result<ExecutionSourceClaim> {
        let now = now_ms();
        let mut guard = self.connection();
        let conn = guard.as_mut().ok_or(QueueError::Closed)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed=tx.execute("UPDATE event_bus_execution_runtime SET source_receipt=?2,source_lease_until=?3 WHERE execution_id=?1 AND source_completed_at IS NULL AND (source_lease_until IS NULL OR source_lease_until <= ?4 OR source_receipt=?2)",params![id,receipt,now+lease_ms,now])?;
        if changed == 0 && !execution_exists(&tx, id)? {
            return Err(QueueError::NotFound);
        }
        if changed == 0 {
            tx.commit()?;
            return Ok(ExecutionSourceClaim {
                claimed: false,
                cursor: None,
                source_fingerprint: None,
                generation: None,
                version: None,
            });
        }
        let (bus_name, checkpoint_name, expected_fingerprint, expected_generation): (
            String,
            String,
            String,
            Option<String>,
        ) = tx.query_row(
            "SELECT bus_name, checkpoint_name, source_fingerprint, checkpoint_generation
             FROM event_bus_execution_runtime WHERE execution_id=?1",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        let checkpoint = tx
            .query_row(
                "SELECT cursor, source_fingerprint, generation, version
                 FROM ingestion_checkpoints WHERE bus_name=?1 AND checkpoint_name=?2",
                params![bus_name, checkpoint_name],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;
        match (&expected_generation, &checkpoint) {
            (None, None) => {}
            (Some(expected), Some((_, fingerprint, generation, version)))
                if expected == generation
                    && fingerprint.as_deref() == Some(&expected_fingerprint) =>
            {
                tx.commit()?;
                return Ok(ExecutionSourceClaim {
                    claimed: true,
                    cursor: checkpoint.as_ref().map(|state| state.0.clone()),
                    source_fingerprint: fingerprint.clone(),
                    generation: Some(generation.clone()),
                    version: Some(*version),
                });
            }
            (_, Some((_, _, generation, version))) => {
                return Err(QueueError::CheckpointConflict {
                    checkpoint_name,
                    expected_generation,
                    expected_version: None,
                    actual_generation: Some(generation.clone()),
                    actual_version: Some(*version),
                });
            }
            (Some(_), None) => {
                return Err(QueueError::CheckpointConflict {
                    checkpoint_name,
                    expected_generation,
                    expected_version: None,
                    actual_generation: None,
                    actual_version: None,
                });
            }
        }
        tx.commit()?;
        Ok(ExecutionSourceClaim {
            claimed: true,
            cursor: None,
            source_fingerprint: None,
            generation: None,
            version: None,
        })
    }
    pub fn execution_extend_source_lease(
        &self,
        id: &str,
        receipt: &str,
        lease_ms: i64,
    ) -> Result<i64> {
        let now = now_ms();
        let until = now + lease_ms;
        let mut guard = self.connection();
        let conn = guard.as_mut().ok_or(QueueError::Closed)?;
        let changed=conn.execute("UPDATE event_bus_execution_runtime SET source_lease_until=?3 WHERE execution_id=?1 AND source_receipt=?2 AND source_completed_at IS NULL AND source_lease_until>?4",params![id,receipt,until,now])?;
        if changed != 1 {
            return Err(QueueError::ExecutionLeaseLost);
        }
        Ok(until)
    }
    pub fn execution_release_source_lease(&self, id: &str, receipt: &str) -> Result<bool> {
        let guard = self.connection();
        let conn = guard.as_ref().ok_or(QueueError::Closed)?;
        let changed=conn.execute("UPDATE event_bus_execution_runtime SET source_receipt=NULL,source_lease_until=NULL WHERE execution_id=?1 AND source_receipt=?2",params![id,receipt])?;
        if changed == 0 && !execution_exists(conn, id)? {
            return Err(QueueError::NotFound);
        }
        Ok(changed == 1)
    }
    pub fn execution_mark_source_completed_claimed(&self, id: &str, receipt: &str) -> Result<bool> {
        let now = now_ms();
        let mut guard = self.connection();
        let conn = guard.as_mut().ok_or(QueueError::Closed)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        validate_execution_claim(&tx, id, receipt, now)?;
        tx.execute("UPDATE event_bus_execution_runtime SET source_completed_at=COALESCE(source_completed_at,?2),source_receipt=NULL,source_lease_until=NULL WHERE execution_id=?1",params![id,now])?;
        tx.execute("UPDATE event_bus_executions SET source_completed=1,updated_at=?2 WHERE execution_id=?1",params![id,now])?;
        tx.commit()?;
        Ok(true)
    }
    pub fn execution_snapshot(&self, id: &str) -> Result<ExecutionRuntimeSnapshot> {
        let guard = self.connection();
        let conn = guard.as_ref().ok_or(QueueError::Closed)?;
        execution_snapshot_conn(conn, id)
    }
    pub fn execution_finalize_if_complete(&self, id: &str) -> Result<bool> {
        let now = now_ms();
        let mut guard = self.connection();
        let conn = guard.as_mut().ok_or(QueueError::Closed)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed=tx.execute("UPDATE event_bus_execution_runtime SET completed_at=COALESCE(completed_at,?2) WHERE execution_id=?1 AND source_completed_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM event_bus_execution_deliveries d JOIN messages m ON m.id=d.message_id WHERE d.execution_id=?1 AND m.status IN (0,1))",params![id,now])?;
        if changed == 0 && !execution_exists(&tx, id)? {
            return Err(QueueError::NotFound);
        }
        tx.commit()?;
        Ok(changed == 1)
    }

    pub fn ack_and_fanout(
        &self,
        queue_name: &str,
        id: i64,
        receipt: &str,
        entries: &[EnqueueEntry<'_>],
        max_attempts: i64,
    ) -> Result<Vec<i64>> {
        Ok(self
            .ack_and_fanout_outcomes(queue_name, id, receipt, entries, max_attempts)?
            .into_iter()
            .map(|outcome| outcome.id)
            .collect())
    }

    pub fn ack_and_fanout_outcomes(
        &self,
        queue_name: &str,
        id: i64,
        receipt: &str,
        entries: &[EnqueueEntry<'_>],
        max_attempts: i64,
    ) -> Result<Vec<EnqueueOutcome>> {
        let now = now_ms();
        let mut guard = self.connection();
        let conn = guard.as_mut().ok_or(QueueError::Closed)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let parent_executions = execution_memberships_for_message(&tx, id)?;
        let changed = tx.execute(
            "UPDATE messages SET
                status = 2,
                receipt = NULL,
                lease_until = NULL,
                updated_at = ?1
             WHERE id = ?2 AND queue = ?3 AND status = 1
                AND receipt = ?4 AND lease_until > ?5",
            params![now, id, queue_name, receipt, now],
        )?;
        if changed != 1 {
            return Err(QueueError::LeaseExpired);
        }
        let ids = insert_entries_in_transaction(&tx, entries, max_attempts, now)?;
        insert_delivery_edges(&tx, id, &ids)?;
        attach_execution_memberships(&tx, &parent_executions, &ids)?;
        #[cfg(feature = "__crash_test")]
        crate::failpoints::hit(crate::failpoints::Failpoint::AckFanoutBeforeCommit);
        tx.commit()?;
        Ok(ids)
    }

    /// Open a short-lived connection for a deadline-bounded enqueue attempt.
    ///
    /// The reusable connection is never reconfigured. Dropping this dedicated
    /// connection after `enqueue_batch_on_connection` succeeds cannot turn its
    /// already-confirmed commit into a cleanup error.
    pub fn open_attempt_connection(&self, busy_timeout_ms: u64) -> Result<Connection> {
        let conn = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_URI,
        )?;
        conn.pragma_update(None, "busy_timeout", busy_timeout_ms)?;
        conn.pragma_update(
            None,
            "synchronous",
            if self.fsync { "FULL" } else { "NORMAL" },
        )?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        Ok(conn)
    }

    /// Move one failed row back to ready while enforcing the same logical
    /// capacity under a BEGIN IMMEDIATE writer lock.
    pub fn retry_failed(
        &self,
        queue_name: &str,
        id: i64,
        max_pending_jobs: Option<i64>,
    ) -> Result<()> {
        let now = now_ms();
        let mut guard = self.connection();
        let conn = guard.as_mut().ok_or(QueueError::Closed)?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: bool = tx.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM messages WHERE id = ?1 AND queue = ?2 AND status = 3
            )",
            params![id, queue_name],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(QueueError::NotFound);
        }
        if let Some(limit) = max_pending_jobs {
            let pending: i64 = tx.query_row(
                "SELECT COUNT(*) FROM messages
                 WHERE queue = ?1 AND status IN (0, 1)",
                params![queue_name],
                |row| row.get(0),
            )?;
            if pending >= limit {
                return Err(QueueError::Full);
            }
        }
        let changed = tx.execute(
            "UPDATE messages SET
                status = 0,
                available_at = ?1,
                attempts = 0,
                receipt = NULL,
                lease_until = NULL,
                last_error = NULL,
                failure_reason = NULL,
                failure_category = NULL,
                updated_at = ?2
             WHERE id = ?3 AND queue = ?4 AND status = 3",
            params![now, now, id, queue_name],
        )?;
        if changed == 0 {
            return Err(QueueError::NotFound);
        }
        tx.execute(
            "UPDATE event_bus_execution_runtime SET completed_at = NULL
             WHERE execution_id IN (SELECT execution_id FROM event_bus_execution_deliveries WHERE message_id = ?1)",
            params![id],
        )?;
        tx.commit()?;
        Ok(())
    }
}

fn migrate_failure_reason(conn: &mut Connection) -> Result<()> {
    if has_column(conn, "failure_reason")? {
        return Ok(());
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if !has_column(&tx, "failure_reason")? {
        tx.execute("ALTER TABLE messages ADD COLUMN failure_reason TEXT", [])?;
    }
    if !has_column(&tx, "failure_reason")? {
        return Err(QueueError::Sqlite(rusqlite::Error::InvalidQuery));
    }
    tx.commit()?;
    Ok(())
}

fn migrate_failure_category(conn: &mut Connection) -> Result<()> {
    if has_column(conn, "failure_category")? {
        return Ok(());
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if !has_column(&tx, "failure_category")? {
        tx.execute("ALTER TABLE messages ADD COLUMN failure_category TEXT", [])?;
    }
    if !has_column(&tx, "failure_category")? {
        return Err(QueueError::Sqlite(rusqlite::Error::InvalidQuery));
    }
    tx.commit()?;
    Ok(())
}

fn migrate_event_identity(conn: &mut Connection) -> Result<()> {
    let key = has_column(conn, "dedup_key")?;
    let fingerprint = has_column(conn, "dedup_fingerprint")?;
    let index: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_messages_dedup_key')",
        [],
        |row| row.get(0),
    )?;
    if key && fingerprint && index {
        return Ok(());
    }
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if !has_column(&tx, "dedup_key")? {
        tx.execute("ALTER TABLE messages ADD COLUMN dedup_key TEXT", [])?;
    }
    if !has_column(&tx, "dedup_fingerprint")? {
        tx.execute("ALTER TABLE messages ADD COLUMN dedup_fingerprint TEXT", [])?;
    }
    tx.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_dedup_key
         ON messages(queue, dedup_key) WHERE dedup_key IS NOT NULL",
        [],
    )?;
    if !has_column(&tx, "dedup_key")? || !has_column(&tx, "dedup_fingerprint")? {
        return Err(QueueError::Sqlite(rusqlite::Error::InvalidQuery));
    }
    tx.commit()?;
    Ok(())
}

fn migrate_ingestion_checkpoints(conn: &mut Connection) -> Result<()> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'ingestion_checkpoints'
        )",
        [],
        |row| row.get(0),
    )?;
    // Opening an already-updated database must remain read-only: callers may
    // share it with an active writer, and there is no migration work to do.
    if exists && has_ingestion_checkpoint_column(conn, "generation")? {
        return Ok(());
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if !exists {
        tx.execute_batch(CHECKPOINTS_SCHEMA_SQL)?;
    } else if !has_ingestion_checkpoint_column(&tx, "generation")? {
        // SQLite cannot add a NOT NULL column without a constant default to
        // an existing table. Add it, backfill every existing incarnation in
        // this transaction, and rely on the fresh-table schema above to keep
        // it non-null for new databases.
        tx.execute(
            "ALTER TABLE ingestion_checkpoints ADD COLUMN generation TEXT",
            [],
        )?;
        tx.execute(
            "UPDATE ingestion_checkpoints
             SET generation = lower(hex(randomblob(16)))
             WHERE generation IS NULL",
            [],
        )?;
    }
    tx.commit()?;
    Ok(())
}

fn migrate_execution_membership(conn: &mut Connection) -> Result<()> {
    let executions: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'event_bus_executions')",
        [], |row| row.get(0),
    )?;
    let deliveries: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'event_bus_execution_deliveries')",
        [], |row| row.get(0),
    )?;
    let message_index: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_event_bus_execution_deliveries_message')",
        [], |row| row.get(0),
    )?;
    let edges = delivery_edges_exist(conn)?;
    if executions
        && deliveries
        && message_index
        && edges
        && membership_message_foreign_key_is_restrict(conn)?
    {
        return Ok(());
    }
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if executions && deliveries && !membership_message_foreign_key_is_restrict(&tx)? {
        tx.execute_batch(
            "ALTER TABLE event_bus_execution_deliveries RENAME TO event_bus_execution_deliveries_old;",
        )?;
        tx.execute_batch(EXECUTION_MEMBERSHIP_SCHEMA_SQL)?;
        tx.execute_batch(
            "INSERT INTO event_bus_execution_deliveries (execution_id, message_id)
             SELECT execution_id, message_id FROM event_bus_execution_deliveries_old;
             DROP TABLE event_bus_execution_deliveries_old;
             CREATE INDEX IF NOT EXISTS idx_event_bus_execution_deliveries_message
                ON event_bus_execution_deliveries(message_id);",
        )?;
    } else if !executions || !deliveries || !message_index || !edges {
        tx.execute_batch(EXECUTION_MEMBERSHIP_SCHEMA_SQL)?;
    }
    tx.commit()?;
    Ok(())
}

fn migrate_execution_runtime(conn: &mut Connection) -> Result<()> {
    let exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_bus_execution_runtime')", [], |r| r.get(0))?;
    if exists {
        return Ok(());
    }
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(EXECUTION_RUNTIME_SCHEMA_SQL)?;
    tx.commit()?;
    Ok(())
}

fn validate_execution_claim(tx: &Transaction<'_>, id: &str, receipt: &str, now: i64) -> Result<()> {
    let valid: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM event_bus_execution_runtime WHERE execution_id=?1 AND source_completed_at IS NULL AND source_receipt=?2 AND source_lease_until>?3)", params![id,receipt,now], |r| r.get(0))?;
    if valid {
        Ok(())
    } else if !execution_exists(tx, id)? {
        Err(QueueError::NotFound)
    } else {
        Err(QueueError::ExecutionLeaseLost)
    }
}

fn execution_snapshot_conn(conn: &Connection, id: &str) -> Result<ExecutionRuntimeSnapshot> {
    conn.query_row(
        "SELECT e.execution_id,e.source_name,r.checkpoint_name,r.source_fingerprint,r.checkpoint_generation,
         e.source_completed,r.source_completed_at,r.completed_at,r.items_committed,r.events_dispatched,r.events_unrouted,
         r.deliveries_inserted,r.deliveries_deduplicated,r.batches_committed,
         COUNT(d.message_id),COALESCE(SUM(m.status=0),0),COALESCE(SUM(m.status=1),0),COALESCE(SUM(m.status=2),0),COALESCE(SUM(m.status=3),0),
         r.source_lease_until,e.created_at,e.updated_at
         FROM event_bus_executions e JOIN event_bus_execution_runtime r USING(execution_id)
         LEFT JOIN event_bus_execution_deliveries d ON d.execution_id=e.execution_id LEFT JOIN messages m ON m.id=d.message_id
         WHERE e.execution_id=?1 GROUP BY e.execution_id", params![id], |r| Ok(ExecutionRuntimeSnapshot {
            execution_id:r.get(0)?,source_name:r.get(1)?,checkpoint_name:r.get(2)?,source_fingerprint:r.get(3)?,checkpoint_generation:r.get(4)?,source_completed:r.get::<_,i64>(5)?!=0,source_completed_at:r.get(6)?,completed_at:r.get(7)?,items_committed:r.get(8)?,events_dispatched:r.get(9)?,events_unrouted:r.get(10)?,deliveries_inserted:r.get(11)?,deliveries_deduplicated:r.get(12)?,batches_committed:r.get(13)?,total:r.get(14)?,ready:r.get(15)?,processing:r.get(16)?,acknowledged:r.get(17)?,failed:r.get(18)?,source_lease_until:r.get(19)?,created_at:r.get(20)?,updated_at:r.get(21)?
        })).optional()?.ok_or(QueueError::NotFound)
}

fn membership_message_foreign_key_is_restrict(conn: &Connection) -> Result<bool> {
    let mut statement = conn.prepare("PRAGMA foreign_key_list(event_bus_execution_deliveries)")?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(2)?, row.get::<_, String>(6)?))
    })?;
    for row in rows {
        let (table, action) = row?;
        if table == "messages" {
            return Ok(
                action.eq_ignore_ascii_case("RESTRICT") || action.eq_ignore_ascii_case("NO ACTION")
            );
        }
    }
    Ok(false)
}

fn delivery_edges_exist(conn: &Connection) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'event_bus_delivery_edges')",
        [], |row| row.get(0),
    ).map_err(QueueError::from)
}

fn has_ingestion_checkpoint_column(conn: &Connection, expected: &str) -> Result<bool> {
    let mut statement = conn.prepare("PRAGMA table_info(ingestion_checkpoints)")?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    for column in columns {
        if column? == expected {
            return Ok(true);
        }
    }
    Ok(false)
}

fn has_column(conn: &Connection, expected: &str) -> Result<bool> {
    let mut statement = conn.prepare("PRAGMA table_info(messages)")?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    for column in columns {
        if column? == expected {
            return Ok(true);
        }
    }
    Ok(false)
}

fn has_messages_table(conn: &Connection) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'messages'
        )",
        [],
        |row| row.get(0),
    )
    .map_err(QueueError::from)
}

fn enqueue_batch_on_connection(
    conn: &mut Connection,
    entries: &[EnqueueEntry<'_>],
    max_attempts: i64,
    capacity: &[CapacityPolicy<'_>],
    checkpoint: Option<&CheckpointUpdate>,
    execution_id: Option<&str>,
) -> Result<(Vec<EnqueueOutcome>, Option<CheckpointCommit>)> {
    let now = now_ms();

    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(QueueError::from)?;

    #[cfg(feature = "__crash_test")]
    crate::failpoints::hit(crate::failpoints::Failpoint::EnqueueAfterBegin);

    // Validate the CAS while the BEGIN IMMEDIATE writer lock is held, before
    // capacity planning or inserts. A stale ingester must get its terminal
    // CheckpointConflict rather than retrying Full forever (or seeing another
    // delivery error that masks the lost right to advance the cursor).
    if let Some(update) = checkpoint {
        validate_checkpoint_precondition(&tx, update)?;
    }

    enforce_capacity_policies(&tx, entries, capacity)?;

    let ids = insert_entries_in_transaction(&tx, entries, max_attempts, now)?;
    if let Some(execution_id) = execution_id {
        attach_execution_memberships(&tx, &[execution_id.to_owned()], &ids)?;
    }

    let new_version = match checkpoint {
        Some(update) => Some(apply_checkpoint_update(&tx, update, now)?),
        None => None,
    };

    #[cfg(feature = "__crash_test")]
    crate::failpoints::hit(crate::failpoints::Failpoint::EnqueueBeforeCommit);
    tx.commit().map_err(QueueError::from)?;
    Ok((ids, new_version))
}

#[allow(clippy::too_many_arguments)]
pub fn enqueue_batch_claimed_execution(
    conn: &mut Connection,
    entries: &[EnqueueEntry<'_>],
    max_attempts: i64,
    capacity: &[CapacityPolicy<'_>],
    checkpoint: &CheckpointUpdate,
    execution_id: &str,
    receipt: &str,
    dispatched: i64,
    unrouted: i64,
) -> Result<(Vec<EnqueueOutcome>, CheckpointCommit)> {
    let now = now_ms();
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    validate_execution_claim(&tx, execution_id, receipt, now)?;
    validate_checkpoint_precondition(&tx, checkpoint)?;
    enforce_capacity_policies(&tx, entries, capacity)?;
    let outcomes = insert_entries_in_transaction(&tx, entries, max_attempts, now)?;
    attach_execution_memberships(&tx, &[execution_id.to_owned()], &outcomes)?;
    let commit = apply_checkpoint_update(&tx, checkpoint, now)?;
    let inserted = outcomes.iter().filter(|o| o.inserted).count() as i64;
    let deduplicated = outcomes.len() as i64 - inserted;
    tx.execute("UPDATE event_bus_execution_runtime SET checkpoint_generation=COALESCE(checkpoint_generation,?2),items_committed=items_committed+?3,events_dispatched=events_dispatched+?4,events_unrouted=events_unrouted+?5,deliveries_inserted=deliveries_inserted+?6,deliveries_deduplicated=deliveries_deduplicated+?7,batches_committed=batches_committed+1 WHERE execution_id=?1",params![execution_id,commit.generation,checkpoint.items_committed,dispatched,unrouted,inserted,deduplicated])?;
    tx.commit()?;
    Ok((outcomes, commit))
}

fn execution_memberships_for_message(tx: &Transaction<'_>, message_id: i64) -> Result<Vec<String>> {
    let mut statement = tx
        .prepare("SELECT execution_id FROM event_bus_execution_deliveries WHERE message_id = ?1")?;
    let memberships = statement
        .query_map(params![message_id], |row| row.get(0))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(QueueError::from)?;
    Ok(memberships)
}

fn execution_exists(conn: &Connection, execution_id: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM event_bus_executions WHERE execution_id = ?1)",
        params![execution_id],
        |row| row.get(0),
    )
    .map_err(QueueError::from)
}

fn insert_delivery_edges(
    tx: &Transaction<'_>,
    parent_message_id: i64,
    outcomes: &[EnqueueOutcome],
) -> Result<()> {
    let mut statement = tx.prepare(
        "INSERT OR IGNORE INTO event_bus_delivery_edges (parent_message_id, child_message_id)
         VALUES (?1, ?2)",
    )?;
    for outcome in outcomes {
        statement.execute(params![parent_message_id, outcome.id])?;
    }
    Ok(())
}

fn attach_execution_memberships(
    tx: &Transaction<'_>,
    execution_ids: &[String],
    outcomes: &[EnqueueOutcome],
) -> Result<()> {
    if execution_ids.is_empty() || outcomes.is_empty() {
        return Ok(());
    }
    for execution_id in execution_ids {
        for outcome in outcomes {
            tx.execute(
                "WITH RECURSIVE descendants(message_id) AS (
                    VALUES (?2)
                    UNION
                    SELECT edge.child_message_id
                    FROM event_bus_delivery_edges edge
                    JOIN descendants ON edge.parent_message_id = descendants.message_id
                 )
                 INSERT OR IGNORE INTO event_bus_execution_deliveries (execution_id, message_id)
                 SELECT ?1, message_id FROM descendants",
                params![execution_id, outcome.id],
            )?;
        }
    }
    Ok(())
}

/// Apply one ingestion checkpoint update inside the open transaction.
///
/// Absent expected generation/version creates the row at version 1; matching
/// tokens bump the stored version. Any mismatch raises
/// `CheckpointConflict`, which propagates before the commit and rolls back the
/// whole transaction, including the delivery inserts.
#[derive(Debug, PartialEq, Eq)]
pub struct CheckpointCommit {
    pub generation: String,
    pub version: i64,
}

fn checkpoint_conflict(update: &CheckpointUpdate, actual: Option<(String, i64)>) -> QueueError {
    QueueError::CheckpointConflict {
        checkpoint_name: update.checkpoint_name.clone(),
        expected_generation: update.expected_generation.clone(),
        expected_version: update.expected_version,
        actual_generation: actual.as_ref().map(|(generation, _)| generation.clone()),
        actual_version: actual.map(|(_, version)| version),
    }
}

/// Check the complete checkpoint token before any capacity or delivery work.
/// The surrounding transaction owns SQLite's writer lock, so the subsequent
/// apply cannot race with this validation.
fn validate_checkpoint_precondition(tx: &Transaction<'_>, update: &CheckpointUpdate) -> Result<()> {
    let actual = stored_checkpoint_token(tx, &update.bus_name, &update.checkpoint_name)?;
    match (
        &update.expected_generation,
        update.expected_version,
        &actual,
    ) {
        (None, None, None) => Ok(()),
        (Some(expected_generation), Some(expected_version), Some((generation, version)))
            if expected_generation == generation && expected_version == *version =>
        {
            Ok(())
        }
        _ => Err(checkpoint_conflict(update, actual)),
    }
}

fn apply_checkpoint_update(
    tx: &Transaction<'_>,
    update: &CheckpointUpdate,
    now: i64,
) -> Result<CheckpointCommit> {
    match (&update.expected_generation, update.expected_version) {
        (None, None) => {
            tx.execute(
                "INSERT INTO ingestion_checkpoints (
                    bus_name, checkpoint_name, cursor, source_fingerprint,
                    generation, version, items_committed, batches_committed,
                    created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, lower(hex(randomblob(16))), 1, ?5, 1, ?6, ?6)",
                params![
                    update.bus_name,
                    update.checkpoint_name,
                    update.new_cursor,
                    update.source_fingerprint,
                    update.items_committed,
                    now,
                ],
            )?;
            let (generation, version) =
                stored_checkpoint_token(tx, &update.bus_name, &update.checkpoint_name)?
                    .expect("inserted checkpoint must be readable in its transaction");
            Ok(CheckpointCommit {
                generation,
                version,
            })
        }
        (Some(generation), Some(expected)) => {
            let changed = tx.execute(
                "UPDATE ingestion_checkpoints SET
                    cursor = ?3,
                    version = version + 1,
                    items_committed = items_committed + ?4,
                    batches_committed = batches_committed + 1,
                    updated_at = ?5
                 WHERE bus_name = ?1 AND checkpoint_name = ?2
                   AND generation = ?6 AND version = ?7",
                params![
                    update.bus_name,
                    update.checkpoint_name,
                    update.new_cursor,
                    update.items_committed,
                    now,
                    generation,
                    expected,
                ],
            )?;
            if changed == 0 {
                return Err(checkpoint_conflict(
                    update,
                    stored_checkpoint_token(tx, &update.bus_name, &update.checkpoint_name)?,
                ));
            }
            Ok(CheckpointCommit {
                generation: generation.clone(),
                version: expected + 1,
            })
        }
        _ => Err(checkpoint_conflict(
            update,
            stored_checkpoint_token(tx, &update.bus_name, &update.checkpoint_name)?,
        )),
    }
}

fn stored_checkpoint_token(
    tx: &Transaction<'_>,
    bus_name: &str,
    checkpoint_name: &str,
) -> Result<Option<(String, i64)>> {
    tx.query_row(
        "SELECT generation, version FROM ingestion_checkpoints
         WHERE bus_name = ?1 AND checkpoint_name = ?2",
        params![bus_name, checkpoint_name],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(QueueError::from)
}

/// Identity used for capacity planning: within a queue, entries sharing an
/// identity collapse into at most one new row. `dedup_key` takes precedence
/// over `job_id`; entries without identity always insert a new row.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum CapacityIdentity<'a> {
    DedupKey(&'a str),
    JobId(&'a str),
}

/// Plan exactly which rows a batch would insert and enforce the per-queue
/// pending limits, all under the open BEGIN IMMEDIATE transaction.
///
/// Conflicting duplicate policies for the same queue are rejected; identical
/// duplicates are normalized away. Planning only checks identity existence —
/// payload/fingerprint conflicts remain the insert phase's job and roll the
/// whole transaction back.
fn enforce_capacity_policies(
    tx: &Transaction<'_>,
    entries: &[EnqueueEntry<'_>],
    capacity: &[CapacityPolicy<'_>],
) -> Result<()> {
    if capacity.is_empty() {
        return Ok(());
    }
    let mut limits: HashMap<&str, i64> = HashMap::new();
    for policy in capacity {
        match limits.entry(policy.queue_name) {
            std::collections::hash_map::Entry::Occupied(existing) => {
                if *existing.get() != policy.max_pending_jobs {
                    return Err(QueueError::ConflictingCapacityPolicies);
                }
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(policy.max_pending_jobs);
            }
        }
    }

    let mut anonymous: HashMap<&str, i64> = HashMap::new();
    let mut identities: HashMap<&str, HashSet<CapacityIdentity<'_>>> = HashMap::new();
    for entry in entries {
        if !limits.contains_key(entry.queue_name) {
            continue;
        }
        let identity = match (entry.dedup_key, entry.job_id) {
            (Some(key), _) => Some(CapacityIdentity::DedupKey(key)),
            (None, Some(job_id)) => Some(CapacityIdentity::JobId(job_id)),
            (None, None) => None,
        };
        match identity {
            Some(identity) => {
                identities
                    .entry(entry.queue_name)
                    .or_default()
                    .insert(identity);
            }
            None => *anonymous.entry(entry.queue_name).or_insert(0) += 1,
        }
    }

    for (&queue_name, &max_pending_jobs) in &limits {
        let mut new_rows = anonymous.get(queue_name).copied().unwrap_or(0);
        if let Some(queue_identities) = identities.get(queue_name) {
            for identity in queue_identities {
                let exists: bool = match identity {
                    CapacityIdentity::DedupKey(key) => tx.query_row(
                        "SELECT EXISTS(
                                SELECT 1 FROM messages WHERE queue = ?1 AND dedup_key = ?2
                            )",
                        params![queue_name, key],
                        |row| row.get(0),
                    )?,
                    CapacityIdentity::JobId(job_id) => tx.query_row(
                        "SELECT EXISTS(
                                SELECT 1 FROM messages WHERE queue = ?1 AND job_id = ?2
                            )",
                        params![queue_name, job_id],
                        |row| row.get(0),
                    )?,
                };
                if !exists {
                    new_rows += 1;
                }
            }
        }
        if new_rows > 0 {
            if new_rows > max_pending_jobs {
                return Err(QueueError::FullImpossible);
            }
            let pending: i64 = tx.query_row(
                "SELECT COUNT(*) FROM messages
                     WHERE queue = ?1 AND status IN (0, 1)",
                params![queue_name],
                |row| row.get(0),
            )?;
            if pending.saturating_add(new_rows) > max_pending_jobs {
                return Err(QueueError::Full);
            }
        }
    }
    Ok(())
}

fn insert_entries_in_transaction(
    tx: &Transaction<'_>,
    entries: &[EnqueueEntry<'_>],
    max_attempts: i64,
    now: i64,
) -> Result<Vec<EnqueueOutcome>> {
    let mut insert = tx
        .prepare(
            "INSERT OR IGNORE INTO messages (
                    queue, payload, status, attempts, max_attempts,
                    available_at, lease_until, receipt, job_id, dedup_key,
                    dedup_fingerprint, created_at, updated_at
                ) VALUES (?1, ?2, ?3, 0, ?4, ?5, NULL, NULL, ?6, ?7, ?8, ?9, ?10)",
        )
        .map_err(QueueError::from)?;

    let mut ids = Vec::with_capacity(entries.len());
    for entry in entries {
        if entry.dedup_key.is_some() != entry.dedup_fingerprint.is_some() {
            return Err(QueueError::InvalidDeduplicationMetadata);
        }
        let changed = insert
            .execute(params![
                entry.queue_name,
                entry.payload,
                0i64, // STATUS_READY
                max_attempts,
                now,
                entry.job_id,
                entry.dedup_key,
                entry.dedup_fingerprint,
                now,
                now,
            ])
            .map_err(QueueError::from)?;

        if changed == 1 {
            ids.push(EnqueueOutcome {
                id: tx.last_insert_rowid(),
                inserted: true,
            });
            continue;
        }
        let by_job = entry
            .job_id
            .map(|job_id| {
                tx.query_row(
                    "SELECT id, dedup_key, dedup_fingerprint FROM messages
                     WHERE queue = ?1 AND job_id = ?2",
                    params![entry.queue_name, job_id],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, Option<String>>(2)?,
                        ))
                    },
                )
                .optional()
            })
            .transpose()?
            .flatten();
        let by_key = entry
            .dedup_key
            .map(|key| {
                tx.query_row(
                    "SELECT id, dedup_fingerprint FROM messages
                     WHERE queue = ?1 AND dedup_key = ?2",
                    params![entry.queue_name, key],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?)),
                )
                .optional()
            })
            .transpose()?
            .flatten();
        if let (Some(job), Some(key)) = (&by_job, &by_key) {
            if job.0 != key.0 {
                return Err(QueueError::DeduplicationConflict);
            }
        }
        if let Some((_, stored_key, stored_fingerprint)) = &by_job {
            if entry.dedup_key.is_some()
                && (stored_key.as_deref() != entry.dedup_key
                    || stored_fingerprint.as_deref() != entry.dedup_fingerprint)
            {
                return Err(QueueError::DeduplicationConflict);
            }
        }
        if let Some((_, stored_fingerprint)) = &by_key {
            if stored_fingerprint.as_deref() != entry.dedup_fingerprint {
                return Err(QueueError::DeduplicationConflict);
            }
        }
        let id = by_job
            .as_ref()
            .map(|row| row.0)
            .or_else(|| by_key.as_ref().map(|row| row.0))
            .ok_or(QueueError::NotFound)?;
        ids.push(EnqueueOutcome {
            id,
            inserted: false,
        });
    }
    drop(insert);

    Ok(ids)
}

fn stable_database_path(path: &str) -> Result<PathBuf> {
    // SQLite URI filenames have their own path semantics. The public Python
    // facade always passes filesystem paths, which are made absolute here.
    if path.starts_with("file:") {
        return Ok(PathBuf::from(path));
    }
    Ok(std::path::absolute(path)?)
}

pub(crate) fn sqlite_sidecar_path(database_path: &Path, suffix: &str) -> PathBuf {
    let mut path = OsString::from(database_path.as_os_str());
    path.push(suffix);
    PathBuf::from(path)
}

fn enable_wal(conn: &Connection) -> Result<()> {
    let deadline = Instant::now() + Duration::from_millis(BUSY_TIMEOUT_MS);

    loop {
        match conn.pragma_update(None, "journal_mode", "WAL") {
            Ok(()) => return Ok(()),
            Err(error)
                if matches!(
                    error,
                    rusqlite::Error::SqliteFailure(ref failure, _)
                        if matches!(failure.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
                ) && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    fn open_storage() -> (tempfile_guard::TempDir, Storage) {
        let dir = tempfile_guard::TempDir::new();
        let path = dir.path().join("test.db");
        let storage = Storage::new(path.to_str().unwrap(), false).unwrap();
        (dir, storage)
    }

    #[test]
    fn failure_reason_migration_fast_path_does_not_take_writer_lock() {
        let dir = tempfile_guard::TempDir::new();
        let path = dir.path().join("migrated.db");
        let setup = Connection::open(&path).unwrap();
        setup.execute_batch(SCHEMA_SQL).unwrap();
        drop(setup);

        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        let mut tested = Connection::open(&path).unwrap();
        tested.pragma_update(None, "busy_timeout", 1).unwrap();

        migrate_failure_reason(&mut tested).unwrap();
        blocker.execute_batch("ROLLBACK").unwrap();
    }

    #[test]
    fn failure_category_migration_fast_path_does_not_take_writer_lock() {
        let dir = tempfile_guard::TempDir::new();
        let path = dir.path().join("migrated.db");
        let setup = Connection::open(&path).unwrap();
        setup.execute_batch(SCHEMA_SQL).unwrap();
        drop(setup);

        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        let mut tested = Connection::open(&path).unwrap();
        tested.pragma_update(None, "busy_timeout", 1).unwrap();

        migrate_failure_category(&mut tested).unwrap();
        blocker.execute_batch("ROLLBACK").unwrap();
    }

    #[test]
    fn checkpoint_migration_fast_path_does_not_take_writer_lock() {
        let dir = tempfile_guard::TempDir::new();
        let path = dir.path().join("migrated.db");
        let setup = Connection::open(&path).unwrap();
        setup.execute_batch(SCHEMA_SQL).unwrap();
        drop(setup);

        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();

        let mut tested = Connection::open(&path).unwrap();
        tested.pragma_update(None, "busy_timeout", 1).unwrap();

        migrate_ingestion_checkpoints(&mut tested).unwrap();
        blocker.execute_batch("ROLLBACK").unwrap();
    }

    #[test]
    fn execution_membership_migration_restricts_tracked_messages() {
        let dir = tempfile_guard::TempDir::new();
        let path = dir.path().join("legacy-membership.db");
        let mut conn = Connection::open(&path).unwrap();
        conn.execute_batch(BASE_SCHEMA_SQL).unwrap();
        conn.execute_batch(CHECKPOINTS_SCHEMA_SQL).unwrap();
        conn.execute_batch(
            "CREATE TABLE event_bus_executions (
                execution_id TEXT PRIMARY KEY, bus_name TEXT NOT NULL, source_name TEXT NOT NULL,
                checkpoint_name TEXT, source_completed INTEGER NOT NULL,
                created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
             );
             CREATE TABLE event_bus_execution_deliveries (
                execution_id TEXT NOT NULL, message_id INTEGER NOT NULL,
                PRIMARY KEY (execution_id, message_id),
                FOREIGN KEY (execution_id) REFERENCES event_bus_executions(execution_id) ON DELETE CASCADE,
                FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
             );
             CREATE INDEX idx_event_bus_execution_deliveries_message
                ON event_bus_execution_deliveries(message_id);",
        )
        .unwrap();

        migrate_execution_membership(&mut conn).unwrap();

        assert!(delivery_edges_exist(&conn).unwrap());
        assert!(membership_message_foreign_key_is_restrict(&conn).unwrap());
        let index: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index'
                 AND name = 'idx_event_bus_execution_deliveries_message')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(index);
    }

    #[test]
    fn enqueue_batch_vazio_nao_abre_transacao() {
        let (_dir, storage) = open_storage();
        let ids = storage.enqueue_batch(&[], 3, &[], None).unwrap();
        assert!(ids.is_empty());
    }

    #[test]
    fn enqueue_rejects_incomplete_deduplication_metadata_as_invalid_input() {
        let (_dir, storage) = open_storage();
        let entries = [EnqueueEntry {
            queue_name: "events",
            payload: b"payload",
            job_id: Some("occurrence"),
            dedup_key: Some("identity"),
            dedup_fingerprint: None,
        }];

        assert!(matches!(
            storage.enqueue_batch_outcomes(&entries, 3, &[], None),
            Err(QueueError::InvalidDeduplicationMetadata)
        ));
    }

    #[test]
    fn enqueue_batch_retorna_ids_na_ordem() {
        let (_dir, storage) = open_storage();
        let entries = [
            EnqueueEntry {
                queue_name: "q",
                payload: b"a",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"b",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"c",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];
        let ids = storage.enqueue_batch(&entries, 3, &[], None).unwrap();
        assert_eq!(ids.len(), 3);
        assert!(ids[0] < ids[1] && ids[1] < ids[2]);
    }

    fn lease_message(storage: &Storage, id: i64, queue: &str, receipt: &str) {
        let mut guard = storage.connection();
        let conn = guard.as_mut().unwrap();
        conn.execute(
            "UPDATE messages SET status = 1, attempts = 1, receipt = ?1,
                lease_until = ?2 WHERE id = ?3 AND queue = ?4",
            params![receipt, now_ms() + 60_000, id, queue],
        )
        .unwrap();
    }

    #[test]
    fn ack_and_fanout_acknowledges_and_inserts_targets_in_order() {
        let (_dir, storage) = open_storage();
        let origin = [EnqueueEntry {
            queue_name: "origin",
            payload: b"parent",
            job_id: Some("parent-id"),
            dedup_key: None,
            dedup_fingerprint: None,
        }];
        let origin_id = storage.enqueue_batch(&origin, 4, &[], None).unwrap()[0];
        lease_message(&storage, origin_id, "origin", "receipt");
        let targets = [
            EnqueueEntry {
                queue_name: "target-b",
                payload: b"child",
                job_id: Some("child-id"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "target-a",
                payload: b"child",
                job_id: Some("child-id"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];

        let ids = storage
            .ack_and_fanout("origin", origin_id, "receipt", &targets, 4)
            .unwrap();

        assert_eq!(ids.len(), 2);
        let mut guard = storage.connection();
        let conn = guard.as_mut().unwrap();
        let status: i64 = conn
            .query_row(
                "SELECT status FROM messages WHERE id = ?1",
                params![origin_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, 2);
        for (index, queue) in ["target-b", "target-a"].iter().enumerate() {
            let row: (i64, i64) = conn
                .query_row(
                    "SELECT id, max_attempts FROM messages
                     WHERE queue = ?1 AND job_id = 'child-id'",
                    params![queue],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(row, (ids[index], 4));
        }
    }

    #[test]
    fn ack_and_fanout_deduplicates_and_rejects_stale_receipt_without_inserts() {
        let (_dir, storage) = open_storage();
        let origin = [EnqueueEntry {
            queue_name: "origin",
            payload: b"parent",
            job_id: None,
            dedup_key: None,
            dedup_fingerprint: None,
        }];
        let origin_id = storage.enqueue_batch(&origin, 3, &[], None).unwrap()[0];
        lease_message(&storage, origin_id, "origin", "valid");
        let existing = [EnqueueEntry {
            queue_name: "target",
            payload: b"first",
            job_id: Some("child"),
            dedup_key: None,
            dedup_fingerprint: None,
        }];
        let existing_id = storage.enqueue_batch(&existing, 3, &[], None).unwrap()[0];
        let targets = [
            EnqueueEntry {
                queue_name: "target",
                payload: b"second",
                job_id: Some("child"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "new-target",
                payload: b"second",
                job_id: Some("child"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];

        assert!(matches!(
            storage.ack_and_fanout("origin", origin_id, "wrong", &targets, 3),
            Err(QueueError::LeaseExpired)
        ));

        let mut guard = storage.connection();
        let conn = guard.as_mut().unwrap();
        let new_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE queue = 'new-target'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(new_count, 0);
        drop(guard);

        let ids = storage
            .ack_and_fanout("origin", origin_id, "valid", &targets, 3)
            .unwrap();
        assert_eq!(ids[0], existing_id);
    }

    #[test]
    fn ack_and_fanout_rolls_back_ack_and_all_targets_on_insert_error() {
        let (_dir, storage) = open_storage();
        let origin = [EnqueueEntry {
            queue_name: "origin",
            payload: b"parent",
            job_id: None,
            dedup_key: None,
            dedup_fingerprint: None,
        }];
        let origin_id = storage.enqueue_batch(&origin, 3, &[], None).unwrap()[0];
        lease_message(&storage, origin_id, "origin", "receipt");
        {
            let mut guard = storage.connection();
            guard
                .as_mut()
                .unwrap()
                .execute_batch(
                    "CREATE TRIGGER reject_second_target
                     BEFORE INSERT ON messages
                     WHEN NEW.queue = 'target-2'
                     BEGIN SELECT RAISE(ABORT, 'injected insert failure'); END;",
                )
                .unwrap();
        }
        let targets = [
            EnqueueEntry {
                queue_name: "target-1",
                payload: b"child",
                job_id: Some("child"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "target-2",
                payload: b"child",
                job_id: Some("child"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];

        assert!(storage
            .ack_and_fanout("origin", origin_id, "receipt", &targets, 3)
            .is_err());

        let mut guard = storage.connection();
        let conn = guard.as_mut().unwrap();
        let status: i64 = conn
            .query_row(
                "SELECT status FROM messages WHERE id = ?1",
                params![origin_id],
                |row| row.get(0),
            )
            .unwrap();
        let target_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE queue LIKE 'target-%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, 1);
        assert_eq!(target_count, 0);
    }

    #[test]
    fn enqueue_batch_dedup_por_job_id() {
        let (_dir, storage) = open_storage();
        let first = storage
            .enqueue_batch(
                &[EnqueueEntry {
                    queue_name: "q",
                    payload: b"orig",
                    job_id: Some("j1"),
                    dedup_key: None,
                    dedup_fingerprint: None,
                }],
                3,
                &[],
                None,
            )
            .unwrap();
        // Mesmo job_id repetido no mesmo batch e em batch posterior.
        let entries = [
            EnqueueEntry {
                queue_name: "q",
                payload: b"dup",
                job_id: Some("j1"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"dup2",
                job_id: Some("j1"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"outro",
                job_id: Some("j2"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];
        let ids = storage.enqueue_batch(&entries, 3, &[], None).unwrap();
        assert_eq!(ids[0], first[0]);
        assert_eq!(ids[1], first[0]);
        assert_ne!(ids[2], first[0]);
    }

    #[test]
    fn enqueue_batch_mesmo_job_id_em_filas_diferentes() {
        let (_dir, storage) = open_storage();
        let entries = [
            EnqueueEntry {
                queue_name: "qa",
                payload: b"x",
                job_id: Some("j1"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "qb",
                payload: b"x",
                job_id: Some("j1"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];
        let ids = storage.enqueue_batch(&entries, 3, &[], None).unwrap();
        assert_ne!(ids[0], ids[1]);
    }

    fn check_capacity_check_and_batch_insert_are_atomic() {
        let (_dir, storage) = open_storage();
        let policy = CapacityPolicy {
            queue_name: "q",
            max_pending_jobs: 2,
        };
        let first = [EnqueueEntry {
            queue_name: "q",
            payload: b"first",
            job_id: None,
            dedup_key: None,
            dedup_fingerprint: None,
        }];
        storage.enqueue_batch(&first, 3, &[policy], None).unwrap();
        let rejected = [
            EnqueueEntry {
                queue_name: "q",
                payload: b"second",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"third",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];

        assert!(matches!(
            storage.enqueue_batch(&rejected, 3, &[policy], None),
            Err(QueueError::Full)
        ));
        let guard = storage.connection();
        let count: i64 = guard
            .as_ref()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    fn check_capacity_counts_only_new_distinct_job_ids() {
        let (_dir, storage) = open_storage();
        let policy = CapacityPolicy {
            queue_name: "q",
            max_pending_jobs: 2,
        };
        let existing = [EnqueueEntry {
            queue_name: "q",
            payload: b"existing",
            job_id: Some("existing"),
            dedup_key: None,
            dedup_fingerprint: None,
        }];
        let existing_id = storage
            .enqueue_batch(&existing, 3, &[policy], None)
            .unwrap()[0];
        let mixed = [
            EnqueueEntry {
                queue_name: "q",
                payload: b"ignored",
                job_id: Some("existing"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"new",
                job_id: Some("new"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"also ignored",
                job_id: Some("new"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];

        let ids = storage.enqueue_batch(&mixed, 3, &[policy], None).unwrap();
        assert_eq!(ids[0], existing_id);
        assert_eq!(ids[1], ids[2]);
    }

    fn check_zero_new_rows_are_allowed_above_limit_in_every_state() {
        let (_dir, storage) = open_storage();
        let job_ids = ["ready", "processing", "acked", "failed"];
        let entries: Vec<_> = job_ids
            .iter()
            .map(|job_id| EnqueueEntry {
                queue_name: "q",
                payload: b"original",
                job_id: Some(job_id),
                dedup_key: None,
                dedup_fingerprint: None,
            })
            .collect();
        let original_ids = storage.enqueue_batch(&entries, 3, &[], None).unwrap();
        {
            let guard = storage.connection();
            let conn = guard.as_ref().unwrap();
            for (id, status) in original_ids.iter().zip([0i64, 1, 2, 3]) {
                conn.execute(
                    "UPDATE messages SET status = ?1 WHERE id = ?2",
                    params![status, id],
                )
                .unwrap();
            }
        }
        let duplicates: Vec<_> = job_ids
            .iter()
            .flat_map(|job_id| {
                [b"duplicate-one".as_slice(), b"duplicate-two".as_slice()].map(move |payload| {
                    EnqueueEntry {
                        queue_name: "q",
                        payload,
                        job_id: Some(job_id),
                        dedup_key: None,
                        dedup_fingerprint: None,
                    }
                })
            })
            .collect();
        let policy = CapacityPolicy {
            queue_name: "q",
            max_pending_jobs: 1,
        };

        let returned = storage
            .enqueue_batch(&duplicates, 3, &[policy], None)
            .unwrap();

        let expected: Vec<_> = original_ids.iter().flat_map(|id| [*id, *id]).collect();
        assert_eq!(returned, expected);
        let new_entry = [EnqueueEntry {
            queue_name: "q",
            payload: b"new",
            job_id: Some("new"),
            dedup_key: None,
            dedup_fingerprint: None,
        }];
        assert!(matches!(
            storage.enqueue_batch(&new_entry, 3, &[policy], None),
            Err(QueueError::Full)
        ));
    }

    fn check_impossible_batch_is_typed_and_never_writes() {
        let (_dir, storage) = open_storage();
        let policy = CapacityPolicy {
            queue_name: "q",
            max_pending_jobs: 2,
        };
        let entries = [
            EnqueueEntry {
                queue_name: "q",
                payload: b"one",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"two",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"three",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];

        assert!(matches!(
            storage.enqueue_batch(&entries, 3, &[policy], None),
            Err(QueueError::FullImpossible)
        ));
        let distinct_ids = [
            EnqueueEntry {
                queue_name: "q",
                payload: b"one",
                job_id: Some("one"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"two",
                job_id: Some("two"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"three",
                job_id: Some("three"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];
        assert!(matches!(
            storage.enqueue_batch(&distinct_ids, 3, &[policy], None),
            Err(QueueError::FullImpossible)
        ));
        let guard = storage.connection();
        let count: i64 = guard
            .as_ref()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    fn check_capacity_is_scoped_to_one_logical_queue() {
        let (_dir, storage) = open_storage();
        let entries = [
            EnqueueEntry {
                queue_name: "alpha",
                payload: b"a",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "beta",
                payload: b"b",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];
        storage.enqueue_batch(&entries, 3, &[], None).unwrap();

        let alpha_duplicate = [EnqueueEntry {
            queue_name: "alpha",
            payload: b"blocked",
            job_id: None,
            dedup_key: None,
            dedup_fingerprint: None,
        }];
        assert!(matches!(
            storage.enqueue_batch(
                &alpha_duplicate,
                3,
                &[CapacityPolicy {
                    queue_name: "alpha",
                    max_pending_jobs: 1,
                }],
                None,
            ),
            Err(QueueError::Full)
        ));
        let beta_duplicate = [EnqueueEntry {
            queue_name: "beta",
            payload: b"allowed",
            job_id: None,
            dedup_key: None,
            dedup_fingerprint: None,
        }];
        storage
            .enqueue_batch(
                &beta_duplicate,
                3,
                &[CapacityPolicy {
                    queue_name: "beta",
                    max_pending_jobs: 2,
                }],
                None,
            )
            .unwrap();
    }

    fn check_opening_below_existing_pending_does_not_delete_rows() {
        let (_dir, storage) = open_storage();
        let entries = [
            EnqueueEntry {
                queue_name: "q",
                payload: b"a",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"b",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];
        storage.enqueue_batch(&entries, 3, &[], None).unwrap();
        let extra = [EnqueueEntry {
            queue_name: "q",
            payload: b"extra",
            job_id: None,
            dedup_key: None,
            dedup_fingerprint: None,
        }];

        assert!(matches!(
            storage.enqueue_batch(
                &extra,
                3,
                &[CapacityPolicy {
                    queue_name: "q",
                    max_pending_jobs: 1,
                }],
                None,
            ),
            Err(QueueError::Full)
        ));
        let guard = storage.connection();
        let count: i64 = guard
            .as_ref()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    fn check_two_connections_never_oversubscribe_capacity() {
        let dir = tempfile_guard::TempDir::new();
        let path = dir.path().join("shared.db");
        let first = Storage::new(path.to_str().unwrap(), false).unwrap();
        let second = Storage::new(path.to_str().unwrap(), false).unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let handles: Vec<_> = [first, second]
            .into_iter()
            .map(|storage| {
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let entry = [EnqueueEntry {
                        queue_name: "q",
                        payload: b"payload",
                        job_id: None,
                        dedup_key: None,
                        dedup_fingerprint: None,
                    }];
                    barrier.wait();
                    storage.enqueue_batch(
                        &entry,
                        3,
                        &[CapacityPolicy {
                            queue_name: "q",
                            max_pending_jobs: 1,
                        }],
                        None,
                    )
                })
            })
            .collect();
        barrier.wait();
        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(QueueError::Full)))
                .count(),
            1
        );
    }

    fn check_sqlite_busy_is_not_reported_as_full_and_timeout_is_restored() {
        let (_dir, storage) = open_storage();
        let path = storage.path().to_owned();
        let blocker = Connection::open(path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let entry = [EnqueueEntry {
            queue_name: "q",
            payload: b"payload",
            job_id: None,
            dedup_key: None,
            dedup_fingerprint: None,
        }];

        let error = storage
            .enqueue_batch(
                &entry,
                3,
                &[CapacityPolicy {
                    queue_name: "q",
                    max_pending_jobs: 1,
                }],
                Some(0),
            )
            .unwrap_err();

        assert!(matches!(error, QueueError::Sqlite(_)));
        assert!(!matches!(error, QueueError::Full));
        blocker.execute_batch("ROLLBACK").unwrap();
        let guard = storage.connection();
        let timeout: i64 = guard
            .as_ref()
            .unwrap()
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();
        assert_eq!(timeout, BUSY_TIMEOUT_MS as i64);
    }

    fn check_bounded_attempt_connection_preserves_confirmation_boundary() {
        let (_dir, storage) = open_storage();
        {
            let attempt = storage.open_attempt_connection(17).unwrap();
            let busy_timeout: i64 = attempt
                .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
                .unwrap();
            let journal_mode: String = attempt
                .query_row("PRAGMA journal_mode", [], |row| row.get(0))
                .unwrap();
            let synchronous: i64 = attempt
                .query_row("PRAGMA synchronous", [], |row| row.get(0))
                .unwrap();
            let foreign_keys: i64 = attempt
                .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
                .unwrap();
            assert_eq!(busy_timeout, 17);
            assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
            assert_eq!(synchronous, 1); // NORMAL
            assert_eq!(foreign_keys, 1);
        }
        let entry = [EnqueueEntry {
            queue_name: "q",
            payload: b"committed",
            job_id: None,
            dedup_key: None,
            dedup_fingerprint: None,
        }];
        let ids = storage
            .enqueue_batch(
                &entry,
                3,
                &[CapacityPolicy {
                    queue_name: "q",
                    max_pending_jobs: 1,
                }],
                Some(17),
            )
            .unwrap();

        let guard = storage.connection();
        let primary = guard.as_ref().unwrap();
        let primary_timeout: i64 = primary
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();
        let persisted: i64 = primary
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE id = ?1",
                [ids[0]],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(primary_timeout, BUSY_TIMEOUT_MS as i64);
        assert_eq!(persisted, 1);
        drop(guard);

        let rejected = [EnqueueEntry {
            queue_name: "q",
            payload: b"rejected",
            job_id: None,
            dedup_key: None,
            dedup_fingerprint: None,
        }];
        assert!(matches!(
            storage.enqueue_batch(
                &rejected,
                3,
                &[CapacityPolicy {
                    queue_name: "q",
                    max_pending_jobs: 1,
                }],
                Some(17),
            ),
            Err(QueueError::Full)
        ));
        let guard = storage.connection();
        let primary_timeout: i64 = guard
            .as_ref()
            .unwrap()
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();
        assert_eq!(primary_timeout, BUSY_TIMEOUT_MS as i64);

        let full_dir = tempfile_guard::TempDir::new();
        let full_path = full_dir.path().join("full.db");
        let full_storage = Storage::new(full_path.to_str().unwrap(), true).unwrap();
        let full_attempt = full_storage.open_attempt_connection(17).unwrap();
        let synchronous: i64 = full_attempt
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap();
        assert_eq!(synchronous, 2); // FULL
    }

    fn check_retry_failed_checks_identity_before_capacity_and_updates_atomically() {
        let (_dir, storage) = open_storage();
        let entries = [
            EnqueueEntry {
                queue_name: "q",
                payload: b"failed",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"pending",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];
        let ids = storage.enqueue_batch(&entries, 3, &[], None).unwrap();
        {
            let guard = storage.connection();
            guard
                .as_ref()
                .unwrap()
                .execute("UPDATE messages SET status = 3 WHERE id = ?1", [ids[0]])
                .unwrap();
        }

        assert!(matches!(
            storage.retry_failed("q", 999_999, Some(1)),
            Err(QueueError::NotFound)
        ));
        assert!(matches!(
            storage.retry_failed("q", ids[0], Some(1)),
            Err(QueueError::Full)
        ));
        {
            let guard = storage.connection();
            guard
                .as_ref()
                .unwrap()
                .execute("UPDATE messages SET status = 2 WHERE id = ?1", [ids[1]])
                .unwrap();
        }
        storage.retry_failed("q", ids[0], Some(1)).unwrap();
        let guard = storage.connection();
        let status: i64 = guard
            .as_ref()
            .unwrap()
            .query_row(
                "SELECT status FROM messages WHERE id = ?1",
                [ids[0]],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, 0);
    }

    fn check_closed_storage_rejects_capacity_operations() {
        let (_dir, storage) = open_storage();
        storage.close().unwrap();
        let entry = [EnqueueEntry {
            queue_name: "q",
            payload: b"payload",
            job_id: None,
            dedup_key: None,
            dedup_fingerprint: None,
        }];

        assert!(matches!(
            storage.enqueue_batch(
                &entry,
                3,
                &[CapacityPolicy {
                    queue_name: "q",
                    max_pending_jobs: 1,
                }],
                Some(0),
            ),
            Err(QueueError::Closed)
        ));
        assert!(matches!(
            storage.retry_failed("q", 1, Some(1)),
            Err(QueueError::Closed)
        ));
    }

    fn count_rows(storage: &Storage, queue: &str) -> i64 {
        let guard = storage.connection();
        guard
            .as_ref()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE queue = ?1",
                params![queue],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn check_multi_queue_batch_keeps_payloads_and_outcome_order() {
        let (_dir, storage) = open_storage();
        let entries = [
            EnqueueEntry {
                queue_name: "beta",
                payload: b"payload-beta-1",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "alpha",
                payload: b"payload-alpha",
                job_id: Some("alpha-job"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "beta",
                payload: b"payload-beta-2",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];

        let outcomes = storage
            .enqueue_batch_outcomes(&entries, 3, &[], None)
            .unwrap();

        assert_eq!(outcomes.len(), 3);
        assert!(outcomes.iter().all(|outcome| outcome.inserted));
        assert!(outcomes[0].id < outcomes[1].id && outcomes[1].id < outcomes[2].id);
        let guard = storage.connection();
        let conn = guard.as_ref().unwrap();
        for (outcome, entry) in outcomes.iter().zip(entries.iter()) {
            let row: (String, Vec<u8>) = conn
                .query_row(
                    "SELECT queue, payload FROM messages WHERE id = ?1",
                    params![outcome.id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!(row, (entry.queue_name.to_owned(), entry.payload.to_vec()));
        }
    }

    fn check_dedup_key_deduplicates_within_batch() {
        let (_dir, storage) = open_storage();
        let entries = [
            EnqueueEntry {
                queue_name: "events",
                payload: b"first",
                job_id: None,
                dedup_key: Some("identity"),
                dedup_fingerprint: Some("fingerprint"),
            },
            EnqueueEntry {
                queue_name: "events",
                payload: b"second",
                job_id: None,
                dedup_key: Some("identity"),
                dedup_fingerprint: Some("fingerprint"),
            },
        ];

        let outcomes = storage
            .enqueue_batch_outcomes(&entries, 3, &[], None)
            .unwrap();

        assert!(outcomes[0].inserted);
        assert!(!outcomes[1].inserted);
        assert_eq!(outcomes[0].id, outcomes[1].id);
        assert_eq!(count_rows(&storage, "events"), 1);
    }

    fn check_fingerprint_conflict_within_batch_rolls_back_all_queues() {
        let (_dir, storage) = open_storage();
        let entries = [
            EnqueueEntry {
                queue_name: "qa",
                payload: b"ok",
                job_id: Some("ok-job"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "qb",
                payload: b"original",
                job_id: None,
                dedup_key: Some("identity"),
                dedup_fingerprint: Some("fingerprint-a"),
            },
            EnqueueEntry {
                queue_name: "qb",
                payload: b"conflicting",
                job_id: None,
                dedup_key: Some("identity"),
                dedup_fingerprint: Some("fingerprint-b"),
            },
        ];

        assert!(matches!(
            storage.enqueue_batch_outcomes(&entries, 3, &[], None),
            Err(QueueError::DeduplicationConflict)
        ));
        assert_eq!(count_rows(&storage, "qa"), 0);
        assert_eq!(count_rows(&storage, "qb"), 0);
    }

    fn check_fingerprint_conflict_against_existing_row_rolls_back_all_queues() {
        let (_dir, storage) = open_storage();
        let existing = [EnqueueEntry {
            queue_name: "qb",
            payload: b"original",
            job_id: None,
            dedup_key: Some("identity"),
            dedup_fingerprint: Some("fingerprint-a"),
        }];
        storage.enqueue_batch(&existing, 3, &[], None).unwrap();
        let entries = [
            EnqueueEntry {
                queue_name: "qa",
                payload: b"ok",
                job_id: Some("ok-job"),
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "qb",
                payload: b"conflicting",
                job_id: None,
                dedup_key: Some("identity"),
                dedup_fingerprint: Some("fingerprint-b"),
            },
        ];

        assert!(matches!(
            storage.enqueue_batch_outcomes(&entries, 3, &[], None),
            Err(QueueError::DeduplicationConflict)
        ));
        assert_eq!(count_rows(&storage, "qa"), 0);
        assert_eq!(count_rows(&storage, "qb"), 1);
    }

    fn check_conflicting_duplicate_policies_are_rejected() {
        let (_dir, storage) = open_storage();
        let entry = [EnqueueEntry {
            queue_name: "q",
            payload: b"payload",
            job_id: None,
            dedup_key: None,
            dedup_fingerprint: None,
        }];
        let conflicting = [
            CapacityPolicy {
                queue_name: "q",
                max_pending_jobs: 1,
            },
            CapacityPolicy {
                queue_name: "q",
                max_pending_jobs: 2,
            },
        ];

        assert!(matches!(
            storage.enqueue_batch(&entry, 3, &conflicting, None),
            Err(QueueError::ConflictingCapacityPolicies)
        ));
        assert_eq!(count_rows(&storage, "q"), 0);

        let identical = [
            CapacityPolicy {
                queue_name: "q",
                max_pending_jobs: 2,
            },
            CapacityPolicy {
                queue_name: "q",
                max_pending_jobs: 2,
            },
        ];
        storage.enqueue_batch(&entry, 3, &identical, None).unwrap();
        assert_eq!(count_rows(&storage, "q"), 1);
    }

    fn check_repeated_dedup_key_counts_as_one_new_row() {
        let (_dir, storage) = open_storage();
        let entries: Vec<_> = (0..1000)
            .map(|_| EnqueueEntry {
                queue_name: "events",
                payload: b"payload",
                job_id: None,
                dedup_key: Some("identity"),
                dedup_fingerprint: Some("fingerprint"),
            })
            .collect();
        let policy = [CapacityPolicy {
            queue_name: "events",
            max_pending_jobs: 1,
        }];

        let outcomes = storage
            .enqueue_batch_outcomes(&entries, 3, &policy, None)
            .unwrap();

        assert_eq!(outcomes.len(), 1000);
        assert!(outcomes[0].inserted);
        assert!(outcomes[1..].iter().all(|outcome| !outcome.inserted));
        assert!(outcomes.iter().all(|outcome| outcome.id == outcomes[0].id));
        assert_eq!(count_rows(&storage, "events"), 1);
    }

    fn check_existing_identity_counts_as_zero_new_rows() {
        let (_dir, storage) = open_storage();
        let existing = [EnqueueEntry {
            queue_name: "events",
            payload: b"original",
            job_id: None,
            dedup_key: Some("identity"),
            dedup_fingerprint: Some("fingerprint"),
        }];
        storage.enqueue_batch(&existing, 3, &[], None).unwrap();
        let duplicates = [EnqueueEntry {
            queue_name: "events",
            payload: b"duplicate",
            job_id: None,
            dedup_key: Some("identity"),
            dedup_fingerprint: Some("fingerprint"),
        }];
        let policy = [CapacityPolicy {
            queue_name: "events",
            max_pending_jobs: 1,
        }];

        let outcomes = storage
            .enqueue_batch_outcomes(&duplicates, 3, &policy, None)
            .unwrap();

        assert!(!outcomes[0].inserted);
        assert_eq!(count_rows(&storage, "events"), 1);
    }

    fn check_capacity_failure_in_one_queue_rolls_back_other_queues() {
        let (_dir, storage) = open_storage();
        let entries = [
            EnqueueEntry {
                queue_name: "unbounded",
                payload: b"fits",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "tight",
                payload: b"one",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "tight",
                payload: b"two",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];
        let policies = [CapacityPolicy {
            queue_name: "tight",
            max_pending_jobs: 1,
        }];

        assert!(matches!(
            storage.enqueue_batch_outcomes(&entries, 3, &policies, None),
            Err(QueueError::FullImpossible)
        ));
        assert_eq!(count_rows(&storage, "unbounded"), 0);
        assert_eq!(count_rows(&storage, "tight"), 0);

        // Queues without a policy remain unlimited within the same batch.
        let entries = [
            EnqueueEntry {
                queue_name: "unbounded",
                payload: b"one",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "unbounded",
                payload: b"two",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];
        storage
            .enqueue_batch_outcomes(&entries, 3, &policies, None)
            .unwrap();
        assert_eq!(count_rows(&storage, "unbounded"), 2);
    }

    fn check_multi_queue_policies_are_enforced_independently() {
        let (_dir, storage) = open_storage();
        let policies = [
            CapacityPolicy {
                queue_name: "qa",
                max_pending_jobs: 2,
            },
            CapacityPolicy {
                queue_name: "qb",
                max_pending_jobs: 1,
            },
        ];
        let first = [
            EnqueueEntry {
                queue_name: "qa",
                payload: b"a1",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "qa",
                payload: b"a2",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "qb",
                payload: b"b1",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
        ];
        storage
            .enqueue_batch_outcomes(&first, 3, &policies, None)
            .unwrap();

        let over_qb = [EnqueueEntry {
            queue_name: "qb",
            payload: b"b2",
            job_id: None,
            dedup_key: None,
            dedup_fingerprint: None,
        }];
        assert!(matches!(
            storage.enqueue_batch_outcomes(&over_qb, 3, &policies, None),
            Err(QueueError::Full)
        ));
        assert_eq!(count_rows(&storage, "qa"), 2);
        assert_eq!(count_rows(&storage, "qb"), 1);
    }

    fn check_malformed_dedup_metadata_writes_nothing() {
        let (_dir, storage) = open_storage();
        let entries = [
            EnqueueEntry {
                queue_name: "qa",
                payload: b"valid",
                job_id: None,
                dedup_key: None,
                dedup_fingerprint: None,
            },
            EnqueueEntry {
                queue_name: "qa",
                payload: b"invalid",
                job_id: None,
                dedup_key: Some("identity"),
                dedup_fingerprint: None,
            },
        ];

        assert!(matches!(
            storage.enqueue_batch_outcomes(&entries, 3, &[], None),
            Err(QueueError::InvalidDeduplicationMetadata)
        ));
        assert_eq!(count_rows(&storage, "qa"), 0);
    }

    #[test]
    fn multi_queue_ingestion_contract() {
        check_multi_queue_batch_keeps_payloads_and_outcome_order();
        check_dedup_key_deduplicates_within_batch();
        check_fingerprint_conflict_within_batch_rolls_back_all_queues();
        check_fingerprint_conflict_against_existing_row_rolls_back_all_queues();
        check_conflicting_duplicate_policies_are_rejected();
        check_repeated_dedup_key_counts_as_one_new_row();
        check_existing_identity_counts_as_zero_new_rows();
        check_capacity_failure_in_one_queue_rolls_back_other_queues();
        check_multi_queue_policies_are_enforced_independently();
        check_malformed_dedup_metadata_writes_nothing();
    }

    #[test]
    fn backpressure_transaction_contract() {
        check_capacity_check_and_batch_insert_are_atomic();
        check_capacity_counts_only_new_distinct_job_ids();
        check_zero_new_rows_are_allowed_above_limit_in_every_state();
        check_impossible_batch_is_typed_and_never_writes();
        check_capacity_is_scoped_to_one_logical_queue();
        check_opening_below_existing_pending_does_not_delete_rows();
        check_two_connections_never_oversubscribe_capacity();
        check_sqlite_busy_is_not_reported_as_full_and_timeout_is_restored();
        check_bounded_attempt_connection_preserves_confirmation_boundary();
        check_retry_failed_checks_identity_before_capacity_and_updates_atomically();
        check_closed_storage_rejects_capacity_operations();
    }

    fn checkpoint_update(
        expected_generation: Option<String>,
        expected_version: Option<i64>,
        items: i64,
    ) -> CheckpointUpdate {
        CheckpointUpdate {
            bus_name: "bus".to_owned(),
            checkpoint_name: "ingest".to_owned(),
            expected_generation,
            expected_version,
            new_cursor: format!("cursor-{items}"),
            source_fingerprint: Some("fp".to_owned()),
            items_committed: items,
        }
    }

    fn entry(payload: &'static [u8]) -> EnqueueEntry<'static> {
        EnqueueEntry {
            queue_name: "q",
            payload,
            job_id: None,
            dedup_key: None,
            dedup_fingerprint: None,
        }
    }

    fn count_messages(storage: &Storage) -> i64 {
        let guard = storage.connection();
        guard
            .as_ref()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn checkpoint_criado_com_expected_version_none() {
        let (_dir, storage) = open_storage();
        let entries = [entry(b"a")];

        let (outcomes, version) = storage
            .enqueue_batch_outcomes_with_checkpoint(
                &entries,
                3,
                &[],
                None,
                Some(&checkpoint_update(None, None, 1)),
            )
            .unwrap();

        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].inserted);
        assert_eq!(version.map(|commit| commit.version), Some(1));
        let snapshot = storage
            .checkpoint_inspect("bus", "ingest")
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.cursor, "cursor-1");
        assert_eq!(snapshot.source_fingerprint.as_deref(), Some("fp"));
        assert_eq!(snapshot.version, 1);
        assert_eq!(snapshot.items_committed, 1);
        assert_eq!(snapshot.batches_committed, 1);
        assert_eq!(snapshot.created_at, snapshot.updated_at);
    }

    #[test]
    fn checkpoint_avanca_via_cas_e_acumula_contadores() {
        let (_dir, storage) = open_storage();
        storage
            .enqueue_batch_outcomes_with_checkpoint(
                &[],
                3,
                &[],
                None,
                Some(&checkpoint_update(None, None, 5)),
            )
            .unwrap();

        let generation = storage
            .checkpoint_inspect("bus", "ingest")
            .unwrap()
            .unwrap()
            .generation;
        let (_, version) = storage
            .enqueue_batch_outcomes_with_checkpoint(
                &[entry(b"a"), entry(b"b")],
                3,
                &[],
                None,
                Some(&checkpoint_update(Some(generation), Some(1), 2)),
            )
            .unwrap();

        assert_eq!(version.map(|commit| commit.version), Some(2));
        let snapshot = storage
            .checkpoint_inspect("bus", "ingest")
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.version, 2);
        assert_eq!(snapshot.items_committed, 7);
        assert_eq!(snapshot.batches_committed, 2);
    }

    #[test]
    fn checkpoint_conflito_de_versao_faz_rollback_das_deliveries() {
        let (_dir, storage) = open_storage();
        storage
            .enqueue_batch_outcomes_with_checkpoint(
                &[],
                3,
                &[],
                None,
                Some(&checkpoint_update(None, None, 1)),
            )
            .unwrap();

        let result = storage.enqueue_batch_outcomes_with_checkpoint(
            &[entry(b"a"), entry(b"b")],
            3,
            &[],
            None,
            Some(&checkpoint_update(Some("stale".to_owned()), Some(99), 2)),
        );

        assert!(matches!(
            result,
            Err(QueueError::CheckpointConflict {
                expected_version: Some(99),
                actual_version: Some(1),
                ..
            })
        ));
        assert_eq!(count_messages(&storage), 0);
        let snapshot = storage
            .checkpoint_inspect("bus", "ingest")
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.version, 1);
    }

    #[test]
    fn checkpoint_stale_conflita_antes_de_capacity_cheia() {
        let (_dir, storage) = open_storage();
        storage
            .enqueue_batch_outcomes_with_checkpoint(
                &[],
                3,
                &[],
                None,
                Some(&checkpoint_update(None, None, 1)),
            )
            .unwrap();
        let generation = storage
            .checkpoint_inspect("bus", "ingest")
            .unwrap()
            .unwrap()
            .generation;
        storage
            .enqueue_batch_outcomes_with_checkpoint(
                &[],
                3,
                &[],
                None,
                Some(&checkpoint_update(Some(generation.clone()), Some(1), 1)),
            )
            .unwrap();
        // Make the delivery queue full. The stale update below must still
        // receive CheckpointConflict, never the retryable Full signal.
        storage
            .enqueue_batch_outcomes(&[entry(b"already-full")], 3, &[], None)
            .unwrap();

        let result = storage.enqueue_batch_outcomes_with_checkpoint(
            &[entry(b"must-not-insert")],
            3,
            &[CapacityPolicy {
                queue_name: "q",
                max_pending_jobs: 1,
            }],
            None,
            Some(&checkpoint_update(Some(generation), Some(1), 1)),
        );

        assert!(matches!(
            result,
            Err(QueueError::CheckpointConflict {
                expected_version: Some(1),
                actual_version: Some(2),
                ..
            })
        ));
        assert_eq!(count_messages(&storage), 1);
        let snapshot = storage
            .checkpoint_inspect("bus", "ingest")
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.version, 2);
        assert_eq!(snapshot.cursor, "cursor-1");
    }

    #[test]
    fn checkpoint_generation_impede_aba_apos_reset() {
        let (_dir, storage) = open_storage();
        storage
            .enqueue_batch_outcomes_with_checkpoint(
                &[],
                3,
                &[],
                None,
                Some(&checkpoint_update(None, None, 1)),
            )
            .unwrap();
        let old = storage
            .checkpoint_inspect("bus", "ingest")
            .unwrap()
            .unwrap();
        assert!(storage.checkpoint_reset("bus", "ingest").unwrap());

        let mut replacement = checkpoint_update(None, None, 9);
        replacement.source_fingerprint = Some("new-source".to_owned());
        replacement.new_cursor = "new-cursor".to_owned();
        storage
            .enqueue_batch_outcomes_with_checkpoint(&[], 3, &[], None, Some(&replacement))
            .unwrap();
        let replacement = storage
            .checkpoint_inspect("bus", "ingest")
            .unwrap()
            .unwrap();
        assert_ne!(old.generation, replacement.generation);

        let result = storage.enqueue_batch_outcomes_with_checkpoint(
            &[entry(b"old-source-delivery")],
            3,
            &[],
            None,
            Some(&checkpoint_update(Some(old.generation), Some(1), 2)),
        );
        assert!(matches!(result, Err(QueueError::CheckpointConflict { .. })));
        assert_eq!(count_messages(&storage), 0);
        let current = storage
            .checkpoint_inspect("bus", "ingest")
            .unwrap()
            .unwrap();
        assert_eq!(current.generation, replacement.generation);
        assert_eq!(current.cursor, "new-cursor");
        assert_eq!(current.source_fingerprint.as_deref(), Some("new-source"));
        assert_eq!(current.version, 1);
    }

    #[test]
    fn checkpoint_insert_duplicado_conflita_e_nao_insere_deliveries() {
        let (_dir, storage) = open_storage();
        storage
            .enqueue_batch_outcomes_with_checkpoint(
                &[],
                3,
                &[],
                None,
                Some(&checkpoint_update(None, None, 1)),
            )
            .unwrap();

        let result = storage.enqueue_batch_outcomes_with_checkpoint(
            &[entry(b"a")],
            3,
            &[],
            None,
            Some(&checkpoint_update(None, None, 1)),
        );

        assert!(matches!(
            result,
            Err(QueueError::CheckpointConflict {
                expected_version: None,
                actual_version: Some(1),
                ..
            })
        ));
        assert_eq!(count_messages(&storage), 0);
    }

    #[test]
    fn checkpoint_only_commit_com_entries_vazio() {
        let (_dir, storage) = open_storage();

        let (outcomes, version) = storage
            .enqueue_batch_outcomes_with_checkpoint(
                &[],
                3,
                &[],
                None,
                Some(&checkpoint_update(None, None, 0)),
            )
            .unwrap();

        assert!(outcomes.is_empty());
        assert_eq!(version.map(|commit| commit.version), Some(1));
        assert_eq!(count_messages(&storage), 0);
        assert!(storage
            .checkpoint_inspect("bus", "ingest")
            .unwrap()
            .is_some());
    }

    #[test]
    fn enqueue_sem_checkpoint_mantem_early_return() {
        let (_dir, storage) = open_storage();

        let (outcomes, version) = storage
            .enqueue_batch_outcomes_with_checkpoint(&[], 3, &[], None, None)
            .unwrap();

        assert!(outcomes.is_empty());
        assert_eq!(version, None);
    }

    #[test]
    fn checkpoint_inspect_ausente_e_reset() {
        let (_dir, storage) = open_storage();
        assert!(storage
            .checkpoint_inspect("bus", "ingest")
            .unwrap()
            .is_none());
        assert!(!storage.checkpoint_reset("bus", "ingest").unwrap());

        storage
            .enqueue_batch_outcomes_with_checkpoint(
                &[entry(b"a")],
                3,
                &[],
                None,
                Some(&checkpoint_update(None, None, 1)),
            )
            .unwrap();
        assert!(storage.checkpoint_reset("bus", "ingest").unwrap());
        assert!(storage
            .checkpoint_inspect("bus", "ingest")
            .unwrap()
            .is_none());
        // Reset não toca em messages.
        assert_eq!(count_messages(&storage), 1);
    }

    // Guard mínimo de diretório temporário para os testes, sem dependência nova.
    mod tempfile_guard {
        use std::path::{Path, PathBuf};

        pub struct TempDir(PathBuf);

        impl TempDir {
            pub fn new() -> Self {
                let path = std::env::temp_dir().join(format!(
                    "localqueue-test-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                ));
                std::fs::create_dir_all(&path).unwrap();
                Self(path)
            }

            pub fn path(&self) -> &Path {
                &self.0
            }
        }

        impl Drop for TempDir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }
}
