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
use crate::schema::{BASE_SCHEMA_SQL, CHECKPOINTS_SCHEMA_SQL, SCHEMA_SQL};

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
/// batch inserts. `expected_version = None` means the checkpoint must be
/// created; `Some(v)` is a compare-and-swap on the stored version.
pub struct CheckpointUpdate {
    pub bus_name: String,
    pub checkpoint_name: String,
    pub expected_version: Option<i64>,
    pub new_cursor: String,
    pub source_fingerprint: Option<String>,
    pub items_committed: i64,
}

/// Read-only view of a stored ingestion checkpoint row.
pub struct CheckpointSnapshot {
    pub cursor: String,
    pub source_fingerprint: Option<String>,
    pub version: i64,
    pub items_committed: i64,
    pub batches_committed: i64,
    pub created_at: i64,
    pub updated_at: i64,
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
    /// checkpoint applied in the same transaction, after the delivery inserts
    /// and before the commit. A checkpoint conflict rolls back the inserts.
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
    ) -> Result<(Vec<EnqueueOutcome>, Option<i64>)> {
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
                )
            }
            None => {
                enqueue_batch_on_connection(primary, entries, max_attempts, capacity, checkpoint)
            }
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
            "SELECT cursor, source_fingerprint, version,
                    items_committed, batches_committed, created_at, updated_at
             FROM ingestion_checkpoints
             WHERE bus_name = ?1 AND checkpoint_name = ?2",
            params![bus_name, checkpoint_name],
            |row| {
                Ok(CheckpointSnapshot {
                    cursor: row.get(0)?,
                    source_fingerprint: row.get(1)?,
                    version: row.get(2)?,
                    items_committed: row.get(3)?,
                    batches_committed: row.get(4)?,
                    created_at: row.get(5)?,
                    updated_at: row.get(6)?,
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
    fn open_attempt_connection(&self, busy_timeout_ms: u64) -> Result<Connection> {
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
    if exists {
        return Ok(());
    }

    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(CHECKPOINTS_SCHEMA_SQL)?;
    tx.commit()?;
    Ok(())
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
) -> Result<(Vec<EnqueueOutcome>, Option<i64>)> {
    let now = now_ms();

    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(QueueError::from)?;

    #[cfg(feature = "__crash_test")]
    crate::failpoints::hit(crate::failpoints::Failpoint::EnqueueAfterBegin);

    enforce_capacity_policies(&tx, entries, capacity)?;

    let ids = insert_entries_in_transaction(&tx, entries, max_attempts, now)?;

    let new_version = match checkpoint {
        Some(update) => Some(apply_checkpoint_update(&tx, update, now)?),
        None => None,
    };

    #[cfg(feature = "__crash_test")]
    crate::failpoints::hit(crate::failpoints::Failpoint::EnqueueBeforeCommit);
    tx.commit().map_err(QueueError::from)?;
    Ok((ids, new_version))
}

/// Apply one ingestion checkpoint update inside the open transaction.
///
/// `expected_version = None` creates the row at version 1; `Some(v)` bumps the
/// stored version only if it still equals `v`. Any mismatch raises
/// `CheckpointConflict`, which propagates before the commit and rolls back the
/// whole transaction, including the delivery inserts.
fn apply_checkpoint_update(
    tx: &Transaction<'_>,
    update: &CheckpointUpdate,
    now: i64,
) -> Result<i64> {
    let conflict = |actual_version: Option<i64>| QueueError::CheckpointConflict {
        checkpoint_name: update.checkpoint_name.clone(),
        expected_version: update.expected_version,
        actual_version,
    };

    match update.expected_version {
        None => {
            let result = tx.execute(
                "INSERT INTO ingestion_checkpoints (
                    bus_name, checkpoint_name, cursor, source_fingerprint,
                    version, items_committed, batches_committed,
                    created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, 1, ?5, 1, ?6, ?6)",
                params![
                    update.bus_name,
                    update.checkpoint_name,
                    update.new_cursor,
                    update.source_fingerprint,
                    update.items_committed,
                    now,
                ],
            );
            match result {
                Ok(_) => Ok(1),
                Err(rusqlite::Error::SqliteFailure(ref failure, _))
                    if failure.code == ErrorCode::ConstraintViolation =>
                {
                    Err(conflict(stored_checkpoint_version(
                        tx,
                        &update.bus_name,
                        &update.checkpoint_name,
                    )?))
                }
                Err(error) => Err(error.into()),
            }
        }
        Some(expected) => {
            let changed = tx.execute(
                "UPDATE ingestion_checkpoints SET
                    cursor = ?3,
                    version = version + 1,
                    items_committed = items_committed + ?4,
                    batches_committed = batches_committed + 1,
                    updated_at = ?5
                 WHERE bus_name = ?1 AND checkpoint_name = ?2 AND version = ?6",
                params![
                    update.bus_name,
                    update.checkpoint_name,
                    update.new_cursor,
                    update.items_committed,
                    now,
                    expected,
                ],
            )?;
            if changed == 0 {
                return Err(conflict(stored_checkpoint_version(
                    tx,
                    &update.bus_name,
                    &update.checkpoint_name,
                )?));
            }
            Ok(expected + 1)
        }
    }
}

fn stored_checkpoint_version(
    tx: &Transaction<'_>,
    bus_name: &str,
    checkpoint_name: &str,
) -> Result<Option<i64>> {
    tx.query_row(
        "SELECT version FROM ingestion_checkpoints
         WHERE bus_name = ?1 AND checkpoint_name = ?2",
        params![bus_name, checkpoint_name],
        |row| row.get(0),
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

    fn checkpoint_update(expected_version: Option<i64>, items: i64) -> CheckpointUpdate {
        CheckpointUpdate {
            bus_name: "bus".to_owned(),
            checkpoint_name: "ingest".to_owned(),
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
                Some(&checkpoint_update(None, 1)),
            )
            .unwrap();

        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].inserted);
        assert_eq!(version, Some(1));
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
                Some(&checkpoint_update(None, 5)),
            )
            .unwrap();

        let (_, version) = storage
            .enqueue_batch_outcomes_with_checkpoint(
                &[entry(b"a"), entry(b"b")],
                3,
                &[],
                None,
                Some(&checkpoint_update(Some(1), 2)),
            )
            .unwrap();

        assert_eq!(version, Some(2));
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
                Some(&checkpoint_update(None, 1)),
            )
            .unwrap();

        let result = storage.enqueue_batch_outcomes_with_checkpoint(
            &[entry(b"a"), entry(b"b")],
            3,
            &[],
            None,
            Some(&checkpoint_update(Some(99), 2)),
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
    fn checkpoint_insert_duplicado_conflita_e_nao_insere_deliveries() {
        let (_dir, storage) = open_storage();
        storage
            .enqueue_batch_outcomes_with_checkpoint(
                &[],
                3,
                &[],
                None,
                Some(&checkpoint_update(None, 1)),
            )
            .unwrap();

        let result = storage.enqueue_batch_outcomes_with_checkpoint(
            &[entry(b"a")],
            3,
            &[],
            None,
            Some(&checkpoint_update(None, 1)),
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
                Some(&checkpoint_update(None, 0)),
            )
            .unwrap();

        assert!(outcomes.is_empty());
        assert_eq!(version, Some(1));
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
                Some(&checkpoint_update(None, 1)),
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
