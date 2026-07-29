//! Typed, short-lived SQLite queries used by the native LocalQueue console.
//!
//! This module is deliberately independent from egui and from `Storage`: reads
//! use a new read-only connection per query, so an idle console cannot retain a
//! WAL snapshot or contend for the queue's reusable writer connection.

use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use std::path::{Path, PathBuf};

use crate::error::{QueueError, Result};
use crate::storage::{now_ms, retry_transient_write};

const STATUS_READY: i64 = 0;
const STATUS_PROCESSING: i64 = 1;
const STATUS_FAILED: i64 = 3;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeliveryCounts {
    pub ready: i64,
    pub processing: i64,
    pub acknowledged: i64,
    pub failed: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionSummary {
    pub queue: String,
    pub counts: DeliveryCounts,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionConfig {
    pub queue: String,
    pub min_max_attempts: i64,
    pub max_max_attempts: i64,
    pub active_leases: i64,
    /// LocalQueue keeps these values in process configuration, not SQLite.
    pub runtime_configuration_available: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub offset: u64,
    pub limit: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionSummary {
    pub execution_id: String,
    pub bus_name: String,
    pub source_name: String,
    pub source_completed: bool,
    pub completed_at: Option<i64>,
    pub updated_at: i64,
    pub counts: DeliveryCounts,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionDetail {
    pub summary: ExecutionSummary,
    pub checkpoint_name: Option<String>,
    pub source_lease_until: Option<i64>,
    pub items_committed: Option<i64>,
    pub deliveries_inserted: Option<i64>,
    pub deliveries_deduplicated: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailedDelivery {
    pub id: i64,
    pub queue: String,
    pub attempts: i64,
    pub max_attempts: i64,
    pub last_error: Option<String>,
    pub failure_reason: Option<String>,
    pub failure_category: Option<String>,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureDetail {
    pub delivery: FailedDelivery,
    pub payload: Vec<u8>,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseInfo {
    pub path: PathBuf,
    pub journal_mode: String,
    pub size_bytes: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct AdminStore {
    path: PathBuf,
}

impl AdminStore {
    /// Accept either LocalQueue's data directory or a direct database path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let supplied = path.as_ref();
        let path = if supplied.is_dir() {
            supplied.join("localqueue.db")
        } else {
            supplied.to_path_buf()
        };
        if !path.is_file() {
            return Err(QueueError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("LocalQueue database not found: {}", path.display()),
            )));
        }
        Ok(Self {
            path: std::path::absolute(path)?,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_connection(&self) -> Result<Connection> {
        let conn = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        )?;
        conn.pragma_update(None, "busy_timeout", 250_i64)?;
        Ok(conn)
    }

    pub fn database_info(&self) -> Result<DatabaseInfo> {
        let conn = self.read_connection()?;
        let journal_mode =
            conn.query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))?;
        Ok(DatabaseInfo {
            path: self.path.clone(),
            journal_mode: journal_mode.to_lowercase(),
            size_bytes: std::fs::metadata(&self.path).ok().map(|m| m.len()),
        })
    }

    pub fn subscriptions(&self) -> Result<Vec<SubscriptionSummary>> {
        let mut conn = self.read_connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let mut stmt = tx.prepare("SELECT queue, COALESCE(SUM(status = 0), 0), COALESCE(SUM(status = 1), 0), COALESCE(SUM(status = 2), 0), COALESCE(SUM(status = 3), 0) FROM messages GROUP BY queue ORDER BY queue")?;
        let items = stmt
            .query_map([], |row| {
                Ok(SubscriptionSummary {
                    queue: row.get(0)?,
                    counts: DeliveryCounts {
                        ready: row.get(1)?,
                        processing: row.get(2)?,
                        acknowledged: row.get(3)?,
                        failed: row.get(4)?,
                    },
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);
        tx.commit()?;
        Ok(items)
    }

    pub fn subscription_config(&self, queue: &str) -> Result<Option<SubscriptionConfig>> {
        let now = now_ms();
        let mut conn = self.read_connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let value = tx.query_row("SELECT MIN(max_attempts), MAX(max_attempts), COALESCE(SUM(status = ?2 AND lease_until > ?3), 0) FROM messages WHERE queue = ?1", params![queue, STATUS_PROCESSING, now], |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?, row.get(2)?))).optional()?;
        tx.commit()?;
        Ok(value.and_then(|(min, max, active_leases)| {
            Some(SubscriptionConfig {
                queue: queue.to_owned(),
                min_max_attempts: min?,
                max_max_attempts: max?,
                active_leases,
                runtime_configuration_available: false,
            })
        }))
    }

    pub fn executions(&self, offset: u64, limit: u64) -> Result<Page<ExecutionSummary>> {
        let limit = page_limit(limit)?;
        let mut conn = self.read_connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let total = tx.query_row("SELECT COUNT(*) FROM event_bus_executions", [], |r| {
            r.get(0)
        })?;
        let mut stmt = tx.prepare("SELECT e.execution_id, e.bus_name, e.source_name, e.source_completed, e.updated_at, r.completed_at, COALESCE(SUM(m.status = 0), 0), COALESCE(SUM(m.status = 1), 0), COALESCE(SUM(m.status = 2), 0), COALESCE(SUM(m.status = 3), 0) FROM event_bus_executions e LEFT JOIN event_bus_execution_runtime r ON r.execution_id = e.execution_id LEFT JOIN event_bus_execution_deliveries d ON d.execution_id = e.execution_id LEFT JOIN messages m ON m.id = d.message_id GROUP BY e.execution_id ORDER BY e.updated_at DESC, e.execution_id LIMIT ?1 OFFSET ?2")?;
        let items = stmt
            .query_map(params![limit, offset], execution_summary_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);
        tx.commit()?;
        Ok(Page {
            items,
            total,
            offset,
            limit: limit as u64,
        })
    }

    pub fn execution_detail(&self, id: &str) -> Result<Option<ExecutionDetail>> {
        let mut conn = self.read_connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let detail = tx.query_row("SELECT e.execution_id, e.bus_name, e.source_name, e.source_completed, e.updated_at, r.completed_at, e.checkpoint_name, r.source_lease_until, r.items_committed, r.deliveries_inserted, r.deliveries_deduplicated, COALESCE(SUM(m.status = 0), 0), COALESCE(SUM(m.status = 1), 0), COALESCE(SUM(m.status = 2), 0), COALESCE(SUM(m.status = 3), 0) FROM event_bus_executions e LEFT JOIN event_bus_execution_runtime r ON r.execution_id=e.execution_id LEFT JOIN event_bus_execution_deliveries d ON d.execution_id=e.execution_id LEFT JOIN messages m ON m.id=d.message_id WHERE e.execution_id=?1 GROUP BY e.execution_id", params![id], |r| Ok(ExecutionDetail { summary: ExecutionSummary { execution_id: r.get(0)?, bus_name: r.get(1)?, source_name: r.get(2)?, source_completed: r.get::<_, i64>(3)? != 0, updated_at: r.get(4)?, completed_at: r.get(5)?, counts: DeliveryCounts { ready: r.get(11)?, processing: r.get(12)?, acknowledged: r.get(13)?, failed: r.get(14)? } }, checkpoint_name: r.get(6)?, source_lease_until: r.get(7)?, items_committed: r.get(8)?, deliveries_inserted: r.get(9)?, deliveries_deduplicated: r.get(10)? })).optional()?;
        tx.commit()?;
        Ok(detail)
    }

    pub fn failed_deliveries(&self, offset: u64, limit: u64) -> Result<Page<FailedDelivery>> {
        let limit = page_limit(limit)?;
        let mut conn = self.read_connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let total = tx.query_row(
            "SELECT COUNT(*) FROM messages WHERE status = ?1",
            params![STATUS_FAILED],
            |r| r.get(0),
        )?;
        let mut stmt = tx.prepare("SELECT id, queue, attempts, max_attempts, last_error, failure_reason, failure_category, updated_at FROM messages WHERE status = ?1 ORDER BY updated_at DESC, id DESC LIMIT ?2 OFFSET ?3")?;
        let items = stmt
            .query_map(params![STATUS_FAILED, limit, offset], failed_from_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);
        tx.commit()?;
        Ok(Page {
            items,
            total,
            offset,
            limit: limit as u64,
        })
    }

    pub fn failure_detail(&self, id: i64) -> Result<Option<FailureDetail>> {
        let mut conn = self.read_connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
        let detail = tx.query_row("SELECT id, queue, attempts, max_attempts, last_error, failure_reason, failure_category, updated_at, payload, created_at FROM messages WHERE id=?1 AND status=?2", params![id, STATUS_FAILED], |r| Ok(FailureDetail { delivery: FailedDelivery { id: r.get(0)?, queue: r.get(1)?, attempts: r.get(2)?, max_attempts: r.get(3)?, last_error: r.get(4)?, failure_reason: r.get(5)?, failure_category: r.get(6)?, updated_at: r.get(7)? }, payload: r.get(8)?, created_at: r.get(9)? })).optional()?;
        tx.commit()?;
        Ok(detail)
    }

    /// The only write exposed to the console. It does not alter producer or
    /// consumer paths and intentionally has no runtime-only capacity policy.
    pub fn retry_failed(&self, id: i64) -> Result<()> {
        let path = self.path.clone();
        retry_transient_write(|| {
            let mut conn = Connection::open_with_flags(
                &path,
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_URI,
            )?;
            conn.pragma_update(None, "busy_timeout", 250_i64)?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let now = now_ms();
            let changed = tx.execute("UPDATE messages SET status=?1, available_at=?2, attempts=0, receipt=NULL, lease_until=NULL, last_error=NULL, failure_reason=NULL, failure_category=NULL, updated_at=?2 WHERE id=?3 AND status=?4", params![STATUS_READY, now, id, STATUS_FAILED])?;
            if changed != 1 {
                return Err(QueueError::NotFound);
            }
            tx.commit()?;
            Ok(())
        })
    }
}

fn page_limit(limit: u64) -> Result<i64> {
    if !(1..=500).contains(&limit) {
        return Err(QueueError::Sqlite(rusqlite::Error::InvalidParameterName(
            "page limit must be 1..=500".into(),
        )));
    }
    Ok(limit as i64)
}

fn execution_summary_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExecutionSummary> {
    Ok(ExecutionSummary {
        execution_id: row.get(0)?,
        bus_name: row.get(1)?,
        source_name: row.get(2)?,
        source_completed: row.get::<_, i64>(3)? != 0,
        updated_at: row.get(4)?,
        completed_at: row.get(5)?,
        counts: DeliveryCounts {
            ready: row.get(6)?,
            processing: row.get(7)?,
            acknowledged: row.get(8)?,
            failed: row.get(9)?,
        },
    })
}
fn failed_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<FailedDelivery> {
    Ok(FailedDelivery {
        id: row.get(0)?,
        queue: row.get(1)?,
        attempts: row.get(2)?,
        max_attempts: row.get(3)?,
        last_error: row.get(4)?,
        failure_reason: row.get(5)?,
        failure_category: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SCHEMA_SQL;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

    fn setup() -> (PathBuf, Connection) {
        let path = std::env::temp_dir().join(format!(
            "localqueue-admin-{}-{}-{}.db",
            std::process::id(),
            now_ms(),
            NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
        ));
        let conn = Connection::open(&path).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.execute_batch(SCHEMA_SQL).unwrap();
        (path, conn)
    }

    fn message(conn: &Connection, queue: &str, status: i64, updated: i64) -> i64 {
        conn.execute("INSERT INTO messages (queue, payload, status, attempts, max_attempts, available_at, created_at, updated_at, last_error, failure_reason) VALUES (?1, x'7B7D', ?2, 3, 4, 0, 0, ?3, 'handler error', 'permanent')", params![queue, status, updated]).unwrap();
        conn.last_insert_rowid()
    }

    #[test]
    fn summaries_reflect_known_delivery_states() {
        let (path, conn) = setup();
        for status in [STATUS_READY, STATUS_PROCESSING, 2, STATUS_FAILED] {
            message(&conn, "contacts", status, status);
        }
        let store = AdminStore::open(&path).unwrap();
        assert_eq!(
            store.subscriptions().unwrap(),
            vec![SubscriptionSummary {
                queue: "contacts".into(),
                counts: DeliveryCounts {
                    ready: 1,
                    processing: 1,
                    acknowledged: 1,
                    failed: 1
                }
            }]
        );
        let config = store.subscription_config("contacts").unwrap().unwrap();
        assert_eq!((config.min_max_attempts, config.max_max_attempts), (4, 4));
        drop(conn);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn failure_pages_are_bounded_and_retry_moves_one_row_to_ready() {
        let (path, conn) = setup();
        let first = message(&conn, "contacts", STATUS_FAILED, 1);
        message(&conn, "contacts", STATUS_FAILED, 2);
        let store = AdminStore::open(&path).unwrap();
        let page = store.failed_deliveries(0, 1).unwrap();
        assert_eq!((page.total, page.items.len()), (2, 1));
        assert_eq!(store.failure_detail(first).unwrap().unwrap().payload, b"{}");
        store.retry_failed(first).unwrap();
        assert!(store.failure_detail(first).unwrap().is_none());
        assert_eq!(store.subscriptions().unwrap()[0].counts.ready, 1);
        drop(conn);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn execution_listing_is_paginated() {
        let (path, conn) = setup();
        for index in 0..3 {
            conn.execute("INSERT INTO event_bus_executions (execution_id, bus_name, source_name, source_completed, created_at, updated_at) VALUES (?1, 'import', 'contacts', 1, 0, ?2)", params![format!("execution-{index}"), index]).unwrap();
        }
        let store = AdminStore::open(&path).unwrap();
        let page = store.executions(1, 1).unwrap();
        assert_eq!(
            (
                page.total,
                page.items.len(),
                page.items[0].execution_id.as_str()
            ),
            (3, 1, "execution-1")
        );
        drop(conn);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn large_delivery_database_returns_only_one_page() {
        let (path, conn) = setup();
        for index in 0..1_000 {
            message(&conn, "bulk", STATUS_FAILED, index);
        }
        let store = AdminStore::open(&path).unwrap();
        let page = store.failed_deliveries(500, 25).unwrap();
        assert_eq!((page.total, page.items.len()), (1_000, 25));
        drop(conn);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn short_read_queries_coexist_with_an_active_writer_and_release_wal_snapshots() {
        let (path, conn) = setup();
        let store = AdminStore::open(&path).unwrap();
        let writer_path = path.clone();
        let writer = std::thread::spawn(move || {
            let writer = Connection::open(writer_path).unwrap();
            for index in 0..50 {
                message(&writer, "active", STATUS_READY, index);
            }
        });
        for _ in 0..50 {
            store.subscriptions().unwrap();
        }
        writer.join().unwrap();
        // The AdminStore retains no Connection after a query returns, so this
        // checkpoint is not held back by a console reader snapshot.
        let checkpoint: (i64, i64, i64) = conn
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        assert_eq!(checkpoint.0, 0);
        drop(conn);
        std::fs::remove_file(path).unwrap();
    }
}
