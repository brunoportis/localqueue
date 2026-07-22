use rusqlite::{params, Connection, ErrorCode, OpenFlags, TransactionBehavior};
use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::error::{QueueError, Result};
use crate::schema::SCHEMA_SQL;

pub(crate) const BUSY_TIMEOUT_MS: u64 = 5_000;

/// An item in a batch insertion. The payload is borrowed to avoid copying data
/// across the PyO3 boundary.
pub struct EnqueueEntry<'a> {
    pub queue_name: &'a str,
    pub payload: &'a [u8],
    pub job_id: Option<&'a str>,
}

#[derive(Clone, Copy)]
pub struct CapacityPolicy<'a> {
    pub queue_name: &'a str,
    pub max_pending_jobs: i64,
}

pub struct Storage {
    conn: Mutex<Option<Connection>>,
    path: PathBuf,
    fsync: bool,
}

impl Storage {
    pub fn new(path: &str, fsync: bool) -> Result<Self> {
        let path = stable_database_path(path)?;
        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI,
        )?;

        conn.pragma_update(None, "busy_timeout", BUSY_TIMEOUT_MS)?;
        enable_wal(&conn)?;
        conn.pragma_update(None, "synchronous", if fsync { "FULL" } else { "NORMAL" })?;
        conn.pragma_update(None, "foreign_keys", "ON")?;

        conn.execute_batch(SCHEMA_SQL)?;

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
        capacity: Option<CapacityPolicy<'_>>,
        busy_timeout_ms: Option<u64>,
    ) -> Result<Vec<i64>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut guard = self.connection();
        let primary = guard.as_mut().ok_or(QueueError::Closed)?;

        match busy_timeout_ms {
            Some(timeout) => {
                let mut attempt = self.open_attempt_connection(timeout)?;
                enqueue_batch_on_connection(&mut attempt, entries, max_attempts, capacity)
            }
            None => enqueue_batch_on_connection(primary, entries, max_attempts, capacity),
        }
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

fn enqueue_batch_on_connection(
    conn: &mut Connection,
    entries: &[EnqueueEntry<'_>],
    max_attempts: i64,
    capacity: Option<CapacityPolicy<'_>>,
) -> Result<Vec<i64>> {
    let now = now_ms();

    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(QueueError::from)?;

    #[cfg(feature = "__crash_test")]
    crate::failpoints::hit(crate::failpoints::Failpoint::EnqueueAfterBegin);

    if let Some(policy) = capacity {
        let pending: i64 = tx.query_row(
            "SELECT COUNT(*) FROM messages
                 WHERE queue = ?1 AND status IN (0, 1)",
            params![policy.queue_name],
            |row| row.get(0),
        )?;
        let mut new_rows = entries
            .iter()
            .filter(|entry| entry.queue_name == policy.queue_name && entry.job_id.is_none())
            .count() as i64;
        let distinct_job_ids: HashSet<&str> = entries
            .iter()
            .filter(|entry| entry.queue_name == policy.queue_name)
            .filter_map(|entry| entry.job_id)
            .collect();
        for job_id in distinct_job_ids {
            let exists: bool = tx.query_row(
                "SELECT EXISTS(
                        SELECT 1 FROM messages WHERE queue = ?1 AND job_id = ?2
                    )",
                params![policy.queue_name, job_id],
                |row| row.get(0),
            )?;
            if !exists {
                new_rows += 1;
            }
        }
        if new_rows > 0 {
            if new_rows > policy.max_pending_jobs {
                return Err(QueueError::FullImpossible);
            }
            if pending.saturating_add(new_rows) > policy.max_pending_jobs {
                return Err(QueueError::Full);
            }
        }
    }

    let mut insert = tx
        .prepare(
            "INSERT OR IGNORE INTO messages (
                    queue, payload, status, attempts, max_attempts,
                    available_at, lease_until, receipt, job_id,
                    created_at, updated_at
                ) VALUES (?1, ?2, ?3, 0, ?4, ?5, NULL, NULL, ?6, ?7, ?8)",
        )
        .map_err(QueueError::from)?;

    let mut ids = Vec::with_capacity(entries.len());
    for entry in entries {
        insert
            .execute(params![
                entry.queue_name,
                entry.payload,
                0i64, // STATUS_READY
                max_attempts,
                now,
                entry.job_id,
                now,
                now,
            ])
            .map_err(QueueError::from)?;

        let id = match entry.job_id {
            Some(jid) => tx
                .query_row(
                    "SELECT id FROM messages WHERE queue = ?1 AND job_id = ?2",
                    params![entry.queue_name, jid],
                    |row| row.get(0),
                )
                .map_err(QueueError::from)?,
            None => tx.last_insert_rowid(),
        };
        ids.push(id);
    }
    drop(insert);

    #[cfg(feature = "__crash_test")]
    crate::failpoints::hit(crate::failpoints::Failpoint::EnqueueBeforeCommit);
    tx.commit().map_err(QueueError::from)?;
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
    fn enqueue_batch_vazio_nao_abre_transacao() {
        let (_dir, storage) = open_storage();
        let ids = storage.enqueue_batch(&[], 3, None, None).unwrap();
        assert!(ids.is_empty());
    }

    #[test]
    fn enqueue_batch_retorna_ids_na_ordem() {
        let (_dir, storage) = open_storage();
        let entries = [
            EnqueueEntry {
                queue_name: "q",
                payload: b"a",
                job_id: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"b",
                job_id: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"c",
                job_id: None,
            },
        ];
        let ids = storage.enqueue_batch(&entries, 3, None, None).unwrap();
        assert_eq!(ids.len(), 3);
        assert!(ids[0] < ids[1] && ids[1] < ids[2]);
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
                }],
                3,
                None,
                None,
            )
            .unwrap();
        // Mesmo job_id repetido no mesmo batch e em batch posterior.
        let entries = [
            EnqueueEntry {
                queue_name: "q",
                payload: b"dup",
                job_id: Some("j1"),
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"dup2",
                job_id: Some("j1"),
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"outro",
                job_id: Some("j2"),
            },
        ];
        let ids = storage.enqueue_batch(&entries, 3, None, None).unwrap();
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
            },
            EnqueueEntry {
                queue_name: "qb",
                payload: b"x",
                job_id: Some("j1"),
            },
        ];
        let ids = storage.enqueue_batch(&entries, 3, None, None).unwrap();
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
        }];
        storage
            .enqueue_batch(&first, 3, Some(policy), None)
            .unwrap();
        let rejected = [
            EnqueueEntry {
                queue_name: "q",
                payload: b"second",
                job_id: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"third",
                job_id: None,
            },
        ];

        assert!(matches!(
            storage.enqueue_batch(&rejected, 3, Some(policy), None),
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
        }];
        let existing_id = storage
            .enqueue_batch(&existing, 3, Some(policy), None)
            .unwrap()[0];
        let mixed = [
            EnqueueEntry {
                queue_name: "q",
                payload: b"ignored",
                job_id: Some("existing"),
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"new",
                job_id: Some("new"),
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"also ignored",
                job_id: Some("new"),
            },
        ];

        let ids = storage
            .enqueue_batch(&mixed, 3, Some(policy), None)
            .unwrap();
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
            })
            .collect();
        let original_ids = storage.enqueue_batch(&entries, 3, None, None).unwrap();
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
                    }
                })
            })
            .collect();
        let policy = CapacityPolicy {
            queue_name: "q",
            max_pending_jobs: 1,
        };

        let returned = storage
            .enqueue_batch(&duplicates, 3, Some(policy), None)
            .unwrap();

        let expected: Vec<_> = original_ids.iter().flat_map(|id| [*id, *id]).collect();
        assert_eq!(returned, expected);
        let new_entry = [EnqueueEntry {
            queue_name: "q",
            payload: b"new",
            job_id: Some("new"),
        }];
        assert!(matches!(
            storage.enqueue_batch(&new_entry, 3, Some(policy), None),
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
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"two",
                job_id: None,
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"three",
                job_id: None,
            },
        ];

        assert!(matches!(
            storage.enqueue_batch(&entries, 3, Some(policy), None),
            Err(QueueError::FullImpossible)
        ));
        let distinct_ids = [
            EnqueueEntry {
                queue_name: "q",
                payload: b"one",
                job_id: Some("one"),
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"two",
                job_id: Some("two"),
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"three",
                job_id: Some("three"),
            },
        ];
        assert!(matches!(
            storage.enqueue_batch(&distinct_ids, 3, Some(policy), None),
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
            },
            EnqueueEntry {
                queue_name: "beta",
                payload: b"b",
                job_id: None,
            },
        ];
        storage.enqueue_batch(&entries, 3, None, None).unwrap();

        let alpha_duplicate = [EnqueueEntry {
            queue_name: "alpha",
            payload: b"blocked",
            job_id: None,
        }];
        assert!(matches!(
            storage.enqueue_batch(
                &alpha_duplicate,
                3,
                Some(CapacityPolicy {
                    queue_name: "alpha",
                    max_pending_jobs: 1,
                }),
                None,
            ),
            Err(QueueError::Full)
        ));
        let beta_duplicate = [EnqueueEntry {
            queue_name: "beta",
            payload: b"allowed",
            job_id: None,
        }];
        storage
            .enqueue_batch(
                &beta_duplicate,
                3,
                Some(CapacityPolicy {
                    queue_name: "beta",
                    max_pending_jobs: 2,
                }),
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
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"b",
                job_id: None,
            },
        ];
        storage.enqueue_batch(&entries, 3, None, None).unwrap();
        let extra = [EnqueueEntry {
            queue_name: "q",
            payload: b"extra",
            job_id: None,
        }];

        assert!(matches!(
            storage.enqueue_batch(
                &extra,
                3,
                Some(CapacityPolicy {
                    queue_name: "q",
                    max_pending_jobs: 1,
                }),
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
                    }];
                    barrier.wait();
                    storage.enqueue_batch(
                        &entry,
                        3,
                        Some(CapacityPolicy {
                            queue_name: "q",
                            max_pending_jobs: 1,
                        }),
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
        }];

        let error = storage
            .enqueue_batch(
                &entry,
                3,
                Some(CapacityPolicy {
                    queue_name: "q",
                    max_pending_jobs: 1,
                }),
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
        }];
        let ids = storage
            .enqueue_batch(
                &entry,
                3,
                Some(CapacityPolicy {
                    queue_name: "q",
                    max_pending_jobs: 1,
                }),
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
        }];
        assert!(matches!(
            storage.enqueue_batch(
                &rejected,
                3,
                Some(CapacityPolicy {
                    queue_name: "q",
                    max_pending_jobs: 1,
                }),
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
            },
            EnqueueEntry {
                queue_name: "q",
                payload: b"pending",
                job_id: None,
            },
        ];
        let ids = storage.enqueue_batch(&entries, 3, None, None).unwrap();
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
        }];

        assert!(matches!(
            storage.enqueue_batch(
                &entry,
                3,
                Some(CapacityPolicy {
                    queue_name: "q",
                    max_pending_jobs: 1,
                }),
                Some(0),
            ),
            Err(QueueError::Closed)
        ));
        assert!(matches!(
            storage.retry_failed("q", 1, Some(1)),
            Err(QueueError::Closed)
        ));
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
