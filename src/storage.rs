use rusqlite::{params, Connection, ErrorCode, OpenFlags, TransactionBehavior};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::error::{QueueError, Result};
use crate::schema::SCHEMA_SQL;

/// An item in a batch insertion. The payload is borrowed to avoid copying data
/// across the PyO3 boundary.
pub struct EnqueueEntry<'a> {
    pub queue_name: &'a str,
    pub payload: &'a [u8],
    pub job_id: Option<&'a str>,
}

pub struct Storage {
    conn: Mutex<Option<Connection>>,
}

impl Storage {
    pub fn new(path: &str, fsync: bool) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI,
        )?;

        conn.pragma_update(None, "busy_timeout", 5000)?;
        enable_wal(&conn)?;
        conn.pragma_update(None, "synchronous", if fsync { "FULL" } else { "NORMAL" })?;
        conn.pragma_update(None, "foreign_keys", "ON")?;

        conn.execute_batch(SCHEMA_SQL)?;

        Ok(Self {
            conn: Mutex::new(Some(conn)),
        })
    }

    pub fn connection(&self) -> std::sync::MutexGuard<'_, Option<Connection>> {
        self.conn.lock().expect("mutex poisoned")
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
    ) -> Result<Vec<i64>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let now = now_ms();
        let mut guard = self.connection();
        let conn = guard.as_mut().ok_or(QueueError::Closed)?;

        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(QueueError::from)?;

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

        tx.commit().map_err(QueueError::from)?;
        Ok(ids)
    }
}

fn enable_wal(conn: &Connection) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);

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

    fn open_storage() -> (tempfile_guard::TempDir, Storage) {
        let dir = tempfile_guard::TempDir::new();
        let path = dir.path().join("test.db");
        let storage = Storage::new(path.to_str().unwrap(), false).unwrap();
        (dir, storage)
    }

    #[test]
    fn enqueue_batch_vazio_nao_abre_transacao() {
        let (_dir, storage) = open_storage();
        let ids = storage.enqueue_batch(&[], 3).unwrap();
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
        let ids = storage.enqueue_batch(&entries, 3).unwrap();
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
        let ids = storage.enqueue_batch(&entries, 3).unwrap();
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
        let ids = storage.enqueue_batch(&entries, 3).unwrap();
        assert_ne!(ids[0], ids[1]);
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
