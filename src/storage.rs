use rusqlite::{Connection, ErrorCode, OpenFlags};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::error::{QueueError, Result};
use crate::schema::SCHEMA_SQL;

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
        self.conn.lock().expect("mutex envenenado")
    }

    pub fn close(&self) -> Result<()> {
        let mut guard = self.connection();
        if let Some(conn) = guard.take() {
            conn.close().map_err(|(_, e)| QueueError::Sqlite(e))?;
        }
        Ok(())
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
        .expect("relógio do sistema anterior ao Unix epoch")
        .as_millis() as i64
}
