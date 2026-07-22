use pyo3::prelude::*;
use rusqlite::backup::{Backup, StepResult};
use rusqlite::{Connection, OpenFlags};
use std::ffi::OsString;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::error::{QueueError, Result};
use crate::storage::{sqlite_sidecar_path, Storage};

static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
#[pyclass(skip_from_py_object)]
pub struct BackupSnapshot {
    #[pyo3(get)]
    pub overwritten: bool,
    #[pyo3(get)]
    pub elapsed_ms: u64,
    #[pyo3(get)]
    pub database_size_bytes: u64,
}

pub fn create(storage: &Storage, destination: &str, overwrite: bool) -> Result<BackupSnapshot> {
    {
        let guard = storage.connection();
        if guard.is_none() {
            return Err(QueueError::Closed);
        }
    }

    let destination = std::path::absolute(destination)?;
    if destination.is_dir() {
        return Err(QueueError::InvalidBackupDestination(
            "destination is a directory".to_owned(),
        ));
    }
    let parent = destination.parent().ok_or_else(|| {
        QueueError::InvalidBackupDestination("destination has no parent directory".to_owned())
    })?;
    if !parent.is_dir() {
        return Err(QueueError::InvalidBackupDestination(
            "destination parent is not an existing directory".to_owned(),
        ));
    }

    let source_identity = std::fs::canonicalize(storage.path())?;
    let destination_identity = if destination.exists() {
        std::fs::canonicalize(&destination)?
    } else {
        std::fs::canonicalize(parent)?.join(destination.file_name().ok_or_else(|| {
            QueueError::InvalidBackupDestination("destination must include a file name".to_owned())
        })?)
    };
    if destination_identity == source_identity {
        return Err(QueueError::InvalidBackupDestination(
            "destination is the active database".to_owned(),
        ));
    }

    let destination_existed = destination.exists();
    if destination_existed && !overwrite {
        return Err(QueueError::BackupDestinationExists(destination));
    }

    let started = Instant::now();
    let temporary = TemporaryBackup::create(parent, &destination)?;
    let source = Connection::open_with_flags(
        storage.path(),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    source.pragma_update(None, "busy_timeout", 5000)?;
    let mut target = Connection::open_with_flags(
        temporary.path(),
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_URI,
    )?;
    target.pragma_update(None, "busy_timeout", 5000)?;

    copy_database(&source, &mut target, 100, || {})?;
    drop(target);
    drop(source);
    verify_backup(temporary.path())?;

    let database_size_bytes = std::fs::metadata(temporary.path())?.len();
    temporary.publish(&destination, overwrite)?;

    Ok(BackupSnapshot {
        overwritten: destination_existed,
        elapsed_ms: started.elapsed().as_millis() as u64,
        database_size_bytes,
    })
}

fn copy_database<F>(
    source: &Connection,
    target: &mut Connection,
    pages_per_step: i32,
    mut after_step: F,
) -> rusqlite::Result<()>
where
    F: FnMut(),
{
    let backup = Backup::new(source, target)?;
    loop {
        let step = backup.step(pages_per_step)?;
        after_step();
        match step {
            StepResult::Done => return Ok(()),
            StepResult::More | StepResult::Busy | StepResult::Locked => {
                thread::sleep(Duration::from_millis(10));
            }
            _ => unreachable!("rusqlite returned an unknown backup step"),
        }
    }
}

fn verify_backup(path: &Path) -> Result<()> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    let mut statement = connection.prepare("PRAGMA integrity_check")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let messages = rows.collect::<std::result::Result<Vec<_>, _>>()?;
    if messages.len() == 1 && messages[0] == "ok" {
        Ok(())
    } else {
        Err(QueueError::BackupIntegrity(messages))
    }
}

struct TemporaryBackup {
    path: PathBuf,
    published: bool,
}

impl TemporaryBackup {
    fn create(parent: &Path, destination: &Path) -> Result<Self> {
        let file_name = destination
            .file_name()
            .ok_or_else(|| {
                QueueError::InvalidBackupDestination(
                    "destination must include a file name".to_owned(),
                )
            })?
            .to_os_string();

        for _ in 0..100 {
            let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let mut temporary_name = OsString::from(".");
            temporary_name.push(&file_name);
            temporary_name.push(format!(".localqueue-{}-{sequence}.tmp", std::process::id()));
            let path = parent.join(temporary_name);
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(file) => {
                    drop(file);
                    return Ok(Self {
                        path,
                        published: false,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }

        Err(QueueError::InvalidBackupDestination(
            "could not allocate a temporary backup file".to_owned(),
        ))
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn publish(mut self, destination: &Path, overwrite: bool) -> Result<()> {
        if overwrite {
            publish_overwriting(&self.path, destination)?;
        } else {
            match std::fs::hard_link(&self.path, destination) {
                Ok(()) => std::fs::remove_file(&self.path)?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    return Err(QueueError::BackupDestinationExists(destination.to_owned()));
                }
                Err(error) => return Err(error.into()),
            }
        }
        self.published = true;
        Ok(())
    }
}

impl Drop for TemporaryBackup {
    fn drop(&mut self) {
        if !self.published {
            let _ = std::fs::remove_file(&self.path);
        }
        for suffix in ["-journal", "-wal", "-shm"] {
            let _ = std::fs::remove_file(sqlite_sidecar_path(&self.path, suffix));
        }
    }
}

#[cfg(unix)]
fn publish_overwriting(temporary: &Path, destination: &Path) -> Result<()> {
    std::fs::rename(temporary, destination)?;
    Ok(())
}

#[cfg(windows)]
fn publish_overwriting(temporary: &Path, destination: &Path) -> Result<()> {
    if destination.exists() {
        std::fs::remove_file(destination)?;
    }
    std::fs::rename(temporary, destination)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::ErrorCode;

    #[test]
    fn destination_full_error_is_preserved() {
        let source = Connection::open_in_memory().unwrap();
        source
            .execute_batch(
                "CREATE TABLE data(value BLOB);
                 INSERT INTO data VALUES (zeroblob(131072));",
            )
            .unwrap();
        let mut target = Connection::open_in_memory().unwrap();
        target.pragma_update(None, "max_page_count", 1).unwrap();

        let error = copy_database(&source, &mut target, 100, || {}).unwrap_err();

        assert!(matches!(
            error,
            rusqlite::Error::SqliteFailure(failure, _)
                if failure.code == ErrorCode::DiskFull
        ));
    }

    #[test]
    fn stepped_backup_is_consistent_when_another_connection_writes() {
        let directory = std::env::temp_dir().join(format!(
            "localqueue-backup-concurrency-{}-{}",
            std::process::id(),
            crate::storage::now_ms()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let source_path = directory.join("source.db");
        let target_path = directory.join("target.db");
        let source = Connection::open(&source_path).unwrap();
        source.pragma_update(None, "journal_mode", "WAL").unwrap();
        source
            .execute_batch(
                "CREATE TABLE data(value BLOB);
                 WITH RECURSIVE counter(value) AS (
                    SELECT 1 UNION ALL SELECT value + 1 FROM counter WHERE value < 200
                 ) INSERT INTO data SELECT zeroblob(4096) FROM counter;",
            )
            .unwrap();
        let writer = Connection::open(&source_path).unwrap();
        let mut target = Connection::open(&target_path).unwrap();
        let mut wrote = false;

        copy_database(&source, &mut target, 1, || {
            if !wrote {
                writer
                    .execute("INSERT INTO data VALUES (zeroblob(4096))", [])
                    .unwrap();
                wrote = true;
            }
        })
        .unwrap();
        drop(target);

        verify_backup(&target_path).unwrap();
        let verified =
            Connection::open_with_flags(&target_path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let count: i64 = verified
            .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
            .unwrap();
        assert!(matches!(count, 200 | 201));
        drop(verified);
        drop(writer);
        drop(source);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
