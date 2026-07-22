use pyo3::prelude::*;
use rusqlite::backup::{Backup, Progress, StepResult};
use rusqlite::{Connection, OpenFlags};
use std::path::{Path, PathBuf};
#[cfg(feature = "__crash_test")]
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::error::{QueueError, Result};
use crate::storage::{sqlite_sidecar_path, Storage, BUSY_TIMEOUT_MS};

const DATABASE_NAME: &str = "localqueue.db";
const INCOMPLETE_NAME: &str = ".localqueue.db.incomplete";
const PAGES_PER_STEP: i32 = 100;
const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(10);
const MAX_RETRY_DELAY: Duration = Duration::from_millis(100);

#[cfg(feature = "__crash_test")]
static TEST_BACKUP_MAX_PAGE_COUNT: AtomicI64 = AtomicI64::new(0);

#[derive(Debug, Clone)]
#[pyclass(skip_from_py_object)]
pub struct BackupSnapshot {
    #[pyo3(get)]
    pub schema_version: u8,
    #[pyo3(get)]
    pub elapsed_ms: u64,
    #[pyo3(get)]
    pub pages_copied: u64,
    #[pyo3(get)]
    pub page_count: u64,
    #[pyo3(get)]
    pub database_size_bytes: u64,
    #[pyo3(get)]
    pub verified: bool,
    #[pyo3(get)]
    pub verification_mode: String,
    #[pyo3(get)]
    pub verification_messages: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct CopyProgress {
    pages_copied: u64,
    page_count: u64,
}

pub fn create(storage: &Storage, destination_directory: &str) -> Result<BackupSnapshot> {
    create_with_copy(storage, destination_directory, |source, target| {
        copy_database(source, target, PAGES_PER_STEP, || {})
    })
}

fn create_with_copy<F>(
    storage: &Storage,
    destination_directory: &str,
    copy: F,
) -> Result<BackupSnapshot>
where
    F: FnOnce(&Connection, &mut Connection) -> rusqlite::Result<CopyProgress>,
{
    create_with_target_setup(storage, destination_directory, |_| Ok(()), copy)
}

fn create_with_target_setup<Setup, F>(
    storage: &Storage,
    destination_directory: &str,
    setup_target: Setup,
    copy: F,
) -> Result<BackupSnapshot>
where
    Setup: FnOnce(&mut Connection) -> rusqlite::Result<()>,
    F: FnOnce(&Connection, &mut Connection) -> rusqlite::Result<CopyProgress>,
{
    {
        let guard = storage.connection();
        if guard.is_none() {
            return Err(QueueError::Closed);
        }
    }

    let destination = validate_destination(storage.path(), destination_directory)?;
    let started = Instant::now();
    let reserved = ReservedDirectory::create(destination)?;

    let source = Connection::open_with_flags(
        storage.path(),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    source.pragma_update(None, "busy_timeout", BUSY_TIMEOUT_MS)?;
    let mut target = Connection::open_with_flags(
        reserved.incomplete_path(),
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    target.pragma_update(None, "busy_timeout", BUSY_TIMEOUT_MS)?;

    setup_target(&mut target)?;

    #[cfg(feature = "__crash_test")]
    {
        let page_limit = TEST_BACKUP_MAX_PAGE_COUNT.swap(0, Ordering::SeqCst);
        if page_limit > 0 {
            target.pragma_update(None, "max_page_count", page_limit)?;
        }
    }

    let progress = copy(&source, &mut target)?;
    target.pragma_update(None, "journal_mode", "DELETE")?;
    // The destination connection must be completely closed before independent
    // verification and before the final database name is published.
    drop(target);
    drop(source);

    let verification_messages = verify_backup(reserved.incomplete_path())?;
    remove_sqlite_sidecars(reserved.incomplete_path())?;
    let database_size_bytes = std::fs::metadata(reserved.incomplete_path())?.len();
    reserved.publish()?;

    Ok(BackupSnapshot {
        schema_version: 1,
        elapsed_ms: started.elapsed().as_millis() as u64,
        pages_copied: progress.pages_copied,
        page_count: progress.page_count,
        database_size_bytes,
        verified: true,
        verification_mode: "full".to_owned(),
        verification_messages,
    })
}

fn validate_destination(source: &Path, destination_directory: &str) -> Result<PathBuf> {
    let requested = std::path::absolute(destination_directory)?;
    let parent = requested.parent().ok_or_else(|| {
        QueueError::InvalidBackupDestination("destination has no parent directory".to_owned())
    })?;
    if !parent.is_dir() {
        return Err(QueueError::InvalidBackupDestination(
            "destination parent is not an existing directory".to_owned(),
        ));
    }
    let name = requested.file_name().ok_or_else(|| {
        QueueError::InvalidBackupDestination("destination must include a directory name".to_owned())
    })?;
    let destination = std::fs::canonicalize(parent)?.join(name);
    let source_identity = std::fs::canonicalize(source)?;
    let source_directory = source_identity.parent().ok_or_else(|| {
        QueueError::InvalidBackupDestination("source database has no parent directory".to_owned())
    })?;

    if destination == source_identity
        || destination.starts_with(&source_identity)
        || destination == source_directory
        || destination.starts_with(source_directory)
    {
        return Err(QueueError::InvalidBackupDestination(
            "destination cannot be the source database directory or be contained in it".to_owned(),
        ));
    }

    Ok(destination)
}

fn copy_database<F>(
    source: &Connection,
    target: &mut Connection,
    pages_per_step: i32,
    mut after_step: F,
) -> rusqlite::Result<CopyProgress>
where
    F: FnMut(),
{
    let backup = Backup::new(source, target)?;
    let started = Instant::now();
    run_backup_steps(
        || {
            let result = backup.step(pages_per_step)?;
            let progress = backup.progress();
            after_step();
            Ok((result, progress))
        },
        || started.elapsed(),
        thread::sleep,
        Duration::from_millis(BUSY_TIMEOUT_MS),
    )
}

fn run_backup_steps<Step, Now, Sleep>(
    mut step: Step,
    mut now: Now,
    mut sleep: Sleep,
    lock_deadline: Duration,
) -> rusqlite::Result<CopyProgress>
where
    Step: FnMut() -> rusqlite::Result<(StepResult, Progress)>,
    Now: FnMut() -> Duration,
    Sleep: FnMut(Duration),
{
    let mut locked_since = None;
    let mut retry_delay = INITIAL_RETRY_DELAY;

    loop {
        let (result, progress) = step()?;
        match result {
            StepResult::Done => {
                let page_count = u64::try_from(progress.pagecount.max(0)).unwrap_or(0);
                let remaining = u64::try_from(progress.remaining.max(0)).unwrap_or(0);
                return Ok(CopyProgress {
                    pages_copied: page_count.saturating_sub(remaining),
                    page_count,
                });
            }
            StepResult::More => {
                locked_since = None;
                retry_delay = INITIAL_RETRY_DELAY;
                sleep(INITIAL_RETRY_DELAY);
            }
            StepResult::Busy | StepResult::Locked => {
                let current = now();
                let lock_started = *locked_since.get_or_insert(current);
                let locked_for = current.saturating_sub(lock_started);
                if locked_for >= lock_deadline {
                    let (code, label) = if result == StepResult::Busy {
                        (rusqlite::ffi::SQLITE_BUSY, "busy")
                    } else {
                        (rusqlite::ffi::SQLITE_LOCKED, "locked")
                    };
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(code),
                        Some(format!(
                            "online backup remained {label} for the {BUSY_TIMEOUT_MS} ms busy timeout"
                        )),
                    ));
                }
                let remaining = lock_deadline - locked_for;
                sleep(retry_delay.min(remaining));
                retry_delay = (retry_delay * 2).min(MAX_RETRY_DELAY);
            }
            _ => unreachable!("rusqlite returned an unknown backup step"),
        }
    }
}

fn verify_backup(path: &Path) -> Result<Vec<String>> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    let mut statement = connection.prepare("PRAGMA integrity_check(100)")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let messages = rows.collect::<std::result::Result<Vec<_>, _>>()?;
    if messages.len() == 1 && messages[0] == "ok" {
        Ok(messages)
    } else {
        Err(QueueError::BackupIntegrity(messages))
    }
}

fn remove_sqlite_sidecars(database: &Path) -> std::io::Result<()> {
    for suffix in ["-journal", "-wal", "-shm"] {
        match std::fs::remove_file(sqlite_sidecar_path(database, suffix)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

struct ReservedDirectory {
    directory: PathBuf,
    incomplete: PathBuf,
    final_database: PathBuf,
    published: bool,
}

impl ReservedDirectory {
    fn create(directory: PathBuf) -> Result<Self> {
        match std::fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(QueueError::BackupDestinationExists(directory));
            }
            Err(error) => return Err(error.into()),
        }
        Ok(Self {
            incomplete: directory.join(INCOMPLETE_NAME),
            final_database: directory.join(DATABASE_NAME),
            directory,
            published: false,
        })
    }

    fn incomplete_path(&self) -> &Path {
        &self.incomplete
    }

    fn publish(mut self) -> Result<()> {
        std::fs::rename(&self.incomplete, &self.final_database)?;
        self.published = true;
        Ok(())
    }
}

impl Drop for ReservedDirectory {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        let _ = std::fs::remove_file(&self.incomplete);
        let _ = remove_sqlite_sidecars(&self.incomplete);
        // This intentionally removes only an empty directory owned by this
        // operation. It never recursively removes a pre-existing path.
        let _ = std::fs::remove_dir(&self.directory);
    }
}

#[cfg(feature = "__crash_test")]
pub fn set_test_backup_max_page_count(pages: i64) {
    TEST_BACKUP_MAX_PAGE_COUNT.store(pages, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::ErrorCode;
    use std::cell::Cell;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "{name}-{}-{}",
                std::process::id(),
                crate::storage::now_ms()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn progress(remaining: i32, pagecount: i32) -> Progress {
        Progress {
            remaining,
            pagecount,
        }
    }

    fn run_fake_steps(results: Vec<StepResult>) -> rusqlite::Result<CopyProgress> {
        let elapsed = Cell::new(Duration::ZERO);
        let mut results = results.into_iter();
        run_backup_steps(
            || {
                let result = results.next().unwrap_or(StepResult::Done);
                Ok((result, progress(i32::from(result != StepResult::Done), 1)))
            },
            || elapsed.get(),
            |duration| elapsed.set(elapsed.get() + duration),
            Duration::from_millis(BUSY_TIMEOUT_MS),
        )
    }

    #[test]
    fn transient_busy_and_locked_steps_recover() {
        for transient in [StepResult::Busy, StepResult::Locked] {
            let result = run_fake_steps(vec![transient, StepResult::More, StepResult::Done])
                .expect("transient lock should recover");
            assert_eq!(result.pages_copied, 1);
            assert_eq!(result.page_count, 1);
        }
    }

    #[test]
    fn persistent_busy_stops_at_the_explicit_deadline() {
        let elapsed = Cell::new(Duration::ZERO);
        let error = run_backup_steps(
            || Ok((StepResult::Busy, progress(1, 1))),
            || elapsed.get(),
            |duration| elapsed.set(elapsed.get() + duration),
            Duration::from_millis(BUSY_TIMEOUT_MS),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            rusqlite::Error::SqliteFailure(failure, Some(message))
                if failure.code == ErrorCode::DatabaseBusy
                    && message.contains("5000 ms busy timeout")
        ));
        assert_eq!(elapsed.get(), Duration::from_millis(BUSY_TIMEOUT_MS));
    }

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

        let error = copy_database(&source, &mut target, PAGES_PER_STEP, || {}).unwrap_err();

        assert!(matches!(
            error,
            rusqlite::Error::SqliteFailure(failure, _)
                if failure.code == ErrorCode::DiskFull
        ));
    }

    #[test]
    fn staged_timeout_preserves_error_cleans_directory_and_allows_retry() {
        let root = TempDir::new("localqueue-backup-timeout");
        let source_directory = root.path().join("source");
        std::fs::create_dir(&source_directory).unwrap();
        let source_path = source_directory.join(DATABASE_NAME);
        let storage = Storage::new(source_path.to_str().unwrap(), false).unwrap();
        let failed_destination = root.path().join("timed-out");

        let error = create_with_copy(&storage, failed_destination.to_str().unwrap(), |_, _| {
            let elapsed = Cell::new(Duration::ZERO);
            run_backup_steps(
                || Ok((StepResult::Busy, progress(1, 1))),
                || elapsed.get(),
                |duration| elapsed.set(elapsed.get() + duration),
                Duration::from_millis(BUSY_TIMEOUT_MS),
            )
        })
        .unwrap_err();

        assert!(matches!(
            error,
            QueueError::Sqlite(rusqlite::Error::SqliteFailure(failure, Some(message)))
                if failure.code == ErrorCode::DatabaseBusy
                    && message.contains("5000 ms busy timeout")
        ));
        assert!(!failed_destination.exists());

        let retry_destination = root.path().join("retry");
        let retry = create(&storage, retry_destination.to_str().unwrap()).unwrap();
        assert!(retry.verified);
        assert!(retry_destination.join(DATABASE_NAME).is_file());
    }

    #[test]
    fn staged_disk_full_preserves_source_cleans_directory_and_allows_retry() {
        let root = TempDir::new("localqueue-backup-full");
        let source_directory = root.path().join("source");
        std::fs::create_dir(&source_directory).unwrap();
        let source_path = source_directory.join(DATABASE_NAME);
        let storage = Storage::new(source_path.to_str().unwrap(), false).unwrap();
        {
            let guard = storage.connection();
            guard
                .as_ref()
                .unwrap()
                .execute_batch(
                    "CREATE TABLE backup_full_fixture(value BLOB);
                     INSERT INTO backup_full_fixture VALUES (zeroblob(262144));",
                )
                .unwrap();
        }
        let source_size = std::fs::metadata(&source_path).unwrap().len();
        let failed_destination = root.path().join("disk-full");
        let error = create_with_target_setup(
            &storage,
            failed_destination.to_str().unwrap(),
            |target| target.pragma_update(None, "max_page_count", 1),
            |source, target| copy_database(source, target, PAGES_PER_STEP, || {}),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            QueueError::Sqlite(rusqlite::Error::SqliteFailure(failure, _))
                if failure.code == ErrorCode::DiskFull
        ));
        assert!(!failed_destination.exists());
        assert_eq!(std::fs::metadata(&source_path).unwrap().len(), source_size);
        let source = storage.connection();
        assert_eq!(
            source
                .as_ref()
                .unwrap()
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        drop(source);

        let retry_destination = root.path().join("retry");
        let retry = create(&storage, retry_destination.to_str().unwrap()).unwrap();
        assert!(retry.verified);
        assert!(retry_destination.join(DATABASE_NAME).is_file());
    }

    #[test]
    fn staged_failure_invocations_keep_page_limits_isolated() {
        use std::sync::{mpsc, Arc, Barrier};

        let barrier = Arc::new(Barrier::new(3));
        let (done_sender, done_receiver) = mpsc::channel();
        let timeout_barrier = Arc::clone(&barrier);
        let timeout_done = done_sender.clone();
        let timeout_thread = thread::spawn(move || {
            let root = TempDir::new("localqueue-backup-isolated-timeout");
            let source_directory = root.path().join("source");
            std::fs::create_dir(&source_directory).unwrap();
            let source_path = source_directory.join(DATABASE_NAME);
            let storage = Storage::new(source_path.to_str().unwrap(), false).unwrap();
            timeout_barrier.wait();

            let failed_destination = root.path().join("timed-out");
            let error = create_with_copy(&storage, failed_destination.to_str().unwrap(), |_, _| {
                let elapsed = Cell::new(Duration::ZERO);
                run_backup_steps(
                    || Ok((StepResult::Busy, progress(1, 1))),
                    || elapsed.get(),
                    |duration| elapsed.set(elapsed.get() + duration),
                    Duration::from_millis(BUSY_TIMEOUT_MS),
                )
            })
            .unwrap_err();
            assert!(matches!(
                error,
                QueueError::Sqlite(rusqlite::Error::SqliteFailure(failure, _))
                    if failure.code == ErrorCode::DatabaseBusy
            ));
            assert!(!failed_destination.exists());
            let retry = create(&storage, root.path().join("retry").to_str().unwrap()).unwrap();
            assert!(retry.verified);
            timeout_done.send(()).unwrap();
        });

        let disk_full_barrier = Arc::clone(&barrier);
        let disk_full_done = done_sender;
        let disk_full_thread = thread::spawn(move || {
            let root = TempDir::new("localqueue-backup-isolated-full");
            let source_directory = root.path().join("source");
            std::fs::create_dir(&source_directory).unwrap();
            let source_path = source_directory.join(DATABASE_NAME);
            let storage = Storage::new(source_path.to_str().unwrap(), false).unwrap();
            {
                let guard = storage.connection();
                guard
                    .as_ref()
                    .unwrap()
                    .execute_batch(
                        "CREATE TABLE backup_full_fixture(value BLOB);
                         INSERT INTO backup_full_fixture VALUES (zeroblob(262144));",
                    )
                    .unwrap();
            }
            let source_size = std::fs::metadata(&source_path).unwrap().len();
            disk_full_barrier.wait();

            let failed_destination = root.path().join("disk-full");
            let error = create_with_target_setup(
                &storage,
                failed_destination.to_str().unwrap(),
                |target| target.pragma_update(None, "max_page_count", 1),
                |source, target| copy_database(source, target, PAGES_PER_STEP, || {}),
            )
            .unwrap_err();
            assert!(matches!(
                error,
                QueueError::Sqlite(rusqlite::Error::SqliteFailure(failure, _))
                    if failure.code == ErrorCode::DiskFull
            ));
            assert!(!failed_destination.exists());
            assert_eq!(std::fs::metadata(&source_path).unwrap().len(), source_size);
            let source = storage.connection();
            assert_eq!(
                source
                    .as_ref()
                    .unwrap()
                    .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
            drop(source);
            let retry = create(&storage, root.path().join("retry").to_str().unwrap()).unwrap();
            assert!(retry.verified);
            disk_full_done.send(()).unwrap();
        });

        barrier.wait();
        done_receiver.recv_timeout(Duration::from_secs(10)).unwrap();
        done_receiver.recv_timeout(Duration::from_secs(10)).unwrap();
        timeout_thread.join().unwrap();
        disk_full_thread.join().unwrap();
    }

    #[test]
    fn stepped_backup_is_consistent_when_another_connection_writes() {
        let directory = TempDir::new("localqueue-backup-concurrency");
        let source_path = directory.path().join("source.db");
        let target_path = directory.path().join("target.db");
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
    }
}
