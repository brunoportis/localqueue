use pyo3::prelude::*;
use std::path::PathBuf;

pyo3::create_exception!(
    localqueue,
    LocalQueueError,
    pyo3::exceptions::PyException,
    "Base exception for localqueue errors."
);

pyo3::create_exception!(
    localqueue,
    Empty,
    LocalQueueError,
    "Raised when no items are available in the queue."
);

pyo3::create_exception!(
    localqueue,
    Full,
    LocalQueueError,
    "Raised when the configured logical queue capacity is exhausted."
);

pyo3::create_exception!(
    localqueue,
    _FullImpossible,
    Full,
    "Private signal for an enqueue batch that can never fit."
);

pyo3::create_exception!(
    localqueue,
    LeaseExpired,
    LocalQueueError,
    "Raised when a job lease has expired."
);

#[derive(thiserror::Error, Debug)]
pub enum QueueError {
    #[error("queue is empty")]
    Empty,

    #[error("queue is full")]
    Full,

    #[error("queue is full")]
    FullImpossible,

    #[error("lease has expired")]
    LeaseExpired,

    #[error("job not found")]
    NotFound,

    #[error("queue is closed")]
    Closed,

    #[error("backup destination already exists: {0}")]
    BackupDestinationExists(PathBuf),

    #[error("invalid backup destination: {0}")]
    InvalidBackupDestination(String),

    #[error("backup integrity check failed: {0:?}")]
    BackupIntegrity(Vec<String>),

    #[error("max_errors must be between 1 and 1000")]
    InvalidIntegrityMaxErrors,

    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, QueueError>;

impl From<QueueError> for PyErr {
    fn from(err: QueueError) -> PyErr {
        match err {
            QueueError::Empty => PyErr::new::<Empty, _>("queue is empty"),
            QueueError::Full => PyErr::new::<Full, _>("queue is full"),
            QueueError::FullImpossible => PyErr::new::<_FullImpossible, _>("queue is full"),
            QueueError::LeaseExpired => PyErr::new::<LeaseExpired, _>("lease has expired"),
            QueueError::NotFound => PyErr::new::<LocalQueueError, _>("job not found"),
            QueueError::Closed => PyErr::new::<LocalQueueError, _>("queue is closed"),
            QueueError::BackupDestinationExists(path) => {
                PyErr::new::<pyo3::exceptions::PyFileExistsError, _>(path)
            }
            QueueError::InvalidBackupDestination(message) => {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(message)
            }
            QueueError::BackupIntegrity(messages) => PyErr::new::<LocalQueueError, _>(format!(
                "backup integrity check failed: {}",
                messages.join("; ")
            )),
            QueueError::InvalidIntegrityMaxErrors => {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(err.to_string())
            }
            QueueError::Sqlite(e) => {
                let message = match &e {
                    rusqlite::Error::SqliteFailure(failure, Some(message))
                        if failure.code == rusqlite::ErrorCode::DiskFull
                            && message == "not an error" =>
                    {
                        "database or disk is full".to_owned()
                    }
                    rusqlite::Error::SqliteFailure(_, Some(message)) => message.clone(),
                    rusqlite::Error::SqliteFailure(failure, None) => match failure.code {
                        rusqlite::ErrorCode::DiskFull => "database or disk is full".to_owned(),
                        rusqlite::ErrorCode::DatabaseBusy => "database is busy".to_owned(),
                        rusqlite::ErrorCode::DatabaseLocked => "database is locked".to_owned(),
                        _ => e.to_string(),
                    },
                    _ => e.to_string(),
                };
                PyErr::new::<LocalQueueError, _>(message)
            }
            QueueError::Io(e) => PyErr::new::<LocalQueueError, _>(format!("{}", e)),
        }
    }
}
