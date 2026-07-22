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
    LeaseExpired,
    LocalQueueError,
    "Raised when a job lease has expired."
);

#[derive(thiserror::Error, Debug)]
pub enum QueueError {
    #[error("queue is empty")]
    Empty,

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
            QueueError::Sqlite(e) => PyErr::new::<LocalQueueError, _>(format!("{}", e)),
            QueueError::Io(e) => PyErr::new::<LocalQueueError, _>(format!("{}", e)),
        }
    }
}
