use pyo3::prelude::*;

pyo3::create_exception!(
    localqueue,
    LocalQueueError,
    pyo3::exceptions::PyException,
    "Erro genérico do localqueue."
);

pyo3::create_exception!(
    localqueue,
    Empty,
    LocalQueueError,
    "Levantada quando não há itens disponíveis na fila."
);

pyo3::create_exception!(
    localqueue,
    LeaseExpired,
    LocalQueueError,
    "Levantada quando o lease de um job expirou."
);

#[derive(thiserror::Error, Debug)]
pub enum QueueError {
    #[error("fila vazia")]
    Empty,

    #[error("lease expirado")]
    LeaseExpired,

    #[error("job não encontrado")]
    NotFound,

    #[error("fila fechada")]
    Closed,

    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, QueueError>;

impl From<QueueError> for PyErr {
    fn from(err: QueueError) -> PyErr {
        match err {
            QueueError::Empty => PyErr::new::<Empty, _>("fila vazia"),
            QueueError::LeaseExpired => PyErr::new::<LeaseExpired, _>("lease expirado"),
            QueueError::NotFound => PyErr::new::<LocalQueueError, _>("job não encontrado"),
            QueueError::Closed => PyErr::new::<LocalQueueError, _>("fila fechada"),
            QueueError::Sqlite(e) => PyErr::new::<LocalQueueError, _>(format!("{}", e)),
            QueueError::Io(e) => PyErr::new::<LocalQueueError, _>(format!("{}", e)),
        }
    }
}
