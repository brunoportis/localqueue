use pyo3::prelude::*;

pyo3::create_exception!(
    simpleq,
    SimpleQError,
    pyo3::exceptions::PyException,
    "Erro genérico do simpleq."
);

pyo3::create_exception!(
    simpleq,
    Empty,
    SimpleQError,
    "Levantada quando não há itens disponíveis na fila."
);

pyo3::create_exception!(
    simpleq,
    LeaseExpired,
    SimpleQError,
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
            QueueError::NotFound => PyErr::new::<SimpleQError, _>("job não encontrado"),
            QueueError::Sqlite(e) => PyErr::new::<SimpleQError, _>(format!("{}", e)),
            QueueError::Io(e) => PyErr::new::<SimpleQError, _>(format!("{}", e)),
        }
    }
}
