mod error;
mod queue;
mod schema;
mod storage;

use pyo3::prelude::*;

use error::{Empty, LeaseExpired, SimpleQError};
use queue::{Lease, NativeQueue, Stats};

/// Módulo nativo `simpleq`.
#[pymodule]
fn simpleq(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeQueue>()?;
    m.add_class::<Lease>()?;
    m.add_class::<Stats>()?;
    m.add("SimpleQError", _py.get_type::<SimpleQError>())?;
    m.add("Empty", _py.get_type::<Empty>())?;
    m.add("LeaseExpired", _py.get_type::<LeaseExpired>())?;
    Ok(())
}
