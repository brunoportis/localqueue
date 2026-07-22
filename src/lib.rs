mod error;
#[cfg(feature = "__crash_test")]
mod failpoints;
mod queue;
mod schema;
mod storage;

use pyo3::prelude::*;

use error::{Empty, LeaseExpired, LocalQueueError};
use queue::{FailedMessage, Lease, NativeQueue, Stats};

/// Native `localqueue` module.
#[pymodule]
fn localqueue(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeQueue>()?;
    m.add_class::<Lease>()?;
    m.add_class::<Stats>()?;
    m.add_class::<FailedMessage>()?;
    m.add("LocalQueueError", _py.get_type::<LocalQueueError>())?;
    m.add("Empty", _py.get_type::<Empty>())?;
    m.add("LeaseExpired", _py.get_type::<LeaseExpired>())?;
    Ok(())
}
