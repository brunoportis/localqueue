mod backup;
mod diagnostics;
mod error;
#[cfg(feature = "__crash_test")]
mod failpoints;
mod integrity;
mod queue;
mod schema;
mod storage;

use pyo3::prelude::*;

use backup::BackupSnapshot;
use diagnostics::DiagnosticsSnapshot;
use error::{Empty, LeaseExpired, LocalQueueError};
use integrity::IntegrityCheckSnapshot;
use queue::{FailedMessage, Lease, NativeQueue, Stats};

/// Native `localqueue` module.
#[pymodule]
fn localqueue(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeQueue>()?;
    m.add_class::<Lease>()?;
    m.add_class::<Stats>()?;
    m.add_class::<FailedMessage>()?;
    m.add_class::<DiagnosticsSnapshot>()?;
    m.add_class::<IntegrityCheckSnapshot>()?;
    m.add_class::<BackupSnapshot>()?;
    m.add("LocalQueueError", _py.get_type::<LocalQueueError>())?;
    m.add("Empty", _py.get_type::<Empty>())?;
    m.add("LeaseExpired", _py.get_type::<LeaseExpired>())?;
    Ok(())
}
