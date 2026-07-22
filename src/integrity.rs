use pyo3::prelude::*;
use std::time::Instant;

use crate::error::{QueueError, Result};
use crate::storage::Storage;

#[derive(Debug, Clone)]
#[pyclass(skip_from_py_object)]
pub struct IntegrityCheckSnapshot {
    #[pyo3(get)]
    pub ok: bool,
    #[pyo3(get)]
    pub messages: Vec<String>,
    #[pyo3(get)]
    pub elapsed_ms: u64,
    #[pyo3(get)]
    pub check_mode: String,
}

pub fn check(storage: &Storage, quick: bool) -> Result<IntegrityCheckSnapshot> {
    let mut guard = storage.connection();
    let conn = guard.as_mut().ok_or(QueueError::Closed)?;
    let started = Instant::now();
    let (statement, check_mode) = if quick {
        ("PRAGMA quick_check", "quick")
    } else {
        ("PRAGMA integrity_check", "full")
    };

    let mut query = conn.prepare(statement)?;
    let rows = query.query_map([], |row| row.get::<_, String>(0))?;
    let messages = rows.collect::<std::result::Result<Vec<_>, _>>()?;
    let ok = messages.len() == 1 && messages[0] == "ok";

    Ok(IntegrityCheckSnapshot {
        ok,
        messages,
        elapsed_ms: started.elapsed().as_millis() as u64,
        check_mode: check_mode.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn storage_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "localqueue-integrity-{name}-{}-{}.db",
            std::process::id(),
            crate::storage::now_ms()
        ))
    }

    #[test]
    fn full_and_quick_checks_report_the_selected_mode() {
        let path = storage_path("healthy");
        let storage = Storage::new(path.to_str().unwrap(), false).unwrap();

        let full = check(&storage, false).unwrap();
        let quick = check(&storage, true).unwrap();

        assert!(full.ok);
        assert_eq!(full.messages, ["ok"]);
        assert_eq!(full.check_mode, "full");
        assert!(quick.ok);
        assert_eq!(quick.messages, ["ok"]);
        assert_eq!(quick.check_mode, "quick");
        storage.close().unwrap();
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn integrity_failure_messages_are_preserved() {
        let path = storage_path("invalid-check");
        let storage = Storage::new(path.to_str().unwrap(), false).unwrap();
        {
            let guard = storage.connection();
            let conn = guard.as_ref().unwrap();
            conn.execute_batch(
                "CREATE TABLE invalid_data(value INTEGER CHECK(value > 0));
                 PRAGMA ignore_check_constraints=ON;
                 INSERT INTO invalid_data VALUES (-1);
                 PRAGMA ignore_check_constraints=OFF;",
            )
            .unwrap();
        }

        let result = check(&storage, false).unwrap();

        assert!(!result.ok);
        assert!(result
            .messages
            .iter()
            .any(|message| message.contains("CHECK constraint failed")));
        storage.close().unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
