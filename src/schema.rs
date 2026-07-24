pub const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS messages (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    queue         TEXT NOT NULL,
    payload       BLOB NOT NULL,
    status        INTEGER NOT NULL,
    attempts      INTEGER NOT NULL DEFAULT 0,
    max_attempts  INTEGER NOT NULL,
    available_at  INTEGER NOT NULL,
    lease_until   INTEGER,
    receipt       TEXT,
    last_error    TEXT,
    failure_reason TEXT,
    job_id        TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_job_id
    ON messages(queue, job_id) WHERE job_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_queue_status
    ON messages(queue, status, available_at, lease_until);
"#;
