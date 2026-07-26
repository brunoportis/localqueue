pub const BASE_SCHEMA_SQL: &str = r#"
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
    failure_category TEXT,
    job_id        TEXT,
    dedup_key     TEXT,
    dedup_fingerprint TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_job_id
    ON messages(queue, job_id) WHERE job_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_queue_status
    ON messages(queue, status, available_at, lease_until);
"#;

pub const CHECKPOINTS_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
    bus_name            TEXT NOT NULL,
    checkpoint_name     TEXT NOT NULL,
    cursor              TEXT NOT NULL,
    source_fingerprint  TEXT,
    generation          TEXT NOT NULL,
    version             INTEGER NOT NULL,
    items_committed     INTEGER NOT NULL,
    batches_committed   INTEGER NOT NULL,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    PRIMARY KEY (bus_name, checkpoint_name)
);
"#;

pub const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT, queue TEXT NOT NULL, payload BLOB NOT NULL,
    status INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL, available_at INTEGER NOT NULL,
    lease_until INTEGER, receipt TEXT, last_error TEXT, failure_reason TEXT,
    failure_category TEXT, job_id TEXT, dedup_key TEXT, dedup_fingerprint TEXT,
    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_job_id
    ON messages(queue, job_id) WHERE job_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_dedup_key
    ON messages(queue, dedup_key) WHERE dedup_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_queue_status
    ON messages(queue, status, available_at, lease_until);
CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
    bus_name TEXT NOT NULL, checkpoint_name TEXT NOT NULL, cursor TEXT NOT NULL,
    source_fingerprint TEXT, generation TEXT NOT NULL, version INTEGER NOT NULL,
    items_committed INTEGER NOT NULL, batches_committed INTEGER NOT NULL,
    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
    PRIMARY KEY (bus_name, checkpoint_name)
);
"#;
