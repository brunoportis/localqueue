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

pub const EXECUTION_MEMBERSHIP_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS event_bus_executions (
    execution_id    TEXT PRIMARY KEY,
    bus_name        TEXT NOT NULL,
    source_name     TEXT NOT NULL,
    checkpoint_name TEXT,
    source_completed INTEGER NOT NULL DEFAULT 0 CHECK (source_completed IN (0, 1)),
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS event_bus_execution_deliveries (
    execution_id TEXT NOT NULL,
    message_id   INTEGER NOT NULL,
    PRIMARY KEY (execution_id, message_id),
    FOREIGN KEY (execution_id) REFERENCES event_bus_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_event_bus_execution_deliveries_message
    ON event_bus_execution_deliveries(message_id);
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
CREATE TABLE IF NOT EXISTS event_bus_executions (
    execution_id TEXT PRIMARY KEY, bus_name TEXT NOT NULL, source_name TEXT NOT NULL,
    checkpoint_name TEXT, source_completed INTEGER NOT NULL DEFAULT 0 CHECK (source_completed IN (0, 1)),
    created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS event_bus_execution_deliveries (
    execution_id TEXT NOT NULL, message_id INTEGER NOT NULL,
    PRIMARY KEY (execution_id, message_id),
    FOREIGN KEY (execution_id) REFERENCES event_bus_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_event_bus_execution_deliveries_message
    ON event_bus_execution_deliveries(message_id);
"#;
