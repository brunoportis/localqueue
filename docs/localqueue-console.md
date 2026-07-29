# LocalQueue Console v0

Run the optional native console against a LocalQueue data directory:

```bash
cargo run --features console --bin localqueue-console -- /data/contacts
```

The console also accepts a direct `localqueue.db` path. It is intentionally a
single-machine native tool; it does not start an HTTP server or access remote
queues.

## Architecture

`egui` runs only presentation and interaction state on the UI thread. It never
opens SQLite or runs a query during `update`. The UI reads a cloned immutable
snapshot and sends small commands such as selecting a subscription, inspecting
a failed delivery, retrying one delivery, or opening another path.

One background sampler owns the `AdminStore`. Every refresh opens a dedicated
SQLite read-only connection for each short query, materializes its page or
summary, then commits and closes it. That deliberately avoids retaining a read
transaction while the console is idle, which would otherwise keep WAL frames
alive and interfere with checkpoints. The retry action is the sole write: it
uses a short independent `BEGIN IMMEDIATE` transaction and never changes the
producer or consumer hot paths.

`admin` is the storage-facing boundary. It exports typed summaries, execution
details, failures, and database information rather than raw SQL or table rows.
Execution and failure lists use `LIMIT`/`OFFSET` and always return at most 500
items; the console requests 25 at a time. The console therefore never loads
all deliveries into memory.

Acknowledgement throughput is derived from successive sampler snapshots and
kept in a 60-second in-memory ring buffer. It disappears when the console
closes by design: v0 adds no metrics table, write amplification, retention
policy, or durable telemetry surface.

## Scope

The overview exposes queue-owned durable state: READY, PROCESSING,
ACKNOWLEDGED, FAILED, stored delivery retry ceilings, active leases, failure
errors, payloads, and EventBus execution metadata. Worker concurrency, lease
duration, and runtime retry policy are process configuration, not LocalQueue
database state, so the console labels them unavailable instead of guessing.

Supported interactions are changing refresh rate, opening another directory,
inspecting/copying a failed payload, and retrying one failed delivery.
