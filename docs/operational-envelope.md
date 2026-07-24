# Operational envelope

## Is localqueue a fit for this workload?

| Scenario | Status | Reason |
| --- | --- | --- |
| One process on a local host | Supported | Public queue contract and tests cover it. |
| Several local processes on one host | Supported with conditions | SQLite coordination and receipt/lease contracts are tested; use a local filesystem. |
| Local EventBus on one host | Supported with conditions | Static topology and bounded subscription concurrency are tested. |
| Bounded backlog with faster producers | Supported | `max_pending_jobs` atomically applies producer backpressure. |
| Raspberry Pi / ARM64 hardware | Untested | A wheel is not a physical-hardware smoke test; #31 remains separate. |
| Local filesystem | Supported | This is the supported deployment model. |
| NFS | Unsupported | No localqueue evidence or distributed coordination. |
| SMB/CIFS | Unsupported | No localqueue evidence or distributed coordination. |
| Dropbox, Google Drive, or OneDrive folder | Unsupported | Synchronisation is not database coordination. |
| Multiple hosts sharing one database | Unsupported | No distributed locking or multi-host contract. |
| Exactly-once processing | Unsupported | Delivery is at-least-once. |
| Kill a synchronous Python handler on timeout | Unsupported | Python threads cannot safely be killed. |
| Payload unreadable by the new serializer | Unsupported | Serializer compatibility is the application's responsibility. |

Use this page to decide whether the workload belongs in the supported model. An inference is not a guarantee.

## How to read claims

### Guarantee

A public contract protected by implementation and tests.

### Measured

An observed result tied to a particular environment, version, and report.

### Recommendation

A conservative operating practice, not a property that localqueue creates automatically.

### Unsupported / untested

An environment that must not be treated as supported. Unsupported means do not deploy it as localqueue; untested means evidence is currently insufficient.

## Supported deployment model

**Guarantee:** localqueue is a single-host, local-filesystem queue: one SQLite database can be opened by one or more local processes and logical queues share it. SQLite supplies local coordination; receipt and lease contracts fence deliveries. There is no external server and no distributed host coordination. See [`SimpleQueue`](../python/localqueue/core.py) and [`test_concurrency.py`](../tests/test_concurrency.py).

Do not use NFS, SMB/CIFS, distributed volumes, cloud-sync folders, multiple hosts opening one database, manual copying while writers are active, or containers on different hosts sharing a network volume. General SQLite advice does not promote these environments to localqueue support.

## Platforms, Python, and wheels

`requires-python` is `>=3.10` with no upper bound. Current classifiers and wheels cover CPython 3.10–3.14; CI exercises only the combinations explicitly listed in [CI](../.github/workflows/ci.yml). Future Python versions are not supported merely because they satisfy `>=3.10`. The wheel workflow builds Linux x86_64 and aarch64, macOS x86_64 and arm64, and Windows x86_64 wheels; it smoke-tests all but Linux ARM64 on CPython 3.13. Linux ARM64 is artifact-validated under QEMU, not smoke-tested. A platform without a matching wheel can require a local Rust build. Linux is the deepest evidence; Windows/macOS have CI tests; physical ARM64 evidence is absent.

## Delivery, leases, and deduplication

**Guarantee:** delivery is at-least-once, not exactly-once. A claim increments the delivery attempt. A crash, lease loss, or retry can redeliver it, so handlers must be idempotent. Eligible messages are claimed FIFO, but concurrent completion order is not FIFO. `ack`, `nack`, `fail`, and lease extension require the current receipt; stale receipts raise `LeaseExpired`. See [`test_contract.py`](../tests/test_contract.py) (`test_worker_a_expires_b_reserves_a_ack_rejected`), [`test_core.py`](../tests/test_core.py), and [`src/queue.rs`](../src/queue.rs).

`nack(delay=...)` returns a transient failure to ready/delayed work; exhaustion of `max_retries` and an expired final lease move it to failed (dead-letter). `fail` moves it there directly; `retry_failed(id)` makes it ready again. Reclaim returns an expired lease to ready unless its maximum attempts are exhausted. The later valid transition wins; a losing stale ACK/NACK cannot overwrite it. Process death after a claim therefore leads to reclaim and possible redelivery (see `TestContract.test_crash_between_get_and_ack`).

The native counter increments on claim. `SimpleQueue` configures
`max_attempts = delivery.max_retries + 1`; the public `Job.attempts` is
zero-based, so the first delivery has `attempts == 0`.
`DeliveryPolicy(max_retries=3)` permits up to four total claims. NACK or lease
expiration after the limit moves the record to failed. `retry_failed()` starts
processing again under the current contract, resets attempts, and clears the
failure reason and human diagnostic while preserving the row ID and exact
payload bytes. See [`SimpleQueue.__init__`](../python/localqueue/core.py),
[`claim_next`](../src/queue.rs), and [`test_contract.py`](../tests/test_contract.py).

`job_id` deduplicates within a logical queue. A duplicate returns the original internal id and does not replace its payload; it remains deduplicated while its record exists, including ACKed and failed states, until applicable `purge()`. It does not mean one processing execution. Evidence: [`test_contract.py`](../tests/test_contract.py) and [`test_batch.py`](../tests/test_batch.py).

## Durability policy

**Guarantee:** `DurabilityMode.RELAXED` selects SQLite `synchronous=NORMAL`;
`DurabilityMode.DURABLE` selects `FULL` (see [`policies.py`](../python/localqueue/policies.py),
[`core.py`](../python/localqueue/core.py), and
[`test_diagnostics.py`](../tests/test_diagnostics.py)). `RELAXED` is the
default and prioritizes throughput. `DURABLE` requests SQLite's stronger
synchronization mode and has a measured cost in the reports below.

**Measured:** the deterministic SIGKILL/failpoint crash harness currently uses
NORMAL. Separate chaos lifecycle/reopen/integrity scenarios exercise RELAXED/NORMAL
and DURABLE/FULL, and the benchmark reports include both SQLite modes; see
[`test_crash_recovery.py`](../tests/test_crash_recovery.py),
[`test_chaos.py`](../tests/test_chaos.py), and
[`operational-chaos.md`](internal/operational-chaos.md). Process crash is not
power loss. Neither mode proves safety through physical power loss, kernel
panic, corrupt filesystem, lying controller cache, defective SD card,
undervoltage, or failed storage. #31's physical campaign is not complete.

## Capacity, concurrency, and backpressure

**Guarantee:** `max_pending_jobs` counts ready plus processing, not ACKed or failed. Capacity changes atomically with enqueue across producers; `put_many` is all-or-nothing for the whole batch, and existing duplicates consume no new slot. `put(..., block=True)` waits to its timeout; `block=False` raises `Full`. Retry/reclaim work remains pending. See [`docs/backpressure.md`](backpressure.md) and [`test_backpressure.py`](../tests/test_backpressure.py).

**Recommendation:** set a finite limit when producers can outrun consumers; alert before `available_slots` reaches zero. The limit bounds logical backlog, not disk bytes or CPU.

## EventBus operations

**Guarantee:** EventBus uses a static topology of events and subscriptions, persists one delivery envelope per subscription, and dispatch records `correlation_id` and `causation_id`. Old envelopes remain backward compatible; `event_id` deduplicates per subscription. A subscription has one claim loop and a bounded delivery set (`concurrency=N`, configured as `bus.subscription("name", concurrency=N)`), not unlimited prefetch. See [`docs/event-bus.md`](event-bus.md), [`bus.py`](../python/localqueue/bus/bus.py), and [`test_bus_concurrency.py`](../tests/test_bus_concurrency.py).

Delivery has heartbeat/lease extension, reclaim, retry, permanent-error dead-lettering, and cooperative cleanup. The tested precedence is external cancellation > lease loss > internal async timeout > permanent error > transient error > success. Lease loss prevents ACK; timeout is exclusive to async handlers; cleanup is cooperative and a slot is released only after cleanup and the state transition. Sibling runners are cancelled during shutdown. Sync handlers run in a thread; Python cannot safely kill that thread, so synchronous handlers do not support timeout. Use subprocess isolation if hard termination is a requirement. Evidence: [`test_bus_timeout.py`](../tests/test_bus_timeout.py) and [`test_bus_concurrency.py`](../tests/test_bus_concurrency.py).

## Performance and practical concurrency

**Measured, not guaranteed:** reports for a wheel built from commit `46c51c9218cd2ee8cc84d575f8ff635d2dd2b8da` are [standard JSON.gz](evidence/operational-envelope/46c51c92/benchmark-standard.json.gz), [standard Markdown](evidence/operational-envelope/46c51c92/benchmark-standard.md), [multiprocess JSON.gz](evidence/operational-envelope/46c51c92/benchmark-multiprocess.json.gz), and [multiprocess Markdown](evidence/operational-envelope/46c51c92/benchmark-multiprocess.md). The checkout metadata still declared `1.1.2`, but this commit contains work after the public v1.1.2 release; this is not the published v1.1.2 wheel. Reports record Linux 7.0.13, x86_64, CPython 3.13.13, Python stdlib sqlite3 3.50.4, native localqueue SQLite 3.46.0, tmpfs, payload, producer/consumer count, durability, samples, throughput and percentiles. The native SQLite version is operationally relevant to queue scenarios. #32 will produce candidate reports with coherent final version metadata.

| Scenario | P/C | payload | mode | sample | throughput | p50 / p95 / p99 | limitation |
| --- | --- | --- | --- | ---: | ---: | --- | --- |
| standard roundtrip | 1/1 | 128 B | NORMAL | 25 | 7,820 ops/s | 123 / 150 / 163 µs | tmpfs, single process |
| multiprocess roundtrip | 4/8 | 100 B | NORMAL | 200 messages | 2,929 ACK/s | 8.0 / 10.7 / 18.7 ms | CI profile, tmpfs |

“Practical concurrency range” means combinations actually exercised: 1P/1C and 4P/8C, with 100 B and 100 KiB payloads in NORMAL and FULL. It is not a universal maximum or production sizing recommendation. Measure on target hardware and filesystem; GitHub-hosted runners are not production capacity.

## Database growth, WAL, retention, and diagnostics

ACKed records remain until `purge`; failed records remain until `retry_failed` or `purge(include_failed=True)`. Backlog and serialized payload size grow the database; WAL and SHM sidecars also consume space. Removing records does not promise immediate filesystem-size reduction. `vacuum()` can contend for the SQLite lock and should be planned, not run indiscriminately. Monitor free disk, DB/WAL/SHM sizes, and investigate growth; never edit SQLite tables manually.

`diagnostics()` supplies queue state, pending/capacity, oldest ready and lease ages, journal mode, synchronous mode, SQLite/package versions, DB/WAL/SHM size, and page/freelist counts. `stats()` supplies ready/processing/acked/failed. Applications must expose their own recent errors, host disk, and alert transport; localqueue does not provide Prometheus or OpenTelemetry. Workload-specific thresholds are a recommendation, not universal limits. See [`docs/diagnostics.md`](diagnostics.md).

## Backup, restore, and corruption response

### Backup

Use public `queue.backup(destination)`, where destination is a new directory outside the active database directory. It returns a verified point-in-time backup; independently open it with a new `SimpleQueue` and run full integrity check. The application owns retention and copying to external media. Backup is not disk monitoring. See [`docs/maintenance.md`](maintenance.md).

### Restore

1. Stop all producers and consumers.
2. Preserve the current database plus WAL/SHM for analysis.
3. Validate the backup by opening it and running full integrity check.
4. Replace data only while every process is closed; do not copy just an active `localqueue.db`.
5. Reopen, check integrity and counts, then start consumers in a controlled way.
6. Observe for expected at-least-once duplicates.

### Corruption

Stop writers, retain artifacts, collect diagnostics and application logs, and run quick then full integrity checks. Do not treat VACUUM as generic repair. Restore a known validated backup, record version/filesystem/host conditions, and report suspected corruption with sanitized evidence.

## Upgrade compatibility

Version 1.0.0 starts this storage lineage. Forward opening is tested from v1.0.0, v1.0.1, v1.1.0, v1.1.1, and v1.1.2 to the candidate; there is no downgrade, mixed-version, or 0.5.0-to-1.x in-place guarantee. Back up first, coordinate all processes to one version, and retain responsibility for custom serializer compatibility. This matrix is limited to Linux x86_64 / CPython 3.14: [`storage policy`](storage-compatibility.md), [`baselines`](../compatibility/baselines.toml), [`policy`](../compatibility/policy.toml), and [`runner`](../compatibility/run_matrix.py).

## Chaos and failure evidence

**Measured:** the Linux campaign exercises deterministic transactional crashes, SQLite busy/locked, simulated disk full, simulated malformed/incompatible database handling, lease recovery, backup/restore, multiprocess correctness, and integrity checking. It proves only those controlled scenarios; it excludes physical power loss, kernel panic, filesystem corruption, controller flush failures, defective SD cards, undervoltage, NFS/SMB, multiple hosts, malicious file modification, and arbitrary killing of non-cooperative Python code. See [`operational-chaos.md`](internal/operational-chaos.md), [`stress/chaos`](../stress/chaos), and [chaos workflow](../.github/workflows/chaos.yml).

## Operational checklists

### Before adopting

- Use a single host and local filesystem.
- Make handlers idempotent and accept duplicates.
- Bound backlog, monitor disk, rehearse backup/restore, and measure target hardware.

### Before deploy

- Coordinate one version; back up and check integrity.
- Set capacity, concurrency, async timeout, supervisor, and graceful shutdown.
- Alert on failed work, backlog, leases, and disk growth.

### During an incident

- Stop writers on suspected corruption; preserve DB/WAL/SHM and diagnostics.
- Check integrity and leases; restore only a validated backup.
- Expect redelivery when consumers resume.

## Release process

Release notes must link here and their public wording must be reviewed against this envelope. Release-candidate reports should replace or complement these preliminary measurements. #32 owns the GO/NO-GO decision; this page does not declare the release production-ready.

## Evidence map

| Claim | Classification | Evidence | Limitation |
| --- | --- | --- | --- |
| multiprocess safety | Guarantee | [`test_concurrency.py`](../tests/test_concurrency.py) | local host/filesystem only |
| at-least-once | Guarantee | [`test_contract.py`](../tests/test_contract.py) | duplicates require idempotency |
| receipt fencing | Guarantee | [`test_worker_a_expires_b_reserves_a_ack_rejected`](../tests/test_contract.py) | not distributed fencing |
| lease reclaim | Guarantee | [`test_expired_job_does_not_exceed_max_attempts`](../tests/test_contract.py) | timed local leases |
| deduplication | Guarantee | [`test_batch.py`](../tests/test_batch.py) | not exactly-once |
| bounded backlog | Guarantee | [`test_backpressure.py`](../tests/test_backpressure.py) | logical count, not bytes |
| diagnostics | Guarantee | [`test_diagnostics.py`](../tests/test_diagnostics.py) | no telemetry exporter |
| integrity / backup | Guarantee | [`test_integrity.py`](../tests/test_integrity.py), [`test_backup.py`](../tests/test_backup.py) | does not repair corruption |
| process crash behavior | Measured | [`test_crash_recovery.py`](../tests/test_crash_recovery.py) | not power loss |
| SQLite operational chaos | Measured | [`stress/chaos`](../stress/chaos) | controlled Linux faults |
| multiprocess benchmark | Measured | [report](evidence/operational-envelope/46c51c92/benchmark-multiprocess.md) and [compressed JSON](evidence/operational-envelope/46c51c92/benchmark-multiprocess.json.gz) | tmpfs CI profile |
| EventBus concurrency | Guarantee | [`test_bus_concurrency.py`](../tests/test_bus_concurrency.py) | static local topology |
| async timeout | Guarantee | [`test_bus_timeout.py`](../tests/test_bus_timeout.py) | sync timeout unsupported |
| storage compatibility | Measured | [`compatibility`](../compatibility) | Linux x86_64 CPython 3.14 |
| ARM64 | Untested | [wheel workflow](../.github/workflows/wheels.yml) | no physical smoke |
| physical power loss | Untested | [#31](https://github.com/brunoportis/localqueue/issues/31) | not performed |
| network filesystems | Unsupported | no localqueue evidence | do not deploy |
