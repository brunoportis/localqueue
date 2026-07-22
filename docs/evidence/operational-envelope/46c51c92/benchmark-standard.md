# localqueue benchmark

- Package: `1.1.2`
- Commit: `46c51c9218cd2ee8cc84d575f8ff635d2dd2b8da`
- Profile: `standard`
- Canonical: `True`
- Overrides: `none`

## Environment

| Field | Value |
|---|---|
| architecture | x86_64 |
| cpu_model | n/a |
| filesystem_type | tmpfs |
| logical_cpu_count | 12 |
| os | Linux |
| os_release | 7.0.13-arch1-2 |
| python_executable | /tmp/localqueue-issue29-XL5IA6/venv/bin/python |
| python_implementation | CPython |
| python_version | 3.13.13 |
| sqlite_version | 3.50.4 |
| timer_implementation | clock_gettime(CLOCK_MONOTONIC) |
| timer_resolution_ns | 1 |
| total_memory_bytes | 29306621952 |
| workdir_filesystem | /tmp |

## Scenarios

| Scenario | Durability | P/C | Payload requested/actual | Messages | Produced/s | ACK/s | Claim p50/p95/p99 ns | Roundtrip p50/p95/p99 ns | Status |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| put | n/a | -/- | 128/128 | 25 | 39339.28877713034 | n/a | 24215/30136/34755 | n/a | passed |
| put_many-1 | n/a | -/- | 128/128 | 25 | 38690.88612962685 | n/a | 24717/29024/36959 | n/a | passed |
| put_many-10 | n/a | -/- | 128/128 | 250 | 183887.21388068958 | n/a | 52308/67857/86211 | n/a | passed |
| put_many-100 | n/a | -/- | 128/128 | 2500 | 309213.2945884953 | n/a | 312684/388837/446043 | n/a | passed |
| put_many-1000 | n/a | -/- | 128/128 | 25000 | 354530.1019739186 | n/a | 2694554/2924413/5392985 | n/a | passed |
| get_ack | n/a | -/- | 128/128 | 25 | 11079.178456759075 | n/a | 88636/99637/100247 | n/a | passed |
| roundtrip | n/a | -/- | 128/128 | 25 | 7820.141744761209 | n/a | 123180/150250/162804 | n/a | passed |
| fanout-1 | n/a | -/- | 128/355 | 25 | n/a | n/a | 62306/71875/84969 | n/a | passed |
| fanout-10 | n/a | -/- | 128/355 | 25 | n/a | n/a | 142436/241552/294610 | n/a | passed |
| fanout-100 | n/a | -/- | 128/355 | 25 | n/a | n/a | 937211/1383054/1831542 | n/a | passed |

### put

Serializer: `localqueue.JsonSerializer`; padding: `n/a`.

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

Correctness: `True`; ID validation: `n/a`; stats: `{'ready': 28, 'processing': 0, 'acked': 0, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}`.

### put_many-1

Serializer: `localqueue.JsonSerializer`; padding: `n/a`.

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

Correctness: `True`; ID validation: `n/a`; stats: `{'ready': 28, 'processing': 0, 'acked': 0, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}`.

### put_many-10

Serializer: `localqueue.JsonSerializer`; padding: `n/a`.

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

Correctness: `True`; ID validation: `n/a`; stats: `{'ready': 280, 'processing': 0, 'acked': 0, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}`.

### put_many-100

Serializer: `localqueue.JsonSerializer`; padding: `n/a`.

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

Correctness: `True`; ID validation: `n/a`; stats: `{'ready': 2800, 'processing': 0, 'acked': 0, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.001}`.

### put_many-1000

Serializer: `localqueue.JsonSerializer`; padding: `n/a`.

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

Correctness: `True`; ID validation: `n/a`; stats: `{'ready': 28000, 'processing': 0, 'acked': 0, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.014}`.

### get_ack

Serializer: `localqueue.JsonSerializer`; padding: `n/a`.

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

Correctness: `True`; ID validation: `n/a`; stats: `{'ready': 0, 'processing': 0, 'acked': 28, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}`.

### roundtrip

Serializer: `localqueue.JsonSerializer`; padding: `n/a`.

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

Correctness: `True`; ID validation: `n/a`; stats: `{'ready': 0, 'processing': 0, 'acked': 28, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}`.

### fanout-1

Serializer: `localqueue.bus.JsonSerializer`; padding: `n/a`.

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

Correctness: `True`; ID validation: `n/a`; stats: `n/a`; integrity: `[{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}]`.

### fanout-10

Serializer: `localqueue.bus.JsonSerializer`; padding: `n/a`.

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

Correctness: `True`; ID validation: `n/a`; stats: `n/a`; integrity: `[{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}]`.

### fanout-100

Serializer: `localqueue.bus.JsonSerializer`; padding: `n/a`.

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

Correctness: `True`; ID validation: `n/a`; stats: `n/a`; integrity: `[{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.002}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}, {'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.003}]`.

## Limitations

Monotonic timestamps are comparable only between processes on the same host. Percentiles describe deterministic samples, while throughput uses total completed messages. Scheduler, CPU frequency, cache, filesystem, temperature, and virtualization affect results; no performance threshold is applied.
