# Canonical benchmarks

For workload selection and limits on interpreting measurements, see the
[operational envelope](operational-envelope.md).

Install the optional benchmark extra. It adds the EventBus dependency without
adding anything to the base runtime:

```bash
uv sync --extra benchmark
```

The canonical single-process command is:

```bash
uv run python -m localqueue.benchmark --profile standard --durability normal --output benchmark-report-normal.json
uv run python -m localqueue.benchmark --profile standard --durability full --output benchmark-report-full.json
```

`smoke` is a short CI validation profile. It covers every scenario family but
is not performance evidence. `standard` has fixed warmups, sample counts,
payload, batch sizes, fan-out sizes, lease, retry settings, and scenario order;
none of those values adapt to host speed.

## Multiprocess profiles

The spawn-only harness runs real producer and consumer processes:

```bash
uv run python -m localqueue.benchmark --profile multiprocess-ci --output multiprocess.json
uv run python -m localqueue.benchmark render multiprocess.json --output multiprocess.md
```

`multiprocess-ci` covers 1P/1C and 4P/8C, NORMAL and FULL, and serialized
targets near 100 B and 100 KiB. It validates correctness and plumbing, not
stable performance. `multiprocess-release` covers 1P/1C, 4P/1C, 1P/8C, and
4P/8C with 100 B, 1 KiB, and 100 KiB payloads under both durability modes. It
also preloads a canonical 1,000,000-row database through public `put_many()`
batches. `--large-db-rows` is a development override and makes the report
non-canonical; `--keep-workdir` preserves databases for diagnosis.

Claim latency spans the successful `get` call; empty polls are excluded.
Roundtrip begins immediately before producer `put` and ends after consumer
ACK. Both use the monotonic clock and are comparable only on the same host.
Samples are systematic and ordered by global message ID; throughput uses real
totals and scenario-wide elapsed time. CI validates exact IDs, while release
records count, sum, XOR, and SHA-256. Reports record RSS when available,
SQLite settings, and DB/WAL/SHM snapshots. JSON and Markdown are artifacts,
never PyPI distributions.

## Scenarios and units

`put` measures one `queue.put(payload)` per sample. `put_many` measures one
complete `queue.put_many(batch)` call per sample; its latency is per batch and
the report separately includes batches/second and messages/second. `get_ack`
preloads the queue outside elapsed timing and measures `get_nowait()` plus
`ack()`. `roundtrip` measures `put`, `get_nowait`, and `ack` together.

EventBus fan-out measures one committed `dispatch()` per sample. Handlers are
never run in the measured region. Deliveries/second is dispatches/second times
the declared subscription count, and final queue counts validate expected
deliveries. In fan-out `messages` means dispatched events and equals
`dispatches`; `deliveries` is a separate work unit. Therefore
`messages_per_second` is null rather than being overloaded with deliveries.

Warmups use the same functional path and are recorded separately from measured
samples. Throughput uses one external `perf_counter_ns()` elapsed interval;
latency percentiles use the individual raw samples. Every raw duration is an
integer number of nanoseconds.

Payload metadata records both the requested size and the actual serialized
size, plus the serializer identity. Payloads are deterministic; UUID and clock
fields are not used as benchmark inputs.

## Reports and comparison

Reports use `schema_version=1` and `profile_schema_version=1`. They contain
package/native version consistency, commit provenance, environment, effective
SQLite settings from the connection under test, raw samples, summaries,
correctness checks, and full integrity results. Reports are written atomically.

Percentiles use nearest-rank exactly: `rank = ceil(p * n)` and
`sorted_samples[rank - 1]`, with rank clamped to 1 through `n` (so p=0 and
p=1 are explicit boundary cases). Do not compare NORMAL and FULL as equivalent
durability guarantees: NORMAL is the default crash-oriented setting, while
FULL requests the stronger SQLite synchronization mode.

Compare reports only when profile configuration, package/native versions,
durability, host, and filesystem are meaningful matches. GitHub-hosted runners
are noisy and are not regression gates. Cache state, background load, CPU
frequency, thermal conditions, OS, SQLite build, and filesystem all affect
results. This project currently defines no performance thresholds.

`benchmarks/queue_bench.py` is a deprecated wrapper and its output is not
release evidence. A future comparative persist-queue tool belongs to the
separate `benchmark-compare` extra; it is not part of the canonical harness.
