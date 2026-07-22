# Operational-envelope benchmark evidence

These reports measure commit `46c51c9218cd2ee8cc84d575f8ff635d2dd2b8da`, which precedes this documentation-only PR. The wheel was built from that commit; its checkout metadata still declared `1.1.2`, but the commit contains work after the public v1.1.2 release, so it is not the published v1.1.2 wheel. #32 will produce candidate reports with coherent final version metadata. The PR does not change runtime, schema, or benchmark code.

Environment: Linux `7.0.13-arch1-2`, x86_64, CPython 3.13.13, Python stdlib `sqlite3` 3.50.4, native localqueue SQLite 3.46.0, 12 logical CPUs, 29,306,621,952 bytes RAM, and a `tmpfs` work directory. The wheel was built from that commit in an isolated worktree and virtual environment; native SQLite is the operationally relevant version for queue scenarios.

Commands:

```bash
LOCALQUEUE_COMMIT_SHA=46c51c9218cd2ee8cc84d575f8ff635d2dd2b8da python -m localqueue.benchmark --profile standard --output benchmark-standard.json
python -m localqueue.benchmark render benchmark-standard.json --output benchmark-standard.md
LOCALQUEUE_COMMIT_SHA=46c51c9218cd2ee8cc84d575f8ff635d2dd2b8da python -m localqueue.benchmark --profile multiprocess-ci --output benchmark-multiprocess.json
python -m localqueue.benchmark render benchmark-multiprocess.json --output benchmark-multiprocess.md
```

SHA-256 of decompressed JSON: standard `17960cae929ad2badd656aefe6979622f1b71c8e0128265584dc1bd6e1a38ae0`; multiprocess `138cb6d2868854c53774a3d5198dbf3ab4b030a0a3f9314756ae0820e44d5735`. SHA-256 of deterministic gzip: standard `897dae4b574c578e0168108534f56b8c549c875d6c35b421bfae19f25e4dc3ea`; multiprocess `70eae019e762fd0f839835388ea8ce24eb4858e6f382fef3e8d9fccdd2937a00`. Markdown: standard `23ee6814dfc50bd51d7faa9ce9d9f1b6e939cc8bd78662f6c17346a90d8b6da`; multiprocess `169335ecd9d0719d07c3bd9df6afceb9db25c907c5ed1312a9f753c9d2367107`.

These cover small workloads; the Markdown is human-reviewable and the raw JSON is compressed. They are not performance gates or a universal throughput promise. `multiprocess-ci` validates exercised combinations and correctness, not the canonical million-row release profile. CPU model was not recorded; tmpfs does not represent disk-backed filesystems.
