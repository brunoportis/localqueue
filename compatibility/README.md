# Storage compatibility harness

This is a fixed, forward-upgrade matrix. It creates each fixture with the exact
published wheel listed in `baselines.toml`, then opens and exercises it through
a wheel built from the candidate checkout (or an explicit candidate wheel).

Run it locally with:

```bash
uv run python compatibility/run_matrix.py --current . --output compatibility-report.json
```

The matrix is Linux x86_64 / CPython 3.14. Historical wheels are cached outside
fixtures under a key containing the baseline-manifest hash and wheel tags.
`--offline` permits only verified cached wheels. The JSON report is written
atomically even after a partial failure; its `baselines` entries show the exact
wheel, isolation proof, assertions, and sanitized error.

The schema policy fingerprint is computed from normalized `sqlite_master` of a
new database made by the candidate wheel. Changing it requires an explicit
policy update and corresponding compatibility rationale.
