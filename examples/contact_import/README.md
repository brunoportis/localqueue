# Contact import

Self-contained CSV import example using `localqueue`. The importer sends each
row to a local FastAPI API. The API stores contacts in LMDB and returns
`409 Conflict` when the CNPJ already exists. The importer treats this response
as a terminal `duplicate_cnpj` rejection.

The CSV may contain repeated CNPJs. Different rows are sent to the API; exact
duplicate rows are collapsed by the queue.

Replaying the same `Idempotency-Key` and contact is an idempotent success. A
different operation for an existing CNPJ receives `409 Conflict`; unrelated
rows continue, while `result.raise_for_failures()` reports the terminal
duplicate-CNPJ rejection.

This is a local demo API. It does not include the authentication,
authorization, or rate limiting required for a public service.

## Run the example

Enter this directory and install dependencies:

```bash
cd examples/contact_import
uv sync --group dev
```

Generate a test CSV:

```bash
uv run python generate_contacts_csv.py --count 10
```

In another terminal, start the API:

```bash
cd examples/contact_import
uv run uvicorn api:app --reload
```

Run the import:

```bash
cd examples/contact_import
uv run python import_contacts.py
```

API data is stored in `data/contacts.lmdb`. Durable queue state is stored in
`data/queue`. To restart the example from scratch, delete the `data/` directory.

The example uses 20 concurrent requests and batches of 500 rows. These
conservative defaults keep the local demo API and SQLite queue responsive.

For tests or embedding, `run_import(csv_path, queue_path, import_id, http)`
accepts an existing `httpx.AsyncClient`. This allows an in-process FastAPI
transport without starting a network server.

By default, the importer uses `http://127.0.0.1:8000`. To use another API
instance, set `CONTACTS_API_URL`. To change the LMDB location, set
`CONTACTS_DB` before starting the API.

## Tests

```bash
uv run --group dev pytest
```
