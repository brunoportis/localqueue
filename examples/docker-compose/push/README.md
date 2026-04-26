# Push example

This example shows local push-style dispatch inside one container.

`localqueue` persists message first, then `CallbackDispatcher` calls handler in
same process. This is not cross-container push. That limit is important: built-in
push adapters are local and in-process.

## Run

```bash
docker compose up --build
```

## What it demonstrates

- queue semantics with `PushConsumption`
- in-process dispatch after `put()`
- explicit acknowledgement from callback handler
