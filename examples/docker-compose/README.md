# Docker Compose examples

This directory has two small container examples for `localqueue`.

- [`pull/`](pull/README.md): producer and worker in separate containers, sharing one durable queue store through a volume.
- [`push/`](push/README.md): one app container using `PushConsumption` plus `CallbackDispatcher` for local in-process dispatch.

Both examples build from the repo `Dockerfile` and mount the example directory
into the container so the helper scripts stay easy to inspect and edit.
