# `localqueue`

[![Tests](https://github.com/brunoportis/localqueue/actions/workflows/tests.yml/badge.svg)](https://github.com/brunoportis/localqueue/actions/workflows/tests.yml)
[![CodeQL](https://github.com/brunoportis/localqueue/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/brunoportis/localqueue/actions/workflows/github-code-scanning/codeql)
[![PyPI version](https://img.shields.io/pypi/v/localqueue.svg)](https://pypi.org/project/localqueue/)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)

`localqueue` is a small durable queue for one machine. It stores work on the local filesystem by default and keeps retry state with Tenacity.

Use it for scripts, CLI tools, cron jobs, and small Python workers that share one local store. Use `localqueue.retry` when another system already delivers work and you only need retry state that survives restarts.

```bash
localqueue queue exec emails -- python scripts/send_email.py
```

```python
from localqueue import PersistentQueue, persistent_worker

queue = PersistentQueue("emails")
queue.put({"to": "user@example.com"})

@persistent_worker(queue)
def send_email(job: dict[str, str]) -> None:
    deliver(job["to"])
```

Full docs live at [brunoportis.github.io/localqueue](https://brunoportis.github.io/localqueue/). The source docs are in [`docs/`](docs/).

## Install

```bash
pip install localqueue
```

For the CLI:

```bash
pip install "localqueue[cli]"
```

For the optional LMDB backend:

```bash
pip install "localqueue[lmdb]"
```

## Running with Docker

The image is also published on GitHub Container Registry:

```bash
docker pull ghcr.io/brunoportis/localqueue:latest
docker run --rm ghcr.io/brunoportis/localqueue:latest --help
```

## When not to use

`localqueue` is not a distributed broker. If you need multi-host coordination, high write concurrency, managed retention, or strict cross-service ordering, use a system designed for that operating model.
