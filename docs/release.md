---
icon: lucide/package-check
---

# Release

This project currently uses a manual versioning flow. The release version lives
in `pyproject.toml`, and each release should have a matching `CHANGELOG.md`
entry and Git tag.

## Versioning

Use SemVer-style versions:

```text
MAJOR.MINOR.PATCH
```

Before `1.0.0`, use this convention:

| Change | Version bump |
| --- | --- |
| Bug fix only | patch, for example `0.1.1` |
| New CLI/API feature | minor, for example `0.2.0` |
| Breaking user-facing behavior | minor before `1.0.0`, with a clear changelog note |

## Checklist

1. Update `version` in `pyproject.toml`.
2. Update `CHANGELOG.md`.
3. Confirm `requires-python` and PyPI classifiers still match supported Python versions.
4. Run the test suite:

   ```bash
   uv run pytest
   ```

5. Run lint and typecheck:

   ```bash
   uv run ruff check .
   uv run basedpyright
   ```

6. Build the docs:

   ```bash
   uv run zensical build --clean
   ```

7. Confirm the lockfile is current:

   ```bash
   uv lock --check
   ```

8. Build the release artifacts:

   ```bash
   uv build
   ```

9. Inspect the artifacts:

   ```bash
   tar -tzf dist/persistentretry-<version>.tar.gz | less
   python -m zipfile -l dist/persistentretry-<version>-py3-none-any.whl
   ```

10. Install the wheel in a clean virtual environment:

   ```bash
   uv venv /tmp/persistentretry-release-venv
   uv pip install --python /tmp/persistentretry-release-venv/bin/python \
     "dist/persistentretry-<version>-py3-none-any.whl[all]"
   /tmp/persistentretry-release-venv/bin/persistentretry --help
   ```

11. Run a local smoke test:

    ```bash
    uv run python examples/enqueue_email.py user@example.com \
      --store-path /tmp/persistentretry-smoke

    uv run persistentretry queue process emails examples.email_worker:send_email \
      --store-path /tmp/persistentretry-smoke \
      --retry-store-path /tmp/persistentretry-smoke-retries.sqlite3 \
      --worker-id worker-smoke \
      --max-tries 3
    ```

12. Create the release tag:

    ```bash
    git tag v<version>
    ```

13. Publish the artifacts.

14. Confirm the docs deployment finished successfully in the `Documentation`
    workflow. GitHub Pages must be configured to deploy from GitHub Actions in
    the repository settings.

## Current Release

The current planned release is `0.1.0`.
