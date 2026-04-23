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
2. Run `uv lock` and commit the updated `uv.lock`.
3. Update `CHANGELOG.md`.
4. Confirm `requires-python`, license metadata, and PyPI classifiers still match
   supported Python versions and the intended release policy.
5. Run the test suite:

   ```bash
   uv run pytest
   ```

6. Run lint and typecheck:

   ```bash
   uv run ruff check .
   uv run basedpyright
   ```

7. Build the docs:

   ```bash
   uv run zensical build --clean
   ```

8. Confirm the lockfile is current:

   ```bash
   uv lock --check
   ```

9. Build the release artifacts:

   ```bash
   uv build
   ```

10. Inspect the artifacts:

   ```bash
   tar -tzf dist/localqueue-<version>.tar.gz | less
   python -m zipfile -l dist/localqueue-<version>-py3-none-any.whl
   ```

11. Install the wheel in a clean virtual environment:

   ```bash
   uv venv /tmp/localqueue-release-venv
   uv pip install --python /tmp/localqueue-release-venv/bin/python \
     "dist/localqueue-<version>-py3-none-any.whl[all]"
   /tmp/localqueue-release-venv/bin/localqueue --help
   ```

12. Run a local smoke test:

    ```bash
    uv run python examples/enqueue_email.py user@example.com \
      --store-path /tmp/localqueue-smoke

    uv run localqueue queue process emails examples.email_worker:send_email \
      --store-path /tmp/localqueue-smoke \
      --retry-store-path /tmp/localqueue-smoke-retries.sqlite3 \
      --worker-id worker-smoke \
      --max-tries 3
    ```

13. Create the release tag:

    ```bash
    git tag v<version>
    ```

14. Publish the artifacts.

    This release workflow also publishes the CLI image to GitHub Container
    Registry as `ghcr.io/brunoportis/localqueue:<tag>` and `:latest`.

15. Confirm the docs deployment finished successfully in the `Documentation`
    workflow. GitHub Pages must be configured to deploy from GitHub Actions in
    the repository settings.

## Current Release

The current planned release is `0.3.2`.
