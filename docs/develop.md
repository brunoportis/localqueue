---
icon: lucide/hammer
---

# Development Guide

This page describes the day-to-day development flow for `localqueue`: how to
write commits, how versions are released, what quality gates should pass before
merging, and what happens to docs and deployments.

## Commit style

Use Conventional Commits.

Preferred shapes:

```text
feat: add queue health summary
fix: keep forever workers alive on empty queues
docs: describe release flow
chore: refresh release-please config
test: cover version output in the CLI
```

Keep commit messages short and specific. Use a scope only when it helps the
history, for example `fix(cli): ...` or `docs(release): ...`.

Why this matters here:

- `release-please` uses commit history to decide the next version.
- clean commit messages produce cleaner changelogs.
- release PRs become easier to review when each change has a clear intent.

## Versioning

Versions follow SemVer-style `MAJOR.MINOR.PATCH`.

Before `1.0.0`, use this rule:

| Change | Version bump |
| --- | --- |
| Bug fix only | patch |
| New CLI/API feature | minor |
| Breaking user-facing behavior | minor, with a clear changelog note |

In this repo:

- `release-please` opens or updates the release PR.
- merging that release PR creates the tag and the GitHub Release.
- the release tag stays in the form `v<version>`.
- `pyproject.toml` is updated by the release automation, not by hand.

## Quality gates before merge

Run the project checks before opening or merging a release-worthy change.

```bash
env UV_CACHE_DIR=/tmp/uv-cache uv run ruff format .
env UV_CACHE_DIR=/tmp/uv-cache uv run ruff format --check .
env UV_CACHE_DIR=/tmp/uv-cache uv run pytest -q
env UV_CACHE_DIR=/tmp/uv-cache uv run ruff check .
env UV_CACHE_DIR=/tmp/uv-cache uv run basedpyright
env UV_CACHE_DIR=/tmp/uv-cache uv run zensical build --clean
env UV_CACHE_DIR=/tmp/uv-cache uv lock --check
env UV_CACHE_DIR=/tmp/uv-cache uv build
```

Practical notes:

- `pytest` is the main behavior gate.
- `ruff format` applies the formatting fix locally.
- `ruff format --check` verifies formatting stays stable before a merge.
- `ruff check` and `basedpyright` keep the codebase clean and typed where it
  matters.
- `zensical build --clean` catches docs breakage before it lands.
- `uv lock --check` confirms the lockfile is current.
- `uv build` verifies the package artifacts still build cleanly.

## Documentation workflow

Docs should stay close to the code and the release flow.

- Update `docs/` when the CLI, public API, or release process changes.
- Keep `docs/release.md` focused on release automation and tagging.
- Keep `docs/operational-maturity.md` focused on runtime guarantees and
  production boundaries.
- Update `docs/index.md` and the docs navigation when a new top-level doc is
  added.

Good documentation changes usually include:

- a short user-facing description
- exact commands
- concrete examples
- the current expected release or deployment behavior

## Deployments

The docs site is built from `docs/` with `zensical`.

The release workflow currently handles:

- PyPI publication
- GHCR image publication

The docs workflow currently handles:

- building the docs site on pushes and pull requests
- deploying the site from GitHub Actions on push to the configured branches

If a change affects packaging, release tags, or public docs, validate both the
package build and the docs build before merging.

## Local release checklist

When preparing a release or release PR, verify:

1. commits follow Conventional Commits
2. tests pass
3. lint and typecheck pass
4. docs build
5. package build succeeds
6. the release notes and changelog reflect the change

## Secrets and CI

The current release automation expects:

- `RELEASE_PLEASE_TOKEN` for `release-please`
- `SONAR_TOKEN` for SonarCloud, if the Sonar workflow is enabled

Keep secrets in repository settings, not in the repo.

## Daily rule

If a change would confuse the next release PR, fix the commit message, docs, or
tests before merging it.
