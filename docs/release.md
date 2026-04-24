---
icon: lucide/package-check
---

# Release

This project uses `release-please` to automate release PRs, changelog updates,
version bumps, Git tags, and GitHub Releases. The PyPI and GHCR publication
still happens in the existing release workflow after the release tag is
created.

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

## How it works

1. Merge Conventional Commit changes into `master`.
2. `release-please` opens or updates a release PR with the changelog and the
   version bump.
3. Merge the release PR.
4. `release-please` tags the merge commit and creates the GitHub Release.
5. The existing `Release` workflow publishes the artifacts to PyPI and GHCR.

## Setup notes

1. The workflow expects a repository secret named `RELEASE_PLEASE_TOKEN`.
2. The release tag format stays `v<version>` so the current publish workflow can
   keep matching `v*` tags.
3. Keep commit messages in Conventional Commit form so `release-please` can
   classify changes correctly.

## Current Release

The current planned release is `0.3.4`.
