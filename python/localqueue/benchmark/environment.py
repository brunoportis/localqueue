"""Best-effort host and package metadata collection."""

from __future__ import annotations

import os
import platform
import re
import subprocess
import sys
import time
from importlib import metadata
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

_SHA_RE = re.compile(r"^[0-9a-fA-F]{40}$")
_REPOSITORY_PATH = "brunoportis/localqueue"


def _command(command: list[str], *, cwd: Path | None = None) -> str | None:
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=2,
            check=True,
            cwd=cwd,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def package_version() -> str:
    try:
        return metadata.version("localqueue")
    except metadata.PackageNotFoundError:
        return "development"


def _valid_environment_sha() -> str | None:
    value = os.environ.get("LOCALQUEUE_COMMIT_SHA")
    return value if value and _SHA_RE.fullmatch(value) else None


def _normalize_remote(remote: str) -> str | None:
    """Normalize only the supported canonical GitHub origins."""
    value = remote.strip()
    path: str | None = None
    if value.startswith("git@github.com:"):
        path = value.removeprefix("git@github.com:")
    elif value.startswith(("https://", "ssh://")):
        parsed = urlsplit(value)
        if parsed.query or parsed.fragment or parsed.path.endswith("/"):
            return None
        if parsed.scheme == "https" and parsed.netloc == "github.com":
            path = parsed.path.removeprefix("/")
        elif parsed.scheme == "ssh" and parsed.netloc == "git@github.com":
            path = parsed.path.removeprefix("/")
    if path is None:
        return None
    if path.endswith(".git"):
        path = path[:-4]
    return path if path == _REPOSITORY_PATH else None


def _source_checkout(module_path: str | None) -> Path | None:
    if not module_path:
        return None
    module = Path(module_path).resolve()
    for candidate in (module, *module.parents):
        pyproject_path = candidate / "pyproject.toml"
        cargo_path = candidate / "Cargo.toml"
        if not pyproject_path.is_file() or not cargo_path.is_file():
            continue
        pyproject = pyproject_path.read_text(encoding="utf-8", errors="replace")
        cargo = cargo_path.read_text(encoding="utf-8", errors="replace")
        if 'name = "localqueue"' not in pyproject or 'name = "localqueue"' not in cargo:
            continue
        remote = _command(["git", "-C", str(candidate), "remote", "get-url", "origin"])
        if remote is None or _normalize_remote(remote) != _REPOSITORY_PATH:
            continue
        try:
            module.relative_to(candidate)
        except ValueError:
            continue
        top_level = _command(
            ["git", "-C", str(candidate), "rev-parse", "--show-toplevel"]
        )
        if top_level != str(candidate):
            continue
        return candidate
    return None


def subject() -> dict[str, Any]:
    from localqueue import localqueue as native

    package = package_version()
    native_version = getattr(native, "__version__", None)
    module_path = getattr(native, "__file__", None)
    environment_sha = _valid_environment_sha()
    checkout = None if environment_sha else _source_checkout(module_path)
    commit = environment_sha or (
        _command(["git", "-C", str(checkout), "rev-parse", "HEAD"])
        if checkout
        else None
    )
    dirty = (
        _command(["git", "-C", str(checkout), "status", "--porcelain"])
        if checkout
        else None
    )
    return {
        "package_version": package,
        "rust_extension_version": native_version,
        "commit_sha": commit,
        "commit_source": (
            "environment"
            if environment_sha
            else "git"
            if checkout and commit
            else "unavailable"
        ),
        "dirty_worktree": None if checkout is None else bool(dirty),
        "installed_module_path": module_path,
        "package_native_versions_consistent": native_version == package
        if native_version is not None
        else False,
    }


def environment(workdir: Path) -> dict[str, Any]:
    memory = None
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("MemTotal:"):
                memory = int(line.split()[1]) * 1024
                break
    filesystem = _command(["df", "-T", str(workdir)])
    filesystem_type = None
    if filesystem:
        lines = filesystem.splitlines()
        if len(lines) > 1:
            fields = lines[-1].split()
            filesystem_type = fields[1] if len(fields) > 1 else None
    return {
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "sqlite_version": __import__("sqlite3").sqlite_version,
        "os": platform.system() or None,
        "os_release": platform.release() or None,
        "architecture": platform.machine() or None,
        "cpu_model": platform.processor()
        or _command(["sysctl", "-n", "machdep.cpu.brand_string"])
        or None,
        "logical_cpu_count": os.cpu_count(),
        "total_memory_bytes": memory,
        "filesystem_type": filesystem_type,
        "timer_implementation": time.get_clock_info("perf_counter").implementation,
        "timer_resolution_ns": int(
            time.get_clock_info("perf_counter").resolution * 1_000_000_000
        ),
        "workdir_filesystem": str(workdir),
        "python_executable": sys.executable,
    }
