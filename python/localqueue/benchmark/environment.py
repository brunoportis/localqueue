"""Best-effort host and package metadata collection."""

from __future__ import annotations

import os
import platform
import subprocess
import sys
import time
from importlib import metadata
from pathlib import Path
from typing import Any


def _command(command: list[str]) -> str | None:
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=2, check=True
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def package_version() -> str:
    try:
        return metadata.version("localqueue")
    except metadata.PackageNotFoundError:
        return "development"


def subject() -> dict[str, Any]:
    from localqueue import localqueue as native

    package = package_version()
    native_version = getattr(native, "__version__", None)
    commit = _command(["git", "rev-parse", "HEAD"])
    dirty = _command(["git", "status", "--porcelain"])
    origin = (
        "environment"
        if os.environ.get("LOCALQUEUE_COMMIT_SHA")
        else ("git" if commit else "unavailable")
    )
    commit = os.environ.get("LOCALQUEUE_COMMIT_SHA") or commit
    module_path = getattr(native, "__file__", None)
    return {
        "package_version": package,
        "rust_extension_version": native_version,
        "commit_sha": commit,
        "commit_source": origin,
        "dirty_worktree": None if dirty is None and commit is None else bool(dirty),
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
