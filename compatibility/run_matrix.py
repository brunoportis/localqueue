"""Run the deliberately small, wheel-to-wheel storage compatibility matrix."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import sqlite3
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "compatibility" / "baselines.toml"
POLICY = ROOT / "compatibility" / "policy.toml"
REPORT_VERSION = 1


class MatrixError(RuntimeError):
    pass


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sanitize(error: BaseException | str) -> str:
    text = str(error).replace(str(ROOT), "<checkout>")
    return text[:1000]


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(data, sort_keys=True, indent=2), encoding="utf-8")
    temporary.replace(path)


def load_manifest(path: Path = MANIFEST) -> dict[str, Any]:
    import tomllib

    data = tomllib.loads(path.read_text(encoding="utf-8"))
    matrix = data.get("matrix")
    baselines = data.get("baseline")
    if not isinstance(matrix, dict) or not isinstance(baselines, list) or not baselines:
        raise MatrixError("manifest must define matrix and non-empty baseline entries")
    seen: set[str] = set()
    for entry in baselines:
        required = {
            "package",
            "version",
            "release_tag",
            "wheel",
            "sha256",
            "simple_queue",
            "event_bus",
            "batch_enqueue",
        }
        if not isinstance(entry, dict) or required - entry.keys():
            raise MatrixError("baseline has required fields missing")
        version = entry["version"]
        if version in seen:
            raise MatrixError(f"duplicate baseline version: {version}")
        seen.add(version)
        if entry["release_tag"] != f"v{version}":
            raise MatrixError(f"release tag does not match version: {version}")
        if (
            matrix["python_tag"] not in entry["wheel"]
            or matrix["platform_tag"] not in entry["wheel"]
        ):
            raise MatrixError(f"wheel tags do not match matrix: {entry['wheel']}")
        if len(entry["sha256"]) != 64 or any(
            c not in "0123456789abcdef" for c in entry["sha256"]
        ):
            raise MatrixError(f"invalid SHA-256: {entry['wheel']}")
    return data


def wheel_url(entry: dict[str, Any]) -> str:
    return f"https://files.pythonhosted.org/packages/{entry['wheel']}"  # replaced from PyPI JSON below


def download_wheel(
    entry: dict[str, Any], cache: Path, offline: bool
) -> tuple[Path, str]:
    destination = cache / entry["wheel"]
    if destination.exists():
        status = "cache"
    elif offline:
        raise MatrixError(f"offline cache miss: {entry['wheel']}")
    else:
        metadata_url = (
            f"https://pypi.org/pypi/{entry['package']}/{entry['version']}/json"
        )
        with urlopen(metadata_url, timeout=30) as response:
            metadata = json.load(response)
        candidates = [
            item for item in metadata["urls"] if item["filename"] == entry["wheel"]
        ]
        if len(candidates) != 1 or candidates[0].get("yanked"):
            raise MatrixError(f"expected wheel is absent or yanked: {entry['wheel']}")
        source = candidates[0]["url"]
        cache.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(".download")
        with urlopen(source, timeout=60) as response:
            temporary.write_bytes(response.read())
        temporary.replace(destination)
        status = "download"
    if destination.name != entry["wheel"] or sha256(destination) != entry["sha256"]:
        raise MatrixError(f"wheel filename or SHA-256 mismatch: {entry['wheel']}")
    return destination, status


def isolated_python(base: Path, python: str) -> Path:
    subprocess.run(
        [python, "-m", "venv", str(base)], check=True, capture_output=True, text=True
    )
    return base / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def install(python: Path, wheel: Path, event_bus: bool) -> None:
    command = [str(python), "-m", "pip", "install", "--no-deps", str(wheel)]
    subprocess.run(command, check=True, capture_output=True, text=True)
    if event_bus:
        subprocess.run(
            [str(python), "-m", "pip", "install", "pydantic==2.12.5"],
            check=True,
            capture_output=True,
            text=True,
        )


def copied_script(work: Path, name: str) -> Path:
    work.mkdir(parents=True, exist_ok=True)
    target = work / name
    shutil.copy2(ROOT / "compatibility" / name, target)
    return target


def run_child(
    python: Path, script: Path, arguments: list[str], checkout: Path
) -> dict[str, Any]:
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PYTHONPATH", "PYTHONHOME"}
    }
    completed = subprocess.run(
        [str(python), "-I", str(script), *arguments],
        cwd=script.parent,
        env=env,
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        raise MatrixError(
            f"subprocess failed ({completed.returncode}): {sanitize(completed.stderr)}"
        )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise MatrixError(
            f"subprocess returned invalid JSON: {sanitize(error)}"
        ) from error
    return payload


def prove_isolation(python: Path, checkout: Path) -> dict[str, str]:
    script = "import importlib.metadata, json, pathlib, localqueue; from localqueue import localqueue as n; print(json.dumps({'version': importlib.metadata.version('localqueue'), 'localqueue': str(pathlib.Path(localqueue.__file__).resolve()), 'native': str(pathlib.Path(n.__file__).resolve())}))"
    completed = subprocess.run(
        [str(python), "-I", "-c", script],
        cwd=tempfile.gettempdir(),
        capture_output=True,
        text=True,
        check=True,
    )
    proof = json.loads(completed.stdout)
    root = str(checkout.resolve())
    if (
        root in proof["localqueue"]
        or root in proof["native"]
        or "site-packages" not in proof["localqueue"]
        or "site-packages" not in proof["native"]
    ):
        raise MatrixError("historical import contamination detected")
    return proof


def schema_fingerprint(python: Path, work: Path) -> str:
    database = work / "schema" / "localqueue.db"
    database.parent.mkdir(parents=True, exist_ok=True)
    script = "from localqueue import SimpleQueue; import sys; q=SimpleQueue(sys.argv[1]); q.close()"
    subprocess.run(
        [str(python), "-I", "-c", script, str(database.parent)],
        cwd=work,
        check=True,
        capture_output=True,
        text=True,
    )
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type, name, tbl_name"
        ).fetchall()
    normalized = "\n".join(
        "|".join("" if value is None else " ".join(value.split()) for value in row)
        for row in rows
    )
    return hashlib.sha256(normalized.encode()).hexdigest()


def build_current(current: Path, work: Path, python: str) -> Path:
    if current.suffix == ".whl":
        return current.resolve()
    output = work / "current-wheel"
    output.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "maturin",
            "build",
            "--release",
            "--locked",
            "-i",
            python,
            "--out",
            str(output),
        ],
        cwd=current,
        check=True,
    )
    wheels = list(output.glob("*.whl"))
    if not wheels:
        raise MatrixError("maturin did not produce a wheel")
    result = work / wheels[-1].name
    shutil.copy2(wheels[-1], result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--keep-work-dir", action="store_true")
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    report: dict[str, Any] = {
        "report_schema_version": REPORT_VERSION,
        "started_at": time.time(),
        "status": "failed",
        "baselines": [],
    }
    work: Path | None = None
    try:
        manifest = load_manifest()
        report["manifest_sha256"] = sha256(MANIFEST)
        report["environment"] = {
            "os": platform.system(),
            "architecture": platform.machine(),
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
        }
        interpreter = shutil.which(f"python{manifest['matrix']['python']}")
        if not interpreter:
            raise MatrixError(
                f"required Python is unavailable: {manifest['matrix']['python']}"
            )
        work = args.work_dir or Path(tempfile.mkdtemp(prefix="localqueue-compat-"))
        work.mkdir(parents=True, exist_ok=True)
        cache = args.cache_dir or (
            Path.home()
            / ".cache"
            / "localqueue-compat"
            / report["manifest_sha256"]
            / manifest["matrix"]["python_tag"]
            / manifest["matrix"]["platform_tag"]
        )
        current_wheel = build_current(args.current.resolve(), work, interpreter)
        current_venv = isolated_python(work / "current-venv", interpreter)
        install(current_venv, current_wheel, True)
        current_proof = prove_isolation(current_venv, args.current.resolve())
        report["current_wheel"] = {
            "source_kind": "checkout wheel"
            if args.current.is_dir()
            else "explicit wheel",
            "commit_sha": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            "filename": current_wheel.name,
            "sha256": sha256(current_wheel),
            "package_version": current_proof["version"],
            "import_paths": current_proof,
        }
        before = schema_fingerprint(current_venv, work)
        policy = load_toml(POLICY)
        if policy["schema_fingerprint"] != before:
            raise MatrixError("schema fingerprint differs from compatibility policy")
        report["schema_fingerprint_before"] = before
        for entry in manifest["baseline"]:
            result: dict[str, Any] = {
                "version": entry["version"],
                "wheel": entry["wheel"],
                "sha256": entry["sha256"],
                "capabilities": {
                    key: entry[key]
                    for key in ("simple_queue", "event_bus", "batch_enqueue")
                },
                "started_at": time.time(),
            }
            report["baselines"].append(result)
            try:
                wheel, result["download_status"] = download_wheel(
                    entry, cache, args.offline
                )
                historical = isolated_python(
                    work / f"historical-{entry['version']}", interpreter
                )
                install(historical, wheel, entry["event_bus"])
                result["isolation_proof"] = prove_isolation(
                    historical, args.current.resolve()
                )
                fixture = work / "fixtures" / entry["version"]
                create = copied_script(
                    work / f"scripts-{entry['version']}", "create_fixture.py"
                )
                result["fixture_creation"] = run_child(
                    historical,
                    create,
                    [
                        "--output",
                        str(fixture),
                        "--wheel",
                        entry["wheel"],
                        "--wheel-sha256",
                        entry["sha256"],
                        *(["--event-bus"] if entry["event_bus"] else []),
                    ],
                    args.current.resolve(),
                )
                validate = copied_script(
                    work / f"validate-{entry['version']}", "validate_fixture.py"
                )
                result["validation"] = run_child(
                    current_venv,
                    validate,
                    ["--fixture", str(fixture)],
                    args.current.resolve(),
                )
                result["status"] = "passed"
            except BaseException as error:
                result["status"] = "failed"
                result["error"] = sanitize(error)
                raise
            finally:
                result["duration_seconds"] = round(
                    time.time() - result["started_at"], 3
                )
        after = schema_fingerprint(current_venv, work)
        report["schema_fingerprint_after"] = after
        if after != before:
            raise MatrixError("schema fingerprint changed during matrix")
        report["status"] = "passed"
    except BaseException as error:
        report["error"] = sanitize(error)
    finally:
        report["finished_at"] = time.time()
        write_json_atomic(args.output, report)
        if work and not args.keep_work_dir and args.work_dir is None:
            shutil.rmtree(work, ignore_errors=True)
    return 0 if report["status"] == "passed" else 1


def load_toml(path: Path) -> dict[str, Any]:
    import tomllib

    return tomllib.loads(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    raise SystemExit(main())
