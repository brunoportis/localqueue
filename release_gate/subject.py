from __future__ import annotations

import importlib.metadata
from pathlib import Path
from typing import Any


def installed_subject(
    candidate_sha: str,
    candidate_ref: str,
    expected_version: str,
    *,
    require_wheel: bool,
) -> dict[str, Any]:
    import localqueue
    from localqueue import localqueue as native

    package_version = importlib.metadata.version("localqueue")
    native_version = str(native.__version__)
    module_path = str(Path(localqueue.__file__).resolve())
    native_path = str(Path(native.__file__).resolve())
    if package_version != expected_version or native_version != expected_version:
        raise ValueError(
            "installed package/native version differs from candidate: "
            f"{package_version}/{native_version} != {expected_version}"
        )
    normalized = module_path.replace("\\", "/")
    if require_wheel and "site-packages/" not in normalized:
        raise ValueError("candidate evidence must import the installed wheel")
    return {
        "candidate_sha": candidate_sha,
        "package_version": package_version,
        "native_version": native_version,
        "candidate_ref": candidate_ref,
        "installed_module_path": module_path,
        "native_module_path": native_path,
        "package_native_versions_consistent": package_version == native_version,
        "import_source": "site-packages"
        if "site-packages/" in normalized
        else "checkout",
    }
