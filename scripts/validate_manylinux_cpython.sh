#!/usr/bin/env bash
# Validate the exact interpreters selected by maturin inside its manylinux container.
set -euo pipefail

for expected_version in 3.10 3.11 3.12 3.13 3.14; do
  interpreter="python${expected_version}"
  echo "Validating ${interpreter} inside the manylinux container"
  "${interpreter}" --version
  "${interpreter}" - "${expected_version}" <<'PY'
import platform
import sys
import sysconfig

expected = sys.argv[1]
actual = f"{sys.version_info.major}.{sys.version_info.minor}"
implementation = sys.implementation.name
gil_disabled = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))

if actual != expected:
    raise SystemExit(f"expected CPython {expected}, got {actual}")
if implementation != "cpython":
    raise SystemExit(f"expected CPython {expected}, got {implementation}")
if gil_disabled:
    raise SystemExit(f"free-threaded CPython is not allowed: {sys.executable}")

print(
    "manylinux interpreter "
    f"expected={expected} executable={sys.executable} "
    f"implementation={implementation} version={platform.python_version()} "
    f"gil_disabled={gil_disabled}"
)
PY
done
