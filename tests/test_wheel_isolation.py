"""The default extension must not expose crash-harness hooks."""

from importlib import metadata

import localqueue
from localqueue import localqueue as native


def assert_no_failpoint_hooks(native_module=native, package=localqueue) -> None:
    assert not hasattr(native_module.NativeQueue, "_test_configure_failpoint")
    assert not hasattr(native_module.NativeQueue, "_test_set_max_page_count")
    assert not hasattr(native_module.NativeQueue, "_test_busy_timeout")
    assert not hasattr(native_module.NativeQueue, "_test_set_backup_max_page_count")
    assert not any(name.startswith("_test_") for name in dir(native_module.NativeQueue))
    assert not any("failpoint" in name.lower() for name in dir(native_module))
    assert not any("failpoint" in name.lower() for name in dir(package))


def test_normal_extension_has_no_failpoint_hooks() -> None:
    assert_no_failpoint_hooks()
    assert native.__version__ == metadata.version("localqueue")


if __name__ == "__main__":
    assert_no_failpoint_hooks()
