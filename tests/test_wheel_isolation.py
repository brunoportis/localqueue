"""The default extension must not expose crash-harness hooks."""

import localqueue
from localqueue import localqueue as native


def assert_no_failpoint_hooks(native_module=native, package=localqueue) -> None:
    assert not hasattr(native_module.NativeQueue, "_test_configure_failpoint")
    assert not any(name.startswith("_test_") for name in dir(native_module.NativeQueue))
    assert not any("failpoint" in name.lower() for name in dir(native_module))
    assert not any("failpoint" in name.lower() for name in dir(package))


def test_normal_extension_has_no_failpoint_hooks() -> None:
    assert_no_failpoint_hooks()


if __name__ == "__main__":
    assert_no_failpoint_hooks()
