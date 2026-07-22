"""The default wheel must expose only the production native contract."""

from importlib import metadata

import localqueue
from localqueue import localqueue as native


def assert_wheel_contract(native_module=native, package=localqueue) -> None:
    assert "site-packages" in package.__file__ or package.__file__.endswith(
        "python/localqueue/__init__.py"
    )
    assert native_module.__version__ == metadata.version("localqueue")
    assert not hasattr(native_module.NativeQueue, "_test_configure_failpoint")
    assert not hasattr(native_module.NativeQueue, "_test_set_max_page_count")
    assert not hasattr(native_module.NativeQueue, "_test_busy_timeout")
    assert not hasattr(native_module.NativeQueue, "_test_set_backup_max_page_count")
    assert not any(name.startswith("_test_") for name in dir(native_module.NativeQueue))
    assert not any("failpoint" in name.lower() for name in dir(native_module))
    assert not any("failpoint" in name.lower() for name in dir(package))


def test_normal_extension_has_no_failpoint_hooks() -> None:
    assert_wheel_contract()


if __name__ == "__main__":
    assert_wheel_contract()
