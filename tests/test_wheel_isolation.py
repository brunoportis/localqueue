"""The default extension must not expose crash-harness hooks."""

import localqueue
from localqueue import localqueue as native


def test_normal_extension_has_no_failpoint_hooks() -> None:
    assert not hasattr(native.NativeQueue, "_test_configure_failpoint")
    assert not any(name.startswith("_test_") for name in dir(native.NativeQueue))
    assert not any("failpoint" in name.lower() for name in dir(native))
    assert not any("failpoint" in name.lower() for name in dir(localqueue))
