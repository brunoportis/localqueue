"""The default wheel must expose only the production native contract."""

from importlib import metadata
from pathlib import Path

import localqueue
from localqueue import localqueue as native

from scripts import smoke_installed_wheel


def assert_wheel_contract(native_module=native, package=localqueue) -> None:
    package_path = Path(package.__file__).as_posix()
    assert "site-packages" in package_path or package_path.endswith(
        "/python/localqueue/__init__.py"
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


def test_smoke_sqlite_check_closes_its_connection(monkeypatch, tmp_path: Path) -> None:
    class Connection:
        closed = False

        def execute(self, query: str, parameters: tuple[int]) -> "Connection":
            assert query == "SELECT job_id, created_at FROM messages WHERE id = ?"
            assert parameters == (7,)
            return self

        def fetchone(self) -> tuple[str, int]:
            return ("wheel-smoke-retry", 123)

        def close(self) -> None:
            self.closed = True

    connection = Connection()
    monkeypatch.setattr(smoke_installed_wheel.sqlite3, "connect", lambda _: connection)

    assert smoke_installed_wheel._replay_identity(str(tmp_path), 7) == (
        "wheel-smoke-retry",
        123,
    )
    assert connection.closed


if __name__ == "__main__":
    assert_wheel_contract()
