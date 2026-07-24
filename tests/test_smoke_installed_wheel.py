"""Regression tests for the installed-wheel smoke script."""

from pathlib import Path

from scripts import smoke_installed_wheel


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
