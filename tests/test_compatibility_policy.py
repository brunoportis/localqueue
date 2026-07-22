from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path


def normalized_schema_fingerprint(database: Path) -> str:
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type, name, tbl_name"
        ).fetchall()
    normalized = "\n".join(
        "|".join("" if value is None else " ".join(value.split()) for value in row)
        for row in rows
    )
    return hashlib.sha256(normalized.encode()).hexdigest()


def test_fingerprint_is_stable_for_equivalent_schema(tmp_path: Path) -> None:
    first, second = tmp_path / "first.db", tmp_path / "second.db"
    for database in (first, second):
        with sqlite3.connect(database) as connection:
            connection.execute(
                "CREATE TABLE messages (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)"
            )
    assert normalized_schema_fingerprint(first) == normalized_schema_fingerprint(second)


def test_fingerprint_changes_when_schema_changes(tmp_path: Path) -> None:
    first, second = tmp_path / "first.db", tmp_path / "second.db"
    with sqlite3.connect(first) as connection:
        connection.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY)")
    with sqlite3.connect(second) as connection:
        connection.execute(
            "CREATE TABLE messages (id INTEGER PRIMARY KEY, payload BLOB)"
        )
    assert normalized_schema_fingerprint(first) != normalized_schema_fingerprint(second)
