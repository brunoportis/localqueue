from __future__ import annotations

import os
from pathlib import Path

APP_DIR_NAME = "localqueue"
DEFAULT_QUEUE_DB_NAME = "queue.sqlite3"
DEFAULT_RETRY_DB_NAME = "retries.sqlite3"


def data_dir() -> Path:
    data_home = os.environ.get("XDG_DATA_HOME")
    if data_home:
        return Path(data_home) / APP_DIR_NAME
    return Path.home() / ".local" / "share" / APP_DIR_NAME


def default_queue_store_path() -> Path:
    return data_dir() / DEFAULT_QUEUE_DB_NAME


def default_retry_store_path() -> Path:
    return data_dir() / DEFAULT_RETRY_DB_NAME
