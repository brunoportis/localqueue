from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from io import StringIO
from pathlib import Path
from typing import Any
from unittest import mock
import yaml

from persistentqueue import MemoryQueueStore, PersistentQueue
from persistentretry import MemoryAttemptStore, SQLiteAttemptStore
from persistentretry.cli import (
    CONFIG_FILENAME,
    _ShutdownState,
    _build_app,
    _config_path,
    _load_callable,
    _load_config,
    _parse_json,
    _process_message,
    _process_queue_messages,
    _read_value,
    _resolve_retry_store_path,
    _resolve_store_path,
    _write_config,
)


def handle_payload(payload: dict[str, str]) -> None:
    payload["handled"] = "yes"


class _JsonConsole:
    def __init__(self) -> None:
        self.values: list[Any] = []
        self.messages: list[str] = []

    def print_json(self, json_value: str) -> None:
        self.values.append(json.loads(json_value))

    def print(self, message: str) -> None:
        self.messages.append(message)


class CliTests(unittest.TestCase):
    def test_parse_json_accepts_structured_values(self) -> None:
        self.assertEqual(
            _parse_json('{"to":"user@example.com"}'), {"to": "user@example.com"}
        )
        self.assertEqual(_parse_json("[1,2]"), [1, 2])

    def test_load_callable_uses_module_colon_attribute_format(self) -> None:
        module = types.ModuleType("test_cli_handler")
        module.handle = handle_payload  # type: ignore[attr-defined]
        sys.modules[module.__name__] = module

        try:
            self.assertIs(_load_callable("test_cli_handler:handle"), handle_payload)
        finally:
            _ = sys.modules.pop(module.__name__, None)

    def test_load_callable_uses_current_working_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "main.py"
            path.write_text(
                "def teste(payload):\n    payload['handled'] = 'from cwd'\n",
                encoding="utf-8",
            )
            previous = Path.cwd()
            _ = sys.modules.pop("main", None)
            try:
                os.chdir(tmpdir)
                handler = _load_callable("main:teste")
            finally:
                os.chdir(previous)
                _ = sys.modules.pop("main", None)

        payload: dict[str, str] = {}
        handler(payload)
        self.assertEqual(payload, {"handled": "from cwd"})

    def test_read_value_uses_argument_before_stdin(self) -> None:
        self.assertEqual(_read_value("{}"), "{}")

    def test_read_value_falls_back_to_stdin(self) -> None:
        stdin = StringIO('{"to":"user@example.com"}\n')
        with mock.patch("sys.stdin", stdin):
            self.assertEqual(_read_value(None), '{"to":"user@example.com"}\n')

    def test_config_path_uses_xdg_config_home(self) -> None:
        with mock.patch.dict(
            "os.environ", {"XDG_CONFIG_HOME": "/tmp/config"}, clear=False
        ):
            self.assertEqual(
                _config_path(),
                Path("/tmp/config") / "persistentretry" / CONFIG_FILENAME,
            )

    def test_load_and_write_config_use_yaml_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "persistentretry" / "config.yaml"
            _write_config(
                yaml,
                {"store_path": "/tmp/queues", "retry_store_path": "/tmp/retries"},
                path=path,
            )

            self.assertEqual(
                _load_config(yaml, path=path),
                {"store_path": "/tmp/queues", "retry_store_path": "/tmp/retries"},
            )

    def test_config_defaults_are_resolved_for_queue_paths(self) -> None:
        config = {"store_path": "/tmp/queues", "retry_store_path": "/tmp/retries"}

        self.assertEqual(_resolve_store_path(None, config), "/tmp/queues")
        self.assertEqual(_resolve_store_path("/tmp/explicit", config), "/tmp/explicit")
        self.assertEqual(_resolve_retry_store_path(None, config), "/tmp/retries")
        self.assertEqual(
            _resolve_retry_store_path("/tmp/explicit-retries", config),
            "/tmp/explicit-retries",
        )

    def test_process_message_acks_successful_handler(self) -> None:
        queue = PersistentQueue("emails", store=MemoryQueueStore())
        _ = queue.put({"to": "user@example.com"})
        message = queue.get_message()

        def handler(payload: dict[str, Any]) -> None:
            payload["sent"] = True

        result = _process_message(
            queue,
            message,
            handler,
            retry_store=MemoryAttemptStore(),
            retry_store_path=None,
            max_tries=1,
            release_delay=0.0,
            dead_letter_on_exhaustion=True,
        )

        self.assertTrue(result.processed)
        self.assertTrue(queue.empty())

    def test_process_message_releases_failed_handler_when_not_exhausted_to_dead_letter(
        self,
    ) -> None:
        queue = PersistentQueue("emails", store=MemoryQueueStore())
        _ = queue.put({"to": "user@example.com"})
        message = queue.get_message()

        def handler(_: dict[str, Any]) -> None:
            raise ConnectionError("mail server down")

        result = _process_message(
            queue,
            message,
            handler,
            retry_store=MemoryAttemptStore(),
            retry_store_path=None,
            max_tries=1,
            release_delay=0.0,
            dead_letter_on_exhaustion=False,
        )

        self.assertFalse(result.processed)
        assert result.last_error is not None
        self.assertEqual(result.last_error["type"], "ConnectionError")
        self.assertEqual(result.last_error["message"], "mail server down")
        self.assertEqual(queue.qsize(), 1)

        failed = queue.get_message()
        self.assertEqual(failed.last_error, result.last_error)
        self.assertIsNotNone(failed.failed_at)

    def test_process_message_uses_retry_store_path_as_sqlite_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            retry_store_path = str(Path(tmpdir) / "retries.sqlite3")
            queue = PersistentQueue("emails", store=MemoryQueueStore())
            _ = queue.put({"to": "user@example.com"})
            message = queue.get_message()

            def handler(_: dict[str, Any]) -> None:
                raise ConnectionError("mail server down")

            result = _process_message(
                queue,
                message,
                handler,
                retry_store_path=retry_store_path,
                max_tries=1,
                release_delay=0.0,
                dead_letter_on_exhaustion=True,
            )

            self.assertFalse(result.processed)
            self.assertTrue(Path(retry_store_path).is_file())
            store = SQLiteAttemptStore(retry_store_path)
            self.assertIsNotNone(store.load(message.id))
            store.close()
            with sqlite3.connect(retry_store_path) as connection:
                names = connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            self.assertIn(("retry_records",), names)

    def test_process_queue_messages_preserves_batch_empty_exit(self) -> None:
        queue = PersistentQueue("emails", store=MemoryQueueStore())
        console = _JsonConsole()
        err_console = _JsonConsole()

        result = _process_queue_messages(
            queue,
            lambda payload: payload,
            console=console,
            err_console=err_console,
            shutdown=_ShutdownState(),
            retry_store_path=None,
            max_jobs=1,
            forever=False,
            max_tries=1,
            worker_id="worker-a",
            block=False,
            timeout=None,
            idle_sleep=0.001,
            release_delay=0.0,
            dead_letter_on_exhaustion=True,
        )

        self.assertEqual(result, 1)
        self.assertEqual(console.values, [])
        self.assertEqual(err_console.messages, ["[yellow]queue is empty[/yellow]"])

    def test_process_queue_messages_forever_stops_after_current_message(self) -> None:
        queue = PersistentQueue("emails", store=MemoryQueueStore())
        _ = queue.put({"to": "first@example.com"})
        _ = queue.put({"to": "second@example.com"})
        console = _JsonConsole()
        err_console = _JsonConsole()
        shutdown = _ShutdownState()

        def handler(payload: dict[str, str]) -> None:
            payload["handled"] = "yes"
            shutdown.requested = True

        result = _process_queue_messages(
            queue,
            handler,
            console=console,
            err_console=err_console,
            shutdown=shutdown,
            retry_store_path=None,
            max_jobs=1,
            forever=True,
            max_tries=1,
            worker_id=None,
            block=False,
            timeout=None,
            idle_sleep=0.001,
            release_delay=0.0,
            dead_letter_on_exhaustion=True,
        )

        self.assertEqual(result, 0)
        self.assertEqual(
            console.values,
            [
                {"id": console.values[0]["id"], "state": "acked"},
                {"processed": 1, "state": "stopped"},
            ],
        )
        self.assertEqual(queue.qsize(), 1)
        self.assertEqual(err_console.messages, [])

    def test_process_queue_messages_records_worker_id_on_failure(self) -> None:
        queue = PersistentQueue("emails", store=MemoryQueueStore())
        _ = queue.put({"to": "user@example.com"})
        console = _JsonConsole()
        err_console = _JsonConsole()

        def handler(_: dict[str, str]) -> None:
            raise ConnectionError("mail server down")

        result = _process_queue_messages(
            queue,
            handler,
            console=console,
            err_console=err_console,
            shutdown=_ShutdownState(),
            retry_store_path=None,
            max_jobs=1,
            forever=False,
            max_tries=2,
            worker_id="worker-a",
            block=False,
            timeout=None,
            idle_sleep=0.001,
            release_delay=0.0,
            dead_letter_on_exhaustion=False,
        )

        self.assertEqual(result, 1)
        self.assertEqual(console.values[0]["state"], "failed")
        redelivered = queue.get_message()
        self.assertIsNone(redelivered.leased_by)

    def test_process_queue_messages_batch_exits_on_failed_message(self) -> None:
        queue = PersistentQueue("emails", store=MemoryQueueStore())
        _ = queue.put({"to": "user@example.com"})
        console = _JsonConsole()
        err_console = _JsonConsole()

        def handler(_: dict[str, str]) -> None:
            raise ConnectionError("mail server down")

        result = _process_queue_messages(
            queue,
            handler,
            console=console,
            err_console=err_console,
            shutdown=_ShutdownState(),
            retry_store_path=None,
            max_jobs=1,
            forever=False,
            max_tries=1,
            worker_id=None,
            block=False,
            timeout=None,
            idle_sleep=0.001,
            release_delay=0.0,
            dead_letter_on_exhaustion=True,
        )

        self.assertEqual(result, 1)
        self.assertEqual(console.values[0]["state"], "failed")
        self.assertEqual(console.values[0]["last_error"]["type"], "ConnectionError")

    def test_process_queue_messages_forever_continues_after_failed_message(
        self,
    ) -> None:
        queue = PersistentQueue("emails", store=MemoryQueueStore())
        _ = queue.put({"to": "user@example.com"})
        console = _JsonConsole()
        err_console = _JsonConsole()
        shutdown = _ShutdownState()

        def handler(_: dict[str, str]) -> None:
            shutdown.requested = True
            raise ConnectionError("mail server down")

        result = _process_queue_messages(
            queue,
            handler,
            console=console,
            err_console=err_console,
            shutdown=shutdown,
            retry_store_path=None,
            max_jobs=1,
            forever=True,
            max_tries=1,
            worker_id=None,
            block=False,
            timeout=None,
            idle_sleep=0.001,
            release_delay=0.0,
            dead_letter_on_exhaustion=True,
        )

        self.assertEqual(result, 0)
        self.assertEqual(
            [value["state"] for value in console.values],
            ["failed", "stopped"],
        )
        self.assertEqual(console.values[0]["last_error"]["type"], "ConnectionError")
        self.assertEqual(console.values[1]["processed"], 0)
        self.assertEqual(err_console.messages, [])

    def test_requeue_dead_command_returns_dead_message_to_ready_queue(self) -> None:
        from rich.console import Console
        import typer
        from typer.testing import CliRunner

        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            queue = PersistentQueue("emails", store_path=store_path)
            _ = queue.put({"to": "user@example.com"})
            message = queue.get_message()
            self.assertTrue(queue.dead_letter(message, error=RuntimeError("bad")))

            app = _build_app(typer, yaml, Console(), Console(stderr=True))
            result = CliRunner().invoke(
                app,
                [
                    "queue",
                    "requeue-dead",
                    "emails",
                    message.id,
                    "--store-path",
                    store_path,
                ],
            )

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertEqual(
                json.loads(result.stdout), {"id": message.id, "state": "requeued"}
            )
            self.assertEqual(queue.dead_letters(), [])
            self.assertEqual(queue.qsize(), 1)

    def test_inspect_command_prints_message_without_leasing_it(self) -> None:
        from rich.console import Console
        import typer
        from typer.testing import CliRunner

        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            queue = PersistentQueue("emails", store_path=store_path)
            message = queue.put({"to": "user@example.com"})

            app = _build_app(typer, yaml, Console(), Console(stderr=True))
            result = CliRunner().invoke(
                app,
                [
                    "queue",
                    "inspect",
                    "emails",
                    message.id,
                    "--store-path",
                    store_path,
                ],
            )

            self.assertEqual(result.exit_code, 0, result.output)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["id"], message.id)
            self.assertEqual(payload["queue"], "emails")
            self.assertEqual(payload["state"], "ready")
            self.assertEqual(payload["value"], {"to": "user@example.com"})
            self.assertEqual(queue.qsize(), 1)

    def test_pop_command_records_worker_id(self) -> None:
        from rich.console import Console
        import typer
        from typer.testing import CliRunner

        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            queue = PersistentQueue("emails", store_path=store_path)
            message = queue.put({"to": "user@example.com"})

            app = _build_app(typer, yaml, Console(), Console(stderr=True))
            result = CliRunner().invoke(
                app,
                [
                    "queue",
                    "pop",
                    "emails",
                    "--worker-id",
                    "worker-a",
                    "--store-path",
                    store_path,
                ],
            )

            self.assertEqual(result.exit_code, 0, result.output)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["id"], message.id)
            self.assertEqual(payload["state"], "inflight")
            self.assertEqual(payload["leased_by"], "worker-a")

    def test_inspect_command_exits_when_message_is_missing(self) -> None:
        from rich.console import Console
        import typer
        from typer.testing import CliRunner

        with tempfile.TemporaryDirectory() as tmpdir:
            app = _build_app(typer, yaml, Console(), Console(stderr=True))
            result = CliRunner().invoke(
                app,
                [
                    "queue",
                    "inspect",
                    "emails",
                    "missing",
                    "--store-path",
                    str(Path(tmpdir) / "queues"),
                ],
            )

            self.assertEqual(result.exit_code, 1)
            self.assertIn("message not found", result.output)


if __name__ == "__main__":
    _ = unittest.main()
