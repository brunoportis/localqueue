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
from rich.console import Console
import typer
from typer.testing import CliRunner
import yaml

from localqueue import MemoryQueueStore, PersistentQueue
from localqueue.retry import MemoryAttemptStore, SQLiteAttemptStore
from localqueue.cli import (
    CONFIG_FILENAME,
    DEFAULT_RETRY_STORE_PATH,
    _ShutdownState,
    _build_app,
    _command_handler,
    _config_path,
    _load_callable,
    _load_config,
    _parse_json,
    _print_dead_letters,
    _print_queue_stats,
    _process_message,
    _process_queue_messages,
    _read_value,
    _resolve_retry_store_path,
    _resolve_store_path,
    _write_config,
)
from localqueue.retry import configure_default_store


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
    def tearDown(self) -> None:
        configure_default_store(None)

    def _app(self) -> Any:
        return _build_app(typer, yaml, Console(width=120), Console(stderr=True))

    def _invoke(self, args: list[str], *, input: str | None = None) -> Any:
        return CliRunner().invoke(self._app(), args, input=input)

    def _retry_store_path(self) -> str:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return str(Path(directory.name) / "retries.sqlite3")

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
                Path("/tmp/config") / "localqueue" / CONFIG_FILENAME,
            )

    def test_load_and_write_config_use_yaml_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "localqueue" / "config.yaml"
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
        self.assertEqual(_resolve_retry_store_path(None, {}), DEFAULT_RETRY_STORE_PATH)

    def test_config_commands_manage_xdg_config_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(
                "os.environ", {"XDG_CONFIG_HOME": tmpdir}, clear=False
            ):
                path_result = self._invoke(["config", "path"])
                self.assertEqual(path_result.exit_code, 0, path_result.output)
                config_path = Path(path_result.stdout.strip())
                self.assertEqual(
                    config_path,
                    Path(tmpdir) / "localqueue" / CONFIG_FILENAME,
                )

                init_result = self._invoke(
                    [
                        "config",
                        "init",
                        "--store-path",
                        "/tmp/queues",
                        "--retry-store-path",
                        "/tmp/retries.sqlite3",
                    ]
                )
                self.assertEqual(init_result.exit_code, 0, init_result.output)
                self.assertIn("wrote config", init_result.output)

                show_result = self._invoke(["config", "show"])
                self.assertEqual(show_result.exit_code, 0, show_result.output)
                self.assertEqual(
                    json.loads(show_result.stdout),
                    {
                        "retry_store_path": "/tmp/retries.sqlite3",
                        "store_path": "/tmp/queues",
                    },
                )

                set_result = self._invoke(
                    ["config", "set", "store_path", "/tmp/new-queues"]
                )
                self.assertEqual(set_result.exit_code, 0, set_result.output)
                self.assertEqual(
                    json.loads(set_result.stdout)["store_path"], "/tmp/new-queues"
                )

                duplicate_result = self._invoke(["config", "init"])
                self.assertEqual(duplicate_result.exit_code, 1)
                self.assertIn("config already exists", duplicate_result.output)

                invalid_result = self._invoke(["config", "set", "unknown", "value"])
                self.assertEqual(invalid_result.exit_code, 1)
                self.assertIn("unsupported config key", invalid_result.output)

    def test_load_config_rejects_non_mapping_yaml(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.yaml"
            path.write_text("- not\n- a\n- mapping\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "YAML mapping"):
                _ = _load_config(yaml, path=path)

    def test_queue_commands_cover_basic_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")

            add_result = self._invoke(
                [
                    "queue",
                    "add",
                    "emails",
                    "--value",
                    '{"to":"user@example.com"}',
                    "--store-path",
                    store_path,
                ]
            )
            self.assertEqual(add_result.exit_code, 0, add_result.output)
            added = json.loads(add_result.stdout)
            self.assertEqual(added["value"], {"to": "user@example.com"})
            self.assertEqual(added["state"], "ready")

            size_result = self._invoke(
                ["queue", "size", "emails", "--store-path", store_path]
            )
            self.assertEqual(size_result.exit_code, 0, size_result.output)
            self.assertEqual(size_result.stdout.strip(), "1")

            pop_result = self._invoke(
                [
                    "queue",
                    "pop",
                    "emails",
                    "--worker-id",
                    "worker-a",
                    "--store-path",
                    store_path,
                ]
            )
            self.assertEqual(pop_result.exit_code, 0, pop_result.output)
            popped = json.loads(pop_result.stdout)
            self.assertEqual(popped["id"], added["id"])
            self.assertEqual(popped["state"], "inflight")
            self.assertEqual(popped["leased_by"], "worker-a")

            release_result = self._invoke(
                [
                    "queue",
                    "release",
                    "emails",
                    added["id"],
                    "--store-path",
                    store_path,
                ]
            )
            self.assertEqual(release_result.exit_code, 0, release_result.output)
            self.assertEqual(
                json.loads(release_result.stdout),
                {"id": added["id"], "state": "release"},
            )

            stats_result = self._invoke(
                ["queue", "stats", "emails", "--store-path", store_path, "--json"]
            )
            self.assertEqual(stats_result.exit_code, 0, stats_result.output)
            self.assertEqual(json.loads(stats_result.stdout)["ready"], 1)

            second_pop = self._invoke(
                ["queue", "pop", "emails", "--store-path", store_path]
            )
            self.assertEqual(second_pop.exit_code, 0, second_pop.output)

            dead_result = self._invoke(
                [
                    "queue",
                    "dead-letter",
                    "emails",
                    added["id"],
                    "--store-path",
                    store_path,
                ]
            )
            self.assertEqual(dead_result.exit_code, 0, dead_result.output)
            self.assertEqual(
                json.loads(dead_result.stdout),
                {"id": added["id"], "state": "dead-letter"},
            )

            dead_list_result = self._invoke(
                [
                    "queue",
                    "dead",
                    "emails",
                    "--limit",
                    "1",
                    "--store-path",
                    store_path,
                    "--json",
                ]
            )
            self.assertEqual(dead_list_result.exit_code, 0, dead_list_result.output)
            self.assertEqual(json.loads(dead_list_result.stdout)[0]["id"], added["id"])

            filtered_result = self._invoke(
                [
                    "queue",
                    "dead",
                    "emails",
                    "--store-path",
                    store_path,
                    "--min-attempts",
                    "1",
                ]
            )
            self.assertEqual(filtered_result.exit_code, 0, filtered_result.output)
            self.assertEqual(json.loads(filtered_result.stdout)[0]["id"], added["id"])

            summary_result = self._invoke(
                [
                    "queue",
                    "dead",
                    "emails",
                    "--store-path",
                    store_path,
                    "--summary",
                ]
            )
            self.assertEqual(summary_result.exit_code, 0, summary_result.output)
            summary = json.loads(summary_result.stdout)
            self.assertEqual(summary["count"], 1)
            self.assertEqual(summary["by_error_type"]["None"], 1)

    def test_queue_process_rejects_forever_with_max_jobs(self) -> None:
        result = self._invoke(
            [
                "queue",
                "process",
                "emails",
                "main:handle",
                "--forever",
                "--max-jobs",
                "2",
            ]
        )

        self.assertEqual(result.exit_code, 1, result.output)
        self.assertIn("pass either --forever or --max-jobs, not both", result.output)

    def test_queue_exec_rejects_forever_with_max_jobs(self) -> None:
        result = self._invoke(
            [
                "queue",
                "exec",
                "emails",
                "--forever",
                "--max-jobs",
                "2",
                "--",
                "python",
                "-c",
                "print('ok')",
            ]
        )

        self.assertEqual(result.exit_code, 1, result.output)
        self.assertIn("pass either --forever or --max-jobs, not both", result.output)

    def test_queue_dead_filters_by_error_text_and_failed_age(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            queue = PersistentQueue("emails", store_path=store_path)
            _ = queue.put({"to": "user@example.com"})
            message = queue.get_message()
            self.assertTrue(
                queue.dead_letter(message, error=RuntimeError("mail timeout"))
            )

            result = self._invoke(
                [
                    "queue",
                    "dead",
                    "emails",
                    "--store-path",
                    store_path,
                    "--error-contains",
                    "timeout",
                ]
            )
            self.assertEqual(result.exit_code, 0, result.output)
            payload = json.loads(result.stdout)
            self.assertEqual(len(payload), 1)
            self.assertEqual(payload[0]["id"], message.id)

            summary_result = self._invoke(
                [
                    "queue",
                    "dead",
                    "emails",
                    "--store-path",
                    store_path,
                    "--summary",
                ]
            )
            self.assertEqual(summary_result.exit_code, 0, summary_result.output)
            summary = json.loads(summary_result.stdout)
            self.assertEqual(summary["count"], 1)
            self.assertEqual(summary["by_error_type"]["RuntimeError"], 1)

            aged_result = self._invoke(
                [
                    "queue",
                    "dead",
                    "emails",
                    "--store-path",
                    store_path,
                    "--failed-within",
                    "0",
                ]
            )
            self.assertEqual(aged_result.exit_code, 0, aged_result.output)
            self.assertEqual(json.loads(aged_result.stdout), [])

            purge_result = self._invoke(
                ["queue", "purge", "emails", "--store-path", store_path]
            )
            self.assertEqual(purge_result.exit_code, 0, purge_result.output)
            self.assertEqual(purge_result.stdout.strip(), "1")

    def test_queue_dead_summary_groups_by_worker_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            queue = PersistentQueue("emails", store_path=store_path)
            _ = queue.put({"to": "user@example.com"})
            message = queue.get_message(leased_by="worker-a")
            self.assertTrue(
                queue.dead_letter(message, error=RuntimeError("mail timeout"))
            )

            summary_result = self._invoke(
                [
                    "queue",
                    "dead",
                    "emails",
                    "--store-path",
                    store_path,
                    "--summary",
                ]
            )
            self.assertEqual(summary_result.exit_code, 0, summary_result.output)
            summary = json.loads(summary_result.stdout)
            self.assertEqual(summary["count"], 1)
            self.assertEqual(summary["by_error_type"]["RuntimeError"], 1)
            self.assertEqual(summary["by_worker_id"], {"worker-a": 1})

    def test_queue_add_accepts_stdin_and_raw_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")

            stdin_result = self._invoke(
                ["queue", "add", "emails", "--store-path", store_path],
                input='{"to":"stdin@example.com"}',
            )
            self.assertEqual(stdin_result.exit_code, 0, stdin_result.output)
            self.assertEqual(
                json.loads(stdin_result.stdout)["value"],
                {"to": "stdin@example.com"},
            )

            raw_result = self._invoke(
                [
                    "queue",
                    "add",
                    "emails",
                    "--value",
                    "raw-address@example.com",
                    "--raw",
                    "--store-path",
                    store_path,
                ]
            )
            self.assertEqual(raw_result.exit_code, 0, raw_result.output)
            self.assertEqual(
                json.loads(raw_result.stdout)["value"], "raw-address@example.com"
            )

            invalid_result = self._invoke(
                [
                    "queue",
                    "add",
                    "emails",
                    "--value",
                    "not-json",
                    "--store-path",
                    store_path,
                ]
            )
            self.assertNotEqual(invalid_result.exit_code, 0)
            self.assertIn("value must be valid JSON", invalid_result.output)

    def test_queue_commands_report_empty_and_missing_messages(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")

            empty_result = self._invoke(
                ["queue", "pop", "emails", "--store-path", store_path]
            )
            self.assertEqual(empty_result.exit_code, 1)
            self.assertIn("queue is empty", empty_result.output)

            missing_ack = self._invoke(
                ["queue", "ack", "emails", "missing", "--store-path", store_path]
            )
            self.assertEqual(missing_ack.exit_code, 1)
            self.assertIn("message not found", missing_ack.output)

            missing_requeue = self._invoke(
                [
                    "queue",
                    "requeue-dead",
                    "emails",
                    "missing",
                    "--store-path",
                    store_path,
                ]
            )
            self.assertEqual(missing_requeue.exit_code, 1)
            self.assertIn("dead-letter message not found", missing_requeue.output)

    def test_print_dead_letters_watch_stops_on_shutdown(self) -> None:
        queue = PersistentQueue("emails", store=MemoryQueueStore())
        _ = queue.put({"to": "user@example.com"})
        message = queue.get_message()
        self.assertTrue(queue.dead_letter(message, error=RuntimeError("bad")))
        console = _JsonConsole()
        shutdown = _ShutdownState()

        def request_shutdown(_interval: float) -> None:
            shutdown.requested = True

        with mock.patch("time.sleep", side_effect=request_shutdown):
            _print_dead_letters(
                queue,
                console=console,
                limit=None,
                watch=True,
                interval=0.001,
                shutdown=shutdown,
                summary=False,
                min_attempts=None,
                max_attempts=None,
                error_contains=None,
                failed_within=None,
            )

        self.assertEqual(len(console.values), 1)
        self.assertEqual(console.values[0][0]["id"], message.id)

    def test_queue_process_command_acks_successful_handler(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            store_path = str(tmp_path / "queues")
            retry_store_path = str(tmp_path / "retries.sqlite3")
            handler_path = tmp_path / "worker.py"
            handler_path.write_text(
                "def handle(payload):\n    payload['handled'] = True\n",
                encoding="utf-8",
            )
            add_result = self._invoke(
                [
                    "queue",
                    "add",
                    "emails",
                    "--value",
                    '{"to":"user@example.com"}',
                    "--store-path",
                    store_path,
                ]
            )
            self.assertEqual(add_result.exit_code, 0, add_result.output)

            previous = Path.cwd()
            _ = sys.modules.pop("worker", None)
            try:
                os.chdir(tmp_path)
                process_result = self._invoke(
                    [
                        "queue",
                        "process",
                        "emails",
                        "worker:handle",
                        "--store-path",
                        store_path,
                        "--retry-store-path",
                        retry_store_path,
                        "--worker-id",
                        "worker-a",
                        "--max-tries",
                        "1",
                    ]
                )
            finally:
                os.chdir(previous)
                _ = sys.modules.pop("worker", None)

            self.assertEqual(process_result.exit_code, 0, process_result.output)
            self.assertEqual(json.loads(process_result.stdout)["state"], "acked")
            self.assertEqual(
                PersistentQueue("emails", store_path=store_path).qsize(), 0
            )
            self.assertTrue(Path(retry_store_path).is_file())

    def test_queue_process_command_dead_letters_failed_handler(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            store_path = str(tmp_path / "queues")
            retry_store_path = str(tmp_path / "retries.sqlite3")
            handler_path = tmp_path / "worker.py"
            handler_path.write_text(
                "def handle(payload):\n    raise ConnectionError('mail server down')\n",
                encoding="utf-8",
            )
            queue = PersistentQueue("emails", store_path=store_path)
            message = queue.put({"to": "user@example.com"})

            previous = Path.cwd()
            _ = sys.modules.pop("worker", None)
            try:
                os.chdir(tmp_path)
                result = self._invoke(
                    [
                        "queue",
                        "process",
                        "emails",
                        "worker:handle",
                        "--store-path",
                        store_path,
                        "--retry-store-path",
                        retry_store_path,
                        "--max-tries",
                        "1",
                    ]
                )
            finally:
                os.chdir(previous)
                _ = sys.modules.pop("worker", None)

            self.assertEqual(result.exit_code, 1)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["id"], message.id)
            self.assertEqual(payload["state"], "failed")
            self.assertEqual(payload["last_error"]["type"], "ConnectionError")
            dead_letters = PersistentQueue(
                "emails", store_path=store_path
            ).dead_letters()
            self.assertEqual(len(dead_letters), 1)
            self.assertEqual(dead_letters[0].id, message.id)

    def test_queue_process_reports_bad_handler_spec(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = self._invoke(
                [
                    "queue",
                    "process",
                    "emails",
                    "not-a-handler",
                    "--store-path",
                    str(Path(tmpdir) / "queues"),
                ]
            )

            self.assertEqual(result.exit_code, 1)
            self.assertIn("module:function", result.output)

    def test_queue_exec_reports_missing_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            retry_store_path = str(Path(tmpdir) / "retries.sqlite3")
            queue = PersistentQueue("emails", store_path=store_path)
            message = queue.put({"to": "user@example.com"})

            result = self._invoke(
                [
                    "queue",
                    "exec",
                    "emails",
                    "--store-path",
                    store_path,
                    "--retry-store-path",
                    retry_store_path,
                    "--max-tries",
                    "1",
                    "--",
                    "definitely-not-a-real-command-123",
                ]
            )

            self.assertEqual(result.exit_code, 1)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["id"], message.id)
            self.assertEqual(payload["state"], "failed")
            self.assertEqual(payload["last_error"]["type"], "_CommandNotFoundError")
            self.assertEqual(payload["last_error"]["exit_code"], 127)
            self.assertEqual(
                payload["last_error"]["command"], ["definitely-not-a-real-command-123"]
            )
            self.assertIn("command not found", payload["last_error"]["message"])

    def test_command_handler_passes_message_value_as_json_stdin(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            output_path = tmp_path / "payload.json"
            script_path = tmp_path / "write_payload.py"
            script_path.write_text(
                "import pathlib\n"
                "import sys\n"
                f"pathlib.Path({str(output_path)!r}).write_text(sys.stdin.read())\n",
                encoding="utf-8",
            )

            handler = _command_handler([sys.executable, str(script_path)])
            handler({"to": "user@example.com"})

            self.assertEqual(
                json.loads(output_path.read_text(encoding="utf-8")),
                {"to": "user@example.com"},
            )

    def test_queue_exec_command_acks_successful_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            store_path = str(tmp_path / "queues")
            retry_store_path = str(tmp_path / "retries.sqlite3")
            output_path = tmp_path / "payload.json"
            script_path = tmp_path / "write_payload.py"
            script_path.write_text(
                "import pathlib\n"
                "import sys\n"
                f"pathlib.Path({str(output_path)!r}).write_text(sys.stdin.read())\n",
                encoding="utf-8",
            )
            queue = PersistentQueue("emails", store_path=store_path)
            _ = queue.put({"to": "user@example.com"})

            result = self._invoke(
                [
                    "queue",
                    "exec",
                    "emails",
                    "--store-path",
                    store_path,
                    "--retry-store-path",
                    retry_store_path,
                    "--max-tries",
                    "1",
                    "--",
                    sys.executable,
                    str(script_path),
                ]
            )

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertEqual(json.loads(result.stdout)["state"], "acked")
            self.assertEqual(
                PersistentQueue("emails", store_path=store_path).qsize(), 0
            )
            self.assertEqual(
                json.loads(output_path.read_text(encoding="utf-8")),
                {"to": "user@example.com"},
            )

    def test_queue_exec_command_dead_letters_failed_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            store_path = str(tmp_path / "queues")
            retry_store_path = str(tmp_path / "retries.sqlite3")
            script_path = tmp_path / "fail.py"
            script_path.write_text(
                "import sys\n"
                "sys.stderr.write('cannot deliver\\n')\n"
                "raise SystemExit(7)\n",
                encoding="utf-8",
            )
            queue = PersistentQueue("emails", store_path=store_path)
            message = queue.put({"to": "user@example.com"})

            result = self._invoke(
                [
                    "queue",
                    "exec",
                    "emails",
                    "--store-path",
                    store_path,
                    "--retry-store-path",
                    retry_store_path,
                    "--max-tries",
                    "1",
                    "--",
                    sys.executable,
                    str(script_path),
                ]
            )

            self.assertEqual(result.exit_code, 1)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["id"], message.id)
            self.assertEqual(payload["state"], "failed")
            self.assertEqual(payload["last_error"]["type"], "_CommandExecutionError")
            self.assertEqual(payload["last_error"]["exit_code"], 7)
            self.assertEqual(payload["last_error"]["stderr"], "cannot deliver")
            self.assertIn("status 7", payload["last_error"]["message"])
            self.assertIn("cannot deliver", payload["last_error"]["message"])
            self.assertEqual(payload["attempt_history"][0]["type"], "leased")
            self.assertEqual(payload["attempt_history"][-1]["type"], "dead_lettered")

            dead_letters = PersistentQueue(
                "emails", store_path=store_path
            ).dead_letters()
            self.assertEqual(len(dead_letters), 1)
            self.assertEqual(dead_letters[0].id, message.id)
            assert dead_letters[0].last_error is not None
            self.assertEqual(dead_letters[0].last_error["exit_code"], 7)
            self.assertEqual(dead_letters[0].last_error["stderr"], "cannot deliver")

    def test_queue_exec_missing_command_is_dead_lettered(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            store_path = str(tmp_path / "queues")
            retry_store_path = str(tmp_path / "retries.sqlite3")
            queue = PersistentQueue("emails", store_path=store_path)
            message = queue.put({"to": "user@example.com"})

            result = self._invoke(
                [
                    "queue",
                    "exec",
                    "emails",
                    "--store-path",
                    store_path,
                    "--retry-store-path",
                    retry_store_path,
                    "--max-tries",
                    "1",
                    "--release-on-exhaustion",
                    "--",
                    "command-that-does-not-exist-12345",
                ]
            )

            self.assertEqual(result.exit_code, 1, result.output)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["id"], message.id)
            self.assertEqual(payload["state"], "failed")
            self.assertEqual(payload["last_error"]["type"], "_CommandNotFoundError")
            self.assertEqual(payload["last_error"]["exit_code"], 127)
            self.assertEqual(payload["last_error"]["stderr"], "command not found")
            self.assertEqual(payload["attempt_history"][-1]["type"], "dead_lettered")

            dead_letters = PersistentQueue(
                "emails", store_path=store_path
            ).dead_letters()
            self.assertEqual(len(dead_letters), 1)
            self.assertEqual(dead_letters[0].id, message.id)
            assert dead_letters[0].last_error is not None
            self.assertEqual(dead_letters[0].last_error["exit_code"], 127)

    def test_queue_requeue_dead_all_moves_every_dead_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            persistent_queue = PersistentQueue("emails", store_path=store_path)
            first = persistent_queue.put({"to": "one@example.com"})
            second = persistent_queue.put({"to": "two@example.com"})
            self.assertTrue(
                persistent_queue.dead_letter(
                    persistent_queue.get_message(), error=RuntimeError("bad 1")
                )
            )
            self.assertTrue(
                persistent_queue.dead_letter(
                    persistent_queue.get_message(), error=RuntimeError("bad 2")
                )
            )

            result = self._invoke(
                [
                    "queue",
                    "requeue-dead",
                    "emails",
                    "--all",
                    "--store-path",
                    store_path,
                ]
            )

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertEqual(json.loads(result.stdout), {"requeued": 2})
            fresh_queue = PersistentQueue("emails", store_path=store_path)
            self.assertEqual(fresh_queue.dead_letters(), [])
            self.assertEqual(fresh_queue.qsize(), 2)
            first_message = fresh_queue.inspect(first.id)
            second_message = fresh_queue.inspect(second.id)
            self.assertIsNotNone(first_message)
            self.assertIsNotNone(second_message)
            assert first_message is not None
            assert second_message is not None
            self.assertEqual(first_message.state, "ready")
            self.assertEqual(second_message.state, "ready")

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
            connection = sqlite3.connect(retry_store_path)
            try:
                names = connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            finally:
                connection.close()
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
            retry_store_path=self._retry_store_path(),
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

    def test_print_queue_stats_watch_stops_on_shutdown(self) -> None:
        queue = PersistentQueue("emails", store=MemoryQueueStore())
        _ = queue.put({"to": "user@example.com"})
        _ = queue.get_message(leased_by="worker-a")
        console = _JsonConsole()
        shutdown = _ShutdownState()

        def request_shutdown(_interval: float) -> None:
            shutdown.requested = True

        with mock.patch("time.sleep", side_effect=request_shutdown):
            _print_queue_stats(
                queue,
                console=console,
                watch=True,
                interval=0.001,
                shutdown=shutdown,
            )

        self.assertEqual(
            len(console.values),
            1,
        )
        self.assertEqual(console.values[0]["ready"], 0)
        self.assertEqual(console.values[0]["delayed"], 0)
        self.assertEqual(console.values[0]["inflight"], 1)
        self.assertEqual(console.values[0]["dead"], 0)
        self.assertEqual(console.values[0]["total"], 1)
        self.assertEqual(console.values[0]["by_worker_id"], {"worker-a": 1})
        self.assertEqual(console.values[0]["leases_by_worker_id"], {"worker-a": 1})

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
            retry_store_path=self._retry_store_path(),
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
            retry_store_path=self._retry_store_path(),
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
            retry_store_path=self._retry_store_path(),
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
            retry_store_path=self._retry_store_path(),
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
                    "--json",
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
