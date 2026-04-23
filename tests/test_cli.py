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

from localqueue import MemoryQueueStore, PersistentQueue, QueueMessage
from localqueue.retry import (
    MemoryAttemptStore,
    PersistentRetryExhausted,
    RetryRecord,
    SQLiteAttemptStore,
)
from localqueue.cli import (
    _CommandExecutionError,
    _CommandNotFoundError,
    CONFIG_FILENAME,
    DEFAULT_RETRY_STORE_PATH,
    _ShutdownState,
    _build_app,
    _complete_message,
    _command_handler,
    _dead_letter_summary,
    _error_payload,
    _filter_dead_letters,
    _finish_failed_message,
    _format_command,
    _config_path,
    _load_callable,
    _load_config,
    _parse_json,
    _print_dead_letters,
    _print_queue_stats,
    _process_message,
    _process_queue_messages,
    _process_queue_iteration,
    _poll_timeout,
    _queue_worker_command_signature,
    _last_attempt_worker_id,
    _read_value,
    _resolve_retry_store_path,
    _resolve_dead_letter_ttl,
    _resolve_retry_record_ttl,
    _resolve_store_path,
    _coerce_config_value,
    _truncate_output,
    _validate_worker_loop_options,
    _shutdown_state,
    _worker_health_summary,
    _write_config,
    _QueueIterationContext,
    _QueueWorkerOptions,
)
import localqueue.cli as cli_module
from localqueue.retry import configure_default_store

EXAMPLE_CONFIG_HOME = "/home/example/.config"
EXAMPLE_QUEUE_STORE_PATH = "/home/example/localqueue/queues"
EXAMPLE_RETRY_STORE_PATH = "/home/example/localqueue/retries.sqlite3"
EXAMPLE_EXPLICIT_QUEUE_STORE_PATH = "/home/example/localqueue/explicit-queues"
EXAMPLE_EXPLICIT_RETRY_STORE_PATH = "/home/example/localqueue/explicit-retries.sqlite3"


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


def _queue_worker_options(
    *,
    retry_store_path: str,
    max_jobs: int,
    forever: bool,
    max_tries: int,
    worker_id: str | None,
    dead_letter_on_exhaustion: bool,
) -> _QueueWorkerOptions:
    return _QueueWorkerOptions(
        retry_store_path=retry_store_path,
        max_jobs=max_jobs,
        forever=forever,
        max_tries=max_tries,
        worker_id=worker_id,
        block=False,
        timeout=None,
        idle_sleep=0.001,
        release_delay=0.0,
        dead_letter_on_exhaustion=dead_letter_on_exhaustion,
    )


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

    def test_load_callable_rejects_non_callable_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "main.py"
            path.write_text("VALUE = 1\n", encoding="utf-8")
            previous = Path.cwd()
            _ = sys.modules.pop("main", None)
            try:
                os.chdir(tmpdir)
                with self.assertRaisesRegex(
                    TypeError, "^handler is not callable: main:VALUE$"
                ):
                    _ = _load_callable("main:VALUE")
            finally:
                os.chdir(previous)
                _ = sys.modules.pop("main", None)

    def test_complete_message_rejects_unsupported_action(self) -> None:
        with self.assertRaisesRegex(ValueError, "^unsupported queue action: nope$"):
            _complete_message(
                typer,
                _JsonConsole(),
                _JsonConsole(),
                "emails",
                str(Path("queues.sqlite3")),
                "message-id",
                "nope",
            )

    def test_main_rejects_missing_optional_dependencies(self) -> None:
        with mock.patch.dict(
            "sys.modules", {"rich.console": None, "typer": None, "yaml": None}
        ):
            with self.assertRaises(SystemExit) as exc_info:
                cli_module.main()

        self.assertIn("localqueue[cli]", str(exc_info.exception))

    def test_main_invokes_built_app(self) -> None:
        app = mock.Mock()
        with mock.patch("localqueue.cli._build_app", return_value=app) as build_app:
            cli_module.main(["--help"])

        build_app.assert_called_once()
        app.assert_called_once_with(args=["--help"])

    def test_read_value_uses_argument_before_stdin(self) -> None:
        self.assertEqual(_read_value("{}"), "{}")

    def test_read_value_falls_back_to_stdin(self) -> None:
        stdin = StringIO('{"to":"user@example.com"}\n')
        with mock.patch("sys.stdin", stdin):
            self.assertEqual(_read_value(None), '{"to":"user@example.com"}\n')

    def test_read_value_rejects_empty_stdin(self) -> None:
        stdin = StringIO("")
        with mock.patch("sys.stdin", stdin):
            with self.assertRaisesRegex(ValueError, "^stdin did not contain a value$"):
                _ = _read_value(None)

    def test_read_value_rejects_tty_without_argument(self) -> None:
        stdin = mock.Mock()
        stdin.isatty.return_value = True
        with mock.patch("sys.stdin", stdin):
            with self.assertRaisesRegex(
                ValueError, "^missing value; pass an argument or pipe data on stdin$"
            ):
                _ = _read_value(None)

    def test_config_path_uses_xdg_config_home(self) -> None:
        with mock.patch.dict(
            "os.environ", {"XDG_CONFIG_HOME": EXAMPLE_CONFIG_HOME}, clear=False
        ):
            self.assertEqual(
                _config_path(),
                Path(EXAMPLE_CONFIG_HOME) / "localqueue" / CONFIG_FILENAME,
            )

    def test_load_and_write_config_use_yaml_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "localqueue" / "config.yaml"
            _write_config(
                yaml,
                {
                    "store_path": EXAMPLE_QUEUE_STORE_PATH,
                    "retry_store_path": EXAMPLE_RETRY_STORE_PATH,
                },
                path=path,
            )

            self.assertEqual(
                _load_config(yaml, path=path),
                {
                    "store_path": EXAMPLE_QUEUE_STORE_PATH,
                    "retry_store_path": EXAMPLE_RETRY_STORE_PATH,
                },
            )

    def test_load_config_accepts_empty_yaml_document(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "localqueue" / "config.yaml"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("null\n", encoding="utf-8")

            self.assertEqual(_load_config(yaml, path=path), {})

    def test_config_defaults_are_resolved_for_queue_paths(self) -> None:
        config = {
            "store_path": EXAMPLE_QUEUE_STORE_PATH,
            "retry_store_path": EXAMPLE_RETRY_STORE_PATH,
        }

        self.assertEqual(_resolve_store_path(None, config), EXAMPLE_QUEUE_STORE_PATH)
        self.assertEqual(
            _resolve_store_path(EXAMPLE_EXPLICIT_QUEUE_STORE_PATH, config),
            EXAMPLE_EXPLICIT_QUEUE_STORE_PATH,
        )
        self.assertEqual(
            _resolve_retry_store_path(None, config), EXAMPLE_RETRY_STORE_PATH
        )
        self.assertEqual(
            _resolve_retry_store_path(EXAMPLE_EXPLICIT_RETRY_STORE_PATH, config),
            EXAMPLE_EXPLICIT_RETRY_STORE_PATH,
        )
        self.assertEqual(_resolve_retry_store_path(None, {}), DEFAULT_RETRY_STORE_PATH)

    def test_queue_worker_command_signature_rejects_unexpected_values(self) -> None:
        calls: list[Any] = []

        def callback(*, cli_options: Any) -> None:
            calls.append(cli_options)

        wrapped = _queue_worker_command_signature(typer)(callback)

        with self.assertRaisesRegex(
            TypeError, "^unexpected CLI option values: unknown_option$"
        ):
            wrapped(
                store_path=None,
                retry_store_path=None,
                max_jobs=1,
                forever=False,
                max_tries=3,
                lease_timeout=30.0,
                worker_id=None,
                block=False,
                timeout=None,
                idle_sleep=1.0,
                release_delay=0.0,
                min_interval=0.0,
                circuit_breaker_failures=0,
                circuit_breaker_cooldown=0.0,
                dead_letter_on_exhaustion=True,
                log_events=False,
                unknown_option=True,
            )

        self.assertEqual(calls, [])

    def test_default_data_paths_use_xdg_data_home(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict("os.environ", {"XDG_DATA_HOME": tmpdir}, clear=False):
                self.assertEqual(
                    _resolve_store_path(None, {}),
                    str(Path(tmpdir) / "localqueue" / "queue.sqlite3"),
                )
                self.assertEqual(
                    _resolve_retry_store_path(None, {}),
                    str(Path(tmpdir) / "localqueue" / "retries.sqlite3"),
                )

    def test_config_init_defaults_to_xdg_queue_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(
                "os.environ",
                {"XDG_CONFIG_HOME": tmpdir, "XDG_DATA_HOME": tmpdir},
                clear=False,
            ):
                result = self._invoke(["config", "init"])
                self.assertEqual(result.exit_code, 0, result.output)

                config_path = Path(tmpdir) / "localqueue" / CONFIG_FILENAME
                self.assertEqual(
                    _load_config(yaml, config_path),
                    {"store_path": str(Path(tmpdir) / "localqueue" / "queue.sqlite3")},
                )

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
                        EXAMPLE_QUEUE_STORE_PATH,
                        "--retry-store-path",
                        EXAMPLE_RETRY_STORE_PATH,
                        "--dead-letter-ttl-seconds",
                        "86400",
                        "--retry-record-ttl-seconds",
                        "604800",
                    ]
                )
                self.assertEqual(init_result.exit_code, 0, init_result.output)
                self.assertIn("wrote config", init_result.output)

                show_result = self._invoke(["config", "show"])
                self.assertEqual(show_result.exit_code, 0, show_result.output)
                self.assertEqual(
                    json.loads(show_result.stdout),
                    {
                        "dead_letter_ttl_seconds": 86400.0,
                        "retry_store_path": EXAMPLE_RETRY_STORE_PATH,
                        "retry_record_ttl_seconds": 604800.0,
                        "store_path": EXAMPLE_QUEUE_STORE_PATH,
                    },
                )

                set_result = self._invoke(
                    [
                        "config",
                        "set",
                        "dead_letter_ttl_seconds",
                        "43200",
                    ]
                )
                self.assertEqual(set_result.exit_code, 0, set_result.output)
                self.assertEqual(
                    json.loads(set_result.stdout)["dead_letter_ttl_seconds"],
                    43200.0,
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

            health_result = self._invoke(
                ["queue", "health", "emails", "--store-path", store_path]
            )
            self.assertEqual(health_result.exit_code, 0, health_result.output)
            health = json.loads(health_result.stdout)
            self.assertEqual(health["queue"], "emails")
            self.assertEqual(health["stats"]["dead"], 1)
            self.assertEqual(health["dead_letters"]["count"], 1)

    def test_queue_dead_prune_older_than_removes_old_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            queue = PersistentQueue("emails", store_path=store_path)

            with mock.patch(
                "time.time",
                side_effect=[100.0, 100.0, 100.0, 100.0, 100.0],
            ):
                _ = queue.put({"to": "user@example.com"})
                message = queue.get_message()
                self.assertTrue(
                    queue.dead_letter(message, error=RuntimeError("mail timeout"))
                )

            with mock.patch("localqueue.cli.time.time", return_value=200.0):
                prune_result = self._invoke(
                    [
                        "queue",
                        "dead",
                        "emails",
                        "--store-path",
                        store_path,
                        "--prune-older-than",
                        "50",
                    ]
                )

            self.assertEqual(prune_result.exit_code, 0, prune_result.output)
            self.assertEqual(
                json.loads(prune_result.stdout),
                {"deleted": 1, "dry_run": False, "older_than": 50.0},
            )
            self.assertEqual(
                PersistentQueue("emails", store_path=store_path).dead_letters(), []
            )

    def test_queue_dead_prune_preview_does_not_mutate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            queue = PersistentQueue("emails", store_path=store_path)

            with mock.patch(
                "time.time",
                side_effect=[100.0, 100.0, 100.0, 100.0, 100.0],
            ):
                _ = queue.put({"to": "user@example.com"})
                message = queue.get_message()
                self.assertTrue(
                    queue.dead_letter(message, error=RuntimeError("mail timeout"))
                )

            with mock.patch("localqueue.cli.time.time", return_value=200.0):
                prune_result = self._invoke(
                    [
                        "queue",
                        "dead",
                        "emails",
                        "--store-path",
                        store_path,
                        "--dry-run",
                        "--prune-older-than",
                        "50",
                    ]
                )

            self.assertEqual(prune_result.exit_code, 0, prune_result.output)
            self.assertEqual(
                json.loads(prune_result.stdout),
                {"dry_run": True, "older_than": 50.0, "would_delete": 1},
            )
            self.assertEqual(
                len(PersistentQueue("emails", store_path=store_path).dead_letters()), 1
            )

    def test_retry_prune_removes_old_exhausted_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "retries.sqlite3")
            store = SQLiteAttemptStore(store_path)
            store.save(
                "old",
                RetryRecord(attempts=3, first_attempt_at=100.0, exhausted=True),
            )
            store.save(
                "active",
                RetryRecord(attempts=1, first_attempt_at=100.0, exhausted=False),
            )
            store.close()

            with mock.patch("localqueue.cli.time.time", return_value=200.0):
                prune_result = self._invoke(
                    [
                        "retry",
                        "prune",
                        "--retry-store-path",
                        store_path,
                        "--older-than",
                        "50",
                    ]
                )

            self.assertEqual(prune_result.exit_code, 0, prune_result.output)
            self.assertEqual(
                json.loads(prune_result.stdout),
                {"deleted": 1, "dry_run": False, "older_than": 50.0},
            )
            reopened = SQLiteAttemptStore(store_path)
            self.assertIsNone(reopened.load("old"))
            self.assertIsNotNone(reopened.load("active"))
            reopened.close()

    def test_retry_prune_preview_does_not_mutate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "retries.sqlite3")
            store = SQLiteAttemptStore(store_path)
            store.save(
                "old",
                RetryRecord(attempts=3, first_attempt_at=100.0, exhausted=True),
            )
            store.save(
                "active",
                RetryRecord(attempts=1, first_attempt_at=100.0, exhausted=False),
            )
            store.close()

            with mock.patch("localqueue.cli.time.time", return_value=200.0):
                prune_result = self._invoke(
                    [
                        "retry",
                        "prune",
                        "--retry-store-path",
                        store_path,
                        "--dry-run",
                        "--older-than",
                        "50",
                    ]
                )

            self.assertEqual(prune_result.exit_code, 0, prune_result.output)
            self.assertEqual(
                json.loads(prune_result.stdout),
                {"dry_run": True, "older_than": 50.0, "would_delete": 1},
            )
            reopened = SQLiteAttemptStore(store_path)
            self.assertIsNotNone(reopened.load("old"))
            self.assertIsNotNone(reopened.load("active"))
            reopened.close()

    def test_cleanup_commands_use_configured_retention_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = Path(tmpdir) / "config"
            store_path = str(Path(tmpdir) / "queues.sqlite3")
            retry_store_path = str(Path(tmpdir) / "retries.sqlite3")
            with mock.patch.dict(
                "os.environ", {"XDG_CONFIG_HOME": str(config_dir)}, clear=False
            ):
                self._invoke(
                    [
                        "config",
                        "init",
                        "--store-path",
                        store_path,
                        "--retry-store-path",
                        retry_store_path,
                        "--dead-letter-ttl-seconds",
                        "86400",
                        "--retry-record-ttl-seconds",
                        "604800",
                    ]
                )

                queue = PersistentQueue("emails", store_path=store_path)
                with mock.patch(
                    "time.time",
                    side_effect=[100.0, 100.0, 100.0, 100.0, 100.0],
                ):
                    _ = queue.put({"to": "user@example.com"})
                    message = queue.get_message()
                    self.assertTrue(
                        queue.dead_letter(message, error=RuntimeError("mail timeout"))
                    )

                retry_store = SQLiteAttemptStore(retry_store_path)
                retry_store.save(
                    "old",
                    RetryRecord(attempts=3, first_attempt_at=100.0, exhausted=True),
                )
                retry_store.close()

                with mock.patch("localqueue.cli.time.time", return_value=200.0):
                    queue_preview = self._invoke(
                        [
                            "queue",
                            "dead",
                            "emails",
                            "--store-path",
                            store_path,
                            "--dry-run",
                        ]
                    )
                    retry_preview = self._invoke(
                        [
                            "retry",
                            "prune",
                            "--retry-store-path",
                            retry_store_path,
                            "--dry-run",
                        ]
                    )

            self.assertEqual(queue_preview.exit_code, 0, queue_preview.output)
            self.assertEqual(
                json.loads(queue_preview.stdout),
                {"dry_run": True, "older_than": 86400.0, "would_delete": 0},
            )
            self.assertEqual(retry_preview.exit_code, 0, retry_preview.output)
            self.assertEqual(
                json.loads(retry_preview.stdout),
                {"dry_run": True, "older_than": 604800.0, "would_delete": 0},
            )

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

    def test_queue_add_deduplicates_when_key_is_reused(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")

            first = self._invoke(
                [
                    "queue",
                    "add",
                    "emails",
                    "--value",
                    '{"to":"first@example.com"}',
                    "--dedupe-key",
                    "job-1",
                    "--store-path",
                    store_path,
                ]
            )
            second = self._invoke(
                [
                    "queue",
                    "add",
                    "emails",
                    "--value",
                    '{"to":"second@example.com"}',
                    "--dedupe-key",
                    "job-1",
                    "--store-path",
                    store_path,
                ]
            )

            self.assertEqual(first.exit_code, 0, first.output)
            self.assertEqual(second.exit_code, 0, second.output)
            first_payload = json.loads(first.stdout)
            second_payload = json.loads(second.stdout)
            self.assertEqual(first_payload["id"], second_payload["id"])
            self.assertEqual(first_payload["value"], {"to": "first@example.com"})
            self.assertEqual(second_payload["value"], {"to": "first@example.com"})

    def test_queue_add_logs_structured_enqueue_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")

            result = self._invoke(
                [
                    "queue",
                    "add",
                    "emails",
                    "--value",
                    '{"to":"user@example.com"}',
                    "--store-path",
                    store_path,
                    "--log-events",
                ]
            )

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertIn('"event": "queue.enqueue"', result.output)

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

    def test_queue_pop_logs_lease_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            queue = PersistentQueue("emails", store_path=store_path)
            _ = queue.put({"to": "user@example.com"})

            result = self._invoke(
                [
                    "queue",
                    "pop",
                    "emails",
                    "--store-path",
                    store_path,
                    "--worker-id",
                    "worker-a",
                    "--log-events",
                ]
            )

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertIn('"event": "queue.lease"', result.output)

    def test_queue_requeue_dead_rejects_message_id_with_all(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            queue = PersistentQueue("emails", store_path=store_path)
            _ = queue.put({"to": "user@example.com"})
            message = queue.get_message()
            self.assertTrue(queue.dead_letter(message))

            result = self._invoke(
                [
                    "queue",
                    "requeue-dead",
                    "emails",
                    message.id,
                    "--store-path",
                    store_path,
                    "--all",
                ]
            )

            self.assertEqual(result.exit_code, 1, result.output)
            self.assertIn("either a message id or --all", result.output)

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

    def test_queue_process_command_releases_when_retry_is_not_exhausted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            store_path = str(tmp_path / "queues")
            retry_store_path = str(tmp_path / "retries.sqlite3")
            handler_path = tmp_path / "worker.py"
            handler_path.write_text(
                "def handle(payload):\n    raise ConnectionError('mail server down')\n",
                encoding="utf-8",
            )
            _ = PersistentQueue("emails", store_path=store_path).put(
                {"to": "user@example.com"}
            )

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
                        "2",
                    ]
                )
            finally:
                os.chdir(previous)
                _ = sys.modules.pop("worker", None)

            self.assertEqual(result.exit_code, 1)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["state"], "failed")
            self.assertEqual(payload["last_error"]["type"], "ConnectionError")
            self.assertTrue(PersistentQueue("emails", store_path=store_path).empty())

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

    def test_queue_process_command_dead_letters_when_handler_is_permanent_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            store_path = str(tmp_path / "queues")
            retry_store_path = str(tmp_path / "retries.sqlite3")
            handler_path = tmp_path / "worker.py"
            handler_path.write_text(
                "def handle(payload):\n    raise SystemExit(127)\n",
                encoding="utf-8",
            )
            queue = PersistentQueue("emails", store_path=store_path)
            queue.put({"to": "user@example.com"})

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

            self.assertEqual(result.exit_code, 127)

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

    def test_queue_exec_logs_structured_transition_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            retry_store_path = str(Path(tmpdir) / "retries.sqlite3")
            queue = PersistentQueue("webhooks", store_path=store_path)
            _ = queue.put({"to": "user@example.com"})

            result = self._invoke(
                [
                    "queue",
                    "exec",
                    "webhooks",
                    "--store-path",
                    store_path,
                    "--retry-store-path",
                    retry_store_path,
                    "--log-events",
                    "--",
                    "python",
                    "-c",
                    "import json, sys; json.load(sys.stdin); print('ok')",
                ]
            )

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertIn('"event": "exec.lease"', result.output)
            self.assertIn('"event": "exec.ack"', result.output)

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

    def test_queue_exec_command_releases_when_command_is_not_exhausted(self) -> None:
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
                    "2",
                    "--",
                    sys.executable,
                    str(script_path),
                ]
            )

            self.assertEqual(result.exit_code, 1)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["state"], "failed")
            self.assertEqual(payload["last_error"]["type"], "_CommandExecutionError")
            self.assertTrue(PersistentQueue("emails", store_path=store_path).empty())

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

    def test_queue_requeue_dead_all_logs_event_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            persistent_queue = PersistentQueue("emails", store_path=store_path)
            _ = persistent_queue.put({"to": "one@example.com"})
            message = persistent_queue.get_message()
            self.assertTrue(
                persistent_queue.dead_letter(message, error=RuntimeError("bad"))
            )

            result = self._invoke(
                [
                    "queue",
                    "requeue-dead",
                    "emails",
                    "--all",
                    "--log-events",
                    "--store-path",
                    store_path,
                ]
            )

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertIn('"event": "queue.requeue"', result.stderr)

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
            options=_queue_worker_options(
                retry_store_path=self._retry_store_path(),
                max_jobs=1,
                forever=False,
                max_tries=1,
                worker_id="worker-a",
                dead_letter_on_exhaustion=True,
            ),
        )

        self.assertEqual(result, 1)
        self.assertEqual(console.values, [])
        self.assertEqual(err_console.messages, ["[yellow]queue is empty[/yellow]"])

    def test_process_queue_iteration_handles_forever_empty_queue_without_blocking(
        self,
    ) -> None:
        queue = PersistentQueue("emails", store=MemoryQueueStore())
        console = _JsonConsole()
        err_console = _JsonConsole()
        policy_state = mock.Mock()
        resolved_policy = mock.Mock()

        with (
            mock.patch("localqueue.cli._sleep_for_policy") as sleep_for_policy,
            mock.patch("localqueue.cli.time.sleep") as sleep,
        ):
            iteration, owned_retry_store, should_continue = _process_queue_iteration(
                _QueueIterationContext(
                    queue=queue,
                    handler=lambda payload: payload,
                    console=console,
                    err_console=err_console,
                    policy_state=policy_state,
                    resolved_policy=resolved_policy,
                    retry_store_path=None,
                    owned_retry_store=None,
                    max_tries=1,
                    worker_id=None,
                    block=False,
                    timeout=None,
                    idle_sleep=0.001,
                    release_delay=0.0,
                    dead_letter_on_exhaustion=True,
                    log_events=False,
                    mode="process",
                    forever=True,
                )
            )

        self.assertIsNone(iteration)
        self.assertIsNone(owned_retry_store)
        self.assertTrue(should_continue)
        sleep_for_policy.assert_called_once()
        sleep.assert_called_once()

    def test_process_queue_messages_reuses_retry_store_for_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            retry_store_path = str(Path(tmpdir) / "retries.sqlite3")

            class TrackingSQLiteAttemptStore(SQLiteAttemptStore):
                closed: bool

                def __init__(self, path: str) -> None:
                    super().__init__(path)
                    self.closed = False
                    created.append(self)

                def close(self) -> None:
                    self.closed = True
                    super().close()

            created: list[TrackingSQLiteAttemptStore] = []
            queue = PersistentQueue("emails", store=MemoryQueueStore())
            _ = queue.put({"to": "one@example.com"})
            _ = queue.put({"to": "two@example.com"})
            console = _JsonConsole()

            with mock.patch(
                "localqueue.cli.SQLiteAttemptStore",
                TrackingSQLiteAttemptStore,
            ):
                result = _process_queue_messages(
                    queue,
                    lambda payload: payload,
                    console=console,
                    err_console=_JsonConsole(),
                    shutdown=_ShutdownState(),
                    options=_queue_worker_options(
                        retry_store_path=retry_store_path,
                        max_jobs=2,
                        forever=False,
                        max_tries=1,
                        worker_id=None,
                        dead_letter_on_exhaustion=True,
                    ),
                )

            self.assertEqual(result, 0)
            self.assertEqual(len(created), 1)
            self.assertTrue(created[0].closed)
            self.assertEqual(
                [value["state"] for value in console.values], ["acked", "acked"]
            )

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
                stale_after=None,
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
            options=_queue_worker_options(
                retry_store_path=self._retry_store_path(),
                max_jobs=1,
                forever=True,
                max_tries=1,
                worker_id=None,
                dead_letter_on_exhaustion=True,
            ),
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

    def test_process_queue_messages_forever_breaks_after_processing_then_empty(
        self,
    ) -> None:
        queue = PersistentQueue("emails", store=MemoryQueueStore())
        _ = queue.put({"to": "first@example.com"})
        console = _JsonConsole()
        err_console = _JsonConsole()

        result = _process_queue_messages(
            queue,
            lambda payload: payload,
            console=console,
            err_console=err_console,
            shutdown=_ShutdownState(),
            options=_queue_worker_options(
                retry_store_path=self._retry_store_path(),
                max_jobs=2,
                forever=True,
                max_tries=1,
                worker_id=None,
                dead_letter_on_exhaustion=True,
            ),
        )

        self.assertEqual(result, 0)
        self.assertEqual([value["state"] for value in console.values], ["acked"])
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
            options=_queue_worker_options(
                retry_store_path=self._retry_store_path(),
                max_jobs=1,
                forever=False,
                max_tries=2,
                worker_id="worker-a",
                dead_letter_on_exhaustion=False,
            ),
        )

        self.assertEqual(result, 1)
        self.assertEqual(console.values[0]["state"], "failed")
        redelivered = queue.get_message()
        self.assertIsNone(redelivered.leased_by)

    def test_queue_exec_command_records_worker_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues")
            queue = PersistentQueue("emails", store_path=store_path)
            _ = queue.put({"to": "user@example.com"})

            result = self._invoke(
                [
                    "queue",
                    "exec",
                    "emails",
                    "--worker-id",
                    "worker-a",
                    "--store-path",
                    store_path,
                    "--",
                    sys.executable,
                    "-c",
                    "import sys; sys.stdout.write(sys.stdin.read())",
                ]
            )

            self.assertEqual(result.exit_code, 0, result.output)
            self.assertEqual(json.loads(result.stdout)["state"], "acked")
            self.assertGreater(
                PersistentQueue("emails", store_path=store_path)
                .stats()
                .last_seen_by_worker_id["worker-a"],
                0,
            )

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
            options=_queue_worker_options(
                retry_store_path=self._retry_store_path(),
                max_jobs=1,
                forever=False,
                max_tries=1,
                worker_id=None,
                dead_letter_on_exhaustion=True,
            ),
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
            options=_queue_worker_options(
                retry_store_path=self._retry_store_path(),
                max_jobs=1,
                forever=True,
                max_tries=1,
                worker_id=None,
                dead_letter_on_exhaustion=True,
            ),
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

    def test_cli_helpers_cover_small_utility_branches(self) -> None:
        self.assertEqual(
            _format_command(["localqueue", "queue", "add"]), "localqueue queue add"
        )
        self.assertEqual(_truncate_output("abcdef", limit=5), "ab...")
        self.assertEqual(_poll_timeout(True, True, None), 0.5)
        self.assertEqual(_poll_timeout(False, False, 12.0), 12.0)
        self.assertIsNone(_error_payload(None))
        self.assertEqual(
            _error_payload("oops"),
            {"type": None, "module": None, "message": "oops"},
        )
        queue = PersistentQueue("jobs", store=MemoryQueueStore())
        message = queue.put("value")
        self.assertIsNotNone(message)
        self.assertIsNone(_last_attempt_worker_id(message))

    def test_cli_helpers_cover_command_handler_paths(self) -> None:
        command = _command_handler(["sh", "-c", "printf ok"])
        self.assertIsNone(command({"value": 1}))

        with self.assertRaises(ValueError):
            _ = _command_handler([])

        with self.assertRaises(_CommandExecutionError) as exc_info:
            _command_handler(["sh", "-c", "printf boom >&2; exit 3"])({"value": 1})

        self.assertEqual(exc_info.exception.exit_code, 3)
        self.assertIn("boom", exc_info.exception.stderr)
        self.assertEqual(
            exc_info.exception.command, ["sh", "-c", "printf boom >&2; exit 3"]
        )

        with self.assertRaises(_CommandNotFoundError):
            _command_handler(["this-command-should-not-exist-12345"])({"value": 1})

    def test_cli_helpers_cover_config_and_retry_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "config.yaml"
            _write_config(yaml, {"store_path": "queues.sqlite3"}, path)
            self.assertEqual(_load_config(yaml, path), {"store_path": "queues.sqlite3"})

            path.write_text("- bad\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                _ = _load_config(yaml, path)

        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertTrue(str(_config_path()).endswith("localqueue/config.yaml"))

        self.assertEqual(_resolve_store_path(None, {"store_path": "a"}), "a")
        self.assertEqual(_resolve_store_path("b", {}), "b")
        self.assertEqual(_resolve_retry_store_path(None, {}), DEFAULT_RETRY_STORE_PATH)
        self.assertIsNone(_resolve_dead_letter_ttl(None, {}))
        self.assertEqual(_resolve_dead_letter_ttl(12.5, {}), 12.5)
        self.assertIsNone(_resolve_retry_record_ttl(None, {}))
        self.assertEqual(_resolve_retry_record_ttl(18.0, {}), 18.0)
        self.assertEqual(_coerce_config_value("dead_letter_ttl_seconds", "12.5"), 12.5)
        with self.assertRaises(ValueError):
            _ = _coerce_config_value("retry_record_ttl_seconds", "-1")
        self.assertEqual(
            _coerce_config_value("store_path", "queues.sqlite3"), "queues.sqlite3"
        )

        with self.assertRaises(ValueError):
            _validate_worker_loop_options(max_jobs=2, forever=True)

    def test_cli_helpers_cover_dead_letter_filter_and_health_summary(self) -> None:
        messages = [
            QueueMessage(
                id="1",
                queue="jobs",
                value="a",
                last_error={"type": "RuntimeError", "message": "boom"},
                failed_at=100.0,
                attempts=2,
                leased_by="worker-a",
            ),
            QueueMessage(
                id="2",
                queue="jobs",
                value="b",
                last_error={"type": "ValueError", "stderr": "bad"},
                failed_at=150.0,
                attempts=3,
                leased_by="worker-b",
            ),
        ]
        with mock.patch("localqueue.cli.time.time", return_value=200.0):
            self.assertEqual(
                [
                    message.id
                    for message in _filter_dead_letters(
                        messages,
                        min_attempts=3,
                        max_attempts=None,
                        error_contains="bad",
                        failed_within=100.0,
                    )
                ],
                ["2"],
            )

        with mock.patch("localqueue.cli.time.time", return_value=110.0):
            health = _worker_health_summary(
                {"worker-a": 100.0, "worker-b": 1.0},
                stale_after=10.0,
            )
        self.assertEqual(health["active"]["count"], 1)
        self.assertEqual(health["stale"]["count"], 1)
        self.assertIn("worker-b", health["stale"]["by_worker_id"])

    def test_cli_helpers_cover_dead_letter_filter_max_attempts_and_error_miss(
        self,
    ) -> None:
        messages = [
            QueueMessage(
                id="1",
                queue="jobs",
                value="a",
                attempts=1,
                last_error={"type": "RuntimeError", "message": "boom"},
                failed_at=100.0,
            ),
            QueueMessage(
                id="2",
                queue="jobs",
                value="b",
                attempts=4,
                last_error={"type": "ValueError", "message": "other"},
                failed_at=100.0,
            ),
        ]
        with mock.patch("localqueue.cli.time.time", return_value=200.0):
            filtered = _filter_dead_letters(
                messages,
                min_attempts=None,
                max_attempts=2,
                error_contains="missing",
                failed_within=None,
            )

        self.assertEqual(filtered, [])

    def test_cli_helpers_cover_empty_summary_and_filter_edge_cases(self) -> None:
        empty_summary = _dead_letter_summary([])
        self.assertEqual(empty_summary["count"], 0)
        self.assertNotIn("failed_at", empty_summary)

        filtered = _filter_dead_letters(
            [
                QueueMessage(
                    id="1",
                    queue="jobs",
                    value="a",
                    attempts=1,
                    failed_at=None,
                )
            ],
            min_attempts=2,
            max_attempts=3,
            error_contains=None,
            failed_within=None,
        )
        self.assertEqual(filtered, [])

    def test_process_message_handles_persistent_retry_exhausted(self) -> None:
        queue = PersistentQueue("emails", store=MemoryQueueStore())
        _ = queue.put({"to": "user@example.com"})
        message = queue.get_message()
        err_console = _JsonConsole()

        class FakeRetryer:
            def __call__(self, handler: Any, value: Any) -> None:
                raise PersistentRetryExhausted(message.id, 3)

        with (
            mock.patch("localqueue.cli.PersistentRetrying", return_value=FakeRetryer()),
            mock.patch.object(
                queue, "dead_letter", wraps=queue.dead_letter
            ) as dead_letter,
            mock.patch.object(queue, "release", wraps=queue.release) as release,
        ):
            result = _process_message(
                queue,
                message,
                lambda payload: payload,
                retry_store=MemoryAttemptStore(),
                retry_store_path=None,
                max_tries=1,
                release_delay=0.0,
                dead_letter_on_exhaustion=True,
                log_events=True,
                err_console=err_console,
            )

        self.assertFalse(result.processed)
        self.assertEqual(result.final_state, "dead-letter")
        self.assertIsNotNone(result.last_error)
        dead_letter.assert_called_once()
        release.assert_not_called()
        self.assertTrue(
            any(item["event"] == "process.dead_letter" for item in err_console.values)
        )

    def test_process_message_releases_on_unhandled_exception(self) -> None:
        queue = PersistentQueue("emails", store=MemoryQueueStore())
        _ = queue.put({"to": "user@example.com"})
        message = queue.get_message()
        err_console = _JsonConsole()

        class FakeRetryer:
            def __call__(self, handler: Any, value: Any) -> None:
                raise ConnectionError("mail server down")

            def get_record(self, key: str) -> Any:
                return None

        with (
            mock.patch("localqueue.cli.PersistentRetrying", return_value=FakeRetryer()),
            mock.patch("localqueue.cli.is_permanent_failure", return_value=False),
            mock.patch.object(queue, "release", wraps=queue.release) as release,
            mock.patch.object(
                queue, "dead_letter", wraps=queue.dead_letter
            ) as dead_letter,
        ):
            result = _process_message(
                queue,
                message,
                lambda payload: payload,
                retry_store=MemoryAttemptStore(),
                retry_store_path=None,
                max_tries=2,
                release_delay=0.0,
                dead_letter_on_exhaustion=False,
                log_events=True,
                err_console=err_console,
            )

        self.assertFalse(result.processed)
        self.assertEqual(result.final_state, "release")
        release.assert_called_once()
        dead_letter.assert_not_called()
        self.assertTrue(
            any(item["event"] == "process.release" for item in err_console.values)
        )

    def test_cli_helpers_cover_queue_summary_and_shutdown_paths(self) -> None:
        queue = PersistentQueue("jobs", store=MemoryQueueStore())
        _ = queue.put("first")
        second = queue.put("second")
        leased = queue.get_message(leased_by="worker-a")
        assert leased is not None
        queue.record_worker_heartbeat("worker-a")
        self.assertTrue(queue.dead_letter(leased, error=RuntimeError("boom")))
        queue.release(second, error=RuntimeError("retry"))

        messages = queue.dead_letters()
        self.assertEqual(_dead_letter_summary(messages)["count"], 1)
        self.assertEqual(
            _filter_dead_letters(
                messages,
                min_attempts=1,
                max_attempts=None,
                error_contains=None,
                failed_within=None,
            ),
            messages,
        )
        self.assertEqual(
            _filter_dead_letters(
                messages,
                min_attempts=None,
                max_attempts=None,
                error_contains="boom",
                failed_within=None,
            )[0].id,
            messages[0].id,
        )
        self.assertEqual(
            _filter_dead_letters(
                messages,
                min_attempts=None,
                max_attempts=None,
                error_contains=None,
                failed_within=10_000,
            )[0].id,
            messages[0].id,
        )

        dead_console = _JsonConsole()
        _print_dead_letters(
            queue,
            console=dead_console,
            watch=True,
            interval=0.0,
            shutdown=_ShutdownState(requested=True),
            limit=None,
            summary=False,
            min_attempts=None,
            max_attempts=None,
            error_contains=None,
            failed_within=None,
        )
        stats_console = _JsonConsole()
        _print_queue_stats(
            queue,
            console=stats_console,
            watch=True,
            interval=0.0,
            stale_after=10.0,
            shutdown=_ShutdownState(requested=True),
        )
        self.assertGreaterEqual(len(dead_console.values), 1)
        self.assertGreaterEqual(len(stats_console.values), 1)
        self.assertIn("worker_health", stats_console.values[0])

    def test_shutdown_state_request_callback_sets_requested(self) -> None:
        import signal

        callbacks: dict[int, Any] = {}

        def capture_signal(sig: int, handler: Any) -> Any:
            callbacks[sig] = handler
            return signal.SIG_DFL

        with mock.patch("localqueue.cli.signal.signal", side_effect=capture_signal):
            with _shutdown_state() as state:
                callbacks[signal.SIGINT](signal.SIGINT, None)
                self.assertTrue(state.requested)

    def test_queue_health_reports_worker_liveness(self) -> None:
        from rich.console import Console
        import typer
        from typer.testing import CliRunner

        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues.sqlite3")
            queue = PersistentQueue("emails", store_path=store_path)
            queue.record_worker_heartbeat("worker-a")

            app = _build_app(typer, yaml, Console(), Console(stderr=True))
            result = CliRunner().invoke(
                app,
                [
                    "queue",
                    "health",
                    "emails",
                    "--store-path",
                    store_path,
                    "--stale-after",
                    "10",
                    "--json",
                ],
            )

            self.assertEqual(result.exit_code, 0, result.output)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["queue"], "emails")
            self.assertEqual(
                payload["worker_health"]["active"]["count"],
                1,
            )
            self.assertEqual(
                payload["worker_health"]["stale"]["count"],
                0,
            )
            self.assertIn(
                "worker-a", payload["worker_health"]["active"]["by_worker_id"]
            )
            self.assertGreaterEqual(
                payload["worker_health"]["active"]["by_worker_id"]["worker-a"],
                0.0,
            )

    def test_finish_failed_message_switches_between_release_and_dead_letter(
        self,
    ) -> None:
        queue = PersistentQueue("jobs", store=MemoryQueueStore())
        message = queue.put("payload")

        self.assertTrue(
            _finish_failed_message(
                queue,
                message,
                release_delay=0.0,
                dead_letter_on_exhaustion=False,
                error=RuntimeError("retry"),
            )
        )
        leased = queue.get_message()
        assert leased is not None
        self.assertTrue(
            _finish_failed_message(
                queue,
                leased,
                release_delay=0.0,
                dead_letter_on_exhaustion=True,
                error=RuntimeError("dead"),
            )
        )

    def test_cli_rejects_invalid_retention_and_requeue_combinations(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues.sqlite3")

            dead_missing_ttl = self._invoke(
                [
                    "queue",
                    "dead",
                    "emails",
                    "--store-path",
                    store_path,
                    "--dry-run",
                ]
            )
            self.assertEqual(dead_missing_ttl.exit_code, 1)
            self.assertIn("dead_letter_ttl_seconds", dead_missing_ttl.output)

            dead_conflict = self._invoke(
                [
                    "queue",
                    "dead",
                    "emails",
                    "--store-path",
                    store_path,
                    "--prune-older-than",
                    "10",
                    "--summary",
                ]
            )
            self.assertEqual(dead_conflict.exit_code, 1)
            self.assertIn("cannot be combined", dead_conflict.output)

            retry_missing_ttl = self._invoke(["retry", "prune"])
            self.assertEqual(retry_missing_ttl.exit_code, 1)
            self.assertIn("retry_record_ttl_seconds", retry_missing_ttl.output)

            requeue_missing_id = self._invoke(["queue", "requeue-dead", "emails"])
            self.assertEqual(requeue_missing_id.exit_code, 1)
            self.assertIn("pass a message id", requeue_missing_id.output)

            requeue_conflict = self._invoke(
                ["queue", "requeue-dead", "emails", "abc", "--all"]
            )
            self.assertEqual(requeue_conflict.exit_code, 1)
            self.assertIn("either a message id or --all", requeue_conflict.output)

    def test_cli_logs_requeue_and_purge_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queues.sqlite3")

            seeded = self._invoke(
                [
                    "queue",
                    "add",
                    "emails",
                    "--store-path",
                    store_path,
                    "--value",
                    '{"to":"user@example.com"}',
                ]
            )
            self.assertEqual(seeded.exit_code, 0, seeded.output)
            message_id = json.loads(seeded.stdout)["id"]

            dead = self._invoke(
                [
                    "queue",
                    "dead-letter",
                    "emails",
                    message_id,
                    "--store-path",
                    store_path,
                    "--log-events",
                ]
            )
            self.assertEqual(dead.exit_code, 0, dead.output)
            self.assertIn('"event": "queue.dead_letter"', dead.output)

            requeue = self._invoke(
                [
                    "queue",
                    "requeue-dead",
                    "emails",
                    message_id,
                    "--store-path",
                    store_path,
                    "--log-events",
                ]
            )
            self.assertEqual(requeue.exit_code, 0, requeue.output)
            self.assertIn('"event": "queue.requeue"', requeue.output)

            purge = self._invoke(
                [
                    "queue",
                    "purge",
                    "emails",
                    "--store-path",
                    store_path,
                    "--log-events",
                ]
            )
            self.assertEqual(purge.exit_code, 0, purge.output)
            self.assertIn('"event": "queue.purge"', purge.output)


if __name__ == "__main__":
    _ = unittest.main()
