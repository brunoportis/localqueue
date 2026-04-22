from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class ProcessHarnessTests(unittest.TestCase):
    def _run_harness(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        script = (
            Path(__file__).resolve().parents[1]
            / "examples"
            / "sqlite_process_harness.py"
        )
        return subprocess.run(
            [sys.executable, str(script), *args],
            capture_output=True,
            text=True,
            check=False,
        )

    def test_throughput_processes_share_one_sqlite_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queue.sqlite3")
            result = self._run_harness(
                [
                    "--mode",
                    "throughput",
                    "--messages",
                    "60",
                    "--producers",
                    "2",
                    "--consumers",
                    "2",
                    "--store-path",
                    store_path,
                ]
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            summary = json.loads(result.stdout)
            self.assertEqual(summary["mode"], "throughput")
            self.assertTrue(summary["invariants_ok"])
            self.assertEqual(summary["errors"], 0)
            self.assertEqual(summary["consumed"], 60)
            self.assertEqual(summary["unique_messages"], 60)
            self.assertEqual(summary["remaining"]["total"], 0)

    def test_crash_recovery_processes_reclaim_after_lease_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store_path = str(Path(tmpdir) / "queue.sqlite3")
            result = self._run_harness(
                [
                    "--mode",
                    "crash-recovery",
                    "--messages",
                    "20",
                    "--lease-timeout",
                    "0.2",
                    "--store-path",
                    store_path,
                ]
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            summary = json.loads(result.stdout)
            self.assertEqual(summary["mode"], "crash-recovery")
            self.assertTrue(summary["invariants_ok"])
            self.assertEqual(summary["errors"], 0)
            self.assertEqual(summary["crash_exit_code"], 3)
            self.assertEqual(summary["consumed"], 20)
            self.assertEqual(summary["unique_messages"], 20)
            self.assertEqual(summary["remaining"]["total"], 0)
            self.assertEqual(summary["remaining"]["dead"], 0)
