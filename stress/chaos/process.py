from __future__ import annotations

import os
import signal
import socket
import subprocess
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


class ProcessFailure(RuntimeError):
    def __init__(self, message: str, *, stdout: str = "", stderr: str = "") -> None:
        super().__init__(message)
        self.stdout = stdout
        self.stderr = stderr


def _write_logs(directory: Path, stdout: str, stderr: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "child.stdout.txt").write_text(stdout, encoding="utf-8")
    (directory / "child.stderr.txt").write_text(stderr, encoding="utf-8")


def _collect(process: subprocess.Popen[str], timeout: float) -> tuple[str, str]:
    try:
        return process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        terminate_process(process)
        return process.communicate(timeout=timeout)


@dataclass
class SynchronizedProcess:
    process: subprocess.Popen[str]
    notification: str
    connection: socket.socket
    listener: socket.socket
    artifacts_dir: Path

    def kill_and_collect(self) -> tuple[int | None, str, str]:
        terminate_process(self.process)
        self.connection.close()
        self.listener.close()
        stdout, stderr = _collect(self.process, 2.0)
        _write_logs(self.artifacts_dir, stdout, stderr)
        return self.process.returncode, stdout, stderr

    def release_and_collect(self) -> tuple[int | None, str, str]:
        self.connection.sendall(b"x")
        self.connection.close()
        self.listener.close()
        stdout, stderr = _collect(self.process, 2.0)
        _write_logs(self.artifacts_dir, stdout, stderr)
        return self.process.returncode, stdout, stderr


def _receive_line(connection: socket.socket, timeout: float) -> str:
    connection.settimeout(timeout)
    payload = b""
    while not payload.endswith(b"\n"):
        try:
            chunk = connection.recv(256)
        except TimeoutError:
            raise ProcessFailure("child synchronization receive timed out") from None
        if not chunk:
            raise ProcessFailure("child closed synchronization channel")
        payload += chunk
    return payload.decode("utf-8", errors="replace").strip()


def wait_for_notification(
    args: Sequence[str],
    *,
    expected: str,
    timeout: float,
    artifacts_dir: Path,
) -> SynchronizedProcess:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(0.1)
    address = f"127.0.0.1:{listener.getsockname()[1]}"
    command = [part.replace("{control_address}", address) for part in args]
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=os.name == "posix",
    )
    deadline = time.monotonic() + timeout
    connection: socket.socket | None = None
    try:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                stdout, stderr = _collect(process, 1.0)
                _write_logs(artifacts_dir, stdout, stderr)
                raise ProcessFailure(
                    "child exited before synchronization",
                    stdout=stdout,
                    stderr=stderr,
                )
            try:
                connection, _ = listener.accept()
                break
            except socket.timeout:
                continue
        if connection is None:
            terminate_process(process)
            stdout, stderr = _collect(process, 1.0)
            _write_logs(artifacts_dir, stdout, stderr)
            raise ProcessFailure(
                "child synchronization timed out", stdout=stdout, stderr=stderr
            )
        notification = _receive_line(connection, max(0.1, deadline - time.monotonic()))
        if notification != expected:
            terminate_process(process)
            stdout, stderr = _collect(process, 1.0)
            _write_logs(artifacts_dir, stdout, stderr)
            raise ProcessFailure(
                f"unexpected child notification: {notification}",
                stdout=stdout,
                stderr=stderr,
            )
        return SynchronizedProcess(
            process, notification, connection, listener, artifacts_dir
        )
    except Exception:
        if process.poll() is None:
            terminate_process(process)
        if connection is not None:
            connection.close()
        listener.close()
        raise


def run_process(
    args: Sequence[str], *, timeout: float, input_text: str | None = None
) -> subprocess.CompletedProcess[str]:
    process = subprocess.Popen(
        args,
        stdin=subprocess.PIPE if input_text is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=os.name == "posix",
    )
    try:
        stdout, stderr = process.communicate(input=input_text, timeout=timeout)
        return subprocess.CompletedProcess(
            args=args,
            returncode=process.returncode,
            stdout=stdout,
            stderr=stderr,
        )
    except subprocess.TimeoutExpired:
        terminate_process(process)
        _collect(process, 2.0)
        raise TimeoutError(f"subprocess exceeded {timeout:.1f}s timeout") from None


def terminate_process(
    process: subprocess.Popen[str], timeout: float = 2.0
) -> int | None:
    if process.poll() is None:
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        else:
            process.kill()
    try:
        return process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        return process.wait(timeout=timeout)
