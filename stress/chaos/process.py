from __future__ import annotations

import os
import signal
import subprocess
from collections.abc import Sequence


def run_process(
    args: Sequence[str], *, timeout: float, input_text: str | None = None
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            args,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        raise TimeoutError(f"subprocess exceeded {timeout:.1f}s timeout") from None


def terminate_process(
    process: subprocess.Popen[str], timeout: float = 2.0
) -> int | None:
    if process.poll() is None:
        if os.name == "posix":
            process.send_signal(signal.SIGKILL)
        else:
            process.kill()
    try:
        return process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        return process.wait(timeout=timeout)
