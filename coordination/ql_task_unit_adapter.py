"""Maintained Quota Launcher Task-Unit Adapter for Agent Bus.

Bridges Agent Bus consumer tasks with the maintained Quota Launcher route:
- Replaces raw direct provider loops with transient task-units in app.slice (MemoryMax=768M, TasksMax=100).
- Config dir: /home/alexey/git/agent-quota-launcher/.local/scale50/wt-gemini-head/.config/ql
- Refill worktree: /home/alexey/git/agent-quota-launcher/.local/scale50/wt-refill-runtime
- Submits via: python3 -m launcher submit --id <id> --key <key> --payload '<json>' --paths '<paths>'
- Maps QL terminal receipt (exit_code, unit, memory_peak_mb, cpu_seconds, artifacts) to
  Agent Bus 4-state lifecycle (accepted_at -> outcome with digest -> reply on bus).
- Respects the 50-GiB root filesystem floor: checks statvfs before submitting tasks.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
QL_CONFIG_DIR = Path("/home/alexey/git/agent-quota-launcher/.local/scale50/wt-gemini-head/.config/ql")
QL_WORKTREE = Path("/home/alexey/git/agent-quota-launcher/.local/scale50/wt-refill-runtime")
MIN_ROOT_FREE_GIB = 50.0


class StorageFloorBreach(RuntimeError):
    """Raised when host root filesystem is below the 50 GiB admission floor."""


def check_storage_floor(path: str = "/") -> float:
    total, used, free = shutil.disk_usage(path)
    free_gib = free / (1024 ** 3)
    if free_gib < MIN_ROOT_FREE_GIB:
        raise StorageFloorBreach(
            f"Host root filesystem free space ({free_gib:.2f} GiB) is below required {MIN_ROOT_FREE_GIB:.1f} GiB floor. "
            "Worker task-unit submission halted."
        )
    return free_gib


def format_ql_payload(
    goal: str,
    owner: str,
    cwd: str,
    timeout: int = 600,
    memory_mb: int = 768,
    provider: str = "antigravity",
    task_args: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "goal": goal,
        "owner": owner,
        "cwd": cwd,
        "timeout": timeout,
        "memory_mb": memory_mb,
        "provider": provider,
        "task_args": task_args or [],
    }


def submit_task_unit(
    task_id: str,
    idempotency_key: str,
    payload: dict[str, Any],
    paths: list[str],
    config_dir: Path = QL_CONFIG_DIR,
    worktree: Path = QL_WORKTREE,
) -> dict[str, Any]:
    """Submits a task unit to the maintained quota launcher with path leases."""
    check_storage_floor()

    cmd = [
        sys.executable,
        "-m",
        "launcher",
        "--config-dir",
        str(config_dir),
        "submit",
        "--id",
        task_id,
        "--key",
        idempotency_key,
        "--payload",
        json.dumps(payload),
        "--paths",
        ",".join(paths),
    ]

    proc = subprocess.run(
        cmd,
        cwd=worktree,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    return json.loads(proc.stdout)


def map_terminal_receipt_to_bus_outcome(
    terminal_receipt: dict[str, Any],
    artifact_path: str,
) -> dict[str, Any]:
    """Converts a QL terminal receipt into an Agent Bus outcome dictionary."""
    exit_code = terminal_receipt.get("exit_code", 1)
    status = "ok" if exit_code == 0 else "error"

    outcome: dict[str, Any] = {
        "status": status,
        "artifact": artifact_path,
        "ql_unit": terminal_receipt.get("unit"),
        "ql_invocation_id": terminal_receipt.get("invocation_id"),
        "cpu_seconds": terminal_receipt.get("cpu_seconds"),
        "memory_peak_mb": terminal_receipt.get("memory_peak_mb"),
    }
    return outcome
