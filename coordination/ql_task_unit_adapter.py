"""Maintained Quota Launcher Task-Unit Adapter for Agent Bus.

Bridges Agent Bus consumer tasks with the maintained Quota Launcher route:
- Replaces raw direct provider loops with transient task-units in app.slice (MemoryMax=768M, TasksMax=100).
- Config dir: /home/alexey/git/agent-quota-launcher/.local/scale50/wt-gemini-head/.config/ql
- Refill worktree: /home/alexey/git/agent-quota-launcher/.local/scale50/wt-refill-runtime
- Submits via: python3 -m launcher submit --id <id> --key <key> --payload '<json>' --paths '<paths>'
- Maps QL terminal receipt (exit_code, unit, memory_peak_mb, cpu_seconds, artifacts) to
  Agent Bus 4-state lifecycle (accepted_at -> outcome with digest -> reply on bus).
- Respects the 50-GiB root filesystem floor: checks statvfs before submitting tasks.
- Preserves cursor/idempotency/lease recovery across process crashes and restarts.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from coordination.bus import BusMessage, FileBus
from coordination.cursors import payload_digest

QL_CONFIG_DIR = Path("/home/alexey/git/agent-quota-launcher/.local/scale50/wt-gemini-head/.config/ql")
QL_WORKTREE = Path("/home/alexey/git/agent-quota-launcher/.local/scale50/wt-refill-runtime")
MIN_ROOT_FREE_GIB = 50.0

FILEBUS_LIVE_IDENTITY_REQUIRED = (
    "task_id",
    "bus_identity",
    "task_message_id",
    "provider",
)
FILEBUS_TERMINAL_RECEIPT_REQUIRED = (
    "ack_id",
    "reply_id",
    "outcome",
)
FILEBUS_IDENTITY_REQUIRED = FILEBUS_LIVE_IDENTITY_REQUIRED + FILEBUS_TERMINAL_RECEIPT_REQUIRED
NATIVE_WHOAMI_MARKERS = ("worker_cgroup", "workload_cgroup", "socket_path")

HEAD_CRED_GRANT_RE = re.compile(
    r"--cred(?:\s+|=)\S*filebus/head\.cred\b",
    re.IGNORECASE,
)
HEAD_CRED_FIELD_KEYS = ("cred", "cred_path", "credential", "filebus_cred")
HEAD_BUS_IDENTITY_ID = "ad6251d7-49f8-4f20-b8a1-d5af62412c5c"
HEAD_BUS_AGENT = "quota-launcher-head"


class StorageFloorBreach(RuntimeError):
    """Raised when host root filesystem is below the 50 GiB admission floor."""


class HeadCredentialLeakError(RuntimeError):
    """Raised when worker payload attempts to inherit the head FileBus credential."""


def file_digest(path: Path) -> str:
    """Calculates SHA-256 hex digest of file contents."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check_storage_floor(path: str = "/") -> float:
    """Checks host root filesystem free space against the mandatory 50-GiB floor."""
    total, used, free = shutil.disk_usage(path)
    free_gib = free / (1024 ** 3)
    if free_gib < MIN_ROOT_FREE_GIB:
        raise StorageFloorBreach(
            f"Host root filesystem free space ({free_gib:.2f} GiB) is below required {MIN_ROOT_FREE_GIB:.1f} GiB floor. "
            "Worker task-unit submission halted."
        )
    return free_gib


def _walk_strings(obj: Any):
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for key, val in obj.items():
            yield str(key)
            yield from _walk_strings(val)
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            yield from _walk_strings(item)


def payload_grants_head_cred(payload: Any) -> bool:
    """True when a worker payload would inherit the head FileBus credential."""
    if not isinstance(payload, dict):
        return False
    worker_agent = payload.get("bus_identity") or payload.get("agent_name")
    if worker_agent == HEAD_BUS_AGENT:
        return True
    worker_id = payload.get("identity_id") or payload.get("bus_identity_id")
    if worker_id == HEAD_BUS_IDENTITY_ID:
        return True
    for key in HEAD_CRED_FIELD_KEYS:
        val = payload.get(key)
        if isinstance(val, str) and "filebus/head.cred" in val.replace("\\", "/"):
            return True
    for text in _walk_strings(payload):
        if HEAD_CRED_GRANT_RE.search(text.replace("\\", "/")):
            return True
    return False


def reject_head_cred_inheritance(payload: Any) -> None:
    """Fails closed if payload attempts to pass head.cred to a worker."""
    if payload_grants_head_cred(payload):
        raise HeadCredentialLeakError(
            "head.cred must stay private from workers; payload grants HEAD mailbox"
        )


def _nonempty_str(record: dict[str, Any], key: str) -> bool:
    val = record.get(key)
    return isinstance(val, str) and bool(val.strip())


def is_native_whoami(record: dict[str, Any]) -> bool:
    if not isinstance(record, dict):
        return False
    return any(k in record for k in NATIVE_WHOAMI_MARKERS)


def validate_filebus_live_identity(record: dict[str, Any]) -> bool:
    """Start/enrollment identity. Must not require future ack/reply/outcome."""
    if not isinstance(record, dict):
        return False
    if is_native_whoami(record):
        return False
    if any(_nonempty_str(record, k) for k in FILEBUS_TERMINAL_RECEIPT_REQUIRED):
        return False
    return all(_nonempty_str(record, k) for k in FILEBUS_LIVE_IDENTITY_REQUIRED)


def validate_filebus_terminal_receipt(record: dict[str, Any]) -> bool:
    """Terminal receipt, distinct from live identity. Owned bus events only."""
    if not isinstance(record, dict):
        return False
    if is_native_whoami(record):
        return False
    if not all(_nonempty_str(record, k) for k in FILEBUS_IDENTITY_REQUIRED):
        return False
    if record.get("outcome") not in ("accepted", "completed", "failed", "blocked"):
        return False
    return True


def validate_filebus_identity(record: dict[str, Any]) -> bool:
    """Full correlated record (live + terminal). Not for enrollment-at-start."""
    return validate_filebus_terminal_receipt(record)


def format_ql_payload(
    goal: str,
    owner: str,
    cwd: str,
    timeout: int = 600,
    memory_mb: int = 768,
    provider: str = "antigravity",
    task_id: str | None = None,
    task_message_id: str | None = None,
    task_args: list[str] | None = None,
) -> dict[str, Any]:
    payload = {
        "backend": "filebus",
        "goal": goal,
        "owner": owner,
        "cwd": cwd,
        "timeout": timeout,
        "memory_mb": memory_mb,
        "provider": provider,
        "task_id": task_id,
        "task_message_id": task_message_id,
        "task_args": task_args or [],
    }
    reject_head_cred_inheritance(payload)
    return payload


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
    reject_head_cred_inheritance(payload)

    # Submit first
    cmd_submit = [
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

    proc_submit = subprocess.run(
        cmd_submit,
        cwd=worktree,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    
    # Run synchronously
    cmd_run = [
        sys.executable,
        "-m",
        "launcher",
        "--config-dir",
        str(config_dir),
        "run",
        "--id",
        task_id,
        "--cwd",
        payload.get("cwd", str(worktree)),
        "--tmpdir",
        payload.get("cwd", str(worktree)),
        "--backend",
        "task-units",
        "--as-controller",
    ]

    proc_run = subprocess.run(
        cmd_run,
        cwd=worktree,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    
    try:
        # The output might have multiple lines, the JSON receipt is usually on a line by itself
        for line in proc_run.stdout.splitlines():
            if line.startswith("{"):
                res = json.loads(line)
                if "receipt" in res:
                    return res["receipt"]
                return res
        return {"exit_code": proc_run.returncode, "state": "failed", "error": "No JSON receipt found", "stdout": proc_run.stdout, "stderr": proc_run.stderr}
    except Exception as e:
        return {"exit_code": 1, "state": "failed", "error": str(e), "stdout": proc_run.stdout, "stderr": proc_run.stderr}


def map_terminal_receipt_to_bus_outcome(
    terminal_receipt: dict[str, Any],
    artifact_path: str,
) -> dict[str, Any]:
    """Converts a QL terminal receipt into an Agent Bus outcome dictionary."""
    exit_code = terminal_receipt.get("exit_code", 0 if terminal_receipt.get("state") == "completed" else 1)
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


class QLSessionlessConsumer:
    """Sessionless Agent Bus consumer that drives tasks via Quota Launcher.

    - Consumes messages from FileBus without APLEXER_* session inheritance.
    - Transitions messages through 4 states: delivered -> accepted_at -> outcome -> acked.
    - Checks 50-GiB host root filesystem admission floor.
    - Enforces idempotent execution using task idempotency keys.
    - Preserves receive cursors and performs crash/lease recovery on unacked messages.
    """

    def __init__(
        self,
        bus: FileBus,
        identity_id: str,
        token: str,
        allow_root: Path,
        config_dir: Path = QL_CONFIG_DIR,
        worktree: Path = QL_WORKTREE,
    ):
        self.bus = bus
        self.identity_id = identity_id
        self.token = token
        self.allow_root = Path(allow_root).resolve()
        self.config_dir = Path(config_dir)
        self.worktree = Path(worktree)

    def _contained_path(self, target: Path) -> Path:
        resolved = target.resolve()
        if resolved != self.allow_root and not str(resolved).startswith(str(self.allow_root) + os.sep):
            raise ValueError(f"path_outside_allowed_root: {target}")
        return resolved

    def recover_leases(self) -> list[dict[str, Any]]:
        """Recovers unacknowledged messages where accepted_at is set but acked_at is None."""
        recovered: list[dict[str, Any]] = []
        unacked = self.bus.inbox(self.identity_id, self.token, unread_only=True)
        for msg in unacked:
            if not msg.accepted_at:
                continue
            # Case 1: Outcome already exists, but crashed before reply/ack
            if msg.outcome:
                reply_key = f"{msg.idempotency_key}:reply"
                reply_msg = self.bus.reply(
                    sender_id=self.identity_id,
                    token=self.token,
                    message_id=msg.message_id,
                    body=f"outcome-{msg.outcome.get('status', 'ok')}",
                    data=msg.outcome,
                    idempotency_key=reply_key,
                )
                self.bus.ack(self.identity_id, self.token, msg.message_id)
                self.bus._cursors.advance(self.identity_id, msg.message_id)
                recovered.append({
                    "message_id": msg.message_id,
                    "action": "recovered_ack",
                    "reply_id": reply_msg.message_id,
                })
            else:
                # Case 2: Accepted but no outcome recorded; re-process if storage permits
                try:
                    check_storage_floor()
                    res = self.process_message(msg)
                    recovered.append({
                        "message_id": msg.message_id,
                        "action": "reprocessed",
                        "result": res,
                    })
                except StorageFloorBreach:
                    recovered.append({
                        "message_id": msg.message_id,
                        "action": "held_storage_floor",
                    })
        return recovered

    def process_message(self, msg: BusMessage) -> dict[str, Any]:
        """Processes one BusMessage through QL with full lifecycle verification."""
        # Fetch fresh message state from bus store
        current_msg = self.bus.get(self.identity_id, self.token, msg.message_id)

        # 1. Accept message (transitions to accepted_at)
        if not current_msg.accepted_at:
            current_msg = self.bus.accept(self.identity_id, self.token, msg.message_id)

        # 2. Check storage floor: must fail closed before launching worker
        check_storage_floor()

        # 3. Idempotency check: if outcome already exists, replay cleanly
        if current_msg.outcome:
            reply_key = f"{current_msg.idempotency_key}:reply"
            reply_msg = self.bus.reply(
                sender_id=self.identity_id,
                token=self.token,
                message_id=current_msg.message_id,
                body=f"outcome-{current_msg.outcome.get('status', 'ok')}",
                data=current_msg.outcome,
                idempotency_key=reply_key,
            )
            if type(current_msg.acked_at) is not str:
                self.bus.ack(self.identity_id, self.token, current_msg.message_id)
            self.bus._cursors.advance(self.identity_id, current_msg.message_id)
            return {
                "message_id": current_msg.message_id,
                "status": current_msg.outcome.get("status", "ok"),
                "reply_id": reply_msg.message_id,
                "artifact": current_msg.outcome.get("artifact"),
                "digest": current_msg.outcome.get("digest"),
                "idempotent_replay": True,
            }

        # 4. Extract data and build QL payload
        data = current_msg.data or {}
        goal = data.get("goal") or data.get("prompt") or current_msg.body
        artifact_rel = data.get("artifact", f"task_{current_msg.message_id[:8]}.txt")
        artifact_path = self._contained_path(self.allow_root / artifact_rel)
        artifact_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)

        paths = data.get("paths", [])
        cwd = data.get("cwd", str(ROOT))
        timeout = int(data.get("timeout", 300))
        memory_mb = int(data.get("memory_mb", 768))
        provider = data.get("provider", "antigravity")
        task_id = f"ql-{current_msg.message_id[:12]}"

        payload = format_ql_payload(
            goal=goal,
            owner=self.identity_id,
            cwd=cwd,
            timeout=timeout,
            memory_mb=memory_mb,
            provider=provider,
            task_id=task_id,
            task_message_id=current_msg.message_id,
        )

        # 5. Live identity record validation
        live_identity = {
            "task_id": task_id,
            "bus_identity": self.identity_id,
            "task_message_id": current_msg.message_id,
            "provider": provider,
        }
        if not validate_filebus_live_identity(live_identity):
            raise RuntimeError(f"Invalid FileBus live identity: {live_identity}")

        # 6. Submit task unit to QL
        ql_key = f"ql:{current_msg.idempotency_key}"
        terminal_receipt = submit_task_unit(
            task_id=task_id,
            idempotency_key=ql_key,
            payload=payload,
            paths=paths,
            config_dir=self.config_dir,
            worktree=self.worktree,
        )

        # 7. Map terminal receipt to bus outcome
        outcome = map_terminal_receipt_to_bus_outcome(terminal_receipt, str(artifact_path))
        digest = ""
        if artifact_path.is_file():
            digest = file_digest(artifact_path)
            outcome["digest"] = digest

        # 8. Complete message on bus
        completed = self.bus.complete(
            self.identity_id,
            self.token,
            current_msg.message_id,
            status=outcome["status"],
            artifact=outcome.get("artifact"),
            digest=digest or None,
            extra=outcome,
        )

        # 9. Send reply on bus
        reply_msg = self.bus.reply(
            sender_id=self.identity_id,
            token=self.token,
            message_id=current_msg.message_id,
            body=f"outcome-{outcome['status']}",
            data=completed.outcome,
            idempotency_key=f"{current_msg.idempotency_key}:reply",
        )

        # 10. Record terminal receipt validation
        terminal_record = {
            "task_id": task_id,
            "bus_identity": self.identity_id,
            "task_message_id": current_msg.message_id,
            "provider": provider,
            "ack_id": current_msg.message_id,
            "reply_id": reply_msg.message_id,
            "outcome": "completed" if outcome["status"] == "ok" else "failed",
        }
        if not validate_filebus_terminal_receipt(terminal_record):
            raise RuntimeError(f"Invalid FileBus terminal receipt: {terminal_record}")

        # 11. Acknowledge message
        self.bus.ack(self.identity_id, self.token, current_msg.message_id)

        # 12. Advance cursor
        self.bus._cursors.advance(self.identity_id, current_msg.message_id)

        return {
            "message_id": current_msg.message_id,
            "status": outcome["status"],
            "reply_id": reply_msg.message_id,
            "artifact": str(artifact_path),
            "digest": digest,
            "terminal_record": terminal_record,
        }

    def run_once(self, timeout: float = 0.0) -> dict[str, Any] | None:
        """Runs one consumption cycle: recovers leases, then processes next message."""
        self.recover_leases()
        if timeout > 0:
            items = self.bus.wait(self.identity_id, self.token, timeout=timeout)
        else:
            items = self.bus.inbox(self.identity_id, self.token, unread_only=True)

        for msg in items:
            if msg.accepted_at:
                continue
            return self.process_message(msg)
        return None
