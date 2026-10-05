#!/usr/bin/env python3
"""Sessionless Headless Model Bus Consumer.

Consumes a task envelope from FileBus, executes a real coding task via an allowed
headless model backend (opencode-go/glm-5.3-flash), writes a real code/analysis artifact,
records 4-state lifecycle (delivered_at, acked_at, accepted_at, outcome with digest),
and posts the reply on the bus.

Zero APLEXER_* environment variables are used.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from coordination.bus import FileBus


def clean_env() -> dict[str, str]:
    """Sanitize environment to ensure zero APLEXER_* session inheritance."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("APLEXER_")}
    env["PYTHONPATH"] = str(ROOT)
    return env


def contained_path(target: Path, root: Path) -> Path:
    resolved = target.resolve()
    root_r = root.resolve()
    if resolved != root_r and not str(resolved).startswith(str(root_r) + os.sep):
        raise SystemExit(f"path_outside_allowed_root: {target}")
    return resolved


def load_credential(cred_path: Path) -> dict:
    return json.loads(cred_path.read_text(encoding="utf-8"))


def invoke_model(prompt: str, model: str = "opencode-go/glm-5.3-flash", timeout: int = 150) -> str:
    """Invokes real headless model via opencode run in pure non-interactive mode."""
    cmd = [
        "opencode",
        "run",
        "-m",
        model,
        "--pure",
        prompt,
    ]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=clean_env(),
        cwd=ROOT,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Model invocation failed (exit {proc.returncode}): {proc.stderr}")
    return proc.stdout.strip()


def run_consumer(
    store_dir: Path,
    cred_file: Path,
    allow_root: Path,
    timeout: float = 30.0,
    model: str = "opencode-go/glm-5.3-flash",
) -> int:
    cred = load_credential(cred_file)
    ident = cred["identity_id"]
    token = cred["token"]

    bus = FileBus(store_dir)
    items = bus.wait(ident, token, timeout=timeout)
    if not items:
        print(json.dumps({"status": "timeout", "message": "No pending bus messages"}, indent=2))
        return 2

    msg = items[0]
    # 1. Accept message (transitions to accepted_at)
    bus.accept(ident, token, msg.message_id)

    data = msg.data or {}
    task_type = data.get("task", "model_code_task")
    artifact_rel = data.get("artifact", "model_output.txt")
    artifact_path = contained_path(Path(artifact_rel), allow_root)
    artifact_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)

    prompt = data.get("prompt", msg.body)
    context_files = data.get("context_files", [])

    context_str = ""
    for cf in context_files:
        p = Path(cf).resolve()
        if p.is_file():
            context_str += f"\n--- Context File: {cf} ---\n{p.read_text(encoding='utf-8')[:2000]}\n"

    full_prompt = (
        f"{prompt}\n\n"
        f"{context_str}\n\n"
        "Provide your concrete analysis or implementation directly. Output clean code or structured text."
    )

    extra: dict = {
        "task": task_type,
        "worker_id": ident,
        "model": model,
        "backend": "opencode-go",
    }

    try:
        model_output = invoke_model(full_prompt, model=model)
        artifact_path.write_text(model_output + "\n", encoding="utf-8")
        status = "ok"
        body = f"outcome-ok:{model}"
        extra["artifact_bytes"] = len(model_output.encode("utf-8"))
        extra["artifact_lines"] = model_output.count("\n") + 1
    except Exception as exc:
        status = "error"
        body = f"outcome-error:{exc}"
        extra["error"] = str(exc)
        if not artifact_path.exists():
            artifact_path.write_text(json.dumps({"status": "error", "error": str(exc)}, indent=2), encoding="utf-8")

    # 2. Record completion with SHA-256 digest
    completed = bus.complete(
        ident,
        token,
        msg.message_id,
        status=status,
        artifact=str(artifact_path),
        extra=extra,
    )

    # 3. Send reply on the bus
    bus.reply(
        sender_id=ident,
        token=token,
        message_id=msg.message_id,
        body=body,
        data=completed.outcome,
        idempotency_key=f"{msg.idempotency_key}:outcome",
    )

    # 4. Final ack
    bus.ack(ident, token, msg.message_id)

    result = {
        "message_id": msg.message_id,
        "status": status,
        "artifact": str(artifact_path),
        "outcome": completed.outcome,
        "model": model,
        "backend": "opencode-go",
    }
    print(json.dumps(result, indent=2))
    return 0 if status == "ok" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sessionless Headless Model Bus Consumer")
    parser.add_argument("--store", required=True, help="Path to FileBus store directory")
    parser.add_argument("--cred", required=True, help="Path to worker credential JSON")
    parser.add_argument("--allow-root", required=True, help="Allowed directory for writing artifacts")
    parser.add_argument("--timeout", type=float, default=30.0, help="Wait timeout in seconds")
    parser.add_argument("--model", default="opencode-go/glm-5.3-flash", help="Model to invoke")
    args = parser.parse_args(argv)

    return run_consumer(
        store_dir=Path(args.store),
        cred_file=Path(args.cred),
        allow_root=Path(args.allow_root),
        timeout=args.timeout,
        model=args.model,
    )


if __name__ == "__main__":
    raise SystemExit(main())
