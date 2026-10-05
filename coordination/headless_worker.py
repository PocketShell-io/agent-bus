#!/usr/bin/env python3
"""Independent headless bus worker.

Consumes one bus message, writes a real artifact, then records ACK / accept /
outcome (with digest) and a reply on the bus. No aplexer executable, PID, or
APLEXER_* environment is used.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from coordination.bus import FileBus


def _contained(path: Path, root: Path) -> Path:
    resolved = path.resolve()
    root_r = root.resolve()
    if resolved != root_r and not str(resolved).startswith(str(root_r) + os.sep):
        raise SystemExit(f"path_outside_allow_root:{path}")
    return resolved


def _cred(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _review_file(source: Path) -> dict:
    text = source.read_text(encoding="utf-8")
    return {
        "source": str(source),
        "lines": text.count("\n") + (0 if text.endswith("\n") or not text else 1),
        "bytes": len(text.encode("utf-8")),
        "has_filebus": "class FileBus" in text,
        "has_project_scope": "project_scope" in text,
        "has_delivered_at": "delivered_at" in text,
        "has_accepted_at": "accepted_at" in text,
        "has_write_all": "write_all" in text or "def write_all" in text,
        "worker": "headless_worker",
    }


def run_once(bus: FileBus, cred: dict, allow_root: Path, timeout: float) -> int:
    ident = cred["identity_id"]
    token = cred["token"]
    items = bus.wait(ident, token, timeout=timeout)
    if not items:
        return 2
    msg = items[0]
    # ACK last so a crash before outcome still redelivers the unacked message.
    bus.accept(ident, token, msg.message_id)
    data = msg.data or {}
    task = data.get("task") or "write_artifact"
    artifact = _contained(Path(data["artifact"]), allow_root)
    artifact.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    extra: dict = {"task": task, "worker_id": ident}
    try:
        if task == "write_artifact":
            contents = data.get("contents", msg.body)
            if isinstance(contents, (dict, list)):
                payload = json.dumps(contents, indent=2) + "\n"
            else:
                payload = str(contents)
                if not payload.endswith("\n"):
                    payload += "\n"
            artifact.write_text(payload, encoding="utf-8")
            extra["kind"] = "write_artifact"
        elif task == "review_file":
            source = Path(data["source"]).resolve()
            report = _review_file(source)
            report["artifact"] = str(artifact)
            artifact.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
            extra["kind"] = "review_file"
            extra["source"] = str(source)
        else:
            raise ValueError(f"unknown_task:{task}")
        status = "ok"
        body = "outcome-ok"
    except Exception as exc:  # noqa: BLE001 — worker must record failure on the bus
        status = "error"
        body = f"outcome-error:{exc}"
        extra["error"] = str(exc)
        if not artifact.exists():
            artifact.write_text(json.dumps({"status": "error", "error": str(exc)}, indent=2) + "\n", encoding="utf-8")
    completed = bus.complete(
        ident,
        token,
        msg.message_id,
        status=status,
        artifact=str(artifact),
        extra=extra,
    )
    bus.reply(
        sender_id=ident,
        token=token,
        message_id=msg.message_id,
        body=body,
        data=completed.outcome,
        idempotency_key=f"{msg.idempotency_key}:outcome",
    )
    bus.ack(ident, token, msg.message_id)
    print(json.dumps({"message_id": msg.message_id, "outcome": completed.outcome}, indent=2))
    return 0 if status == "ok" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Headless Agent Bus worker")
    parser.add_argument("--store", required=True)
    parser.add_argument("--cred", required=True)
    parser.add_argument("--allow-root", required=True)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args(argv)
    cred = _cred(args.cred)
    return run_once(FileBus(args.store), cred, Path(args.allow_root), args.timeout)


if __name__ == "__main__":
    raise SystemExit(main())
