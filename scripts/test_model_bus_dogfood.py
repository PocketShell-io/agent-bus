#!/usr/bin/env python3
"""Execute real model coding agent dogfood over Agent Bus for 14:15 UTC checkpoint.

Verifies:
1. Fresh private store under umask 077.
2. Distinct sender & receiver bus identities.
3. Clean environment without APLEXER_* environment variables.
4. Enqueues a real Python coding task: `validate_bus_envelope`.
5. Headless model consumer (coordination/model_bus_consumer.py) invokes real LLM
   (opencode-go/glm-5.3-flash), writes artifact `envelope_validator.py`.
6. Validates 4-state lifecycle (delivered_at, acked_at, accepted_at, outcome digest).
7. Verifies reply on bus matches outcome digest.
8. Writes durable evidence to .local/model-dogfood-1415-evidence.json.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from coordination.bus import FileBus


def clean_env() -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("APLEXER_")}
    env["PYTHONPATH"] = str(ROOT)
    return env


def main() -> int:
    old_umask = os.umask(0o077)
    try:
        tmp_dir = ROOT / ".local" / "tmp" / "model_dogfood_1415"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

        store_dir = tmp_dir / "store"
        store_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

        bus = FileBus(store_dir)

        # 1. Register controller
        parent, pt = bus.register(
            agent_name="bus-controller",
            device_id="hetzner-rmthz",
            project_id="agent-bus",
            task_id="model-dispatch-1415",
        )

        # 2. Register distinct worker identity
        worker_ident, wt = bus.register(
            agent_name="bus-model-coder",
            device_id="hetzner-rmthz",
            project_id="agent-bus",
            task_id="model-coding-task",
            parent_id=parent.identity_id,
            parent_token=pt,
        )

        # Credential with 0600 permissions
        cred_path = tmp_dir / "worker_cred.json"
        payload = worker_ident.public()
        payload["token"] = wt
        cred_path.write_text(json.dumps(payload), encoding="utf-8")
        os.chmod(cred_path, 0o600)

        artifact_path = tmp_dir / "envelope_validator.py"

        # 3. Spawn model consumer subprocess WITHOUT an aplexer session
        consumer_script = ROOT / "coordination" / "model_bus_consumer.py"
        consumer_proc = subprocess.Popen(
            [
                sys.executable,
                str(consumer_script),
                "--store",
                str(store_dir),
                "--cred",
                str(cred_path),
                "--allow-root",
                str(tmp_dir),
                "--timeout",
                "30",
                "--model",
                "opencode-go/glm-5.3-flash",
            ],
            cwd=ROOT,
            env=clean_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # 4. Enqueue real coding task on the bus
        prompt = (
            "Write a production-quality Python helper module with function "
            "`validate_bus_envelope(envelope: dict) -> tuple[bool, str]`. "
            "It must check that an Agent Bus envelope dictionary has all required fields: "
            "'message_id', 'sender_id', 'recipient_id', 'body', 'created_at', 'idempotency_key'. "
            "If any field is missing or empty, return (False, 'missing: <field>'). "
            "If valid, return (True, 'ok'). Include unit tests in a `if __name__ == '__main__':` block."
        )

        msg = bus.send(
            sender_id=parent.identity_id,
            token=pt,
            recipient_id=worker_ident.identity_id,
            body="Implement validate_bus_envelope helper",
            data={
                "task": "code_task",
                "prompt": prompt,
                "artifact": str(artifact_path),
            },
            idempotency_key="model-dogfood-1415-key-1",
        )

        stdout, stderr = consumer_proc.communicate(timeout=180)
        if consumer_proc.returncode != 0:
            print(f"Consumer failed (code {consumer_proc.returncode}):\nSTDOUT: {stdout}\nSTDERR: {stderr}", file=sys.stderr)
            return 1

        # 5. Verify artifact was created and is non-empty
        if not artifact_path.is_file():
            print(f"Artifact not found: {artifact_path}", file=sys.stderr)
            return 1

        code_text = artifact_path.read_text(encoding="utf-8")
        if len(code_text.strip()) < 50:
            print(f"Artifact too short ({len(code_text)} bytes):\n{code_text}", file=sys.stderr)
            return 1

        # 6. Verify bus message states
        seen = bus.get(parent.identity_id, pt, msg.message_id)
        assert seen.delivered_at is not None, "Missing delivered_at"
        assert seen.acked_at is not None, "Missing acked_at"
        assert seen.accepted_at is not None, "Missing accepted_at"
        assert seen.outcome["status"] == "ok", f"Outcome status not ok: {seen.outcome}"
        assert seen.outcome["digest"] is not None, "Missing outcome digest"
        assert seen.outcome["artifact"] == str(artifact_path), "Mismatched artifact path"

        # 7. Check reply in controller inbox
        replies = bus.inbox(parent.identity_id, pt)
        assert len(replies) == 1, f"Expected 1 reply, got {len(replies)}"
        reply = replies[0]
        assert reply.reply_to == msg.message_id
        assert reply.data["digest"] == seen.outcome["digest"]

        # 8. Record durable evidence
        consumer_out = json.loads(stdout)
        evidence = {
            "task_id": "REMOTE-AUTONOMY-BUS-1830",
            "checkpoint": "14:15UTC",
            "status": "PASS",
            "model": "opencode-go/glm-5.3-flash",
            "backend": "opencode-go",
            "store_umask": "077",
            "controller_identity": parent.identity_id,
            "worker_identity": worker_ident.identity_id,
            "distinct_identities": parent.identity_id != worker_ident.identity_id,
            "message_id": msg.message_id,
            "artifact_path": str(artifact_path),
            "artifact_size_bytes": len(code_text.encode("utf-8")),
            "artifact_lines": code_text.count("\n") + 1,
            "artifact_excerpt": code_text[:300],
            "four_states_verified": {
                "delivered_at": seen.delivered_at,
                "acked_at": seen.acked_at,
                "accepted_at": seen.accepted_at,
                "outcome_status": seen.outcome["status"],
                "outcome_digest": seen.outcome["digest"],
            },
            "reply_message_id": reply.message_id,
            "sessionless_proof": "No APLEXER_* environment variables passed to worker process; clean execution",
            "worker_exit_code": consumer_proc.returncode,
        }

        evidence_path = ROOT / ".local" / "model-dogfood-1415-evidence.json"
        evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"status": "SUCCESS", "evidence": str(evidence_path)}, indent=2))
        return 0

    finally:
        os.umask(old_umask)


if __name__ == "__main__":
    raise SystemExit(main())
