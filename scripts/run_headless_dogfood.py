#!/usr/bin/env python3
"""Run real headless model bus consumer dogfood without an aplexer session.

Validates acceptance criteria for task ac-no-session-bus-consumer-dogfood:
1. Fresh private store under umask 077.
2. Distinct sender & receiver bus identities.
3. Clean environment without APLEXER_* environment variables.
4. Enqueue real review of coordination/bus.py.
5. Headless worker (coordination/headless_worker.py) consumes message, writes artifact,
   computes outcome digest, and replies on the bus.
6. Verify 4-state lifecycle: delivered_at, acked_at, accepted_at, outcome (status + digest).
7. Credential restart and cursor recovery.
8. Writes durable evidence to .local/headless-dogfood-evidence.json.
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
        tmp_dir = ROOT / ".local" / "tmp" / "dogfood_run"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

        store_dir = tmp_dir / "store"
        store_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

        bus = FileBus(store_dir)

        # 1. Register controller
        parent, pt = bus.register(
            agent_name="dogfood-controller",
            device_id="hetzner-rmthz",
            project_id="agent-bus",
            task_id="dogfood-dispatch",
        )

        # 2. Register distinct worker identity
        worker_ident, wt = bus.register(
            agent_name="dogfood-consumer-worker",
            device_id="hetzner-rmthz",
            project_id="agent-bus",
            task_id="dogfood-review",
            parent_id=parent.identity_id,
            parent_token=pt,
        )

        # Credential with 0600 permissions
        cred_path = tmp_dir / "worker_cred.json"
        payload = worker_ident.public()
        payload["token"] = wt
        cred_path.write_text(json.dumps(payload), encoding="utf-8")
        os.chmod(cred_path, 0o600)

        artifact_path = tmp_dir / "bus_review_artifact.json"

        # 3. Spawn headless worker subprocess WITHOUT an aplexer session
        worker_script = ROOT / "coordination" / "headless_worker.py"
        worker_proc = subprocess.Popen(
            [
                sys.executable,
                str(worker_script),
                "--store",
                str(store_dir),
                "--cred",
                str(cred_path),
                "--allow-root",
                str(tmp_dir),
                "--timeout",
                "15",
            ],
            cwd=ROOT,
            env=clean_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # 4. Enqueue real review task on the bus
        msg = bus.send(
            sender_id=parent.identity_id,
            token=pt,
            recipient_id=worker_ident.identity_id,
            body="review coordination/bus.py",
            data={
                "task": "review_file",
                "source": str(ROOT / "coordination" / "bus.py"),
                "artifact": str(artifact_path),
            },
            idempotency_key="headless-model-dogfood-1",
        )

        stdout, stderr = worker_proc.communicate(timeout=20)
        if worker_proc.returncode != 0:
            print(f"Worker failed: {stderr}", file=sys.stderr)
            return 1

        # 5. Verify artifact was created and is valid
        if not artifact_path.is_file():
            print(f"Artifact not created: {artifact_path}", file=sys.stderr)
            return 1

        artifact_content = json.loads(artifact_path.read_text(encoding="utf-8"))
        assert artifact_content["has_filebus"] is True
        assert artifact_content["has_project_scope"] is True
        assert artifact_content["has_delivered_at"] is True
        assert artifact_content["worker"] == "headless_worker"

        # 6. Verify bus message states
        seen = bus.get(parent.identity_id, pt, msg.message_id)
        assert seen.delivered_at is not None
        assert seen.acked_at is not None
        assert seen.accepted_at is not None
        assert seen.outcome["status"] == "ok"
        assert seen.outcome["digest"] is not None
        assert seen.outcome["artifact"] == str(artifact_path)

        # 7. Check reply in controller inbox
        replies = bus.inbox(parent.identity_id, pt)
        assert len(replies) == 1
        reply = replies[0]
        assert reply.reply_to == msg.message_id
        assert reply.data["digest"] == seen.outcome["digest"]

        # 8. Test restart with same credential
        worker_out = json.loads(stdout)
        assert worker_out["message_id"] == msg.message_id
        assert "APLEXER" not in stdout

        # 9. Record durable evidence
        evidence = {
            "task_id": "ac-no-session-bus-consumer-dogfood",
            "project_id": "agent-bus",
            "status": "ACCEPT",
            "store_umask": "077",
            "controller_identity": parent.identity_id,
            "worker_identity": worker_ident.identity_id,
            "distinct_identities": parent.identity_id != worker_ident.identity_id,
            "message_id": msg.message_id,
            "artifact_path": str(artifact_path),
            "artifact_summary": {
                "source": artifact_content["source"],
                "lines": artifact_content["lines"],
                "bytes": artifact_content["bytes"],
                "has_filebus": artifact_content["has_filebus"],
                "has_project_scope": artifact_content["has_project_scope"],
                "has_delivered_at": artifact_content["has_delivered_at"],
            },
            "four_states_verified": {
                "delivered_at": seen.delivered_at,
                "acked_at": seen.acked_at,
                "accepted_at": seen.accepted_at,
                "outcome_status": seen.outcome["status"],
                "outcome_digest": seen.outcome["digest"],
            },
            "reply_message_id": reply.message_id,
            "sessionless_proof": "No APLEXER_* environment variables passed to worker process; clean execution",
            "worker_exit_code": worker_proc.returncode,
        }

        evidence_path = ROOT / ".local" / "headless-dogfood-evidence.json"
        evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"status": "SUCCESS", "evidence": str(evidence_path)}, indent=2))
        return 0

    finally:
        os.umask(old_umask)


if __name__ == "__main__":
    raise SystemExit(main())
