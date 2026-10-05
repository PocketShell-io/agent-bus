"""Independent headless worker consumes a real bus task and records outcome."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from coordination.bus import FileBus

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "coordination" / "headless_worker.py"


def _clean_env() -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("APLEXER_")}
    env["PYTHONPATH"] = str(ROOT)
    return env


def test_headless_worker_reviews_source_and_records_outcome(tmp_path: Path):
    bus = FileBus(tmp_path / "store")
    parent, pt = bus.register(
        agent_name="controller",
        device_id="hetzner-rmthz",
        project_id="agent-bus",
        task_id="dispatch",
    )
    worker_ident, wt = bus.register(
        agent_name="review-worker",
        device_id="hetzner-rmthz",
        project_id="agent-bus",
        task_id="review-file",
        parent_id=parent.identity_id,
        parent_token=pt,
    )
    cred = tmp_path / "worker.json"
    payload = worker_ident.public()
    payload["token"] = wt
    cred.write_text(json.dumps(payload), encoding="utf-8")
    os.chmod(cred, 0o600)
    artifact = tmp_path / "review.json"
    proc = subprocess.Popen(
        [
            sys.executable,
            str(WORKER),
            "--store",
            str(tmp_path / "store"),
            "--cred",
            str(cred),
            "--allow-root",
            str(tmp_path),
            "--timeout",
            "15",
        ],
        cwd=ROOT,
        env=_clean_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    msg = bus.send(
        sender_id=parent.identity_id,
        token=pt,
        recipient_id=worker_ident.identity_id,
        body="review coordination/bus.py",
        data={
            "task": "review_file",
            "source": str(ROOT / "coordination" / "bus.py"),
            "artifact": str(artifact),
        },
        idempotency_key="headless-review-1",
    )
    stdout, stderr = proc.communicate(timeout=20)
    assert proc.returncode == 0, stderr
    assert artifact.is_file()
    report = json.loads(artifact.read_text(encoding="utf-8"))
    assert report["has_filebus"] is True
    assert report["has_project_scope"] is True
    assert report["has_delivered_at"] is True
    seen = bus.get(parent.identity_id, pt, msg.message_id)
    assert seen.delivered_at
    assert seen.acked_at
    assert seen.accepted_at
    assert seen.outcome["status"] == "ok"
    assert seen.outcome["digest"]
    assert seen.outcome["artifact"] == str(artifact)
    replies = bus.inbox(parent.identity_id, pt)
    assert replies[0].reply_to == msg.message_id
    assert replies[0].data["digest"] == seen.outcome["digest"]
    worker_out = json.loads(stdout)
    assert worker_out["message_id"] == msg.message_id
    assert "APLEXER" not in stdout
    assert not any(k.startswith("APLEXER_") for k in _clean_env())
