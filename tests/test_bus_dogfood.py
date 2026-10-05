"""Pytest fallback: two headless CLI processes plus an independent worker.

The worker subprocess consumes the bus message and writes the artifact.
The controller does not write the artifact. No aplexer executable is invoked.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "coordination" / "bus_cli.py"
WORKER = ROOT / "coordination" / "headless_worker.py"


def _clean_env() -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("APLEXER_")}
    env["PYTHONPATH"] = str(ROOT)
    return env


def _run(store: Path, args: list[str]) -> subprocess.CompletedProcess:
    cmd = [sys.executable, str(CLI), "--store", str(store), *args]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=True, cwd=ROOT, env=_clean_env())


def test_two_headless_processes_and_restart(tmp_path: Path):
    store = tmp_path / "bus"
    a_cred = tmp_path / "a.json"
    b_cred = tmp_path / "b.json"
    artifact = tmp_path / "artifact.txt"
    _run(store, ["register", "--agent", "proc-a", "--device", "hetzner-rmthz", "--task", "dogfood", "--cred", str(a_cred)])
    _run(
        store,
        [
            "register",
            "--agent",
            "proc-b",
            "--device",
            "hetzner-rmthz",
            "--task",
            "dogfood",
            "--cred",
            str(b_cred),
            "--parent-cred",
            str(a_cred),
        ],
    )
    assert stat.S_IMODE(a_cred.stat().st_mode) == 0o600
    assert stat.S_IMODE(b_cred.stat().st_mode) == 0o600
    a_id = json.loads(a_cred.read_text())["identity_id"]
    b_id = json.loads(b_cred.read_text())["identity_id"]
    worker = subprocess.Popen(
        [
            sys.executable,
            str(WORKER),
            "--store",
            str(store),
            "--cred",
            str(b_cred),
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
    sent = json.loads(
        _run(
            store,
            [
                "send",
                "--cred",
                str(a_cred),
                "--to",
                b_id,
                "--body",
                "write artifact",
                "--data",
                json.dumps({"task": "write_artifact", "artifact": str(artifact), "contents": "done-by-proc-b\n"}),
                "--idempotency-key",
                "dogfood-1",
            ],
        ).stdout
    )
    stdout, stderr = worker.communicate(timeout=20)
    assert worker.returncode == 0, stderr
    assert artifact.read_text() == "done-by-proc-b\n"
    shown = json.loads(_run(store, ["show", "--cred", str(a_cred), "--message-id", sent["message_id"]]).stdout)
    assert shown["delivered_at"]
    assert shown["acked_at"]
    assert shown["accepted_at"]
    assert shown["outcome"]["status"] == "ok"
    assert shown["outcome"]["digest"]
    a_inbox = json.loads(_run(store, ["inbox", "--cred", str(a_cred)]).stdout)
    assert a_inbox[0]["reply_to"] == sent["message_id"]
    assert a_inbox[0]["kind"] == "reply"
    restarted = json.loads(_run(store, ["inbox", "--cred", str(a_cred)]).stdout)
    assert restarted[0]["message_id"] == a_inbox[0]["message_id"]
    _run(store, ["ack", "--cred", str(a_cred), "--message-id", a_inbox[0]["message_id"]])
    assert json.loads(_run(store, ["inbox", "--cred", str(a_cred)]).stdout) == []
    assert a_id != b_id
    assert "APLEXER" not in stdout
