"""Two headless processes exchange a task+artifact over the bus.

No aplexer executable is invoked. One process restarts and still sees
the unacked message (redelivery).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "coordination" / "bus_cli.py"


def _run(store: Path, args: list[str], cred: Path | None = None) -> subprocess.CompletedProcess:
    cmd = [sys.executable, str(CLI), "--store", str(store), *args]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=True, cwd=ROOT)


def test_two_headless_processes_and_restart(tmp_path: Path):
    store = tmp_path / "bus"
    a_cred = tmp_path / "a.json"
    b_cred = tmp_path / "b.json"
    artifact = tmp_path / "artifact.txt"
    _run(store, ["register", "--agent", "proc-a", "--device", "hetzner-rmthz", "--task", "dogfood", "--cred", str(a_cred)])
    _run(store, ["register", "--agent", "proc-b", "--device", "hetzner-rmthz", "--task", "dogfood", "--cred", str(b_cred)])
    a_id = json.loads(a_cred.read_text())["identity_id"]
    b_id = json.loads(b_cred.read_text())["identity_id"]
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
                f"write {artifact}",
                "--idempotency-key",
                "dogfood-1",
            ],
        ).stdout
    )
    # Simulate process B crash before ACK: new process reads inbox.
    inbox = json.loads(_run(store, ["inbox", "--cred", str(b_cred)]).stdout)
    assert inbox[0]["message_id"] == sent["message_id"]
    artifact.write_text("done-by-proc-b\n", encoding="utf-8")
    _run(store, ["ack", "--cred", str(b_cred), "--message-id", sent["message_id"]])
    reply = json.loads(
        _run(
            store,
            [
                "reply",
                "--cred",
                str(b_cred),
                "--message-id",
                sent["message_id"],
                "--body",
                "artifact-written",
                "--idempotency-key",
                "dogfood-1-reply",
            ],
        ).stdout
    )
    a_inbox = json.loads(_run(store, ["inbox", "--cred", str(a_cred)]).stdout)
    assert a_inbox[0]["message_id"] == reply["message_id"]
    assert a_inbox[0]["reply_to"] == sent["message_id"]
    assert artifact.read_text() == "done-by-proc-b\n"
    # Restart A still sees unacked reply.
    restarted = json.loads(_run(store, ["inbox", "--cred", str(a_cred)]).stdout)
    assert restarted[0]["message_id"] == reply["message_id"]
    _run(store, ["ack", "--cred", str(a_cred), "--message-id", reply["message_id"]])
    assert json.loads(_run(store, ["inbox", "--cred", str(a_cred)]).stdout) == []
