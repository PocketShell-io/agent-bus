#!/usr/bin/env python3
"""Setup script for trial t-bus-headless-first-use-c2932.

Creates dedicated bus store, registers head and worker, saves 0600 credentials,
and dispatches initial task envelope to worker inbox.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

BUS_ROOT = Path(__file__).resolve().parent.parent
if str(BUS_ROOT) not in sys.path:
    sys.path.insert(0, str(BUS_ROOT))

from coordination.bus import FileBus
from coordination.completion_callback import HeadCompletionCallbackEngine, HeadTaskBacklog, TaskDefinition


def main():
    bus_dir = Path("/home/alexey/git/agent-bus/.local/bus_headless_trial_c2932")
    ws_dir = bus_dir / "workspace"

    bus_dir.mkdir(parents=True, exist_ok=True)
    ws_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(bus_dir, 0o700)
    os.chmod(ws_dir, 0o700)

    bus = FileBus(bus_dir)

    head_ident, head_tok = bus.register(
        agent_name="head-trial-c2932",
        device_id="standalone-device",
        project_id="agent-bus",
    )
    worker_ident, worker_tok = bus.register(
        agent_name="worker-zcode-c2932",
        device_id="standalone-device",
        project_id="agent-bus",
    )

    head_cred_file = bus_dir / "head.cred.json"
    worker_cred_file = bus_dir / "worker.cred.json"

    head_cred_file.write_text(json.dumps({"identity": head_ident.public(), "token": head_tok}, indent=2), encoding="utf-8")
    worker_cred_file.write_text(json.dumps({"identity": worker_ident.public(), "token": worker_tok}, indent=2), encoding="utf-8")

    os.chmod(head_cred_file, 0o600)
    os.chmod(worker_cred_file, 0o600)

    task = TaskDefinition(
        task_id="t-bus-headless-first-use-c2932",
        task_type="agent_bus_cli_first_use",
        body="Inspect AgentBus CLI help, verify sessionless enrollment, generate command audit artifact on disk, compute SHA-256 digest, and send completion reply on FileBus",
        data={
            "task_id": "t-bus-headless-first-use-c2932",
            "required_output": "first_use_report.json",
            "workspace_dir": str(ws_dir),
        },
        priority=10,
    )
    backlog = HeadTaskBacklog([task])

    engine = HeadCompletionCallbackEngine(
        bus=bus,
        head_identity=head_ident,
        head_token=head_tok,
        backlog=backlog,
        workspace_root=ws_dir,
        workers=[(worker_ident, worker_tok)],
    )

    dispatched = engine.dispatch_initial_batch()
    engine.close()

    result = {
        "status": "SETUP_SUCCESS",
        "bus_dir": str(bus_dir),
        "workspace_dir": str(ws_dir),
        "worker_cred_path": str(worker_cred_file),
        "head_identity": head_ident.identity_id,
        "worker_identity": worker_ident.identity_id,
        "task_id": task.task_id,
        "dispatched_count": dispatched,
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
