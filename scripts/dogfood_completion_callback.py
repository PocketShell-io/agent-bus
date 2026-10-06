#!/usr/bin/env python3
"""Dogfood test script for Head Completion/Refill Callback Engine (Task scale50-21 / C2913).

Demonstrates:
1. Dispatch of a batch of 3 tasks to child workers via FileBus.
2. Worker execution producing real artifact files on disk with SHA-256 digests.
3. Inotify kernel event trigger detecting completion immediately without polling sleep loops (sleep_poll_count == 0).
4. Digest validation verifying file content and SHA-256 checksum on disk.
5. Durable ACK advancement recorded in cursors/receipts.
6. Automatic slot refill from backlog until all tasks are completed.
7. Negative test verification:
   - Tampered digest (bit flip) fails closed.
   - Path traversal fails closed.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

BUS_ROOT = Path(__file__).resolve().parent.parent
if str(BUS_ROOT) not in sys.path:
    sys.path.insert(0, str(BUS_ROOT))

from coordination.bus import FileBus
from coordination.completion_callback import (
    DigestValidator,
    HeadCompletionCallbackEngine,
    HeadTaskBacklog,
    TaskDefinition,
)


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def run_dogfood(output_path: Path | None = None) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="dogfood_callback_") as td:
        base_dir = Path(td)
        bus_dir = base_dir / "bus"
        bus_dir.mkdir()
        ws_dir = base_dir / "workspace"
        ws_dir.mkdir()

        bus = FileBus(bus_dir)

        # Register head and worker identities
        head_ident, head_tok = bus.register(agent_name="head-callback", device_id="dev-host-1", project_id="agent-bus-scale50")
        worker_ident, worker_tok = bus.register(agent_name="worker-executor", device_id="dev-host-1", project_id="agent-bus-scale50")

        # Create backlog with 3 useful tasks
        tasks = [
            TaskDefinition(
                task_id=f"task-batch-{i}",
                task_type="compute_shard",
                body=f"Process batch item {i}",
                data={"item_index": i, "operation": "compute_matrix_shard"},
                priority=10 - i,
            )
            for i in range(1, 4)
        ]
        backlog = HeadTaskBacklog(tasks)

        # Initialize engine with 1 worker to verify sequential automated refills
        engine = HeadCompletionCallbackEngine(
            bus=bus,
            head_identity=head_ident,
            head_token=head_tok,
            backlog=backlog,
            workspace_root=ws_dir,
            workers=[(worker_ident, worker_tok)],
        )

        worker_records: list[dict[str, Any]] = []
        stop_worker = threading.Event()

        def worker_loop():
            worker_bus = FileBus(bus_dir)
            while not stop_worker.is_set():
                items = worker_bus.inbox(worker_ident.identity_id, worker_tok, unread_only=True)
                for msg in items:
                    if msg.kind == "task_dispatch":
                        worker_bus.ack(worker_ident.identity_id, worker_tok, msg.message_id)
                        p = msg.data or {}
                        task_id = p.get("task_id")

                        # Produce real artifact file on disk
                        art_file = ws_dir / f"artifact_{task_id}.json"
                        art_content = json.dumps({
                            "task_id": task_id,
                            "result": f"computed_shard_{task_id}",
                            "timestamp": time.time(),
                        })
                        art_file.write_text(art_content, encoding="utf-8")
                        digest = file_sha256(art_file)

                        # Send reply with validated artifact and digest
                        worker_bus.reply(
                            sender_id=worker_ident.identity_id,
                            token=worker_tok,
                            message_id=msg.message_id,
                            body=f"task {task_id} completed",
                            data={
                                "status": "ok",
                                "artifact": str(art_file),
                                "digest": digest,
                            },
                        )
                        worker_records.append({
                            "task_id": task_id,
                            "artifact": str(art_file),
                            "digest": digest,
                        })
                time.sleep(0.02)

        worker_thread = threading.Thread(target=worker_loop, daemon=True)
        worker_thread.start()

        # Run engine event loop driven strictly by inotify
        stats = engine.run_event_loop(timeout_sec=10.0)

        stop_worker.set()
        worker_thread.join(timeout=1.0)

        assert backlog.all_done(), "All backlog tasks must be completed"
        assert backlog.completed_count() == 3, f"Expected 3 completed tasks, got {backlog.completed_count()}"
        assert backlog.pending_count() == 0, f"Expected 0 pending tasks, got {backlog.pending_count()}"
        assert engine.refill_count == 2, f"Expected 2 automatic refills for 3 tasks on 1 worker, got {engine.refill_count}"
        assert engine.synthetic_sleep_count == 0, f"synthetic_sleep_count must be 0, got {engine.synthetic_sleep_count}"
        assert engine.external_poke_count == 0, f"external_poke_count must be 0, got {engine.external_poke_count}"
        assert engine.watcher.sleep_poll_count == 0, f"watcher sleep_poll_count must be 0, got {engine.watcher.sleep_poll_count}"
        assert engine.watcher.kernel_event_count > 0, "watcher kernel_event_count must be > 0"

        # Negative Case 1: Tampered Digest
        tamper_art = ws_dir / "tampered.json"
        tamper_art.write_text('{"bad": true}', encoding="utf-8")
        bad_digest = "f" * 64
        tamper_outcome = {"status": "ok", "artifact": str(tamper_art), "digest": bad_digest}
        v_ok, v_err, _ = DigestValidator.validate_outcome(tamper_outcome, workspace_root=ws_dir)
        assert not v_ok, "DigestValidator must reject tampered digest"
        assert "digest_mismatch" in v_err

        # Negative Case 2: Path Traversal
        outside_art = base_dir / "outside_secret.json"
        outside_art.write_text('{"secret": "leak"}', encoding="utf-8")
        outside_digest = file_sha256(outside_art)
        outside_outcome = {"status": "ok", "artifact": str(outside_art), "digest": outside_digest}
        t_ok, t_err, _ = DigestValidator.validate_outcome(outside_outcome, workspace_root=ws_dir)
        assert not t_ok, "DigestValidator must reject path traversal outside workspace"
        assert "path_traversal_outside_workspace" in t_err

        engine.close()

        receipt_data = {
            "test_status": "SUCCESS",
            "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "tasks_dispatched": 3,
            "tasks_completed": backlog.completed_count(),
            "refill_count": engine.refill_count,
            "synthetic_sleep_count": engine.synthetic_sleep_count,
            "external_poke_count": engine.external_poke_count,
            "watcher_kernel_event_count": engine.watcher.kernel_event_count,
            "watcher_sleep_poll_count": engine.watcher.sleep_poll_count,
            "worker_records": worker_records,
            "audit_log_events": len(engine.audit_log),
            "negative_cases_verified": {
                "tampered_digest_rejected": not v_ok,
                "tampered_reason": v_err,
                "path_traversal_rejected": not t_ok,
                "path_traversal_reason": t_err,
            },
        }

        if output_path:
            output_path.write_text(json.dumps(receipt_data, indent=2), encoding="utf-8")

        return receipt_data


if __name__ == "__main__":
    result = run_dogfood()
    print(json.dumps(result, indent=2))
