"""Unit Tests for Head Completion/Refill Callback Engine in agent-bus."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from coordination.bus import BusError, BusIdentity, BusMessage, FileBus
from coordination.completion_callback import (
    ArtifactNotFoundError,
    DigestValidationError,
    DigestValidator,
    HeadCompletionCallbackEngine,
    HeadTaskBacklog,
    InotifyBusWatcher,
    TamperDetectedError,
    TaskDefinition,
    UnauthorizedSenderError,
    _file_sha256,
)


def _file_digest(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def test_inotify_watcher_lifecycle(tmp_path: Path):
    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True)

    with InotifyBusWatcher(store_dir) as watcher:
        assert watcher.sleep_poll_count == 0
        assert not watcher.wait_event(timeout_ms=50)

        # Trigger inotify event by creating file
        test_file = store_dir / "trigger.txt"
        test_file.write_text("hello", encoding="utf-8")

        assert watcher.wait_event(timeout_ms=500)
        assert watcher.kernel_event_count > 0
        assert watcher.sleep_poll_count == 0


def test_digest_validator(tmp_path: Path):
    ws = tmp_path / "ws"
    ws.mkdir()
    art = ws / "output.json"
    content = '{"result": "ok"}'
    art.write_text(content, encoding="utf-8")
    expected_digest = _file_digest(art)

    # Valid outcome
    valid_outcome = {"status": "ok", "artifact": str(art), "digest": expected_digest}
    ok, reason, dig = DigestValidator.validate_outcome(valid_outcome, workspace_root=ws)
    assert ok is True
    assert reason == "valid"
    assert dig == expected_digest

    # Tampered outcome (digest mismatch)
    bad_digest_outcome = {"status": "ok", "artifact": str(art), "digest": "0" * 64}
    ok, reason, dig = DigestValidator.validate_outcome(bad_digest_outcome, workspace_root=ws)
    assert ok is False
    assert "digest_mismatch" in reason

    # Missing artifact
    missing_outcome = {"status": "ok", "artifact": str(ws / "nonexistent.json"), "digest": expected_digest}
    ok, reason, dig = DigestValidator.validate_outcome(missing_outcome, workspace_root=ws)
    assert ok is False
    assert "artifact_not_found" in reason

    # Path traversal outside workspace
    outside = tmp_path / "outside.json"
    outside.write_text("evil", encoding="utf-8")
    outside_outcome = {"status": "ok", "artifact": str(outside), "digest": _file_digest(outside)}
    ok, reason, dig = DigestValidator.validate_outcome(outside_outcome, workspace_root=ws)
    assert ok is False
    assert "path_traversal_outside_workspace" in reason


def test_head_completion_callback_dispatch_and_refill(tmp_path: Path):
    bus_dir = tmp_path / "bus"
    bus_dir.mkdir()
    bus = FileBus(bus_dir)

    ws_dir = tmp_path / "workspace"
    ws_dir.mkdir()

    head_ident, head_tok = bus.register(agent_name="head", device_id="dev-1", project_id="proj-1")
    worker_ident, worker_tok = bus.register(agent_name="worker", device_id="dev-1", project_id="proj-1")

    task1 = TaskDefinition(task_id="task-1", task_type="compute", body="do task 1")
    task2 = TaskDefinition(task_id="task-2", task_type="compute", body="do task 2")

    backlog = HeadTaskBacklog([task1, task2])

    engine = HeadCompletionCallbackEngine(
        bus=bus,
        head_identity=head_ident,
        head_token=head_tok,
        backlog=backlog,
        workspace_root=ws_dir,
        workers=[(worker_ident, worker_tok)],
    )

    # Dispatch task 1
    engine.dispatch_initial_batch()
    assert backlog.dispatched_count() == 1
    assert backlog.pending_count() == 1

    # Worker checks inbox and sees task 1
    w_inbox = bus.inbox(worker_ident.identity_id, worker_tok, unread_only=True)
    assert len(w_inbox) == 1
    dispatch_msg = w_inbox[0]
    assert dispatch_msg.data["task_id"] == "task-1"
    bus.ack(worker_ident.identity_id, worker_tok, dispatch_msg.message_id)

    # Worker produces outcome artifact
    out1_path = ws_dir / "out1.txt"
    out1_path.write_text("task 1 outcome bytes", encoding="utf-8")
    out1_digest = _file_digest(out1_path)

    # Worker sends reply
    bus.reply(
        sender_id=worker_ident.identity_id,
        token=worker_tok,
        message_id=dispatch_msg.message_id,
        body="task 1 finished",
        data={"status": "ok", "artifact": str(out1_path), "digest": out1_digest},
    )

    # Head consumes reply and triggers automatic refill (task 2)
    processed = engine.process_pending_inbox()
    assert processed == 1
    assert backlog.completed_count() == 1
    assert backlog.get_task("task-1").status == "completed"
    assert engine.refill_count == 1
    assert backlog.dispatched_count() == 1
    assert backlog.get_task("task-2").status == "dispatched"

    # Worker sees task 2 refilled in inbox
    w_inbox2 = bus.inbox(worker_ident.identity_id, worker_tok, unread_only=True)
    assert len(w_inbox2) == 1
    dispatch_msg2 = w_inbox2[0]
    assert dispatch_msg2.data["task_id"] == "task-2"
    bus.ack(worker_ident.identity_id, worker_tok, dispatch_msg2.message_id)

    # Worker produces outcome artifact for task 2
    out2_path = ws_dir / "out2.txt"
    out2_path.write_text("task 2 outcome bytes", encoding="utf-8")
    out2_digest = _file_digest(out2_path)

    bus.reply(
        sender_id=worker_ident.identity_id,
        token=worker_tok,
        message_id=dispatch_msg2.message_id,
        body="task 2 finished",
        data={"status": "ok", "artifact": str(out2_path), "digest": out2_digest},
    )

    # Head consumes task 2 completion
    processed2 = engine.process_pending_inbox()
    assert processed2 == 1
    assert backlog.completed_count() == 2
    assert backlog.all_done() is True
    assert engine.synthetic_sleep_count == 0
    assert engine.external_poke_count == 0

    engine.close()
