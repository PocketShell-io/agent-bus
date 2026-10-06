"""Comprehensive unit and integration tests for coordination/ql_task_unit_adapter.py."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from coordination.bus import FileBus
from coordination.ql_task_unit_adapter import (
    MIN_ROOT_FREE_GIB,
    HeadCredentialLeakError,
    QLSessionlessConsumer,
    StorageFloorBreach,
    check_storage_floor,
    file_digest,
    format_ql_payload,
    map_terminal_receipt_to_bus_outcome,
    reject_head_cred_inheritance,
    submit_task_unit,
    validate_filebus_identity,
    validate_filebus_live_identity,
    validate_filebus_terminal_receipt,
)


def test_check_storage_floor_raises_when_below_floor():
    with patch("shutil.disk_usage") as mock_usage:
        mock_usage.return_value = (436 * 1024**3, 388 * 1024**3, 48 * 1024**3)
        with pytest.raises(StorageFloorBreach, match="below required 50.0 GiB floor"):
            check_storage_floor("/")


def test_check_storage_floor_passes_when_above_floor():
    with patch("shutil.disk_usage") as mock_usage:
        mock_usage.return_value = (436 * 1024**3, 381 * 1024**3, 55 * 1024**3)
        free_gib = check_storage_floor("/")
        assert free_gib == 55.0


def test_reject_head_cred_inheritance():
    # Attempting to grant head.cred or quota-launcher-head identity must fail closed
    with pytest.raises(HeadCredentialLeakError):
        reject_head_cred_inheritance({"agent_name": "quota-launcher-head"})

    with pytest.raises(HeadCredentialLeakError):
        reject_head_cred_inheritance({"cred": "/path/to/filebus/head.cred"})

    with pytest.raises(HeadCredentialLeakError):
        reject_head_cred_inheritance({"goal": "run worker with --cred /home/alexey/filebus/head.cred"})

    # Safe payload must pass without error
    safe = {"backend": "filebus", "owner": "worker-1", "goal": "clean task"}
    reject_head_cred_inheritance(safe)


def test_validate_filebus_identity_contracts():
    live_valid = {
        "task_id": "ql-12345",
        "bus_identity": "worker-abc",
        "task_message_id": "msg-xyz",
        "provider": "antigravity",
    }
    assert validate_filebus_live_identity(live_valid) is True

    # Must reject if future terminal fields are invented at live enrollment
    live_with_future = dict(live_valid, ack_id="ack-1")
    assert validate_filebus_live_identity(live_with_future) is False

    # Terminal receipt requires ack_id, reply_id, outcome
    terminal_valid = dict(
        live_valid,
        ack_id="msg-xyz",
        reply_id="reply-123",
        outcome="completed",
    )
    assert validate_filebus_terminal_receipt(terminal_valid) is True
    assert validate_filebus_identity(terminal_valid) is True

    # Terminal receipt rejects invalid outcome status
    invalid_outcome = dict(terminal_valid, outcome="unknown_status")
    assert validate_filebus_terminal_receipt(invalid_outcome) is False


def test_format_ql_payload_structure():
    payload = format_ql_payload(
        goal="Run model consumer on Bus task",
        owner="agentbus-consumer",
        cwd="/home/alexey/git/agent-bus",
        timeout=300,
        memory_mb=768,
        provider="antigravity",
        task_id="ql-task-1",
        task_message_id="msg-1",
    )
    assert payload["backend"] == "filebus"
    assert payload["goal"] == "Run model consumer on Bus task"
    assert payload["owner"] == "agentbus-consumer"
    assert payload["cwd"] == "/home/alexey/git/agent-bus"
    assert payload["timeout"] == 300
    assert payload["memory_mb"] == 768
    assert payload["provider"] == "antigravity"
    assert payload["task_id"] == "ql-task-1"
    assert payload["task_message_id"] == "msg-1"


def test_map_terminal_receipt_to_bus_outcome_success():
    receipt = {
        "exit_code": 0,
        "unit": "ql-ctl-task-01.service",
        "invocation_id": "inv-12345",
        "cpu_seconds": 1.25,
        "memory_peak_mb": 42.5,
    }
    outcome = map_terminal_receipt_to_bus_outcome(receipt, artifact_path="/tmp/test_artifact.txt")
    assert outcome["status"] == "ok"
    assert outcome["artifact"] == "/tmp/test_artifact.txt"
    assert outcome["ql_unit"] == "ql-ctl-task-01.service"
    assert outcome["ql_invocation_id"] == "inv-12345"
    assert outcome["cpu_seconds"] == 1.25
    assert outcome["memory_peak_mb"] == 42.5


def test_map_terminal_receipt_to_bus_outcome_failure():
    receipt = {
        "exit_code": 1,
        "unit": "ql-ctl-task-02.service",
        "invocation_id": "inv-67890",
        "cpu_seconds": 0.5,
        "memory_peak_mb": 15.0,
    }
    outcome = map_terminal_receipt_to_bus_outcome(receipt, artifact_path="/tmp/err.log")
    assert outcome["status"] == "error"
    assert outcome["artifact"] == "/tmp/err.log"


def test_submit_task_unit_dry_run(tmp_path: Path):
    with patch("coordination.ql_task_unit_adapter.check_storage_floor") as mock_floor, \
         patch("subprocess.run") as mock_run:
        mock_floor.return_value = 60.0
        mock_run.return_value = MagicMock(
            stdout=json.dumps({"id": "task-test", "state": "queued"}),
            returncode=0,
        )

        payload = format_ql_payload(goal="test", owner="tester", cwd="/tmp")
        res = submit_task_unit(
            task_id="task-test",
            idempotency_key="key-test",
            payload=payload,
            paths=["tests/**"],
            config_dir=tmp_path / "config",
            worktree=tmp_path / "worktree",
        )
        assert res["id"] == "task-test"
        assert res["state"] == "queued"
        mock_floor.assert_called_once()
        assert mock_run.call_count == 2


def test_consumer_processes_message_end_to_end(tmp_path: Path):
    bus = FileBus(tmp_path / "store")
    parent, pt = bus.register(
        agent_name="controller",
        device_id="host-1",
        project_id="test-proj",
    )
    consumer_ident, ct = bus.register(
        agent_name="consumer-worker",
        device_id="host-1",
        project_id="test-proj",
        parent_id=parent.identity_id,
        parent_token=pt,
    )

    allow_root = tmp_path / "work"
    allow_root.mkdir()
    consumer = QLSessionlessConsumer(
        bus=bus,
        identity_id=consumer_ident.identity_id,
        token=ct,
        allow_root=allow_root,
        config_dir=tmp_path / "ql_config",
        worktree=tmp_path / "ql_worktree",
    )

    # Dispatch a task message to the consumer
    msg = bus.send(
        sender_id=parent.identity_id,
        token=pt,
        recipient_id=consumer_ident.identity_id,
        body="Execute analysis task",
        data={
            "goal": "Generate sample code",
            "artifact": "result.txt",
            "paths": ["src/**"],
        },
        idempotency_key="task-unit-key-1",
    )

    artifact_file = allow_root / "result.txt"

    def mock_submit(*args, **kwargs):
        # Simulate worker creating artifact
        artifact_file.write_text("print('hello world')\n", encoding="utf-8")
        return {
            "id": f"ql-{msg.message_id[:12]}",
            "state": "completed",
            "exit_code": 0,
            "unit": "ql-unit-01.service",
            "invocation_id": "inv-001",
            "cpu_seconds": 0.45,
            "memory_peak_mb": 35.0,
        }

    with patch("coordination.ql_task_unit_adapter.check_storage_floor") as mock_floor, \
         patch("coordination.ql_task_unit_adapter.submit_task_unit", side_effect=mock_submit):
        mock_floor.return_value = 55.0

        res = consumer.run_once()
        assert res is not None
        assert res["message_id"] == msg.message_id
        assert res["status"] == "ok"
        assert res["artifact"] == str(artifact_file)
        assert res["digest"] == file_digest(artifact_file)
        assert res["terminal_record"]["outcome"] == "completed"

    # Verify message state in FileBus: delivered -> acked -> accepted -> outcome
    seen = bus.get(parent.identity_id, pt, msg.message_id)
    assert seen.delivered_at
    assert seen.acked_at
    assert seen.accepted_at
    assert seen.outcome["status"] == "ok"
    assert seen.outcome["digest"] == res["digest"]

    # Verify reply message received by parent
    replies = bus.inbox(parent.identity_id, pt, unread_only=True)
    assert len(replies) == 1
    assert replies[0].reply_to == msg.message_id
    assert replies[0].kind == "reply"
    assert replies[0].data["digest"] == seen.outcome["digest"]

    # Consumer inbox should now be empty (message was acknowledged)
    assert bus.inbox(consumer_ident.identity_id, ct, unread_only=True) == []


def test_consumer_idempotent_replay(tmp_path: Path):
    bus = FileBus(tmp_path / "store")
    parent, pt = bus.register(agent_name="p", device_id="d", project_id="proj")
    consumer_ident, ct = bus.register(agent_name="c", device_id="d", project_id="proj", parent_id=parent.identity_id, parent_token=pt)

    allow_root = tmp_path / "work"
    allow_root.mkdir()
    consumer = QLSessionlessConsumer(bus=bus, identity_id=consumer_ident.identity_id, token=ct, allow_root=allow_root)

    msg = bus.send(
        sender_id=parent.identity_id,
        token=pt,
        recipient_id=consumer_ident.identity_id,
        body="Idempotent task",
        data={"artifact": "out.txt"},
        idempotency_key="key-idemp-1",
    )
    out_file = allow_root / "out.txt"
    out_file.write_text("done\n", encoding="utf-8")

    with patch("coordination.ql_task_unit_adapter.check_storage_floor") as mock_floor, \
         patch("coordination.ql_task_unit_adapter.submit_task_unit") as mock_submit:
        mock_floor.return_value = 55.0
        mock_submit.return_value = {"exit_code": 0, "state": "completed", "unit": "u1"}

        res1 = consumer.run_once()
        assert res1["status"] == "ok"
        assert mock_submit.call_count == 1

        # Direct re-processing of the message should be an idempotent replay without calling submit again
        res2 = consumer.process_message(msg)
        assert res2["idempotent_replay"] is True
        assert mock_submit.call_count == 1  # Not called again


def test_consumer_recover_leases_after_crash(tmp_path: Path):
    bus = FileBus(tmp_path / "store")
    parent, pt = bus.register(agent_name="p", device_id="d", project_id="proj")
    consumer_ident, ct = bus.register(agent_name="c", device_id="d", project_id="proj", parent_id=parent.identity_id, parent_token=pt)

    allow_root = tmp_path / "work"
    allow_root.mkdir()
    consumer = QLSessionlessConsumer(bus=bus, identity_id=consumer_ident.identity_id, token=ct, allow_root=allow_root)

    msg = bus.send(
        sender_id=parent.identity_id,
        token=pt,
        recipient_id=consumer_ident.identity_id,
        body="Crashed task",
        data={"artifact": "crash.txt"},
        idempotency_key="key-crash-1",
    )

    # Simulate crash right after accept and complete, before reply and ack
    bus.accept(consumer_ident.identity_id, ct, msg.message_id)
    crash_file = allow_root / "crash.txt"
    crash_file.write_text("computed-before-crash\n", encoding="utf-8")
    bus.complete(
        consumer_ident.identity_id,
        ct,
        msg.message_id,
        status="ok",
        artifact=str(crash_file),
    )

    # Inbox still has the unacked message
    assert len(bus.inbox(consumer_ident.identity_id, ct, unread_only=True)) == 1

    with patch("coordination.ql_task_unit_adapter.check_storage_floor") as mock_floor:
        mock_floor.return_value = 55.0
        recovered = consumer.recover_leases()
        assert len(recovered) == 1
        assert recovered[0]["action"] == "recovered_ack"

    # Message must now be acked and parent has the reply
    assert bus.inbox(consumer_ident.identity_id, ct, unread_only=True) == []
    parent_inbox = bus.inbox(parent.identity_id, pt, unread_only=True)
    assert len(parent_inbox) == 1
    assert parent_inbox[0].reply_to == msg.message_id


def test_consumer_held_when_storage_floor_breached(tmp_path: Path):
    bus = FileBus(tmp_path / "store")
    parent, pt = bus.register(agent_name="p", device_id="d", project_id="proj")
    consumer_ident, ct = bus.register(agent_name="c", device_id="d", project_id="proj", parent_id=parent.identity_id, parent_token=pt)

    allow_root = tmp_path / "work"
    allow_root.mkdir()
    consumer = QLSessionlessConsumer(bus=bus, identity_id=consumer_ident.identity_id, token=ct, allow_root=allow_root)

    msg = bus.send(
        sender_id=parent.identity_id,
        token=pt,
        recipient_id=consumer_ident.identity_id,
        body="Low disk task",
        idempotency_key="key-low-disk",
    )

    with patch("coordination.ql_task_unit_adapter.check_storage_floor") as mock_floor:
        # Mock 47.5 GiB (below 50 GiB floor)
        mock_floor.side_effect = StorageFloorBreach("Disk below 50.0 GiB floor")

        with pytest.raises(StorageFloorBreach):
            consumer.run_once()

    # The message was accepted, but NOT completed or acked, holding the lease safely
    seen = bus.get(parent.identity_id, pt, msg.message_id)
    assert seen.accepted_at
    assert seen.acked_at is None
    assert seen.outcome is None
