"""Targeted unit tests for standalone SessionlessWorkerBus in agent-bus."""

from __future__ import annotations

import json
import stat
from pathlib import Path
import pytest

from coordination.bus import FileBus
from coordination.namespaced import NamespacedId, ReadAck, SendReceipt, TransportState
from coordination.worker_bus import SessionlessWorkerBus, WorkerSendOutcome, ReceiptStore


def test_sessionless_registration_and_0600_credentials(tmp_path: Path):
    bus_store = tmp_path / "bus"
    bus_store.mkdir()
    cred_file = tmp_path / "worker.cred.json"

    worker = SessionlessWorkerBus.register(
        bus_store,
        agent_name="test-worker",
        device_id="test-device",
        project_id="agent-bus",
        task_id="t-001",
    )

    # Verify session_id is None and renders as '-'
    assert worker.namespaced_id.session_id is None
    assert worker.namespaced_id.render() == "test-device/agent-bus/test-worker/-/t-001"

    worker.save_credentials(cred_file)
    assert cred_file.exists()
    mode = stat.S_IMODE(cred_file.stat().st_mode)
    assert mode == 0o600

    # Reload from credentials
    reloaded = SessionlessWorkerBus.from_credentials(bus_store, cred_file)
    assert reloaded.identity_id == worker.identity_id
    assert reloaded.agent_name == "test-worker"


def test_send_receive_ack_and_receipts(tmp_path: Path):
    bus_store = tmp_path / "bus"
    bus_store.mkdir()

    # Register coordinator and worker
    coord = SessionlessWorkerBus.register(
        bus_store,
        agent_name="coord",
        device_id="test-device",
        project_id="agent-bus",
        task_id="coord-task",
    )
    worker = SessionlessWorkerBus.register(
        bus_store,
        agent_name="worker",
        device_id="test-device",
        project_id="agent-bus",
        task_id="worker-task",
    )

    # Worker sends task result to coordinator
    outcome = worker.send(
        recipient_id=coord.identity_id,
        body="task_result: completed",
        data={"result": "ok", "verdict": "ACCEPTED"},
    )
    assert isinstance(outcome, WorkerSendOutcome)
    assert outcome.receipt.state == TransportState.SEND_RECEIPT
    assert outcome.message.body == "task_result: completed"

    # Coordinator receives message
    msgs = coord.receive(unread_only=True)
    assert len(msgs) == 1
    assert msgs[0].message_id == outcome.message_id

    # Coordinator ACKs message
    read_ack = coord.ack(msgs[0].message_id)
    assert isinstance(read_ack, ReadAck)
    assert read_ack.state == TransportState.RECIPIENT_READ_ACK
    assert read_ack.acked_by.agent_tag == "coord"

    # Coordinator inbox now empty
    assert len(coord.receive(unread_only=True)) == 0


def test_cursor_persistence_across_restart(tmp_path: Path):
    bus_store = tmp_path / "bus"
    bus_store.mkdir()
    cred_file = tmp_path / "worker.cred.json"

    sender = SessionlessWorkerBus.register(
        bus_store,
        agent_name="sender",
        device_id="test-device",
        project_id="agent-bus",
    )
    worker = SessionlessWorkerBus.register(
        bus_store,
        agent_name="receiver",
        device_id="test-device",
        project_id="agent-bus",
    )
    worker.save_credentials(cred_file)

    import time
    msg1 = sender.send(recipient_id=worker.identity_id, body="msg 1").message
    time.sleep(1.05)
    msg2 = sender.send(recipient_id=worker.identity_id, body="msg 2").message

    # Worker consumes first message and ACKs
    inbox1 = worker.receive(limit=1)
    assert len(inbox1) == 1
    assert inbox1[0].message_id == msg1.message_id
    worker.ack(msg1.message_id)
    assert worker.current_cursor() == msg1.message_id

    # Simulate worker process termination and reload from disk credentials
    reloaded_worker = SessionlessWorkerBus.from_credentials(bus_store, cred_file)
    assert reloaded_worker.current_cursor() == msg1.message_id

    # Next receive only returns msg2
    inbox2 = reloaded_worker.receive()
    assert len(inbox2) == 1
    assert inbox2[0].message_id == msg2.message_id
    reloaded_worker.ack(msg2.message_id)
    assert reloaded_worker.current_cursor() == msg2.message_id

    # Subsequent receive returns 0 messages
    assert len(reloaded_worker.receive()) == 0
