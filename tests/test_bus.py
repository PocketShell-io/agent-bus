from pathlib import Path

import pytest

from coordination.bus import BusError, FileBus
from coordination.errors import IdempotencyConflict


def test_register_send_inbox_ack_reply(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="worker-a", device_id="hetzner-rmthz", project_id="agent-coordination", task_id="dogfood")
    b, tb = bus.register(agent_name="worker-b", device_id="hetzner-rmthz", project_id="agent-coordination", task_id="dogfood")
    assert a.identity_id != b.identity_id
    msg = bus.send(
        sender_id=a.identity_id,
        token=ta,
        recipient_id=b.identity_id,
        body="task: write artifact",
        data={"artifact": "out.txt"},
        idempotency_key="dog-1",
    )
    inbox = bus.inbox(b.identity_id, tb)
    assert len(inbox) == 1
    assert inbox[0].message_id == msg.message_id
    acked = bus.ack(b.identity_id, tb, msg.message_id)
    assert acked.acked_at
    assert bus.inbox(b.identity_id, tb) == []
    reply = bus.reply(sender_id=b.identity_id, token=tb, message_id=msg.message_id, body="done", idempotency_key="dog-1-r")
    assert reply.reply_to == msg.message_id
    assert reply.recipient_id == a.identity_id


def test_idempotent_send_and_conflict(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="p")
    b, _ = bus.register(agent_name="b", device_id="d", project_id="p")
    m1 = bus.send(sender_id=a.identity_id, token=ta, recipient_id=b.identity_id, body="x", idempotency_key="k")
    m2 = bus.send(sender_id=a.identity_id, token=ta, recipient_id=b.identity_id, body="x", idempotency_key="k")
    assert m1.message_id == m2.message_id
    with pytest.raises(IdempotencyConflict):
        bus.send(sender_id=a.identity_id, token=ta, recipient_id=b.identity_id, body="y", idempotency_key="k")


def test_crash_restart_redelivers_unacked(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="p")
    b, tb = bus.register(agent_name="b", device_id="d", project_id="p")
    msg = bus.send(sender_id=a.identity_id, token=ta, recipient_id=b.identity_id, body="keep", idempotency_key="k2")
    restarted = FileBus(tmp_path)
    inbox = restarted.inbox(b.identity_id, tb)
    assert [m.message_id for m in inbox] == [msg.message_id]


def test_auth_and_unknown_recipient(tmp_path: Path):
    bus = FileBus(tmp_path)
    a, ta = bus.register(agent_name="a", device_id="d", project_id="p")
    with pytest.raises(BusError) as err:
        bus.send(sender_id=a.identity_id, token="wrong", recipient_id=a.identity_id, body="x")
    assert err.value.code == "auth_failed"
    with pytest.raises(BusError) as err2:
        bus.send(sender_id=a.identity_id, token=ta, recipient_id="missing", body="x")
    assert err2.value.code == "unknown_recipient"


def test_identity_is_not_aplexer_session(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("APLEXER_SESSION_ID", "81e8010c-89e4-478b-be3a-4ee6991607f3")
    bus = FileBus(tmp_path)
    ident, _ = bus.register(agent_name="plain", device_id="hetzner-rmthz", project_id="agent-coordination")
    assert ident.identity_id != "81e8010c-89e4-478b-be3a-4ee6991607f3"
    assert ident.kind == "bus-agent"
